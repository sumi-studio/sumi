package filesvc

// capture_decode.go — private immutable-capture byte path.
//
// Everything in this file implements the reviewed direction: bytes come
// ONLY from captured immutable objects inside the trusted storage
// service. There is no live-mount read, no double-walk stability
// argument, and no fallback reader. The manifest supplies ordered slice
// records; this layer resolves them into effective ranges (later slices
// override earlier, id==0 erases, output clips to node length) and
// streams object blocks one at a time — never a whole file in memory.
//
// Format gates are up front and total: any compression, encryption,
// unknown MetaVersion, unknown schema, or out-of-range block size
// refuses the capture entirely rather than mis-decoding later.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// chunkSize is JuiceFS's fixed 64 MiB chunk. Slice `pos` is relative to
// the chunk start (indx*chunkSize).
const chunkSize = uint64(1 << 26)

// JuiceFS node types (pkg/meta/interface.go).
const (
	jfsTypeFile     = 1
	jfsTypeDir      = 2
	jfsTypeSymlink  = 3
	jfsTypeFIFO     = 4
	jfsTypeBlockDev = 5
	jfsTypeCharDev  = 6
	jfsTypeSocket   = 7
	jfsMaxVersion   = 1
	sliceRecBytes   = 24
	maxBlockBytes   = 16 << 20 // sanity ceiling on format BlockSize
)

var (
	// ErrCaptureRefused is a permanent refusal — format, schema, anchor,
	// or corrupt metadata. Retrying the same capture cannot help.
	ErrCaptureRefused = errors.New("capture refused")
	// ErrCapturePending is bounded unavailability: a captured object is
	// missing or short (e.g. compacted past retention). The mover must
	// retake a fresh coherent manifest; the old one cannot serve.
	ErrCapturePending = errors.New("capture content unavailable")
	// ErrCaptureNotFound: unknown capture id.
	ErrCaptureNotFound = errors.New("capture not found")
	// ErrCaptureGone: capture released or expired — rows may remain as
	// evidence but reads are closed.
	ErrCaptureGone = errors.New("capture released or expired")
	// ErrCaptureUnconfigured: no capture config was supplied at startup.
	// The service refuses rather than falling back to a live-tree read.
	ErrCaptureUnconfigured = errors.New("capture service not configured")
	// ErrCaptureUnsupported marks entries whose node type is outside the
	// supported copy set (fifo/device/socket). They are IN the manifest —
	// a truthful smaller tree is never presented as complete.
	ErrCaptureUnsupported = errors.New("entry type is not copyable")
)

func supportedNodeType(t uint8) bool {
	return t == jfsTypeFile || t == jfsTypeDir || t == jfsTypeSymlink
}

// captureFormat is the gated, decoder-relevant subset of the volume's
// jfs_setting.format row. Anything outside this exact supported shape is
// refused before the manifest is taken.
type captureFormat struct {
	VolumeUUID  string `json:"uuid"`
	Storage     string `json:"storage"`
	Bucket      string `json:"bucket"`
	MetaVersion int    `json:"meta_version"`
	BlockBytes  int    `json:"block_bytes"`
	HashPrefix  bool   `json:"hash_prefix"`
	TrashDays   int    `json:"trash_days"`
}

// rawFormat mirrors the fields of jfs_setting.format that gate decode.
// Unknown extra fields are tolerated; unknown values of KNOWN fields are
// not — the gate is on semantics, not key recognition.
type rawFormat struct {
	UUID         string `json:"UUID"`
	Storage      string `json:"Storage"`
	Bucket       string `json:"Bucket"`
	BlockSize    int    `json:"BlockSize"` // KiB
	Compression  string `json:"Compression"`
	HashPrefix   bool   `json:"HashPrefix"`
	EncryptKey   string `json:"EncryptKey"`
	EncryptAlgo  string `json:"EncryptAlgo"`
	KeyEncrypted bool   `json:"KeyEncrypted"`
	TrashDays    int    `json:"TrashDays"`
	MetaVersion  int    `json:"MetaVersion"`
}

// gateFormat converts the raw volume format into the decoder's gated
// form, or refuses. This is the ONLY acceptance point: every decode uses
// the gated struct, never the raw JSON.
func gateFormat(f rawFormat) (captureFormat, error) {
	if f.UUID == "" {
		return captureFormat{}, fmt.Errorf("%w: volume format has no UUID", ErrCaptureRefused)
	}
	if f.MetaVersion != jfsMaxVersion {
		return captureFormat{}, fmt.Errorf("%w: meta version %d unsupported", ErrCaptureRefused, f.MetaVersion)
	}
	if f.Compression != "" && f.Compression != "none" {
		return captureFormat{}, fmt.Errorf("%w: compression %q unsupported", ErrCaptureRefused, f.Compression)
	}
	// EncryptAlgo is always populated by juicefs format (default
	// aes256gcm-rsa) even on plaintext volumes; the real encryption
	// signal is a configured key.
	if f.EncryptKey != "" || f.KeyEncrypted {
		return captureFormat{}, fmt.Errorf("%w: encrypted volume unsupported", ErrCaptureRefused)
	}
	bs := f.BlockSize * 1024
	if f.BlockSize <= 0 || bs <= 0 || bs > maxBlockBytes {
		return captureFormat{}, fmt.Errorf("%w: block size %d KiB unsupported", ErrCaptureRefused, f.BlockSize)
	}
	return captureFormat{
		VolumeUUID: f.UUID, Storage: f.Storage, Bucket: f.Bucket,
		MetaVersion: f.MetaVersion, BlockBytes: bs,
		HashPrefix: f.HashPrefix, TrashDays: f.TrashDays,
	}, nil
}

// objectKey reproduces pkg/chunk/cached_store.go rSlice.key exactly for
// the gated layouts: with hash prefix `chunks/%02X/<id/1e6>/<id>_<i>_<sz>`,
// without `chunks/<id/1e6>/<id/1e3>/<id>_<i>_<sz>`. blockSize is the
// per-index object size embedded in the key (min(blockBytes, remaining)).
func objectKey(sliceID uint64, indx, blockSize, blockBytes int, hashPrefix bool) string {
	if hashPrefix {
		return fmt.Sprintf("chunks/%02X/%d/%d_%d_%d",
			sliceID%256, sliceID/1000/1000, sliceID, indx, blockSize)
	}
	return fmt.Sprintf("chunks/%d/%d/%d_%d_%d",
		sliceID/1000/1000, sliceID/1000, sliceID, indx, blockSize)
}

// objectStore is the private byte source. Only the service holds it;
// keys never leave this layer. A missing object is errObjMissing — the
// read path maps it to ErrCapturePending, never to corruption.
type objectStore interface {
	// open returns the object body and its exact stored length.
	open(ctx context.Context, key string) (io.ReadCloser, int64, error)
}

var errObjMissing = errors.New("object missing")

// fileObjStore reads objects from a filesystem-rooted store (the
// file:// backend used by the synthetic fixture; also any POSIX-mounted
// object gateway). It is NOT claimed as production object-backend
// acceptance — other kinds are a config refusal, not a fallback.
type fileObjStore struct{ root string }

func (s *fileObjStore) open(_ context.Context, key string) (io.ReadCloser, int64, error) {
	if strings.Contains(key, "..") || strings.HasPrefix(key, "/") {
		return nil, 0, fmt.Errorf("%w: bad object key", ErrCaptureRefused)
	}
	f, err := os.Open(filepath.Join(s.root, filepath.FromSlash(key)))
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, errObjMissing
	}
	if err != nil {
		return nil, 0, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, st.Size(), nil
}

// sliceRec is one 24-byte JuiceFS slice record (big-endian):
// pos(4) id(8) size(4) off(4) len(4).
type sliceRec struct {
	pos    uint32
	id     uint64
	size   uint32
	off    uint32
	length uint32
}

// parseSliceRecs validates a jfs_chunk.slices bytea into records.
// Malformed metadata refuses the capture — a silently skipped record
// would produce a manifest that looks complete but isn't.
func parseSliceRecs(b []byte) ([]sliceRec, error) {
	if len(b)%sliceRecBytes != 0 {
		return nil, fmt.Errorf("%w: slice bytea len %d not a multiple of %d",
			ErrCaptureRefused, len(b), sliceRecBytes)
	}
	recs := make([]sliceRec, 0, len(b)/sliceRecBytes)
	for i := 0; i+sliceRecBytes <= len(b); i += sliceRecBytes {
		r := sliceRec{
			pos:    binary.BigEndian.Uint32(b[i:]),
			id:     binary.BigEndian.Uint64(b[i+4:]),
			size:   binary.BigEndian.Uint32(b[i+12:]),
			off:    binary.BigEndian.Uint32(b[i+16:]),
			length: binary.BigEndian.Uint32(b[i+20:]),
		}
		if uint64(r.pos)+uint64(r.length) > chunkSize {
			return nil, fmt.Errorf("%w: slice %d crosses chunk boundary", ErrCaptureRefused, r.id)
		}
		if r.id > 0 {
			// Hole/erase records (id=0) carry size=0,off=0
			// (newSlice(pos,0,0,0,len)) — the off+len<=size invariant only
			// applies to data slices. The record itself still appends:
			// it erases earlier data at resolution time.
			if uint64(r.off)+uint64(r.length) > uint64(r.size) {
				return nil, fmt.Errorf("%w: slice %d bounds off+len %d+%d > size %d",
					ErrCaptureRefused, r.id, r.off, r.length, r.size)
			}
			if r.size == 0 {
				return nil, fmt.Errorf("%w: data slice %d has zero size", ErrCaptureRefused, r.id)
			}
		}
		recs = append(recs, r)
	}
	return recs, nil
}

// rangeSeg is a resolved (effective) mapping segment inside one chunk:
// file offsets [pos, pos+length) in this chunk come from slice id at
// slice offset off. id==0 is an explicit hole — zeros, NOT missing data.
type rangeSeg struct {
	pos    uint64
	length uint64
	id     uint64
	off    uint64
	size   uint64
}

// resolveChunk applies ordered overlap semantics: each record lands in
// sequence and later records override earlier ones wherever they
// intersect; gaps between records are implicit holes; the result is
// clipped to the file's captured length. This mirrors baseMeta's
// buildSlice — the resolved view is what the manifest hashes.
func resolveChunk(recs []sliceRec, fileLen, chunkBase uint64) []rangeSeg {
	var out []rangeSeg
	for _, r := range recs {
		if r.length == 0 {
			continue
		}
		out = insertSeg(out, rangeSeg{
			pos: uint64(r.pos), length: uint64(r.length),
			id: r.id, off: uint64(r.off), size: uint64(r.size),
		})
	}
	// Clip to [0, fileLen-chunkBase): post-truncate history past EOF is
	// neutralized, never emitted.
	limit := uint64(0)
	if fileLen > chunkBase {
		limit = fileLen - chunkBase
		if limit > chunkSize {
			limit = chunkSize
		}
	}
	var clipped []rangeSeg
	for _, s := range out {
		if s.pos >= limit {
			continue
		}
		if s.pos+s.length > limit {
			s.length = limit - s.pos
		}
		clipped = append(clipped, s)
	}
	return clipped
}

// insertSeg substitutes s over its span: earlier segments overlapping
// [s.pos, s.pos+s.length) lose that span (split or trimmed), then s is
// inserted keeping the list sorted and non-overlapping.
func insertSeg(list []rangeSeg, s rangeSeg) []rangeSeg {
	lo, hi := s.pos, s.pos+s.length
	// Fresh backing array: appending into list[:0] would overwrite the
	// elements being iterated.
	out := make([]rangeSeg, 0, len(list)+1)
	for _, e := range list {
		eLo, eHi := e.pos, e.pos+e.length
		if eHi <= lo || eLo >= hi {
			out = append(out, e)
			continue
		}
		// Head survives.
		if eLo < lo {
			head := e
			head.length = lo - eLo
			out = append(out, head)
		}
		// Tail survives — the source offset shifts with the cut.
		if eHi > hi {
			tail := e
			shift := hi - eLo
			tail.off += shift
			tail.pos = hi
			tail.length = eHi - hi
			out = append(out, tail)
		}
	}
	// Insert sorted by pos.
	i := len(out)
	for i > 0 && out[i-1].pos > s.pos {
		i--
	}
	out = append(out, rangeSeg{})
	copy(out[i+1:], out[i:])
	out[i] = s
	return out
}

// streamRange copies the file bytes spanned by [fileOff, fileOff+n) of
// one resolved segment to w, fetching only the object blocks needed.
// Each block is at most blockBytes in memory at once.
func streamRange(ctx context.Context, objs objectStore, fmt2 captureFormat,
	seg rangeSeg, chunkBase, fileOff, n uint64, w io.Writer) error {
	if seg.id == 0 {
		return writeZeros(w, int64(n))
	}
	// Offset of the first needed byte inside the slice.
	sliceOff := seg.off + (fileOff - (chunkBase + seg.pos))
	remaining := n
	for remaining > 0 {
		bi := int(sliceOff / uint64(fmt2.BlockBytes))
		bsize := int(seg.size) - bi*fmt2.BlockBytes
		if bsize > fmt2.BlockBytes {
			bsize = fmt2.BlockBytes
		}
		if bsize <= 0 {
			return fmt.Errorf("%w: slice %d block %d beyond slice size",
				ErrCaptureRefused, seg.id, bi)
		}
		key := objectKey(seg.id, bi, bsize, fmt2.BlockBytes, fmt2.HashPrefix)
		rc, stored, err := objs.open(ctx, key)
		if errors.Is(err, errObjMissing) {
			return fmt.Errorf("%w: block %d of slice %d missing", ErrCapturePending, bi, seg.id)
		}
		if err != nil {
			return fmt.Errorf("%w: object store: %v", ErrCapturePending, err)
		}
		if stored != int64(bsize) {
			rc.Close()
			return fmt.Errorf("%w: block %d of slice %d short (%d!=%d)",
				ErrCapturePending, bi, seg.id, stored, bsize)
		}
		inBlock := sliceOff - uint64(bi)*uint64(fmt2.BlockBytes)
		want := uint64(bsize) - inBlock
		if want > remaining {
			want = remaining
		}
		if inBlock > 0 {
			if _, err := io.CopyN(io.Discard, rc, int64(inBlock)); err != nil {
				rc.Close()
				return fmt.Errorf("%w: block %d of slice %d short", ErrCapturePending, bi, seg.id)
			}
		}
		got, err := io.CopyN(w, rc, int64(want))
		rc.Close()
		if err != nil || got < int64(want) {
			return fmt.Errorf("%w: block %d of slice %d truncated", ErrCapturePending, bi, seg.id)
		}
		sliceOff += uint64(want)
		remaining -= uint64(want)
	}
	return nil
}

var zeroPage = make([]byte, 64<<10)

func writeZeros(w io.Writer, n int64) error {
	for n > 0 {
		k := int64(len(zeroPage))
		if k > n {
			k = n
		}
		if _, err := w.Write(zeroPage[:k]); err != nil {
			return err
		}
		n -= k
	}
	return nil
}
