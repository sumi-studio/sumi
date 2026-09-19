package filesvc

// capture_decode_test.go — unit coverage for the byte path that needs
// no PG or mount: slice-record validation, ordered overlap resolution,
// clipping, format gating, key layout, and bounded streaming against a
// synthetic object store.

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"testing"
)

func recBytes(recs []sliceRec) []byte {
	b := make([]byte, 0, len(recs)*sliceRecBytes)
	for _, r := range recs {
		var tmp [sliceRecBytes]byte
		binary.BigEndian.PutUint32(tmp[0:], r.pos)
		binary.BigEndian.PutUint64(tmp[4:], r.id)
		binary.BigEndian.PutUint32(tmp[12:], r.size)
		binary.BigEndian.PutUint32(tmp[16:], r.off)
		binary.BigEndian.PutUint32(tmp[20:], r.length)
		b = append(b, tmp[:]...)
	}
	return b
}

func TestParseSliceRecs_Validation(t *testing.T) {
	if _, err := parseSliceRecs(make([]byte, 25)); err == nil {
		t.Fatal("odd bytea accepted")
	}
	// off+len > size refused
	b := recBytes([]sliceRec{{pos: 0, id: 7, size: 10, off: 8, length: 5}})
	if _, err := parseSliceRecs(b); err == nil || !errors.Is(err, ErrCaptureRefused) {
		t.Fatalf("off+len>size not refused: %v", err)
	}
	// pos+len crossing chunk boundary refused
	b = recBytes([]sliceRec{{pos: uint32(chunkSize) - 4, id: 7, size: 10, off: 0, length: 8}})
	if _, err := parseSliceRecs(b); err == nil {
		t.Fatal("cross-chunk slice not refused")
	}
	// data slice with zero size refused
	b = recBytes([]sliceRec{{pos: 0, id: 7, size: 0, off: 0, length: 0}})
	if _, err := parseSliceRecs(b); err == nil {
		t.Fatal("zero-size data slice not refused")
	}
	// hole record (id=0) legal
	b = recBytes([]sliceRec{{pos: 0, id: 0, size: 100, off: 0, length: 50}})
	if _, err := parseSliceRecs(b); err != nil {
		t.Fatalf("hole record refused: %v", err)
	}
}

func TestResolveChunk_OverlapHoleClip(t *testing.T) {
	// records: [0,100) slice1, [50,150) slice2 (overrides tail of 1),
	// hole [20,30), and a post-EOF record clipped away.
	recs := []sliceRec{
		{pos: 0, id: 1, size: 100, off: 0, length: 100},
		{pos: 50, id: 2, size: 100, off: 0, length: 100},
		{pos: 20, id: 0, size: 10, off: 0, length: 10},
		{pos: 400, id: 3, size: 50, off: 0, length: 50},
	}
	got := resolveChunk(recs, 120, 0)
	// expected resolved: [0,20)id1 [20,30)hole [30,50)id1 [50,120)id2
	// (record4 clipped: beyond fileLen 120)
	want := []rangeSeg{
		{pos: 0, length: 20, id: 1, off: 0, size: 100},
		{pos: 20, length: 10, id: 0, off: 0, size: 10},
		{pos: 30, length: 20, id: 1, off: 30, size: 100},
		{pos: 50, length: 70, id: 2, off: 0, size: 100},
	}
	if len(got) != len(want) {
		t.Fatalf("resolved %d segs want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("seg %d got %+v want %+v", i, got[i], want[i])
		}
	}
}

func TestResolveChunk_MiddleOverride(t *testing.T) {
	// overwrite [30,40) inside a [0,100) mapping: head/tail of the old
	// slice keep their correct source offsets.
	got := resolveChunk([]sliceRec{
		{pos: 0, id: 1, size: 100, off: 0, length: 100},
		{pos: 30, id: 2, size: 10, off: 5, length: 10},
	}, 100, 0)
	want := []rangeSeg{
		{pos: 0, length: 30, id: 1, off: 0, size: 100},
		{pos: 30, length: 10, id: 2, off: 5, size: 10},
		{pos: 40, length: 60, id: 1, off: 40, size: 100},
	}
	if len(got) != len(want) {
		t.Fatalf("got %+v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("seg %d got %+v want %+v", i, got[i], want[i])
		}
	}
}

func TestGateFormat(t *testing.T) {
	base := rawFormat{UUID: "v1", Storage: "file", Bucket: "/b",
		BlockSize: 4096, MetaVersion: 1}
	if _, err := gateFormat(base); err != nil {
		t.Fatalf("clean format refused: %v", err)
	}
	for name, mut := range map[string]func(*rawFormat){
		"compression": func(f *rawFormat) { f.Compression = "zstd" },
		"encrypt_key": func(f *rawFormat) { f.EncryptKey = "x" },
		// EncryptAlgo alone is NOT a refusal: juicefs format sets it by
		// default on plaintext volumes.
		"key_enc":      func(f *rawFormat) { f.KeyEncrypted = true },
		"metaversion":  func(f *rawFormat) { f.MetaVersion = 2 },
		"blocksize0":   func(f *rawFormat) { f.BlockSize = 0 },
		"blocksizebig": func(f *rawFormat) { f.BlockSize = 64 << 10 },
		"no_uuid":      func(f *rawFormat) { f.UUID = "" },
	} {
		f := base
		mut(&f)
		if _, err := gateFormat(f); !errors.Is(err, ErrCaptureRefused) {
			t.Fatalf("%s not refused: %v", name, err)
		}
	}
	// HashPrefix is supported (exact key layout), not refused.
	f := base
	f.HashPrefix = true
	g, err := gateFormat(f)
	if err != nil || !g.HashPrefix {
		t.Fatalf("hash prefix gate: %v %+v", err, g)
	}
}

func TestObjectKey(t *testing.T) {
	// id=1234567: non-hash chunks/1/1234/1234567_0_4194304
	if k := objectKey(1234567, 0, 4194304, 4194304, false); k != "chunks/1/1234/1234567_0_4194304" {
		t.Fatalf("non-hash key %s", k)
	}
	// hash: 1234567%256 = 135 → "87"
	if k := objectKey(1234567, 0, 4194304, 4194304, true); k != "chunks/87/1/1234567_0_4194304" {
		t.Fatalf("hash key %s", k)
	}
}

// fakeObjs is an in-memory object store for stream tests.
type fakeObjs struct {
	m      map[string][]byte
	short  map[string]int
	opened []string
}

func (f *fakeObjs) open(_ context.Context, key string) (io.ReadCloser, int64, error) {
	b, ok := f.m[key]
	if !ok {
		return nil, 0, errObjMissing
	}
	f.opened = append(f.opened, key)
	n := len(b)
	if s, ok := f.short[key]; ok {
		n = s
	}
	return io.NopCloser(bytes.NewReader(b[:n])), int64(n), nil
}

func captureFmt() captureFormat {
	return captureFormat{VolumeUUID: "v", MetaVersion: 1, BlockBytes: 1024}
}

func TestStreamRange_BoundedFetch(t *testing.T) {
	// slice 9 size 3072 (3 blocks of 1024), we need only [1024,2048)
	// — exactly one block fetch, no obsolete reads.
	var blk bytes.Buffer
	for i := 0; i < 3072; i++ {
		blk.WriteByte(byte(i / 1024))
	}
	objs := &fakeObjs{m: map[string][]byte{
		"chunks/0/0/9_0_1024": blk.Bytes()[:1024],
		"chunks/0/0/9_1_1024": blk.Bytes()[1024:2048],
		"chunks/0/0/9_2_1024": blk.Bytes()[2048:],
	}}
	seg := rangeSeg{pos: 0, length: 2048, id: 9, off: 0, size: 3072}
	var out bytes.Buffer
	err := streamRange(context.Background(), objs, captureFmt(), seg, 0, 1024, 1024, &out)
	if err != nil {
		t.Fatal(err)
	}
	if len(objs.opened) != 1 || objs.opened[0] != "chunks/0/0/9_1_1024" {
		t.Fatalf("fetched %v — obsolete/extra blocks fetched", objs.opened)
	}
	for i, b := range out.Bytes() {
		if b != 1 {
			t.Fatalf("byte %d = %d want 1", i, b)
		}
	}
}

func TestStreamRange_MissingShort(t *testing.T) {
	objs := &fakeObjs{m: map[string][]byte{}}
	seg := rangeSeg{pos: 0, length: 100, id: 9, off: 0, size: 1024}
	err := streamRange(context.Background(), objs, captureFmt(), seg, 0, 0, 100, io.Discard)
	if !errors.Is(err, ErrCapturePending) {
		t.Fatalf("missing -> %v want pending", err)
	}
	objs.m["chunks/0/0/9_0_1024"] = make([]byte, 1024)
	objs.short = map[string]int{"chunks/0/0/9_0_1024": 500}
	err = streamRange(context.Background(), objs, captureFmt(), seg, 0, 0, 100, io.Discard)
	if !errors.Is(err, ErrCapturePending) {
		t.Fatalf("short -> %v want pending", err)
	}
}

func TestWriteZeros(t *testing.T) {
	var b bytes.Buffer
	if err := writeZeros(&b, 200000); err != nil {
		t.Fatal(err)
	}
	if b.Len() != 200000 || !bytes.Equal(b.Bytes(), make([]byte, 200000)) {
		t.Fatal("zero fill wrong")
	}
}

func TestPrivateName(t *testing.T) {
	for name, want := range map[string]bool{
		".filesv-tmp-abc": true,
		".filesv-op-x":    true,
		".filesv-op-":     true,
		".filesv-notes":   false, // service-legal user file
		".filesv-other":   false,
		".filesv":         false,
		"a.txt":           false,
		"日本語.txt":         false,
	} {
		if privateName([]byte(name)) != want {
			t.Fatalf("%q want %v", name, want)
		}
	}
}

func TestManifestSHA_Stable(t *testing.T) {
	// The hash must change on rename (path bytes differ) — identity
	// covers path/content mapping, not a truncated filename hash.
	e := []captureEntryRow{{Seq: 0, Path: []byte("a"), Name: []byte("a"), Ino: 2, NodeType: 1}}
	c := &captureRow{Volume: "v", Scope: "s", AnchorIno: 2}
	h1 := manifestSHA(c, e, nil)
	e[0].Path = []byte("b")
	h2 := manifestSHA(c, e, nil)
	if h1 == h2 {
		t.Fatal("rename did not change manifest identity")
	}
	_ = fmt.Sprint()
}
