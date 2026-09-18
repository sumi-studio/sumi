package runtimeprovision

// Durable bounded scrollback for interactive operations.
//
// The pump appends raw PTY bytes to <opid>.ttylog. Absolute byte
// offsets are stable across compaction: `base` (journaled in
// <opid>.ttybase) is the offset of the first retained byte, so the
// retained window is [base, total) of the emitted stream.
// Detach/reattach boundaries and compaction both surface as explicit
// gap events in <opid>.ttyevents (JSON lines): a gap means "output may
// have been lost here" — never silent truncation.

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Retention for the provisioner-side scrollback. This is deliberately
// larger than the API serving cap: it is the crash-recovery buffer,
// not the product surface.
const (
	ttyLogMaxRetain  = 16 << 20 // compact when retained output exceeds this
	ttyLogKeepRetain = 12 << 20 // keep this much tail after compaction
)

type ttyEvent struct {
	Kind string `json:"kind"` // "gap"
	// At is the absolute emitted offset where the gap boundary sits.
	At int64 `json:"at"`
	// Note distinguishes "possible loss during output reattach" from
	// other gap causes; it is evidence, not authority.
	Note string `json:"note,omitempty"`
}

type ttyLog struct {
	mu    sync.Mutex
	log   *os.File
	ev    *os.File
	dir   string
	id    string
	base  int64 // absolute offset of first retained byte
	total int64 // absolute emitted bytes appended
}

func ttyLogPath(directory, operationID string) string {
	return filepath.Join(directory, operationID+".ttylog")
}

func openTTYLog(directory, operationID string) (*ttyLog, error) {
	f, err := os.OpenFile(ttyLogPath(directory, operationID), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	var base int64
	if b, err := os.ReadFile(filepath.Join(directory, operationID+".ttybase")); err == nil && len(b) == 8 {
		base = int64(binary.LittleEndian.Uint64(b))
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	ev, err := os.OpenFile(filepath.Join(directory, operationID+".ttyevents"), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		f.Close()
		return nil, err
	}
	return &ttyLog{log: f, ev: ev, dir: directory, id: operationID, base: base, total: base + info.Size()}, nil
}

func (t *ttyLog) append(p []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, err := t.log.Write(p); err != nil {
		return err
	}
	t.total += int64(len(p))
	if err := t.log.Sync(); err != nil {
		return err
	}
	if t.total-t.base > ttyLogMaxRetain {
		return t.compactLocked()
	}
	return nil
}

// recordGap journals an explicit boundary where emitted output may
// have been lost (e.g. the output stream was reattached). It is a
// marker, not padding: readers still observe contiguous absolute
// offsets, and the gap event tells them the window existed.
func (t *ttyLog) recordGap(note string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	b, err := json.Marshal(ttyEvent{Kind: "gap", At: t.total, Note: note})
	if err != nil {
		return err
	}
	if _, err = t.ev.Write(append(b, '\n')); err != nil {
		return err
	}
	return t.ev.Sync()
}

// compactLocked rewrites the log to its retained tail and advances
// base. [oldBase, newBase) is a retention gap that reads report
// explicitly via the BaseOffset/Gap fields.
func (t *ttyLog) compactLocked() error {
	size := t.total - t.base
	keep := int64(ttyLogKeepRetain)
	if keep > size {
		keep = size
	}
	buf := make([]byte, keep)
	if _, err := t.log.ReadAt(buf, size-keep); err != nil {
		return fmt.Errorf("ttylog compaction read: %w", err)
	}
	tmp, err := os.CreateTemp(t.dir, ".ttylog-")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err = tmp.Write(buf); err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tmpName, ttyLogPath(t.dir, t.id)); err != nil {
		return err
	}
	f, err := os.OpenFile(ttyLogPath(t.dir, t.id), os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	old := t.log
	t.log = f
	old.Close()
	t.base = t.total - keep
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(t.base))
	baseFile := filepath.Join(t.dir, t.id+".ttybase")
	if err = os.WriteFile(baseFile, b[:], 0600); err != nil {
		return err
	}
	d, err := os.Open(t.dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// stats returns the absolute [base, total) retained window.
func (t *ttyLog) stats() (base, total int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.base, t.total
}

// ttySrc is the durable read position into the docker json-file
// journal: inode + byte offset. Persisted beside the scrollback so a
// provisioner restart resumes the pump at the exact byte it committed,
// never replaying the daemon ring into duplicate scrollback.
type ttySrc struct {
	Ino uint64
	Off int64
}

func (t *ttyLog) srcPath() string {
	return filepath.Join(t.dir, t.id+".ttysrc")
}

// loadSrc returns the persisted journal position. A missing or
// malformed file is a zero position — the pump then treats the whole
// journal as backlog (correct for a first-ever attach).
func (t *ttyLog) loadSrc() ttySrc {
	var pos ttySrc
	b, err := os.ReadFile(t.srcPath())
	if err != nil || len(b) != 16 {
		return pos
	}
	pos.Ino = binary.LittleEndian.Uint64(b[:8])
	pos.Off = int64(binary.LittleEndian.Uint64(b[8:]))
	return pos
}

// saveSrc commits the journal position atomically after the appended
// bytes are durable, so crash ordering can never place the cursor
// ahead of the scrollback (which would silently skip bytes on resume).
// The file and its directory are fsynced so a committed cursor survives
// a provisioner or host crash; a crash before this call leaves appended
// bytes the cursor does not cover, which recoverSource reconciles.
func (t *ttyLog) saveSrc(pos ttySrc) error {
	var b [16]byte
	binary.LittleEndian.PutUint64(b[:8], pos.Ino)
	binary.LittleEndian.PutUint64(b[8:], uint64(pos.Off))
	tmp := t.srcPath() + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(b[:]); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp, t.srcPath()); err != nil {
		return err
	}
	if d, derr := os.Open(t.dir); derr == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// read returns up to limit retained bytes starting at absolute
// offset. gap is true when offset precedes the retained base — the
// caller must surface that bytes [offset, base) are gone.
func (t *ttyLog) read(offset int64, limit int) (data []byte, base, next int64, gap bool, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	base, total := t.base, t.total
	gap = offset < base
	start := offset
	if start < base {
		start = base
	}
	if start > total {
		start = total
	}
	end := start + int64(limit)
	if end > total {
		end = total
	}
	if end > start {
		data = make([]byte, end-start)
		if _, err = t.log.ReadAt(data, start-base); err != nil {
			return nil, base, next, gap, fmt.Errorf("ttylog read: %w", err)
		}
	}
	return data, base, end, gap, nil
}

// gapsAtOrAfter returns journaled gap events at offsets >= offset,
// so a reader catching up can mark detach boundaries it skipped past.
func (t *ttyLog) gapsAtOrAfter(offset int64) ([]ttyEvent, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, err := t.ev.Seek(0, 0); err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(t.ev.Name())
	if err != nil {
		return nil, err
	}
	var events []ttyEvent
	for _, line := range splitLines(raw) {
		var e ttyEvent
		if json.Unmarshal(line, &e) == nil && e.Kind == "gap" && e.At >= offset {
			events = append(events, e)
		}
	}
	return events, nil
}

func splitLines(b []byte) [][]byte {
	var lines [][]byte
	start := 0
	for i, c := range b {
		if c == '\n' {
			if i > start {
				lines = append(lines, b[start:i])
			}
			start = i + 1
		}
	}
	if len(b) > start {
		lines = append(lines, b[start:])
	}
	return lines
}

func (t *ttyLog) close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return errors.Join(t.log.Close(), t.ev.Close())
}
