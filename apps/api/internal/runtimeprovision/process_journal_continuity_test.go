package runtimeprovision

// TREV2-08 (reviewer B R1): an in-place truncation of the SAME journal
// inode — logrotate copytruncate, or filesystems without stable inode
// identity — must not silently lose output under a live tailer. The
// preserved schedule truncates and regrows PAST the committed offset
// before the next poll, so a size < offset check alone cannot observe
// it; pumpJournal verifies continuity against witnessed raw journal
// bytes at the resume boundary instead.
//
// Repaired contract under test:
//   - detected discontinuity records an explicit gap (ttyevents),
//   - a restarted journal replays its whole new era (no silent loss,
//     no torn resume at a stale absolute offset),
//   - a head-preserved prefix is never re-emitted (no duplicate
//     replay); the unknowable divergence window is covered by the gap
//     and the pump resumes at end-of-file,
//   - a terminal op's drain verdict stays honest — 'drained' is only
//     returned after the real tail is consumed,
//   - ordinary appends — including byte-identical repeated lines —
//     never false-positive.

import (
	"os"
	"sync"
	"testing"
	"time"
)

func startJournalPump(t *testing.T, journal string, alive bool) (*processStore, *interactiveIO, *processRecord, chan bool) {
	t.Helper()
	dir := t.TempDir()
	b := &interactiveTestBackend{
		processTestBackend: &processTestBackend{fakeBackend: newFakeBackend()},
		journal:            journal,
	}
	s, err := newProcessStore(dir+"/state", b)
	if err != nil {
		t.Fatal(err)
	}
	op := interactiveOp(t)
	rec := &processRecord{mu: &sync.Mutex{}, Operation: op}
	s.mu.Lock()
	s.records[op.OperationID] = rec
	s.mu.Unlock()
	io_ := newInteractiveIO(t, s, op)
	done := make(chan bool, 1)
	go func() { done <- s.pumpJournal(io_, journal, alive) }()
	return s, io_, rec, done
}

func waitTTYExact(t *testing.T, io_ *interactiveIO, want string) {
	t.Helper()
	deadline := time.Now().Add(6 * time.Second)
	for readAllTTY(t, io_.tty) != want {
		if time.Now().After(deadline) {
			t.Fatalf("tty = %q, want %q", readAllTTY(t, io_.tty), want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func endPump(t *testing.T, rec *processRecord, done chan bool) bool {
	t.Helper()
	rec.mu.Lock()
	rec.Operation.State = ProcessSucceeded
	rec.mu.Unlock()
	select {
	case drained := <-done:
		return drained
	case <-time.After(6 * time.Second):
		t.Fatal("pump did not exit after op went terminal")
		return false
	}
}

func ttyGaps(t *testing.T, io_ *interactiveIO) int {
	t.Helper()
	gaps, err := io_.tty.gapsAtOrAfter(0)
	if err != nil {
		t.Fatal(err)
	}
	return len(gaps)
}

// The preserved reproduction: truncate the same inode and regrow past
// the old committed offset BEFORE the next poll. Every post-truncate
// record must arrive intact, preceded by an explicit gap marker.
func TestJournalInPlaceTruncateRegrowMarksGap(t *testing.T) {
	dir := t.TempDir()
	journal := journalFile(t, dir, "old\r\n")
	_, io_, rec, done := startJournalPump(t, journal, true)
	waitTTYExact(t, io_, "old\r\n")
	// Let the committed cursor settle past the initial drain.
	waitForSrcOff(t, io_, int64(len(journalRecord(t, "old\r\n"))))

	if err := os.Truncate(journal, 0); err != nil {
		t.Fatal(err)
	}
	// Regrow past the old offset immediately — no settle window; the
	// next poll already sees size > pos.Off on the same inode.
	honest := "old\r\n"
	for i := 0; i < 8; i++ {
		appendJournal(t, journal, "new-era-line-abcdef\r\n")
		honest += "new-era-line-abcdef\r\n"
	}
	waitTTYExact(t, io_, honest)
	drained := endPump(t, rec, done)
	if !drained {
		t.Fatal("terminal drain returned false after in-place truncation")
	}
	if n := ttyGaps(t, io_); n != 1 {
		t.Fatalf("in-place truncate+regrow produced %d gap markers (want exactly 1); tty=%q", n, readAllTTY(t, io_.tty))
	}
	if got := readAllTTY(t, io_.tty); got != honest {
		t.Fatalf("tty = %q, want %q — silent loss or duplicate replay", got, honest)
	}
}

// Companion probe: truncate then a single record that regrows past the
// old offset. The stale-offset resume would tear this record's head
// and emit nothing; the repair must deliver it whole with a gap.
func TestJournalInPlaceTruncateSingleAppend(t *testing.T) {
	dir := t.TempDir()
	journal := journalFile(t, dir, "old\r\n")
	_, io_, rec, done := startJournalPump(t, journal, true)
	waitTTYExact(t, io_, "old\r\n")
	waitForSrcOff(t, io_, int64(len(journalRecord(t, "old\r\n"))))

	if err := os.Truncate(journal, 0); err != nil {
		t.Fatal(err)
	}
	appendJournal(t, journal, "only-record-after-truncate\r\n")

	want := "old\r\n" + "only-record-after-truncate\r\n"
	waitTTYExact(t, io_, want)
	endPump(t, rec, done)
	if n := ttyGaps(t, io_); n != 1 {
		t.Fatalf("single-append truncation produced %d gap markers (want exactly 1); tty=%q", n, readAllTTY(t, io_.tty))
	}
	if got := readAllTTY(t, io_.tty); got != want {
		t.Fatalf("tty = %q, want %q", got, want)
	}
}

// Observed shrink on a terminal op: the drain verdict must stay honest
// — a gap marks the lost window before the pump may certify drained.
func TestJournalInPlaceShrinkTerminalDrainHonest(t *testing.T) {
	dir := t.TempDir()
	journal := journalFile(t, dir, "old\r\n")
	_, io_, rec, done := startJournalPump(t, journal, true)
	waitTTYExact(t, io_, "old\r\n")
	waitForSrcOff(t, io_, int64(len(journalRecord(t, "old\r\n"))))

	if err := os.Truncate(journal, 0); err != nil {
		t.Fatal(err)
	}
	// The op ends while the journal is still shrunk: the pump must
	// record the loss and then may certify drained (the file is
	// genuinely empty now) — but never silently.
	drained := endPump(t, rec, done)
	if !drained {
		t.Fatal("terminal drain returned false on empty post-truncate journal")
	}
	if n := ttyGaps(t, io_); n != 1 {
		t.Fatalf("terminal drain on truncated journal produced %d gap markers (want exactly 1)", n)
	}
	if got := readAllTTY(t, io_.tty); got != "old\r\n" {
		t.Fatalf("tty = %q, want %q", got, "old\r\n")
	}
}

// Head-preserved truncation (truncate to a nonzero size, then regrow):
// the surviving prefix is genuine already-delivered output — the pump
// must not replay it — but the divergence point is unknowable, so the
// uncertain window is covered by the gap and the tail resumes at EOF.
func TestJournalInPlaceTruncatePreservedHeadNoDuplicate(t *testing.T) {
	dir := t.TempDir()
	journal := journalFile(t, dir, "first\r\n", "second\r\n", "third\r\n")
	_, io_, rec, done := startJournalPump(t, journal, true)
	want := "first\r\nsecond\r\nthird\r\n"
	waitTTYExact(t, io_, want)

	// Keep exactly the first record; drop the rest of the committed
	// tail — the divergence point sits inside delivered content.
	f, err := os.Open(journal)
	if err != nil {
		t.Fatal(err)
	}
	var firstLine int64
	buf := make([]byte, 4096)
	if n, _ := f.Read(buf); n > 0 {
		for i := 0; i < n; i++ {
			if buf[i] == '\n' {
				firstLine = int64(i) + 1
				break
			}
		}
	}
	f.Close()
	if firstLine == 0 {
		t.Fatal("journal record boundary not found")
	}
	if err := os.Truncate(journal, firstLine); err != nil {
		t.Fatal(err)
	}
	appendJournal(t, journal, "post-divergence\r\n")

	// The pump detects the shrink, records the gap, and resumes at
	// end-of-file: 'post-divergence' — appended inside the uncertain
	// window — is honestly lost under the gap marker rather than
	// replayed as trustworthy continuation or duplicated.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && ttyGaps(t, io_) == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if ttyGaps(t, io_) != 1 {
		t.Fatalf("preserved-head truncation produced %d gap markers, want 1", ttyGaps(t, io_))
	}
	endPump(t, rec, done)
	got := readAllTTY(t, io_.tty)
	if got != want {
		t.Fatalf("tty = %q, want %q — duplicate replay or unexpected content", got, want)
	}
}

// Ordinary append traffic — including byte-identical repeated lines —
// never trips the continuity witness.
func TestJournalOrdinaryAppendDuplicateTextNoFalseGap(t *testing.T) {
	dir := t.TempDir()
	journal := journalFile(t, dir, "line\r\n")
	_, io_, rec, done := startJournalPump(t, journal, true)
	waitTTYExact(t, io_, "line\r\n")

	want := "line\r\n"
	for i := 0; i < 10; i++ {
		appendJournal(t, journal, "line\r\n") // identical content
		want += "line\r\n"
	}
	waitTTYExact(t, io_, want)
	drained := endPump(t, rec, done)
	if !drained {
		t.Fatal("drain returned false on ordinary appends")
	}
	if ttyGaps(t, io_) != 0 {
		t.Fatalf("ordinary duplicate appends produced %d false gap markers", ttyGaps(t, io_))
	}
	if got := readAllTTY(t, io_.tty); got != want {
		t.Fatalf("tty = %q, want %q", got, want)
	}
}

func waitForSrcOff(t *testing.T, io_ *interactiveIO, want int64) {
	t.Helper()
	deadline := time.Now().Add(6 * time.Second)
	for {
		if src := io_.tty.loadSrc(); src.Off >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("committed src = %+v, want off >= %d", io_.tty.loadSrc(), want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// journalRecord renders one journal line exactly as journalFile/
// appendJournal write it, for byte-length expectations.
func journalRecord(t *testing.T, s string) string {
	t.Helper()
	return `{"log":` + mustJSON(t, s) + `,"stream":"stdout","time":"2026-09-19T00:00:00Z"}` + "\n"
}

// Equal-size rewrite while the op is alive, then an ordinary later
// append: the rewrite is detected (mtime moved, no growth), the new
// era is replayed whole, and the later append lands after it — one
// marker, no duplicates, no silent loss.
func TestJournalEqualSizeRewriteThenAppend(t *testing.T) {
	dir := t.TempDir()
	journal := journalFile(t, dir, "old\r\n")
	_, io_, rec, done := startJournalPump(t, journal, true)
	waitTTYExact(t, io_, "old\r\n")
	waitForSrcOff(t, io_, int64(len(journalRecord(t, "old\r\n"))))

	// Same inode, same length, changed content — avail never moves.
	newRecord := journalRecord(t, "new\r\n")
	if len(newRecord) != len(journalRecord(t, "old\r\n")) {
		t.Fatal("fixture sizes differ")
	}
	if err := os.WriteFile(journal, []byte(newRecord), 0600); err != nil {
		t.Fatal(err)
	}
	// The rewrite alone must already surface the marker and the new
	// record — before any later append.
	waitTTYExact(t, io_, "old\r\nnew\r\n")

	appendJournal(t, journal, "after-rewrite\r\n")
	waitTTYExact(t, io_, "old\r\nnew\r\nafter-rewrite\r\n")
	if !endPump(t, rec, done) {
		t.Fatal("drain returned false")
	}
	if n := ttyGaps(t, io_); n != 1 {
		t.Fatalf("equal-size rewrite produced %d gap markers, want exactly 1", n)
	}
}
