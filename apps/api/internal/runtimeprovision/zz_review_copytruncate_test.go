package runtimeprovision

// REVIEW OVERLAY — independent reviewer B, 2026-09-19.
// Bounded reproduction for a journal-tail defect in
// process_interactive_service.go pumpJournal: rotation/truncation is
// detected only at open (pos.Off > st.Size()) or by inode change. An
// in-place truncation of the SAME inode under a live tailer is never
// detected — the pump sits at a negative avail forever, and once the
// file regrows past the committed offset it reads the NEW content at
// the OLD absolute offset, silently skipping the window with no gap.

import (
	"os"
	"sync"
	"testing"
	"time"
)

func TestReviewCopytruncateInPlaceLosesSilently(t *testing.T) {
	dir := t.TempDir()
	journal := journalFile(t, dir, "old\r\n")
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
	go func() { done <- s.pumpJournal(io_, journal, true) }()

	deadline := time.Now().Add(5 * time.Second)
	for readAllTTY(t, io_.tty) != "old\r\n" {
		if time.Now().After(deadline) {
			t.Fatal("initial drain did not complete")
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)
	src := io_.tty.loadSrc()
	st, _ := os.Stat(journal)
	t.Logf("pre-truncate: committed src=%+v journalSize=%d", src, st.Size())

	// copytruncate: same inode, size resets below the committed offset.
	if err := os.Truncate(journal, 0); err != nil {
		t.Fatal(err)
	}
	// Regrow past the old committed offset so avail > 0 again and the
	// tailer resumes reading — at the stale absolute offset.
	for i := 0; i < 8; i++ {
		appendJournal(t, journal, "new-era-line-abcdef\r\n")
	}
	time.Sleep(800 * time.Millisecond)
	src2 := io_.tty.loadSrc()
	st2, _ := os.Stat(journal)
	t.Logf("post-regrow: committed src=%+v journalSize=%d tty=%q", src2, st2.Size(), readAllTTY(t, io_.tty))

	// Terminate the op so the pump drains and exits.
	rec.mu.Lock()
	rec.Operation.State = ProcessSucceeded
	rec.mu.Unlock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pump did not exit after op went terminal")
	}

	gaps, err := io_.tty.gapsAtOrAfter(0)
	if err != nil {
		t.Fatal(err)
	}
	got := readAllTTY(t, io_.tty)
	t.Logf("gaps=%d tty=%q", len(gaps), got)

	honest := "old\r\n"
	for i := 0; i < 8; i++ {
		honest += "new-era-line-abcdef\r\n"
	}
	if len(gaps) == 0 && got != honest {
		t.Fatalf("in-place truncation: no gap marker and tail misaligned — skipped window lost silently (tty=%q)", got)
	}
	if len(gaps) == 0 {
		t.Fatalf("in-place truncation: bytes survived only by luck — no gap marker recorded")
	}
}

// Companion probe: truncate then append exactly ONE record past the
// committed offset. If the pump resumes at the stale offset, the sole
// record is torn at its head and ZERO new lines are captured — a
// silently lost record with no gap marker.
func TestReviewCopytruncateSingleAppend(t *testing.T) {
	dir := t.TempDir()
	journal := journalFile(t, dir, "old\r\n")
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
	go func() { done <- s.pumpJournal(io_, journal, true) }()

	deadline := time.Now().Add(5 * time.Second)
	for readAllTTY(t, io_.tty) != "old\r\n" {
		if time.Now().After(deadline) {
			t.Fatal("initial drain did not complete")
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)

	if err := os.Truncate(journal, 0); err != nil {
		t.Fatal(err)
	}
	appendJournal(t, journal, "only-record-after-truncate\r\n")
	time.Sleep(800 * time.Millisecond)
	t.Logf("post-append tty=%q src=%+v", readAllTTY(t, io_.tty), io_.tty.loadSrc())

	rec.mu.Lock()
	rec.Operation.State = ProcessSucceeded
	rec.mu.Unlock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pump did not exit after op went terminal")
	}

	gaps, _ := io_.tty.gapsAtOrAfter(0)
	got := readAllTTY(t, io_.tty)
	t.Logf("gaps=%d tty=%q", len(gaps), got)
	if got == "old\r\n" && len(gaps) == 0 {
		t.Fatalf("post-truncate record silently lost: torn read at stale offset, no gap marker")
	}
}
