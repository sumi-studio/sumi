package runtimeprovision

import (
	"os"
	"testing"
)

// Root counterexample: a journal can truncate and regrow to exactly
// the committed size. The next poll has avail == 0 although bytes differ.
func TestRootEqualLengthRewriteMustNotSilentlyDrain(t *testing.T) {
	dir := t.TempDir()
	journal := journalFile(t, dir, "old\r\n")
	_, io_, rec, done := startJournalPump(t, journal, true)
	waitTTYExact(t, io_, "old\r\n")
	oldRecord := journalRecord(t, "old\r\n")
	newRecord := journalRecord(t, "new\r\n")
	if len(oldRecord) != len(newRecord) {
		t.Fatal("fixture sizes differ")
	}
	waitForSrcOff(t, io_, int64(len(oldRecord)))
	// O_TRUNC + one write, same inode; no sleep between shrink and regrowth.
	if err := os.WriteFile(journal, []byte(newRecord), 0600); err != nil {
		t.Fatal(err)
	}
	drained := endPump(t, rec, done)
	got := readAllTTY(t, io_.tty)
	gaps := ttyGaps(t, io_)
	t.Logf("drained=%v tty=%q gaps=%d", drained, got, gaps)
	if drained && got == "old\r\n" && gaps == 0 {
		t.Fatal("same-size rewritten journal silently lost new output and certified drained")
	}
}
