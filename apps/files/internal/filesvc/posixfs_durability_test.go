package filesvc

// Durability regression: a verified effect issued by a retired operation
// (declared before ownership moved, landing after a successor's save)
// must never destroy the newer acknowledged bytes. These tests run the
// REAL posixfs effect functions on a real filesystem (no fakes) — the
// exchange/verify/undo shapes are kernel-level, not simulated.

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func durRoot(t *testing.T) (*posixRoot, string) {
	t.Helper()
	dir := t.TempDir()
	p, err := newRoot(dir)
	if err != nil {
		t.Fatalf("newRoot: %v", err)
	}
	return p, dir
}

func durRead(t *testing.T, dir, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, rel))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return string(b)
}

func durExists(t *testing.T, dir, rel string) bool {
	t.Helper()
	_, err := os.Lstat(filepath.Join(dir, rel))
	return err == nil
}

// durFP returns the current fingerprint of (scope, path) via the real
// stat path.
func durFP(t *testing.T, p *posixRoot, scope, path string) string {
	t.Helper()
	info, err := p.stat(scope, path)
	if err != nil {
		t.Fatalf("stat %s/%s: %v", scope, path, err)
	}
	return info.Fingerprint
}

// staleWrite exercises the full sequence: an op declared while path held
// fpOld, whose filesystem effect only lands AFTER a successor wrote
// "new" — the verified effect must undo itself, never destroy "new".
func TestVerifiedWriteStalePreservesSuccessor(t *testing.T) {
	p, dir := durRoot(t)
	// The acknowledged "old" object the stale op declared against.
	if _, _, err := p.atomicWrite("ws", "a.txt", []byte("old"), true, "", ""); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	fpOld := durFP(t, p, "ws", "a.txt")
	// Successor acknowledges "new".
	if _, _, err := p.atomicWrite("ws", "a.txt", []byte("new"), false, fpOld, ""); err != nil {
		t.Fatalf("successor write: %v", err)
	}
	// The stale effect lands now, carrying the declare-time expectation.
	_, committed, err := p.atomicWrite("ws", "a.txt", []byte("stale"), false, fpOld,
		opStagePrefix+"1")
	if err == nil || !errors.Is(err, ErrExternalChange) {
		t.Fatalf("stale write err = %v, want external_change", err)
	}
	if committed {
		t.Fatal("stale write reported committed")
	}
	if got := durRead(t, dir, "ws/a.txt"); got != "new" {
		t.Fatalf("a.txt = %q, want successor's %q — acknowledged bytes destroyed", got, "new")
	}
	if durExists(t, dir, "ws/"+opStagePrefix+"1") {
		t.Fatal("staged slot left behind after a clean undo")
	}
}

// A successor's REMOVE is equally protected: the stale write must refuse
// when the expected object is gone — not recreate it.
func TestVerifiedWriteStaleVsRemoved(t *testing.T) {
	p, dir := durRoot(t)
	if _, _, err := p.atomicWrite("ws", "a.txt", []byte("old"), true, "", ""); err != nil {
		t.Fatalf("seed: %v", err)
	}
	fpOld := durFP(t, p, "ws", "a.txt")
	if err := os.Remove(filepath.Join(dir, "ws/a.txt")); err != nil {
		t.Fatalf("successor remove: %v", err)
	}
	_, _, err := p.atomicWrite("ws", "a.txt", []byte("stale"), false, fpOld,
		opStagePrefix+"2")
	if !errors.Is(err, ErrExternalChange) {
		t.Fatalf("stale write err = %v, want external_change", err)
	}
	if durExists(t, dir, "ws/a.txt") || durExists(t, dir, "ws/"+opStagePrefix+"2") {
		t.Fatal("stale write recreated the removed path or left the staged slot")
	}
}

// Stale remove must not delete a successor's newer object: the capture
// finds foreign content and restores it.
func TestVerifiedRemoveStalePreservesSuccessor(t *testing.T) {
	p, dir := durRoot(t)
	if _, _, err := p.atomicWrite("ws", "a.txt", []byte("old"), true, "", ""); err != nil {
		t.Fatalf("seed: %v", err)
	}
	fpOld := durFP(t, p, "ws", "a.txt")
	if _, _, err := p.atomicWrite("ws", "a.txt", []byte("new"), false, fpOld, ""); err != nil {
		t.Fatalf("successor write: %v", err)
	}
	committed, err := p.remove("ws", "a.txt", fpOld, opStagePrefix+"3")
	if !errors.Is(err, ErrExternalChange) || committed {
		t.Fatalf("stale remove = (%v, %v), want (false, external_change)", committed, err)
	}
	if got := durRead(t, dir, "ws/a.txt"); got != "new" {
		t.Fatalf("a.txt = %q after stale remove, want %q", got, "new")
	}
	if durExists(t, dir, "ws/"+opStagePrefix+"3") {
		t.Fatal("captured object not restored")
	}
}

// Stale rename must restore both names: the successor's destination
// object and the stale op's own source.
func TestVerifiedRenameStalePreservesSuccessor(t *testing.T) {
	p, dir := durRoot(t)
	if _, _, err := p.atomicWrite("ws", "src.txt", []byte("moved"), true, "", ""); err != nil {
		t.Fatalf("seed src: %v", err)
	}
	if _, _, err := p.atomicWrite("ws", "dst.txt", []byte("old-dst"), true, "", ""); err != nil {
		t.Fatalf("seed dst: %v", err)
	}
	fpSrc := durFP(t, p, "ws", "src.txt")
	fpDstOld := durFP(t, p, "ws", "dst.txt")
	// Successor acknowledges a newer destination object.
	if _, _, err := p.atomicWrite("ws", "dst.txt", []byte("new-dst"), false, fpDstOld, ""); err != nil {
		t.Fatalf("successor write: %v", err)
	}
	_, committed, err := p.rename("ws", "src.txt", "dst.txt", false,
		fpDstOld, fpSrc, opStagePrefix+"4")
	if !errors.Is(err, ErrExternalChange) || committed {
		t.Fatalf("stale rename = (%v, %v), want (false, external_change)", committed, err)
	}
	if got := durRead(t, dir, "ws/dst.txt"); got != "new-dst" {
		t.Fatalf("dst.txt = %q after stale rename, want %q", got, "new-dst")
	}
	if got := durRead(t, dir, "ws/src.txt"); got != "moved" {
		t.Fatalf("src.txt = %q after stale rename, want %q", got, "moved")
	}
	if durExists(t, dir, "ws/"+opStagePrefix+"4") {
		t.Fatal("parked object left after a clean undo")
	}
}

// Clean paths still work: verified effects commit normally when the
// displaced object is the declared one.
func TestVerifiedEffectsCleanCommit(t *testing.T) {
	p, dir := durRoot(t)
	if _, _, err := p.atomicWrite("ws", "a.txt", []byte("old"), true, "", ""); err != nil {
		t.Fatalf("seed: %v", err)
	}
	fpOld := durFP(t, p, "ws", "a.txt")
	if _, c, err := p.atomicWrite("ws", "a.txt", []byte("new"), false, fpOld, ""); err != nil || !c {
		t.Fatalf("verified overwrite = (%v,%v)", c, err)
	}
	if got := durRead(t, dir, "ws/a.txt"); got != "new" {
		t.Fatalf("a.txt = %q", got)
	}
	// write over a now-absent path with expected-absent evidence.
	if _, _, err := p.atomicWrite("ws", "b.txt", []byte("b"), false, "", ""); err != nil {
		t.Fatalf("absent-expected write: %v", err)
	}
	// rename to an absent destination (dstFP "" = expected absent).
	fpB := durFP(t, p, "ws", "b.txt")
	if _, c, err := p.rename("ws", "b.txt", "c.txt", false, "", fpB, ""); err != nil || !c {
		t.Fatalf("rename err: %v", err)
	}
	if got := durRead(t, dir, "ws/c.txt"); got != "b" {
		t.Fatalf("c.txt = %q", got)
	}
}

// Verified rename over an existing, expected destination displaces it.
func TestVerifiedRenameDisplacesExpected(t *testing.T) {
	p, dir := durRoot(t)
	if _, _, err := p.atomicWrite("ws", "s.txt", []byte("S"), true, "", ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.atomicWrite("ws", "d.txt", []byte("D"), true, "", ""); err != nil {
		t.Fatal(err)
	}
	fpS := durFP(t, p, "ws", "s.txt")
	fpD := durFP(t, p, "ws", "d.txt")
	if _, c, err := p.rename("ws", "s.txt", "d.txt", false, fpD, fpS,
		opStagePrefix+"5"); err != nil || !c {
		t.Fatalf("verified rename = (%v,%v)", c, err)
	}
	if got := durRead(t, dir, "ws/d.txt"); got != "S" {
		t.Fatalf("d.txt = %q, want moved source", got)
	}
	if durExists(t, dir, "ws/s.txt") {
		t.Fatal("source still present after clean rename")
	}
}

// The verify→discard window on the PUBLIC source name: a successor
// write lands at the source pathname after the displaced object passed
// verification. The discard must capture whatever is actually at the
// name into the intent's private slot and re-verify it there — the
// racer's content is restored to its name, never unlinked.
func TestVerifiedRenameRacerAtSourceName(t *testing.T) {
	p, dir := durRoot(t)
	if _, _, err := p.atomicWrite("ws", "s.txt", []byte("S"), true, "", ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.atomicWrite("ws", "d.txt", []byte("D"), true, "", ""); err != nil {
		t.Fatal(err)
	}
	fpS := durFP(t, p, "ws", "s.txt")
	fpD := durFP(t, p, "ws", "d.txt")
	// Between the post-exchange verify and the discard, a racing writer
	// claims the source pathname — the shape a resumed retired actor
	// would face.
	p.faultHook = func(tag string) {
		if tag == "rename.postVerify" {
			if err := os.WriteFile(filepath.Join(dir, "ws", "s.txt"),
				[]byte("racer"), 0o644); err != nil {
				panic(err)
			}
		}
	}
	if _, c, err := p.rename("ws", "s.txt", "d.txt", false, fpD, fpS,
		opStagePrefix+"6"); err != nil || !c {
		t.Fatalf("verified rename = (%v,%v)", c, err)
	}
	p.faultHook = nil
	if got := durRead(t, dir, "ws/d.txt"); got != "S" {
		t.Fatalf("d.txt = %q, want moved source", got)
	}
	if got := durRead(t, dir, "ws/s.txt"); got != "racer" {
		t.Fatalf("s.txt = %q, want the racer's content restored to its name", got)
	}
	if durExists(t, dir, "ws/"+opStagePrefix+"6") {
		t.Fatal("recovery slot left occupied after racer restore")
	}
}

// undoDisplaced: the exact shape of the reproduced O2 counterexample —
// a racing writer lands between the mismatch verdict and the undo. The
// newest foreign object keeps the name; the older stays parked at the
// staging slot; nothing foreign is ever unlinked.
func TestUndoDisplacedRacedKeepsNewest(t *testing.T) {
	p, dir := durRoot(t)
	_ = p
	// Construct the raced state directly: staged slot holds the object
	// displaced by our effect (older), the name holds a racing writer's
	// newer object. "Ours" is neither — the stale op's own content is
	// already gone (a racer overwrote it).
	if err := os.MkdirAll(filepath.Join(dir, "ws"), 0o755); err != nil {
		t.Fatal(err)
	}
	staged := opStagePrefix + "7"
	if err := os.WriteFile(filepath.Join(dir, "ws", staged), []byte("displaced"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ws", "a.txt"), []byte("newest"), 0o644); err != nil {
		t.Fatal(err)
	}
	dfd, err := os.Open(filepath.Join(dir, "ws"))
	if err != nil {
		t.Fatal(err)
	}
	defer dfd.Close()
	err = undoDisplaced(dfd, dfd, staged, "a.txt", "",
		func(t3 string) bool { return false }) // nothing is ours
	if !errors.Is(err, errUndoParked) {
		t.Fatalf("undo = %v, want errUndoParked", err)
	}
	if got := durRead(t, dir, "ws/a.txt"); got != "newest" {
		t.Fatalf("a.txt = %q, want the racing writer's newest content", got)
	}
	if got := durRead(t, dir, "ws/"+staged); got != "displaced" {
		t.Fatalf("staged = %q, want the preserved displaced object", got)
	}
}

// Clean undo: staged holds foreign displaced, name holds our object —
// the exchange restores both and returns nil.
func TestUndoDisplacedClean(t *testing.T) {
	_, dir := durRoot(t)
	if err := os.MkdirAll(filepath.Join(dir, "ws"), 0o755); err != nil {
		t.Fatal(err)
	}
	staged := opStagePrefix + "8"
	if err := os.WriteFile(filepath.Join(dir, "ws", "ours.txt"), []byte("ours"), 0o644); err != nil {
		t.Fatal(err)
	}
	dfd, err := os.Open(filepath.Join(dir, "ws"))
	if err != nil {
		t.Fatal(err)
	}
	defer dfd.Close()
	our3, _, _, err := fp3at(dfd, "ours.txt")
	if err != nil {
		t.Fatal(err)
	}
	// ours.txt → a.txt (the "committed" stale effect), displaced → staged.
	if err := os.Rename(filepath.Join(dir, "ws", "ours.txt"), filepath.Join(dir, "ws", "a.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ws", staged), []byte("displaced"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := undoDisplaced(dfd, dfd, staged, "a.txt", "",
		func(t3 string) bool { return t3 == our3 }); err != nil {
		t.Fatalf("undo = %v", err)
	}
	if got := durRead(t, dir, "ws/a.txt"); got != "displaced" {
		t.Fatalf("a.txt = %q, want restored displaced object", got)
	}
	// Our object is back at the staged slot — the caller may unlink it.
	st3, _, _, err := fp3at(dfd, staged)
	if err != nil || st3 != our3 {
		t.Fatalf("staged holds %v (err %v), want our object", st3, err)
	}
}

// A parked recovery object survives the staging sweeper — the 10-minute
// staging cleanup must never touch .filesv-op- names.
func TestOpStageNotSwept(t *testing.T) {
	p, dir := durRoot(t)
	if err := os.MkdirAll(filepath.Join(dir, "ws"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ws", opStagePrefix+"9"), []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	p.sweepStaging()
	if !durExists(t, dir, "ws/"+opStagePrefix+"9") {
		t.Fatal("sweeper removed a recovery object")
	}
}

// RemoveStaged is a two-hop delete: it moves the object to a unique
// quarantine name, re-verifies, and unlinks only on a match. A
// mismatched capture stays parked (bytes preserved) and is enumerable
// via ListStaged for a later pass.
func TestRemoveStagedQuarantine(t *testing.T) {
	p, dir := durRoot(t)
	if err := os.MkdirAll(filepath.Join(dir, "ws"), 0o755); err != nil {
		t.Fatal(err)
	}
	slot := opStagePrefix + "10"
	if err := os.WriteFile(filepath.Join(dir, "ws", slot), []byte("victim"), 0o644); err != nil {
		t.Fatal(err)
	}
	view, err := p.pin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	fp := durFP(t, p, "ws", slot)
	// Wrong identity: the object must NOT be deleted — it is quarantined.
	if err := view.RemoveStaged("ws", slot, "1:2:3", ""); !errors.Is(err, ErrConflict) {
		t.Fatalf("mismatched RemoveStaged = %v, want ErrConflict", err)
	}
	if durExists(t, dir, "ws/"+slot) {
		t.Fatal("object still at the staging slot after quarantine move")
	}
	names, err := view.ListStaged("ws", "", slot+"-q-")
	if err != nil || len(names) != 1 {
		t.Fatalf("ListStaged = %v, %v", names, err)
	}
	if got := durRead(t, dir, "ws/"+names[0]); got != "victim" {
		t.Fatalf("quarantined content = %q, want preserved bytes", got)
	}
	// Correct identity on the quarantined name: verified in place,
	// unlinked.
	if err := view.RemoveStaged("ws", names[0], fp3(fp), ""); err != nil {
		t.Fatalf("matching RemoveStaged = %v", err)
	}
	if durExists(t, dir, "ws/"+names[0]) {
		t.Fatal("quarantined object not deleted after matching verify")
	}
}

// The op-stage namespace is reserved and hidden like the tmp prefix.
func TestOpStageReservedAndHidden(t *testing.T) {
	p, dir := durRoot(t)
	if _, _, err := p.atomicWrite("ws", opStagePrefix+"x", []byte("y"), true, "", ""); !errors.Is(err, ErrReserved) {
		t.Fatalf("write to op-stage name: %v, want ErrReserved", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "ws"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ws", opStagePrefix+"z"), []byte("z"), 0o644); err != nil {
		t.Fatal(err)
	}
	ents, _, err := p.list("ws", "", 100, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if strings.HasPrefix(e.Name, opStagePrefix) {
			t.Fatalf("recovery object %q visible in list", e.Name)
		}
	}
	_ = strconv.Itoa
}
