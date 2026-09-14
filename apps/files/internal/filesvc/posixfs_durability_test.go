package filesvc

// Durability regression: a verified effect issued by a retired operation
// (declared before ownership moved, landing after a successor's save)
// must never destroy the newer acknowledged bytes. These tests run the
// REAL posixfs effect functions on a real filesystem (no fakes) — the
// exchange/verify/undo shapes are kernel-level, not simulated.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
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

// ---- Sealed-quarantine regression tests (review-witnessed races) ----
//
// The independent review demonstrated two reachable losses in the prior
// candidate: a retired reconciler's delayed SwapStaged could write newer
// acknowledged bytes into a "-q-" name another pass was about to unlink,
// and the actor-side post-undo slot unlink could delete foreign bytes a
// delayed swap landed there. The repair makes "-q-" names SEALED — no
// syscall may write into one once it exists — and routes every unlink
// through capture → seal → re-verify → unlink. These tests replay both
// interleavings on a real filesystem through the real production
// functions; only the landing instant of the retired actor's syscall is
// controlled (identical to a FUSE/kernel delayed completion).

// waitOr fails the test if a gate does not fire — keeps a broken premise
// from hanging the suite.
func waitOr(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(30 * time.Second):
		t.Fatalf("timeout waiting for %s", what)
	}
}

// gatedView wraps a real ReconView and defers the first mutating effect
// until released — a syscall issued by a retired actor that completes
// arbitrarily late.
type gatedView struct {
	ReconView
	waiting chan struct{}
	gate    chan struct{}
	done    chan struct{}
	once    sync.Once
}

// hold defers the first mutating call until the gate is released;
// done closes only after that deferred syscall has actually landed —
// the test then knows the retired actor's effect is in the window it
// was meant to race.
func (g *gatedView) hold(fn func() error) error {
	first := false
	g.once.Do(func() {
		close(g.waiting)
		first = true
	})
	if !first {
		return fn()
	}
	<-g.gate
	err := fn()
	close(g.done)
	return err
}

func (g *gatedView) SwapStaged(scope, staged, name string) error {
	return g.hold(func() error { return g.ReconView.SwapStaged(scope, staged, name) })
}

func (g *gatedView) MoveStaged(scope, from, to string) error {
	return g.hold(func() error { return g.ReconView.MoveStaged(scope, from, to) })
}

// scanDirFor returns the name of the directory entry under dir/scope
// holding exactly want bytes, or "".
func scanDirFor(t *testing.T, dir, scope string, want []byte) string {
	t.Helper()
	ents, err := os.ReadDir(filepath.Join(dir, scope))
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, scope, e.Name()))
		if err != nil {
			continue
		}
		if sha(string(b)) == sha(string(want)) {
			return e.Name()
		}
	}
	return ""
}

// Seal enforcement, directly: nothing may write INTO a sealed name —
// not an exchange, not a restore move — while the sealed object may be
// moved out or verified-then-unlinked in place.
func TestSealedNameRejectsWrites(t *testing.T) {
	p, dir := durRoot(t)
	if err := os.MkdirAll(filepath.Join(dir, "ws"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ws", "f.txt"), []byte("live"), 0o644); err != nil {
		t.Fatal(err)
	}
	q := opStagePrefix + "9-q-aaaaaa"
	if err := os.WriteFile(filepath.Join(dir, "ws", q), []byte("parked"), 0o644); err != nil {
		t.Fatal(err)
	}
	view, err := p.pin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	if err := view.SwapStaged("ws", q, "f.txt"); !errors.Is(err, ErrReserved) {
		t.Fatalf("SwapStaged into sealed name = %v, want ErrReserved", err)
	}
	if err := view.MoveStaged("ws", "f.txt", q); !errors.Is(err, ErrReserved) {
		t.Fatalf("MoveStaged into sealed name = %v, want ErrReserved", err)
	}
	if got := durRead(t, dir, "ws/"+q); got != "parked" {
		t.Fatalf("sealed object modified: %q", got)
	}
	if got := durRead(t, dir, "ws/f.txt"); got != "live" {
		t.Fatalf("live name modified: %q", got)
	}
	// Sealed objects remain enumerable and may move OUT — that is the
	// only way a preserved object leaves quarantine.
	if names, err := view.ListStaged("ws", "", opStagePrefix+"9-q-"); err != nil || len(names) != 1 {
		t.Fatalf("sealed name not enumerable: %v %v", names, err)
	}
	if err := view.MoveStaged("ws", q, "restored.txt"); err != nil {
		t.Fatalf("sealed move-out = %v", err)
	}
	if got := durRead(t, dir, "ws/restored.txt"); got != "parked" {
		t.Fatalf("restored = %q, want %q", got, "parked")
	}
}

// Replayed finding 1: a retired reconciler's delayed effects land inside
// another pass's verify→unlink window on a quarantine name. Under the
// seal there is no syscall that writes into that name, so the successor's
// acknowledged save must survive intact.
func TestSealedRetiredEffectsCannotReachPendingUnlink(t *testing.T) {
	pR1, dir := durRoot(t)
	pR2, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	pSucc, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	A := []byte("stale-op-bytes")
	O1 := []byte("row-content")
	N := []byte("successor-acked")
	if err := os.MkdirAll(filepath.Join(dir, "ws"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ws", "f.txt"), A, 0o644); err != nil {
		t.Fatal(err)
	}
	qrel := opStagePrefix + "7-q-aaaaaa"
	if err := os.WriteFile(filepath.Join(dir, "ws", qrel), O1, 0o644); err != nil {
		t.Fatal(err)
	}
	it := intent{id: 7, scope: "ws", op: "write", path: "f.txt",
		expectSHA: sha(string(A)), dstFP: "999:9:9:9"}

	// R1 (retired): the real settleStaged decides the -q object's
	// disposition — with sealing that decision is a drain, and its
	// first move is deferred.
	gv := &gatedView{
		waiting: make(chan struct{}),
		gate:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	v1, err := pR1.pin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer v1.Close()
	gv.ReconView = v1
	var s1 Store
	r1done := make(chan struct{})
	go func() {
		defer close(r1done)
		s1.settleStaged(context.Background(), it, gv, false)
	}()
	select {
	case <-gv.waiting:
	case <-r1done:
		t.Fatal("R1 pass finished without a pending effect — premise failed")
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for R1's decision")
	}

	// R2 (live): ungated pass — drains the sealed object onto the name
	// and parks the stale bytes at the base slot.
	v2, err := pR2.pin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer v2.Close()
	var s2 Store
	s2.settleStaged(context.Background(), it, v2, false)
	if got := durRead(t, dir, "ws/f.txt"); got != string(O1) {
		t.Fatalf("f.txt = %q, want restored row content", got)
	}

	// The successor acknowledges a newer save at f.txt through the real
	// verified write path (displaces O1 with authority).
	fpO1 := durFP(t, pSucc, "ws", "f.txt")
	if _, c, err := pSucc.atomicWrite("ws", "f.txt", N, false, fpO1, ""); err != nil || !c {
		t.Fatalf("successor write: %v", err)
	}

	// R2's second pass deletes the parked stale bytes — inside its
	// verify→unlink window, release R1's delayed effects: they must all
	// land harmlessly (ENOENT/EEXIST), never writing into the pending
	// unlink's name.
	removeStagedPreUnlinkHook = func() {
		close(gv.gate)
		waitOr(t, gv.done, "retired effects landing")
	}
	s2.settleStaged(context.Background(), it, v2, false)
	removeStagedPreUnlinkHook = nil
	<-r1done

	// The retired pass's delayed drain may have parked the acknowledged
	// object at the base slot — displaced, never destroyed. The pending
	// unlink deleted only the verified stale bytes; N must survive
	// either at its name or parked where the next pass restores it.
	where := scanDirFor(t, dir, "ws", N)
	if where == "" {
		t.Fatal("successor's acknowledged bytes were destroyed")
	}
	if durExists(t, dir, "ws/f.txt") {
		if got := durRead(t, dir, "ws/f.txt"); got != string(N) {
			t.Fatalf("f.txt = %q, want the acknowledged save %q", got, N)
		}
	}
	if where := scanDirFor(t, dir, "ws", A); where != "" {
		t.Fatalf("stale op bytes still parked at %q — drain did not complete", where)
	}
}

// Replayed finding 2: the actor's post-undo slot cleanup lands after a
// retired reconciler's delayed swap placed the successor's acknowledged
// bytes at the slot. The capture→seal→verify→unlink discipline must park
// those bytes instead of destroying them, and a reconcile pass must be
// able to restore them to the recorded name.
func TestSealedActorSlotUnlinkPreservesSuccessor(t *testing.T) {
	pOp, dir := durRoot(t)
	pR1, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	pSucc, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := pOp.atomicWrite("ws", "f.txt", []byte("old"), true, "", ""); err != nil {
		t.Fatalf("seed: %v", err)
	}
	fpOld := durFP(t, pOp, "ws", "f.txt")
	succ := []byte("succ-acked")
	if _, _, err := pSucc.atomicWrite("ws", "f.txt", succ, false, fpOld, ""); err != nil {
		t.Fatalf("successor write: %v", err)
	}
	stale := []byte("stale")
	slot := opStagePrefix + "42"
	it := intent{id: 42, scope: "ws", op: "write", path: "f.txt",
		expectSHA: sha(string(stale)), dstFP: fpOld}

	gv := &gatedView{
		waiting: make(chan struct{}),
		gate:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	v1, err := pR1.pin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer v1.Close()
	gv.ReconView = v1

	var r1sync sync.WaitGroup
	r1sync.Add(1)
	var s1 Store
	pOp.faultHook = func(tag string) {
		switch tag {
		case "write.preUndo":
			// Post-exchange, pre-undo: slot=succ-acked (foreign),
			// f.txt=stale (our bytes). A concurrent pass observes this
			// and legitimately decides SwapStaged — run the real one;
			// its exchange stalls.
			go func() {
				defer r1sync.Done()
				s1.settleStaged(context.Background(), it, gv, false)
			}()
			waitOr(t, gv.waiting, "retired pass swap decision")
		case "write.postUndo":
			// Undo done: f.txt=succ-acked, slot=stale. The retired
			// pass's pending exchange lands in the undo→cleanup gap:
			// slot <- succ-acked, f.txt <- stale.
			close(gv.gate)
			waitOr(t, gv.done, "retired swap landing")
		}
	}
	_, committed, err := pOp.atomicWrite("ws", "f.txt", stale, false, fpOld, slot)
	pOp.faultHook = nil
	if err == nil || !errors.Is(err, ErrExternalChange) {
		t.Fatalf("stale write err = %v, want external_change", err)
	}
	if committed {
		t.Fatal("stale write reported committed")
	}
	r1sync.Wait()

	// Two landing orders are both repaired outcomes: the delayed swap
	// either placed the acknowledged bytes at the slot before the
	// sealed capture (→ parked at a -q- name, name holds the late
	// swap's stale residue) or found the slot already moved (→ ENOENT,
	// undo's restore stands). In both, the acknowledged bytes survive.
	where := scanDirFor(t, dir, "ws", succ)
	if where == "" {
		t.Fatal("successor's acknowledged bytes were destroyed — the race still bites")
	}
	if durExists(t, dir, "ws/f.txt") {
		if got := durRead(t, dir, "ws/f.txt"); got != string(stale) && got != string(succ) {
			t.Fatalf("f.txt = %q, want stale residue or restored save", got)
		}
	}

	// A reconcile pass restores the recorded version: the sealed object
	// drains back onto the name and the stale bytes park for discard.
	var s2 Store
	v2, err := pOp.pin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer v2.Close()
	for i := 0; i < 4; i++ {
		s2.settleStaged(context.Background(), it, v2, false)
	}
	if !durExists(t, dir, "ws/f.txt") {
		t.Fatal("reconcile never restored the name")
	}
	if got := durRead(t, dir, "ws/f.txt"); got != string(succ) {
		t.Fatalf("f.txt = %q after reconcile, want restored %q", got, succ)
	}
	if where := scanDirFor(t, dir, "ws", stale); where != "" {
		t.Fatalf("stale bytes still parked at %q — settle did not discard them", where)
	}
}
