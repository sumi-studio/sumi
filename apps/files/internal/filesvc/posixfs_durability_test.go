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
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
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

// durReadGlob reads the single directory entry beneath dir matching a
// filepath.Match pattern — used to find random-suffixed recovery names
// (-p-/-q-) whose bytes must be preserved.
func durReadGlob(t *testing.T, dir, pattern string) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, pattern))
	if err != nil || len(matches) != 1 {
		t.Fatalf("glob %s: %v (%d matches)", pattern, err, len(matches))
	}
	b, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("read %s: %v", matches[0], err)
	}
	return string(b)
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

// durIntent builds the intent-shaped argument the fs ops now take: the
// declared displaced/source fingerprints plus the private-name journal
// entry whose name backs the op's slot.
func durIntent(preFP, dstFP, stage string) intent {
	it := intent{preFP: preFP, dstFP: dstFP}
	if stage != "" {
		it.names = []nameRec{{Name: stage}}
	}
	return it
}

// staleWrite exercises the full sequence: an op declared while path held
// fpOld, whose filesystem effect only lands AFTER a successor wrote
// "new" — the verified effect must undo itself, never destroy "new".
func TestVerifiedWriteStalePreservesSuccessor(t *testing.T) {
	p, dir := durRoot(t)
	// The acknowledged "old" object the stale op declared against.
	if _, _, err := p.atomicWrite("ws", "a.txt", []byte("old"), true,
		durIntent("", "", "")); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	fpOld := durFP(t, p, "ws", "a.txt")
	// Successor acknowledges "new".
	if _, _, err := p.atomicWrite("ws", "a.txt", []byte("new"), false,
		durIntent("", fpOld, "")); err != nil {
		t.Fatalf("successor write: %v", err)
	}
	// The stale effect lands now, carrying the declare-time expectation.
	_, committed, err := p.atomicWrite("ws", "a.txt", []byte("stale"), false,
		durIntent("", fpOld, opStagePrefix+"1"))
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
	if _, _, err := p.atomicWrite("ws", "a.txt", []byte("old"), true,
		durIntent("", "", "")); err != nil {
		t.Fatalf("seed: %v", err)
	}
	fpOld := durFP(t, p, "ws", "a.txt")
	if err := os.Remove(filepath.Join(dir, "ws/a.txt")); err != nil {
		t.Fatalf("successor remove: %v", err)
	}
	_, _, err := p.atomicWrite("ws", "a.txt", []byte("stale"), false,
		durIntent("", fpOld, opStagePrefix+"2"))
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
	if _, _, err := p.atomicWrite("ws", "a.txt", []byte("old"), true,
		durIntent("", "", "")); err != nil {
		t.Fatalf("seed: %v", err)
	}
	fpOld := durFP(t, p, "ws", "a.txt")
	if _, _, err := p.atomicWrite("ws", "a.txt", []byte("new"), false,
		durIntent("", fpOld, "")); err != nil {
		t.Fatalf("successor write: %v", err)
	}
	committed, err := p.remove("ws", "a.txt", durIntent("", fpOld, opStagePrefix+"3"))
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
	if _, _, err := p.atomicWrite("ws", "src.txt", []byte("moved"), true,
		durIntent("", "", "")); err != nil {
		t.Fatalf("seed src: %v", err)
	}
	if _, _, err := p.atomicWrite("ws", "dst.txt", []byte("old-dst"), true,
		durIntent("", "", "")); err != nil {
		t.Fatalf("seed dst: %v", err)
	}
	fpSrc := durFP(t, p, "ws", "src.txt")
	fpDstOld := durFP(t, p, "ws", "dst.txt")
	// Successor acknowledges a newer destination object.
	if _, _, err := p.atomicWrite("ws", "dst.txt", []byte("new-dst"), false,
		durIntent("", fpDstOld, "")); err != nil {
		t.Fatalf("successor write: %v", err)
	}
	_, committed, err := p.rename("ws", "src.txt", "dst.txt", false,
		durIntent(fpSrc, fpDstOld, opStagePrefix+"4"))
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
	if _, _, err := p.atomicWrite("ws", "a.txt", []byte("old"), true,
		durIntent("", "", "")); err != nil {
		t.Fatalf("seed: %v", err)
	}
	fpOld := durFP(t, p, "ws", "a.txt")
	if _, c, err := p.atomicWrite("ws", "a.txt", []byte("new"), false,
		durIntent("", fpOld, "")); err != nil || !c {
		t.Fatalf("verified overwrite = (%v,%v)", c, err)
	}
	if got := durRead(t, dir, "ws/a.txt"); got != "new" {
		t.Fatalf("a.txt = %q", got)
	}
	// write over a now-absent path with expected-absent evidence.
	if _, _, err := p.atomicWrite("ws", "b.txt", []byte("b"), false,
		durIntent("", "", "")); err != nil {
		t.Fatalf("absent-expected write: %v", err)
	}
	// rename to an absent destination (dstFP "" = expected absent).
	fpB := durFP(t, p, "ws", "b.txt")
	if _, c, err := p.rename("ws", "b.txt", "c.txt", false,
		durIntent(fpB, "", "")); err != nil || !c {
		t.Fatalf("rename err: %v", err)
	}
	if got := durRead(t, dir, "ws/c.txt"); got != "b" {
		t.Fatalf("c.txt = %q", got)
	}
}

// Verified rename over an existing, expected destination displaces it.
func TestVerifiedRenameDisplacesExpected(t *testing.T) {
	p, dir := durRoot(t)
	if _, _, err := p.atomicWrite("ws", "s.txt", []byte("S"), true,
		durIntent("", "", "")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.atomicWrite("ws", "d.txt", []byte("D"), true,
		durIntent("", "", "")); err != nil {
		t.Fatal(err)
	}
	fpS := durFP(t, p, "ws", "s.txt")
	fpD := durFP(t, p, "ws", "d.txt")
	if _, c, err := p.rename("ws", "s.txt", "d.txt", false,
		durIntent(fpS, fpD, opStagePrefix+"5")); err != nil || !c {
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
	if _, _, err := p.atomicWrite("ws", "s.txt", []byte("S"), true,
		durIntent("", "", "")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.atomicWrite("ws", "d.txt", []byte("D"), true,
		durIntent("", "", "")); err != nil {
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
	if _, c, err := p.rename("ws", "s.txt", "d.txt", false,
		durIntent(fpS, fpD, opStagePrefix+"6")); err != nil || !c {
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
	err = undoDisplaced(dfd, dfd, staged, "a.txt",
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
	if err := undoDisplaced(dfd, dfd, staged, "a.txt",
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

// A parked recovery object survives service startup — nothing in the
// startup path deletes private or temp-prefixed names.
func TestOpStageSurvivesStartup(t *testing.T) {
	_, dir := durRoot(t)
	if err := os.MkdirAll(filepath.Join(dir, "ws"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ws", opStagePrefix+"9"), []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewAt(dir, nil, nil); err != nil {
		t.Fatal(err)
	}
	if !durExists(t, dir, "ws/"+opStagePrefix+"9") {
		t.Fatal("startup removed a recovery object")
	}
}

// discardOwned is a verify-then-unlink on an owned private name: a
// mismatched identity refuses with ErrConflict and leaves the object in
// place (enumerable, preserved); a match unlinks in place.
func TestDiscardOwnedVerifyThenUnlink(t *testing.T) {
	p, dir := durRoot(t)
	if err := os.MkdirAll(filepath.Join(dir, "ws"), 0o755); err != nil {
		t.Fatal(err)
	}
	slot := opStagePrefix + "10"
	if err := os.WriteFile(filepath.Join(dir, "ws", slot), []byte("victim"), 0o644); err != nil {
		t.Fatal(err)
	}
	dfd, err := os.Open(filepath.Join(dir, "ws"))
	if err != nil {
		t.Fatal(err)
	}
	defer dfd.Close()
	fp := durFP(t, p, "ws", slot)
	// Wrong identity: the object must NOT be deleted — it stays.
	if err := discardOwned(dfd, slot, "1:2:3"); !errors.Is(err, ErrConflict) {
		t.Fatalf("mismatched discardOwned = %v, want ErrConflict", err)
	}
	if got := durRead(t, dir, "ws/"+slot); got != "victim" {
		t.Fatalf("object = %q after refused discard — must be preserved", got)
	}
	// Correct identity: verified in place, unlinked.
	if err := discardOwned(dfd, slot, fp3(fp)); err != nil {
		t.Fatalf("matching discardOwned = %v", err)
	}
	if durExists(t, dir, "ws/"+slot) {
		t.Fatal("object not deleted after matching verify")
	}
}

// The op-stage namespace is reserved and hidden like the tmp prefix.
func TestOpStageReservedAndHidden(t *testing.T) {
	p, dir := durRoot(t)
	if _, _, err := p.atomicWrite("ws", opStagePrefix+"x", []byte("y"), true, durIntent("", "", "")); !errors.Is(err, ErrReserved) {
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
	waiting  chan struct{}
	gate     chan struct{}
	done     chan struct{}
	once     sync.Once
	onlyMove bool // gate MoveStaged only; SwapStaged runs immediately
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

func (g *gatedView) MoveStaged(scope, from, to string) error {
	return g.hold(func() error { return g.ReconView.MoveStaged(scope, from, to) })
}

// scanTreeFor walks dir/scope recursively and returns the
// scope-relative path of the entry holding exactly want bytes, or "".
func scanTreeFor(t *testing.T, dir, scope string, want []byte) string {
	t.Helper()
	var found string
	base := filepath.Join(dir, scope)
	var walk func(d string)
	walk = func(d string) {
		if found != "" {
			return
		}
		ents, err := os.ReadDir(d)
		if err != nil {
			return
		}
		for _, e := range ents {
			p := d + "/" + e.Name()
			if e.IsDir() {
				walk(p)
				continue
			}
			b, err := os.ReadFile(p)
			if err == nil && sha(string(b)) == sha(string(want)) {
				if rel, rerr := filepath.Rel(base, p); rerr == nil {
					found = rel
				}
				return
			}
		}
	}
	walk(base)
	return found
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

// Seal enforcement, directly: a -q- capture name may never be written
// INTO — recovery moves objects out of it or verifies-then-unlinks it
// in place, so a MoveStaged targeting one is refused.
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
	// only way a preserved object leaves a capture name.
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

// Retired-pass delayed effects are covered at PG level in
// late_repair_pg_test.go: under the names journal every occupied
// journaled name is captured into a freshly declared name before
// judgment, so a delayed effect can never be aimed at the name a
// pending disposition acts on.

// Finding 151: public remove must reject reserved staging/recovery
// names — direct, nested, and objects that actually exist on disk —
// while internal recovery keeps its legal move-out/delete paths.
func TestRemoveRejectsReservedObjects(t *testing.T) {
	p, dir := durRoot(t)
	// Real objects at every reserved shape the public op could name.
	for _, rel := range []string{
		"ws/" + stagingPrefix + "abc123",
		"ws/" + opStagePrefix + "7",
		"ws/" + opStagePrefix + "7-q-ab12cd",
		"ws/sub/" + opStagePrefix + "9",
	} {
		if err := os.MkdirAll(filepath.Join(dir, filepath.Dir(rel)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, rel), []byte("recovery-bytes"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{
		stagingPrefix + "abc123",
		opStagePrefix + "7",
		opStagePrefix + "7-q-ab12cd",
		"sub/" + opStagePrefix + "9",       // nested reserved segment
		"sub/" + opStagePrefix + "9/inner", // nested beneath a reserved dir
		stagingPrefix + "nonexistent",      // reserved shape, absent object
	} {
		if _, err := p.remove("ws", path, durIntent("", "", "")); !errors.Is(err, ErrReserved) {
			t.Fatalf("remove(%q) = %v, want ErrReserved", path, err)
		}
	}
	// Nothing was touched.
	for _, rel := range []string{
		"ws/" + stagingPrefix + "abc123",
		"ws/" + opStagePrefix + "7",
		"ws/" + opStagePrefix + "7-q-ab12cd",
		"ws/sub/" + opStagePrefix + "9",
	} {
		if got := durRead(t, dir, rel); got != "recovery-bytes" {
			t.Fatalf("%s = %q — reserved object was mutated", rel, got)
		}
	}
	// Internal recovery keeps its legal paths: moving out of a reserved
	// name and deleting a staged object both still work.
	v, err := p.pin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	if err := v.MoveStaged("ws", opStagePrefix+"7", "free.txt"); err != nil {
		t.Fatalf("recovery move-out of reserved name: %v", err)
	}
	if got := durRead(t, dir, "ws/free.txt"); got != "recovery-bytes" {
		t.Fatalf("moved object = %q", got)
	}
}

// The verify→discard window on a private name: a foreign object landing
// at the slot between the exchange and the discard is never unlinked —
// the re-verify fails and the object stays parked under the journaled
// name for the reconciler (which surfaces it visibly).
func TestDiscardForeignAtSlotPreserves(t *testing.T) {
	p, dir := durRoot(t)
	if _, _, err := p.atomicWrite("ws", "f.txt", []byte("occupied"), true,
		durIntent("", "", "")); err != nil {
		t.Fatal(err)
	}
	fpOld := durFP(t, p, "ws", "f.txt")
	slot := opStagePrefix + "5"
	foreign := []byte("foreign-late-arrival")
	p.faultHook = func(tag string) {
		if tag != "write.preSlotDelete" {
			return
		}
		if err := os.WriteFile(filepath.Join(dir, "ws", slot),
			foreign, 0o644); err != nil {
			panic(err)
		}
	}
	_, committed, err := p.atomicWrite("ws", "f.txt", []byte("new"), false,
		durIntent("", fpOld, slot))
	p.faultHook = nil
	// The publish already committed — the parked foreign object reports
	// as external_change + errUndoParked so the intent tombstones and a
	// reconcile pass settles the name.
	if err == nil || !errors.Is(err, errUndoParked) {
		t.Fatalf("write with foreign slot arrival = %v, want errUndoParked", err)
	}
	if !committed {
		t.Fatal("committed effect must report committed")
	}
	if got := durRead(t, dir, "ws/"+slot); got != string(foreign) {
		t.Fatalf("foreign slot occupant = %q — must be preserved parked", got)
	}
	if got := durRead(t, dir, "ws/f.txt"); got != "new" {
		t.Fatalf("f.txt = %q — committed effect stands", got)
	}
}

// Finding 153: a delayed retired exchange lands foreign bytes at the
// enumerable slot between the O_EXCL create and the pre-commit failure
// cleanup. The cleanup must verify the slot's occupant is OUR object
// (fd-bound inode) before unlinking — foreign bytes stay parked under
// the journaled name, and the op reports errUndoParked so runFs
// tombstones the intent — a definitive-looking early error must not
// orphan live recovery evidence.
func TestDiscardStagedForeignParkKeepsEvidence(t *testing.T) {
	p, dir := durRoot(t)
	slot := opStagePrefix + "77"
	foreign := []byte("foreign-landed-at-slot")
	if _, _, err := p.atomicWrite("ws", "f.txt", []byte("occupied"), true,
		durIntent("", "", "")); err != nil {
		t.Fatal(err)
	}
	// Post-create, pre-publish: a delayed exchange decided by a retired
	// pass lands foreign bytes at the slot; our staged bytes move aside.
	p.faultHook = func(tag string) {
		if tag != "write.postCreate" {
			return
		}
		other := filepath.Join(dir, "ws", opStagePrefix+"77-other")
		if err := os.WriteFile(other, foreign, 0o644); err != nil {
			t.Error(err)
			return
		}
		if err := unix.Renameat2(unix.AT_FDCWD, other,
			unix.AT_FDCWD, filepath.Join(dir, "ws", slot),
			unix.RENAME_EXCHANGE); err != nil {
			t.Error(err)
		}
	}
	// exclusive write onto an occupied name → Linkat EEXIST → the
	// definitive-failure cleanup runs on a slot that now holds foreign
	// bytes.
	_, committed, err := p.atomicWrite("ws", "f.txt", []byte("new"), true,
		durIntent("", "", slot))
	p.faultHook = nil
	if err == nil {
		t.Fatal("exclusive write over occupied name unexpectedly succeeded")
	}
	if !errors.Is(err, errUndoParked) {
		t.Fatalf("err = %v — must carry errUndoParked so the intent tombstones", err)
	}
	if committed {
		t.Fatal("exclusive write reported committed")
	}
	// The foreign bytes survive AT the journaled slot — enumerable,
	// judged by a later pass, never unlinked on a stale expectation.
	if got := durRead(t, dir, "ws/"+slot); got != string(foreign) {
		t.Fatalf("foreign object = %q — must stay parked at the journaled name", got)
	}
	if got := durRead(t, dir, "ws/f.txt"); got != "occupied" {
		t.Fatalf("f.txt = %q — untouched by the refused write", got)
	}
}

// The sealed-drain interleaving is superseded by the names journal: a
// dead intent's names keep enumerable records, the orphan sweep owns
// unjournaled deposits, and every occupied name is captured into a
// freshly declared name before judgment — PG witnesses in
// late_repair_pg_test.go cover the same interleavings.
