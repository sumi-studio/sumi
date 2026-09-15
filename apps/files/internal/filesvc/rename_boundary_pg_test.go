package filesvc

// Real-SIGKILL crash-boundary witness for the names-journal protocol.
//
// A re-exec'ed child of this test binary runs the API rename through the
// real posixfs exchange and is genuinely SIGKILLed inside a fault hook at
// a chosen commit boundary:
//
//   - rename.postVerify: source captured to the intent's journaled slot
//     (cap act res=ok), exchange not yet issued — the displaced
//     destination object is untouched at its public name.
//   - rename.postXch: RENAME_EXCHANGE already issued; the result journal
//     (res) may or may not have committed — the exchange's outcome is
//     UNKNOWN to any survivor: S may sit at new.txt with D displaced to
//     the private slot, or nothing may have moved.
//   - rename.preSlotDelete: exchange verified committed, the displaced
//     object proven at the private slot, its discard not yet attempted —
//     the apply (row + event) never ran.
//   - write.postXch: a write's exchange committed while its result
//     journal had not — the path may hold new bytes and the slot may
//     hold the pre-image, outcome unknown to survivors.
//
// A new owner (fresh Store, successor binding) then runs ordinary
// recovery and ordinary API ops. Assertions are user-visible facts:
// bytes, version rows, events — not mere survival.
//
// Real vs injected: the process stop is REAL SIGKILL. The only injected
// schedule is ageing the dead owner's intent row past deadGrace via SQL
// (standing in for elapsed time) plus the external moves — ordinary
// user-side filesystem operations.

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestBoundaryChild is the fixture process: bind a real Store (real PG
// intent rows), write the acknowledged objects, run the real op, SIGKILL
// at the hook named by BOUNDARY_KILL. Must never return from the op.
func TestBoundaryChild(t *testing.T) {
	if os.Getenv("BOUNDARY_CHILD") != "1" {
		t.Skip("fixture child process — run by the parent test")
	}
	dsn := os.Getenv("FILESV_TEST_DSN")
	dir := os.Getenv("BOUNDARY_DIR")
	kill := os.Getenv("BOUNDARY_KILL")
	ctx := context.Background()
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewStore(ctx, dsn, dir)
	if err != nil {
		t.Fatalf("child NewStore: %v", err)
	}
	if err := os.MkdirAll(dir+"/ws", 0o755); err != nil {
		t.Fatal(err)
	}
	for _, w := range [][2]string{{"old.txt", "S"}, {"new.txt", "D"}} {
		if _, _, err := s.WithWrite(ctx, "ws", w[0], "write",
			IfVersion{Mode: "any"}, sha(w[1]), authProbe(root, w[0]),
			authWriteFn(root, w[0], w[1])); err != nil {
			t.Fatalf("child write %s: %v", w[0], err)
		}
	}
	root.faultHook = func(tag string) {
		if tag == kill {
			syscall.Kill(syscall.Getpid(), syscall.SIGKILL)
		}
	}
	switch kill {
	case "rename.postVerify", "rename.postXch", "rename.preSlotDelete":
		_, _, rerr := s.Rename(ctx, "ws", "old.txt", "new.txt",
			IfVersion{Mode: "any"}, authProbe(root, "new.txt"), authProbe(root, "old.txt"),
			func(it intent) (FileInfo, bool, error) {
				return root.rename("ws", "old.txt", "new.txt", false, it)
			})
		t.Fatalf("rename returned %v — %s hook never ran", rerr, kill)
	case "write.postXch":
		_, _, werr := s.WithWrite(ctx, "ws", "old.txt", "write",
			IfVersion{Mode: "any"}, sha("W2"), authProbe(root, "old.txt"),
			func(it intent) (FileInfo, bool, error) {
				return root.atomicWrite("ws", "old.txt", []byte("W2"), false, it)
			})
		t.Fatalf("write returned %v — %s hook never ran", werr, kill)
	default:
		t.Fatalf("unknown kill point %q", kill)
	}
}

// boundaryKill runs the fixture child and requires a genuine SIGKILL.
func boundaryKill(t *testing.T, dsn, dir, kill string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestBoundaryChild$", "-test.v")
	cmd.Env = append(os.Environ(),
		"BOUNDARY_CHILD=1",
		"BOUNDARY_DIR="+dir,
		"BOUNDARY_KILL="+kill,
		"FILESV_TEST_DSN="+dsn)
	out, err := cmd.CombinedOutput()
	t.Logf("child output:\n%s", out)
	if err == nil {
		t.Fatalf("child returned without being killed — %s hook never ran", kill)
	}
	ee, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("child spawn error: %v", err)
	}
	ws, ok := ee.Sys().(syscall.WaitStatus)
	if !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		t.Fatalf("child exit = %v — want a genuine SIGKILL at %s", err, kill)
	}
}

// boundaryIntent returns the dead child's surviving file_op row. A fully
// settled intent may already be deleted — that also counts as resolved.
func boundaryIntent(t *testing.T, s *Store, op, path string) (id int64, owner string, resolved bool) {
	t.Helper()
	err := s.pool.QueryRow(context.Background(),
		`SELECT id, owner, resolved_at IS NOT NULL FROM file_op
		  WHERE scope='ws' AND op=$1 AND path=$2 ORDER BY id DESC LIMIT 1`,
		op, path).Scan(&id, &owner, &resolved)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, "", true // settled and drained
	}
	if err != nil {
		t.Fatalf("dead child's intent row: %v", err)
	}
	return id, owner, resolved
}

// boundaryRecover binds a NEW owner store and runs ordinary passes after
// ageing the dead child's intent past the dead-owner grace (injected
// schedule standing in for elapsed time — the only non-real element).
func boundaryRecover(t *testing.T, dsn, dir string, root *posixRoot) *Store {
	t.Helper()
	s := newPGStore(t, dsn, dir)
	s.SetReconcileView(authPinned(root, nil))
	authExec(t, s, `UPDATE file_op SET at = now() - interval '10 minutes'`)
	authSettle(t, s)
	return s
}

// boundaryOrdinaryOps proves the store serves ordinary API traffic after
// recovery: a fresh write commits, reads back, and deletes through the
// public surface.
func boundaryOrdinaryOps(t *testing.T, s *Store, root *posixRoot, dir string) {
	t.Helper()
	ctx := context.Background()
	if _, _, err := s.WithWrite(ctx, "ws", "post.txt", "write",
		IfVersion{Mode: "any"}, sha("POST"), authProbe(root, "post.txt"),
		authWriteFn(root, "post.txt", "POST")); err != nil {
		t.Fatalf("post-recovery write: %v", err)
	}
	if got, ok := authReadOpt(dir, "ws/post.txt"); !ok || got != "POST" {
		t.Fatalf("post-recovery read: %q ok=%v", got, ok)
	}
	if err := s.Remove(ctx, "ws", "post.txt",
		IfVersion{Mode: "any"}, authProbe(root, "post.txt"),
		func(it intent) (bool, error) { return root.remove("ws", "post.txt", it) }); err != nil {
		t.Fatalf("post-recovery remove: %v", err)
	}
	if fileExists(dir + "/ws/post.txt") {
		t.Fatal("post-recovery remove left post.txt")
	}
}

// postVerify: killed after the journaled capture, before the exchange.
// The rename never ran — S's capture act committed (res=ok), so the slot
// provably holds the op's own captured source: uncommitted → rolled back
// to its recorded home. D never moved. Then ordinary API use proceeds.
func TestBoundaryKillRenamePostVerify(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	boundaryKill(t, dsn, dir, "rename.postVerify")

	// Pre-recovery: no exchange — S is captured at the dead intent's
	// journaled slot; both public names hold their recorded objects'
	// siblings (old.txt vacant, new.txt = D).
	if got, ok := authReadOpt(dir, "ws/new.txt"); !ok || got != "D" {
		t.Fatalf("pre-recovery new.txt=%q ok=%v — nothing should have moved", got, ok)
	}
	if fileExists(dir + "/ws/old.txt") {
		t.Fatal("pre-recovery old.txt occupied — capture did not run?")
	}

	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	s := boundaryRecover(t, dsn, dir, root)
	id, owner, resolved := boundaryIntent(t, s, "rename", "old.txt")
	if owner == s.owner {
		t.Fatal("intent still owned by the dead child")
	}
	if !resolved {
		t.Fatal("dead owner's intent never settled")
	}

	// The captured source returns to its recorded home — the journaled
	// cap act is operation-bound provenance and row(old.txt) still
	// claims S, so the restore makes the recorded claim truthful.
	if got, ok := authReadOpt(dir, "ws/old.txt"); !ok || got != "S" {
		where := scanDirFor(t, dir, "ws", []byte("S"))
		t.Fatalf("post-recovery old.txt=%q ok=%v — S at %q", got, ok, where)
	}
	if got, ok := authReadOpt(dir, "ws/new.txt"); !ok || got != "D" {
		t.Fatalf("post-recovery new.txt=%q ok=%v — D moved", got, ok)
	}
	// Rows are truthful: old.txt records S again; no private names left.
	if _, fp, found := authRow(t, s, "old.txt"); !found {
		t.Fatal("no row at restored old.txt")
	} else {
		live := durFP(t, root, "ws", "old.txt")
		if fp3(fp) != fp3(live) {
			t.Fatalf("row at old.txt fp=%q does not describe live %q", fp, live)
		}
	}
	if e := scanDirForPrivate(dir, "ws"); e != "" {
		t.Fatalf("private residue left behind: %q", e)
	}
	_ = id
	boundaryOrdinaryOps(t, s, root, dir)
}

// postXch: killed between the exchange syscall and its result journal —
// the exchange may have committed (S at new.txt, D at the private slot,
// res="") or not run at all. Verify by observation, never assumption:
// wherever the evidence landed, both objects must be preserved at public
// names and the outcome must be the one the filesystem shows.
func TestBoundaryKillRenamePostXch(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	boundaryKill(t, dsn, dir, "rename.postXch")

	xCommitted := false
	if got, ok := authReadOpt(dir, "ws/new.txt"); ok && got == "S" {
		xCommitted = true // exchange committed: S at dst, D at the slot
	} else if got, _ := authReadOpt(dir, "ws/old.txt"); got == "S" {
		xCommitted = false // exchange did not run: S still home
	} else {
		t.Fatalf("post-kill shape unrecognizable: new=%q old=%q",
			mustRead(dir+"/ws/new.txt"), mustRead(dir+"/ws/old.txt"))
	}
	t.Logf("exchange committed = %v", xCommitted)

	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	s := boundaryRecover(t, dsn, dir, root)
	_, _, resolved := boundaryIntent(t, s, "rename", "old.txt")
	if !resolved {
		t.Fatal("dead owner's intent never settled")
	}

	// Both acknowledged objects survive at public names — never deleted
	// on an unrecorded exchange result.
	for _, want := range [][2]string{{"S", "S"}, {"D", "D"}} {
		where := scanTreeFor(t, dir, "ws", []byte(want[1]))
		if where == "" || containsPrivateSeg(where) {
			t.Fatalf("%s lost or left private: %q", want[0], where)
		}
	}
	if xCommitted {
		// The committed effect stands; the displaced D cannot be
		// restored over S (occupant wins) — it surfaces visibly.
		if got, ok := authReadOpt(dir, "ws/new.txt"); !ok || got != "S" {
			t.Fatalf("committed exchange reverted: new.txt=%q ok=%v", got, ok)
		}
	} else {
		// No exchange: S returns home, D untouched.
		if got, ok := authReadOpt(dir, "ws/old.txt"); !ok || got != "S" {
			t.Fatalf("S not restored: old.txt=%q ok=%v", got, ok)
		}
		if got, ok := authReadOpt(dir, "ws/new.txt"); !ok || got != "D" {
			t.Fatalf("D disturbed: new.txt=%q ok=%v", got, ok)
		}
	}
	if e := scanDirForPrivate(dir, "ws"); e != "" {
		t.Fatalf("private residue left behind: %q", e)
	}
	boundaryOrdinaryOps(t, s, root, dir)
}

// preSlotDelete: exchange committed and verified, the displaced D proven
// at the slot — killed before the in-place discard AND before apply
// (no event). Recovery must not discard D: without the journaled apply
// event the committed product is not recorded at new.txt, so D surfaces
// visibly while the committed exchange stands.
func TestBoundaryKillRenamePreSlotDelete(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	boundaryKill(t, dsn, dir, "rename.preSlotDelete")

	if got, ok := authReadOpt(dir, "ws/new.txt"); !ok || got != "S" {
		t.Fatalf("pre-recovery new.txt=%q ok=%v — exchange did not commit", got, ok)
	}
	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	s := boundaryRecover(t, dsn, dir, root)
	_, _, resolved := boundaryIntent(t, s, "rename", "old.txt")
	if !resolved {
		t.Fatal("dead owner's intent never settled")
	}
	if got, ok := authReadOpt(dir, "ws/new.txt"); !ok || got != "S" {
		t.Fatalf("committed exchange reverted: new.txt=%q ok=%v", got, ok)
	}
	// D preserved at a visible name — never discarded on journaled acts
	// alone (the apply event is the commit proof).
	dAt := scanTreeFor(t, dir, "ws", []byte("D"))
	if dAt == "" || containsPrivateSeg(dAt) {
		t.Fatalf("displaced D destroyed or left private: %q", dAt)
	}
	if _, _, found := authRow(t, s, dAt); !found && dAt != "new.txt" {
		t.Fatalf("surfaced D at %q has no version row", dAt)
	}
	if e := scanDirForPrivate(dir, "ws"); e != "" {
		t.Fatalf("private residue left behind: %q", e)
	}
	boundaryOrdinaryOps(t, s, root, dir)
}

// write.postXch: killed between a write's exchange and its result
// journal — the path may hold W2 with S displaced to the slot, outcome
// unknown to survivors. Both objects must survive; no unjournaled
// outcome may authorize discarding the pre-image.
func TestBoundaryKillWritePostXch(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	boundaryKill(t, dsn, dir, "write.postXch")

	root, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	s := boundaryRecover(t, dsn, dir, root)
	_, _, resolved := boundaryIntent(t, s, "write", "old.txt")
	if !resolved {
		t.Fatal("dead owner's write intent never settled")
	}
	// The pre-image S is never destroyed on an unrecorded exchange
	// result — whatever landed at old.txt, S holds a public name.
	if where := scanTreeFor(t, dir, "ws", []byte("S")); where == "" || containsPrivateSeg(where) {
		t.Fatalf("pre-image S destroyed or left private: %q", where)
	}
	// The write's own product, if it landed, stands; recovery leaves a
	// coherent row for whatever publicly holds old.txt.
	if live, lerr := root.lstat("ws", "old.txt"); lerr == nil {
		if _, fp, found := authRow(t, s, "old.txt"); !found {
			t.Fatal("no row at old.txt after recovery")
		} else if fp3(fp) != fp3(live.Fingerprint) {
			t.Fatalf("old.txt row fp=%q diverged from live %q — untruthful",
				fp, live.Fingerprint)
		}
	}
	if e := scanDirForPrivate(dir, "ws"); e != "" {
		t.Fatalf("private residue left behind: %q", e)
	}
	boundaryOrdinaryOps(t, s, root, dir)
}

func mustRead(p string) string {
	b, _ := os.ReadFile(p)
	return string(b)
}

// containsPrivateSeg reports whether a scope-relative path has a private
// .filesv-op- segment.
func containsPrivateSeg(rel string) bool {
	for _, seg := range strings.Split(rel, "/") {
		if len(seg) >= len(opStagePrefix) && seg[:len(opStagePrefix)] == opStagePrefix {
			return true
		}
	}
	return false
}

// scanDirForPrivate returns the first private name found beneath scope.
func scanDirForPrivate(dir, scope string) string {
	var found string
	var walk func(d string)
	walk = func(d string) {
		ents, err := os.ReadDir(d)
		if err != nil {
			return
		}
		for _, e := range ents {
			if found != "" {
				return
			}
			if len(e.Name()) >= len(opStagePrefix) &&
				e.Name()[:len(opStagePrefix)] == opStagePrefix {
				found = d + "/" + e.Name()
				return
			}
			if e.IsDir() {
				walk(d + "/" + e.Name())
			}
		}
	}
	walk(dir + "/" + scope)
	return found
}
