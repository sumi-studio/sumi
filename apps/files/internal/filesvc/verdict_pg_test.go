package filesvc

// PG-backed coverage for the settled-verdict receipt (F338): a keyed
// operation whose settle could not confirm its declared effect must
// receipt "diverged" and answer replays with a deterministic
// outcome_uncertain conflict — never a bare success, never a second
// mutation. Confirmed settles keep the "applied" receipt; a diverged
// verdict upgrades to applied when a later judgment confirms the landing.
// Each test uses the real service handler, real posixRoot on a tempdir,
// and a real Store on real Postgres (FILESV_TEST_DSN).

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// declareKeyed registers a pending keyed intent exactly as the service
// does, WITHOUT running its fs mutation — the shape a crash between
// declare and the fs commit leaves behind. The caller simulates owner
// death by dropping the inflight mark before Reconcile runs.
func declareKeyed(t *testing.T, st *Store, scope, op, path string, iv IfVersion, expectSHA, key, reqH string, probe FPProbe) intent {
	t.Helper()
	it, err := st.declare(context.Background(), scope, op, path, "", iv, expectSHA,
		OpIdentity{Key: key, ReqHash: reqH}, probe, probe)
	if err != nil {
		t.Fatalf("declare %s %s: %v", op, path, err)
	}
	st.inflight.Delete(it.id)
	return it
}

func writeFile(t *testing.T, dir, scope, rel, body string) {
	t.Helper()
	full := filepath.Join(dir, scope, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// F338: a keyed write whose intent settles diverged (disk shows foreign
// bytes, our landing unproven) must NOT answer the replay with a bare
// success. The receipt carries verdict=diverged; the replay gets a
// deterministic 409 outcome_uncertain, the foreign bytes are untouched,
// and stat still reports the external change.
func TestPGReceiptDivergedKeyedWriteConflicts(t *testing.T) {
	svc, st, dir := pgReceiptSvc(t)
	ctx := context.Background()
	proot, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	st.SetReconcileView(authPinned(proot, nil))

	rh := reqHash("write", "div.txt", ivCanon(IfVersion{Mode: "none"}), sha("ours"))
	declareKeyed(t, st, "ws", "write", "div.txt", IfVersion{Mode: "none"},
		sha("ours"), "op:div", rh, func() (FileInfo, bool, error) { return FileInfo{}, false, nil })

	// Foreign bytes occupy the path before any judgment.
	writeFile(t, dir, "ws", "div.txt", "foreign")
	st.lastTombScan.Store(0)
	st.Reconcile(ctx)

	// The replay is a deterministic conflict — not a replayed success,
	// not a re-execution.
	w := req(t, svc, "PUT", "/v1/files/ws/write?path=div.txt", "svc", "ours",
		withIfV(keyHdr("op:div"), "none"))
	if w.Code != 409 || !strings.Contains(w.Body.String(), "outcome_uncertain") ||
		!strings.Contains(w.Body.String(), `"replayed":true`) {
		t.Fatalf("diverged replay must be 409 outcome_uncertain+replayed, got %d %s", w.Code, w.Body)
	}
	got, err := os.ReadFile(filepath.Join(dir, "ws", "div.txt"))
	if err != nil || string(got) != "foreign" {
		t.Fatalf("diverged replay touched foreign bytes: %q %v", got, err)
	}
	// Deterministic: the same replay answers identically, forever.
	w = req(t, svc, "PUT", "/v1/files/ws/write?path=div.txt", "svc", "ours",
		withIfV(keyHdr("op:div"), "none"))
	if w.Code != 409 || !strings.Contains(w.Body.String(), "outcome_uncertain") {
		t.Fatalf("second diverged replay must still conflict: %d %s", w.Code, w.Body)
	}
	// Same key, different request is still an idempotency conflict —
	// the diverged receipt is never borrowed.
	w = req(t, svc, "PUT", "/v1/files/ws/write?path=div.txt", "svc", "different",
		withIfV(keyHdr("op:div"), "none"))
	if w.Code != 409 || !strings.Contains(w.Body.String(), "idempotency_conflict") {
		t.Fatalf("key reuse must conflict: %d %s", w.Code, w.Body)
	}
	// Stat surfaces the divergence honestly — no laundering.
	ws := req(t, svc, "GET", "/v1/files/ws/stat?path=div.txt", "svc", "", nil)
	if ws.Code != 200 || !strings.Contains(ws.Body.String(), `"external_change":true`) {
		t.Fatalf("stat must report external_change after a diverged settle: %d %s", ws.Code, ws.Body)
	}
}

// F338: a keyed write whose intent settles while the path is ABSENT —
// landed-then-deleted and never-ran are indistinguishable here, and the
// journal shows no commit act — receipts diverged. The replay is a
// conflict, never a silent recreation of a file a later op may have
// deleted.
func TestPGReceiptDivergedAbsentWriteConflicts(t *testing.T) {
	svc, st, dir := pgReceiptSvc(t)
	ctx := context.Background()
	proot, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	st.SetReconcileView(authPinned(proot, nil))

	rh := reqHash("write", "gone.txt", ivCanon(IfVersion{Mode: "none"}), sha("data"))
	declareKeyed(t, st, "ws", "write", "gone.txt", IfVersion{Mode: "none"},
		sha("data"), "op:gone", rh, func() (FileInfo, bool, error) { return FileInfo{}, false, nil })

	st.lastTombScan.Store(0)
	st.Reconcile(ctx)

	w := req(t, svc, "PUT", "/v1/files/ws/write?path=gone.txt", "svc", "data",
		withIfV(keyHdr("op:gone"), "none"))
	if w.Code != 409 || !strings.Contains(w.Body.String(), "outcome_uncertain") {
		t.Fatalf("absent-settle replay must conflict: %d %s", w.Code, w.Body)
	}
	if _, err := os.Stat(filepath.Join(dir, "ws", "gone.txt")); !os.IsNotExist(err) {
		t.Fatal("diverged replay recreated the file")
	}
}

// The diverged verdict is not a dead end: if the intent's declared bytes
// DO land late (a delayed filesystem effect), the tombstone re-judgment
// confirms them, refreshes the diverged fingerprint to the live one, and
// upgrades the receipt to applied — the replay then answers the recorded
// success. The reverse direction (applied -> diverged) can never happen.
func TestPGReceiptDivergedUpgradesOnConfirmedLanding(t *testing.T) {
	svc, st, dir := pgReceiptSvc(t)
	ctx := context.Background()
	proot, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	st.SetReconcileView(authPinned(proot, nil))

	rh := reqHash("write", "late.txt", ivCanon(IfVersion{Mode: "none"}), sha("ours"))
	it := declareKeyed(t, st, "ws", "write", "late.txt", IfVersion{Mode: "none"},
		sha("ours"), "op:late", rh, func() (FileInfo, bool, error) { return FileInfo{}, false, nil })

	writeFile(t, dir, "ws", "late.txt", "foreign")
	st.lastTombScan.Store(0)
	st.Reconcile(ctx)

	// Diverged receipt — replay conflicts while the outcome is unproven.
	w := req(t, svc, "PUT", "/v1/files/ws/write?path=late.txt", "svc", "ours",
		withIfV(keyHdr("op:late"), "none"))
	if w.Code != 409 || !strings.Contains(w.Body.String(), "outcome_uncertain") {
		t.Fatalf("pre-landing replay must conflict: %d %s", w.Code, w.Body)
	}

	// The delayed effect lands: the op's declared bytes arrive at the
	// path (a late rename completing out-of-band).
	writeFile(t, dir, "ws", "late.txt", "ours")
	st.lastTombScan.Store(0)
	st.Reconcile(ctx)

	// The receipt now answers applied — the landing was confirmed.
	w = req(t, svc, "PUT", "/v1/files/ws/write?path=late.txt", "svc", "ours",
		withIfV(keyHdr("op:late"), "none"))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"replayed":true`) {
		t.Fatalf("confirmed-landing replay must receipt: %d %s", w.Code, w.Body)
	}
	if bodyVersion(t, w) != it.version {
		t.Fatalf("receipt must carry the op's minted version: %s want %d", w.Body, it.version)
	}
	// The version row's diverged marker was refreshed: stat is clean.
	ws := req(t, svc, "GET", "/v1/files/ws/stat?path=late.txt", "svc", "", nil)
	if ws.Code != 200 || strings.Contains(ws.Body.String(), `"external_change":true`) {
		t.Fatalf("stat must clear external_change after a confirmed landing: %d %s", ws.Code, ws.Body)
	}
	// The disk holds the op's bytes, not a second mutation's.
	got, _ := os.ReadFile(filepath.Join(dir, "ws", "late.txt"))
	if string(got) != "ours" {
		t.Fatalf("unexpected content: %q", got)
	}
}

// A keyed remove settled diverged (the object still occupies the path and
// the journal shows no committed capture) must conflict on replay and
// leave the object untouched — never re-execute a remove against content
// the original op may never have deleted.
func TestPGReceiptDivergedRemovePreservesPresent(t *testing.T) {
	svc, st, dir := pgReceiptSvc(t)
	ctx := context.Background()
	proot, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	st.SetReconcileView(authPinned(proot, nil))

	// Seed the file with an unrelated unkeyed write.
	if w := req(t, svc, "PUT", "/v1/files/ws/write?path=victim.txt", "svc", "keep",
		map[string]string{"If-Version": "none"}); w.Code != 200 {
		t.Fatalf("seed write: %d %s", w.Code, w.Body)
	}
	rh := reqHash("remove", "victim.txt", ivCanon(IfVersion{Mode: "any"}))
	declareKeyed(t, st, "ws", "remove", "victim.txt", IfVersion{Mode: "any"},
		"", "op:rm-div", rh, svc.probe("ws", "victim.txt"))

	st.lastTombScan.Store(0)
	st.Reconcile(ctx)

	w := req(t, svc, "DELETE", "/v1/files/ws/remove?path=victim.txt", "svc", "",
		withIfV(keyHdr("op:rm-div"), "any"))
	if w.Code != 409 || !strings.Contains(w.Body.String(), "outcome_uncertain") {
		t.Fatalf("diverged remove replay must conflict: %d %s", w.Code, w.Body)
	}
	got, err := os.ReadFile(filepath.Join(dir, "ws", "victim.txt"))
	if err != nil || string(got) != "keep" {
		t.Fatalf("diverged remove replay deleted content: %q %v", got, err)
	}
}

// A keyed remove whose capture the journal PROVES landed — and whose
// declared object is provably not the current occupant — settles
// "applied": the op did remove what it declared; whatever sits at the
// path now arrived afterward. The replay receipts removed:true and must
// not touch the occupant.
func TestPGReceiptConfirmedRemoveAppliedAtReconcile(t *testing.T) {
	svc, st, dir := pgReceiptSvc(t)
	ctx := context.Background()
	proot, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	st.SetReconcileView(authPinned(proot, nil))

	if w := req(t, svc, "PUT", "/v1/files/ws/write?path=orig.txt", "svc", "original",
		map[string]string{"If-Version": "none"}); w.Code != 200 {
		t.Fatalf("seed write: %d %s", w.Code, w.Body)
	}
	// The applied verdict needs a bound durable identity to prove the
	// present occupant is not the declared object — unbound filesystems
	// (overlayfs, unverified mounts) can only receipt diverged.
	if info, ok, _ := svc.probe("ws", "orig.txt")(); !ok || info.oidCls != idBound {
		t.Skip("fixture filesystem cannot bind durable object identity — see TestJuiceFSConfirmedRemoveReceiptsApplied for the verified-mount path")
	}
	rh := reqHash("remove", "orig.txt", ivCanon(IfVersion{Mode: "any"}))
	it := declareKeyed(t, st, "ws", "remove", "orig.txt", IfVersion{Mode: "any"},
		"", "op:rm-ok", rh, svc.probe("ws", "orig.txt"))

	// Simulate the committed capture: journal cap res=ok exactly as the
	// fs layer does, and move the declared object into the intent's
	// private slot — the state a crash between capture and settle leaves.
	it.journal.act(0, "cap", "orig.txt")
	it.journal.res(0, "ok")
	stage := filepath.Join(dir, "ws", it.names[0].Name)
	if err := os.Rename(filepath.Join(dir, "ws", "orig.txt"), stage); err != nil {
		t.Fatalf("simulate capture: %v", err)
	}
	// A later independent occupant claims the path.
	writeFile(t, dir, "ws", "orig.txt", "replacement")

	st.lastTombScan.Store(0)
	st.Reconcile(ctx)

	w := req(t, svc, "DELETE", "/v1/files/ws/remove?path=orig.txt", "svc", "",
		withIfV(keyHdr("op:rm-ok"), "any"))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"replayed":true`) {
		t.Fatalf("confirmed remove must receipt applied: %d %s", w.Code, w.Body)
	}
	got, err := os.ReadFile(filepath.Join(dir, "ws", "orig.txt"))
	if err != nil || string(got) != "replacement" {
		t.Fatalf("applied-remove replay touched the replacement: %q %v", got, err)
	}
}

// A keyed remove answered "already absent" (ENOENT before any capture)
// still commits a receipt: the op's postcondition held at settle. A
// lost-ledger replay must be answered by that receipt — never re-executed
// against a file created after the absence was observed.
func TestPGReceiptAlreadyAbsentRemoveReceipts(t *testing.T) {
	svc, _, dir := pgReceiptSvc(t)

	w := req(t, svc, "DELETE", "/v1/files/ws/remove?path=nothere.txt", "svc", "",
		withIfV(keyHdr("op:rm-abs"), "any"))
	if w.Code != 404 {
		t.Fatalf("absent remove must 404: %d %s", w.Code, w.Body)
	}
	// A file materializes at the path after the op was answered.
	writeFile(t, dir, "ws", "nothere.txt", "arrived-later")

	w = req(t, svc, "DELETE", "/v1/files/ws/remove?path=nothere.txt", "svc", "",
		withIfV(keyHdr("op:rm-abs"), "any"))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"replayed":true`) {
		t.Fatalf("replayed already-absent remove must receipt: %d %s", w.Code, w.Body)
	}
	got, err := os.ReadFile(filepath.Join(dir, "ws", "nothere.txt"))
	if err != nil || string(got) != "arrived-later" {
		t.Fatalf("replayed remove deleted a later-created file: %q %v", got, err)
	}
}

// A keyed write whose fs commit the journal PROVES landed — but whose
// settle ran through the reconciler against a now-absent path — receipts
// "applied": the write did commit; the path was deleted afterward
// (ordinary post-success divergence, not an unconfirmed outcome).
func TestPGReceiptConfirmedWriteAbsentIsApplied(t *testing.T) {
	svc, st, dir := pgReceiptSvc(t)
	ctx := context.Background()
	proot, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	st.SetReconcileView(authPinned(proot, nil))

	rh := reqHash("write", "v.txt", ivCanon(IfVersion{Mode: "none"}), sha("landed"))
	it := declareKeyed(t, st, "ws", "write", "v.txt", IfVersion{Mode: "none"},
		sha("landed"), "op:v", rh, func() (FileInfo, bool, error) { return FileInfo{}, false, nil })

	// Journal the landed publish exactly as atomicWrite does.
	it.journal.act(0, "pub", "v.txt")
	it.journal.res(0, "ok")

	st.lastTombScan.Store(0)
	st.Reconcile(ctx)

	w := req(t, svc, "PUT", "/v1/files/ws/write?path=v.txt", "svc", "landed",
		withIfV(keyHdr("op:v"), "none"))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"replayed":true`) ||
		bodyVersion(t, w) != it.version {
		t.Fatalf("confirmed write must receipt applied: %d %s", w.Code, w.Body)
	}
	if _, err := os.Stat(filepath.Join(dir, "ws", "v.txt")); !os.IsNotExist(err) {
		t.Fatal("applied replay recreated the file")
	}
}

// The diverged verdict is durable: a new Store on the same DB+root still
// answers the replay with outcome_uncertain — the receipt, not process
// memory, carries the settled outcome.
func TestPGReceiptDivergedSurvivesRestart(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()

	st := newPGStore(t, dsn, dir)
	svc, err := NewAt(dir, st, map[string]map[string]bool{"svc": {"*": true}})
	if err != nil {
		t.Fatal(err)
	}
	_ = svc // the first service only seeds the diverged settle
	proot, err := newRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	st.SetReconcileView(authPinned(proot, nil))

	ctx := context.Background()
	rh := reqHash("write", "durable-div.txt", ivCanon(IfVersion{Mode: "none"}), sha("ours"))
	declareKeyed(t, st, "ws", "write", "durable-div.txt", IfVersion{Mode: "none"},
		sha("ours"), "op:dd", rh, func() (FileInfo, bool, error) { return FileInfo{}, false, nil })
	writeFile(t, dir, "ws", "durable-div.txt", "foreign")
	st.lastTombScan.Store(0)
	st.Reconcile(ctx)
	st.Close()

	st2 := newPGStore(t, dsn, dir)
	svc2, err := NewAt(dir, st2, map[string]map[string]bool{"svc": {"*": true}})
	if err != nil {
		t.Fatal(err)
	}
	w := req(t, svc2, "PUT", "/v1/files/ws/write?path=durable-div.txt", "svc", "ours",
		withIfV(keyHdr("op:dd"), "none"))
	if w.Code != 409 || !strings.Contains(w.Body.String(), "outcome_uncertain") {
		t.Fatalf("post-restart diverged replay must conflict: %d %s", w.Code, w.Body)
	}
	got, _ := os.ReadFile(filepath.Join(dir, "ws", "durable-div.txt"))
	if string(got) != "foreign" {
		t.Fatalf("post-restart replay touched foreign bytes: %q", got)
	}
}
