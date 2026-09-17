package filesvc

// Mount-level settlement probes gated on FILESV_TEST_JFSROOT pointing at
// a real JuiceFS mount root (an isolated test volume — never the
// canonical one). These cover behavior POSIX tempdirs cannot reach:
// durable object identity bound through the verified mount, and the
// orphan sweep over real FUSE listing semantics.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func jfsRoot(t *testing.T) string {
	t.Helper()
	r := os.Getenv("FILESV_TEST_JFSROOT")
	if r == "" {
		t.Skip("FILESV_TEST_JFSROOT not set — JuiceFS mount tests skipped")
	}
	return r
}

// A verified JuiceFS mount must yield a bound durable object identity
// (j:<volume-uuid>:<ino>) so settlement can prove old-object identity:
// the volume UUID comes from /.config's Format block (1.4.x schema).
func TestJuiceFSMountBindsObjectIdentity(t *testing.T) {
	root := jfsRoot(t)
	dsn := pgDSN(t)
	resetTables(t, dsn)
	st := newPGStore(t, dsn, root)
	svc, err := NewAt(root, st, map[string]map[string]bool{"svc": {"*": true}})
	if err != nil {
		t.Fatal(err)
	}
	svc.RequireMount()
	proot, err := newRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	proot.requireMount = true
	st.SetReconcileView(func(context.Context) (ReconView, error) {
		return proot.pin(true)
	})
	if err := os.MkdirAll(filepath.Join(root, "ws"), 0o755); err != nil {
		t.Fatal(err)
	}
	// The seed write forces the mount check on the service root.
	if w := req(t, svc, "PUT", "/v1/files/ws/write?path=probe.txt", "svc", "x",
		map[string]string{"If-Version": "none"}); w.Code != 200 {
		t.Fatalf("seed write on juicefs: %d %s", w.Code, w.Body)
	}
	mi := svc.root.mountIdent()
	t.Logf("mountIdent fstype=%s mntID=%d jfsUUID=%q", mi.fstype, mi.mntID, mi.jfsUUID)
	if mi.fstype != "fuse.juicefs" {
		t.Fatalf("fstype = %q, want fuse.juicefs", mi.fstype)
	}
	if mi.jfsUUID == "" {
		t.Fatal("jfsUUID empty on a verified mount — objectID unbound on JuiceFS")
	}
	info, ok, err := svc.probe("ws", "probe.txt")()
	if err != nil || !ok {
		t.Fatalf("probe probe.txt: ok=%v err=%v", ok, err)
	}
	if info.oidCls != idBound || !strings.HasPrefix(info.Oid, "j:"+mi.jfsUUID+":") {
		t.Fatalf("probe.txt Oid = %q cls=%v — want bound j:%s:<ino>", info.Oid, info.oidCls, mi.jfsUUID)
	}
}

// The verdict_pg_test scenario on a real verified JuiceFS mount: a keyed
// remove whose capture the journal proves, with a foreign replacement at
// the path. With bound identity the settle receipts "applied" — the
// replay reports the recorded effect and never touches the replacement.
func TestJuiceFSConfirmedRemoveReceiptsApplied(t *testing.T) {
	root := jfsRoot(t)
	dsn := pgDSN(t)
	resetTables(t, dsn)
	st := newPGStore(t, dsn, root)
	svc, err := NewAt(root, st, map[string]map[string]bool{"svc": {"*": true}})
	if err != nil {
		t.Fatal(err)
	}
	svc.RequireMount()
	proot, err := newRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	proot.requireMount = true
	st.SetReconcileView(func(context.Context) (ReconView, error) {
		return proot.pin(true)
	})
	ctx := context.Background()

	if w := req(t, svc, "PUT", "/v1/files/ws/write?path=orig.txt", "svc", "original",
		map[string]string{"If-Version": "none"}); w.Code != 200 {
		t.Fatalf("seed write on juicefs: %d %s", w.Code, w.Body)
	}
	rh := reqHash("remove", "orig.txt", ivCanon(IfVersion{Mode: "any"}))
	it := declareKeyed(t, st, "ws", "remove", "orig.txt", IfVersion{Mode: "any"},
		"", "op:jfs-rm", rh, svc.probe("ws", "orig.txt"))
	if it.preOid == "" {
		t.Fatal("declare recorded no bound preOid on verified JuiceFS — identity still unbound")
	}
	it.journal.act(0, "cap", "orig.txt")
	it.journal.res(0, "ok")
	stage := filepath.Join(root, "ws", it.names[0].Name)
	if err := os.Rename(filepath.Join(root, "ws", "orig.txt"), stage); err != nil {
		t.Fatalf("simulate capture on juicefs: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "ws", "orig.txt"), []byte("replacement"), 0o644); err != nil {
		t.Fatal(err)
	}

	st.lastTombScan.Store(0)
	st.Reconcile(ctx)

	w := req(t, svc, "DELETE", "/v1/files/ws/remove?path=orig.txt", "svc", "",
		withIfV(keyHdr("op:jfs-rm"), "any"))
	t.Logf("replayed remove on juicefs: %d %s", w.Code, w.Body)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"replayed":true`) {
		t.Fatalf("confirmed remove did not receipt applied on JuiceFS: %d %s", w.Code, w.Body)
	}
	got, _ := os.ReadFile(filepath.Join(root, "ws", "orig.txt"))
	if string(got) != "replacement" {
		t.Fatalf("applied-remove replay touched the replacement: %q", got)
	}
}

// The orphan sweep over a real JuiceFS mount: a private name visible in
// one listing but gone by the next must never mint a recover intent, and
// a persistent true orphan defers exactly one sweep then surfaces.
func TestJuiceFSOrphanSweep(t *testing.T) {
	root := jfsRoot(t)
	dsn := pgDSN(t)
	resetTables(t, dsn)
	st := newPGStore(t, dsn, root)
	proot, err := newRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	proot.requireMount = true
	st.SetReconcileView(func(context.Context) (ReconView, error) {
		return proot.pin(true)
	})
	ctx := context.Background()

	scopeDir := filepath.Join(root, "jfsws", "sub")
	if err := os.MkdirAll(scopeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	ghost := filepath.Join(scopeDir, ".filesv-op-o4242-a0-deadbeef")

	// Transient ghost: one listing, then gone — no intent may be minted.
	if err := os.WriteFile(ghost, []byte("ghost"), 0o644); err != nil {
		t.Fatal(err)
	}
	st.lastStageSweep.Store(0)
	st.Reconcile(ctx)
	if n := recoverIntents(t, st); n != 0 {
		t.Fatalf("first sighting on juicefs minted %d recover intents", n)
	}
	os.Remove(ghost)
	st.lastStageSweep.Store(0)
	st.Reconcile(ctx)
	if n := recoverIntents(t, st); n != 0 {
		t.Fatalf("transient ghost on juicefs left %d recover intents", n)
	}

	// Persistent true orphan: deferred once, then recovered and surfaced.
	persist := filepath.Join(scopeDir, ".filesv-op-o7777-a0-cafe")
	if err := os.WriteFile(persist, []byte("real-orphan-on-jfs"), 0o644); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		st.lastStageSweep.Store(0)
		st.Reconcile(ctx)
	}
	if n := recoverIntents(t, st); n != 1 {
		t.Fatalf("persistent orphan on juicefs not recovered (%d intents)", n)
	}
	// Content must be surfaced visibly, not parked or destroyed.
	found := false
	filepath.Walk(filepath.Join(root, "jfsws"), func(p string, fi os.FileInfo, err error) error {
		if err == nil && !fi.IsDir() && strings.Contains(fi.Name(), "recovered-o") {
			if b, rerr := os.ReadFile(p); rerr == nil && string(b) == "real-orphan-on-jfs" {
				found = true
			}
		}
		return nil
	})
	if !found {
		t.Fatal("orphan bytes on juicefs were not surfaced at a recovered-* name")
	}
	t.Log("juicefs orphan surfaced")
}
