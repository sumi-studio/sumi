package filesvc

// capture_pg_test.go — composed evidence for the private immutable
// capture primitive. Gated on all of:
//
//	FILESV_TEST_DSN        filesvc DB (durable manifest tables)
//	CAPTURE_TEST_META_DSN  JuiceFS metadata engine DSN (native volume)
//	CAPTURE_TEST_OBJ_ROOT  file:// object store root of that volume
//	CAPTURE_TEST_MOUNT     live JuiceFS mount of the SAME volume (oracle)
//
// The oracle (expected bytes/names) is read through the mount BEFORE
// mutation — an independent pre-mutation baseline, never derived from
// the decoder under test. The file:// backend and mount fixture do NOT
// constitute production object-backend acceptance.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"
)

type capFixture struct {
	t       *testing.T
	mount   string // live JuiceFS mount (oracle path)
	objroot string
	metaDSN string
	scope   string
	owner   string            // return lineage owner asserted on every capture op
	epoch   int64             // return lineage epoch
	oracle  map[string][]byte // rel path -> expected bytes at capture time
	links   map[string]string // rel path -> symlink target
	dirs    map[string]bool
}

func capEnv(t *testing.T) *capFixture {
	t.Helper()
	mount := os.Getenv("CAPTURE_TEST_MOUNT")
	obj := os.Getenv("CAPTURE_TEST_OBJ_ROOT")
	meta := os.Getenv("CAPTURE_TEST_META_DSN")
	if mount == "" || obj == "" || meta == "" {
		t.Skip("CAPTURE_TEST_MOUNT/OBJ_ROOT/META_DSN not set — capture fixture tests skipped")
	}
	if pgDSN(t) == "" {
		t.Skip("FILESV_TEST_DSN not set")
	}
	return &capFixture{
		t: t, mount: mount, objroot: obj, metaDSN: meta,
		scope: "caps" + randHex(4),
		owner: "sessA", epoch: 7,
		oracle: map[string][]byte{}, links: map[string]string{}, dirs: map[string]bool{},
	}
}

func (f *capFixture) wr(rel string, data []byte) {
	f.t.Helper()
	p := filepath.Join(f.mount, f.scope, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(p, data, 0o644); err != nil {
		f.t.Fatal(err)
	}
	f.oracle[rel] = data
}

func (f *capFixture) mkdir(rel string) {
	f.t.Helper()
	if err := os.MkdirAll(filepath.Join(f.mount, f.scope, rel), 0o755); err != nil {
		f.t.Fatal(err)
	}
	f.dirs[rel] = true
}

func (f *capFixture) symlink(target, rel string) {
	f.t.Helper()
	if err := os.Symlink(target, filepath.Join(f.mount, f.scope, rel)); err != nil {
		f.t.Fatal(err)
	}
	f.links[rel] = target
}

func (f *capFixture) newService(t *testing.T, root string) (*Service, *Store, *CaptureService) {
	return f.newServiceVol(t, root, f.metaDSN, f.objroot)
}

// newServiceVol wires a service against an explicit metadata DSN and
// object root — needed for the second (trash=0) and refusal volumes.
func (f *capFixture) newServiceVol(t *testing.T, root, metaDSN, objroot string) (*Service, *Store, *CaptureService) {
	t.Helper()
	dsn := pgDSN(t)
	resetTables(t, dsn)
	st, err := NewStore(context.Background(), dsn, root)
	if err != nil {
		t.Fatal(err)
	}
	// The default cut horizon is deadGrace (20s): fresh test stores
	// cannot assert a barrier until tenure elapses. Shrink it — the
	// predecessor-effect window the horizon bounds does not exist in
	// fixture (no prior writer ever owned these roots).
	st.SetCutHorizon(0)
	st.SetDrainTimeout(2 * time.Second)
	cs, err := NewCaptureService(context.Background(), CaptureConfig{
		MetaDSN: metaDSN, ObjKind: "file", ObjRoot: objroot,
	}, st)
	if err != nil {
		t.Fatal(err)
	}
	tokens := map[string]map[string]bool{
		"adm":   {"*": true},
		"sc":    {f.scope: true},
		"other": {"unrelated": true},
	}
	svc, err := NewAt(root, st, tokens)
	if err != nil {
		t.Fatal(err)
	}
	svc.SetCapture(cs)
	return svc, st, cs
}

// doCapture asserts the fixture's barrier lineage then captures —
// mirroring the product order (returnsession freezes, then captures).
// expected is the scope_id binding passed on retakes ("" on first).
func (f *capFixture) doCapture(t *testing.T, svc *Service, st *Store, expected string) *captureRow {
	t.Helper()
	if err := st.SetScopeFrozen(context.Background(), f.scope, f.owner, f.epoch,
		"capture-test", true); err != nil {
		t.Fatalf("barrier: %v", err)
	}
	row, err := svc.cap.Capture(context.Background(), f.scope, f.owner, f.epoch, expected)
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	return row
}

// readAll streams the whole captured file for entry seq under the
// fixture's lineage.
func (f *capFixture) readAll(t *testing.T, cs *CaptureService, id string, seq int64) ([]byte, error) {
	t.Helper()
	var buf bytes.Buffer
	_, err := cs.Stream(context.Background(), id, f.owner, f.epoch, seq, 0, 0, &buf)
	return buf.Bytes(), err
}

func (f *capFixture) entriesByPath(t *testing.T, cs *CaptureService, id string) map[string]captureEntryRow {
	t.Helper()
	out := map[string]captureEntryRow{}
	var cursor int64 = -1
	for {
		rows, err := cs.Entries(context.Background(), id, f.owner, f.epoch, cursor, 500)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 0 {
			break
		}
		for _, e := range rows {
			out[string(e.Path)] = e
			cursor = e.Seq
		}
		if len(rows) < 500 {
			break
		}
	}
	return out
}

// TestCaptureComposed is the core composed case: native JuiceFS volume +
// real PG metadata + real filesvc store + real HTTP service. Baseline
// through the mount, mutations through the mount, bytes via captured
// objects only.
func TestCaptureComposed(t *testing.T) {
	f := capEnv(t)
	root := t.TempDir()
	svc, st, cs := f.newService(t, root)
	defer st.Close()
	defer cs.Close()

	// --- seed through the mount (oracle baseline) --------------------
	rnd := rand.Reader
	big := make([]byte, 70<<20)
	io.ReadFull(rnd, big) // >64MiB: two chunks
	multi := make([]byte, 6<<20)
	io.ReadFull(rnd, multi) // >4MiB block: two blocks
	f.wr("f_big", big)
	f.wr("f_multi", multi)
	over := make([]byte, 120000)
	io.ReadFull(rnd, over)
	f.wr("f_over", over)
	f.wr("f_over", over) // overwrite → history slices
	f.wr("f_empty", []byte{})
	f.mkdir("emptydir")
	f.mkdir("sub")
	hard := make([]byte, 60000)
	io.ReadFull(rnd, hard)
	f.wr("sub/f_hard", hard)
	if err := os.Link(filepath.Join(f.mount, f.scope, "sub/f_hard"),
		filepath.Join(f.mount, f.scope, "sub/f_hard2")); err != nil {
		t.Fatal(err)
	}
	f.oracle["sub/f_hard2"] = hard
	f.symlink("f_big", "link1")
	f.wr(".filesv-notes", []byte("keep me"))
	f.wr("日本語ファイル", []byte("utf8-name"))
	f.wr("emoji-🚀", []byte("emoji"))
	f.wr("del|im\nbs\\name", []byte("delimiters"))
	// service-private names must NOT be captured
	f.wr(".filesv-tmp-x", []byte("private"))
	f.wr(".filesv-op-y", []byte("private2"))
	delete(f.oracle, ".filesv-tmp-x")
	delete(f.oracle, ".filesv-op-y")
	// sparse file: truncate-extended hole then a tail write
	sp := filepath.Join(f.mount, f.scope, "f_sparse")
	if err := os.WriteFile(sp, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(sp, 100000); err != nil {
		t.Fatal(err)
	}
	fh, err := os.OpenFile(sp, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	fh.WriteAt([]byte("tail"), 100000)
	fh.Close()
	f.oracle["f_sparse"] = append(make([]byte, 100000), []byte("tail")...)
	syncfs(t, f)

	// --- capture ------------------------------------------------------
	row := f.doCapture(t, svc, st, "")
	if row.EntryCount == 0 {
		t.Fatal("empty manifest")
	}
	byPath := f.entriesByPath(t, cs, row.CaptureID)
	t.Logf("capture %s: %d entries sha=%s unsupported=%d",
		row.CaptureID, row.EntryCount, row.ManifestSHA[:12], row.Unsupported)

	// completeness: every oracle path + dirs + links present
	want := map[string]bool{"": true}
	for p := range f.oracle {
		want[p] = true
	}
	for p := range f.dirs {
		want[p] = true
	}
	for p := range f.links {
		want[p] = true
	}
	for p := range want {
		if _, ok := byPath[p]; !ok {
			t.Fatalf("manifest missing path %q (have %v)", p, keys(byPath))
		}
	}
	for p := range byPath {
		if strings.Contains(p, ".filesv-tmp-") || strings.Contains(p, ".filesv-op-") {
			t.Fatalf("private name leaked into manifest: %q", p)
		}
	}
	// hardlink linkage visible via opaque group, not raw inode
	e1, e2 := byPath["sub/f_hard"], byPath["sub/f_hard2"]
	if e1.LinkGroup == "" || e1.LinkGroup != e2.LinkGroup {
		t.Fatalf("hardlink group missing/mismatched: %q vs %q", e1.LinkGroup, e2.LinkGroup)
	}
	if e1.Nlink != 2 {
		t.Fatalf("nlink %d want 2", e1.Nlink)
	}

	// --- post-capture mutations (live tree diverges wildly; oracle
	// baseline stays pre-mutation) -----------------------------------
	if err := os.WriteFile(filepath.Join(f.mount, f.scope, "f_over"),
		[]byte("LATE-WRITE"), 0o644); err != nil {
		t.Fatal(err)
	}
	os.Remove(filepath.Join(f.mount, f.scope, "f_empty"))
	os.Rename(filepath.Join(f.mount, f.scope, "sub/f_hard2"),
		filepath.Join(f.mount, f.scope, "sub/f_moved"))
	os.WriteFile(filepath.Join(f.mount, f.scope, "late.txt"), []byte("late"), 0o644)
	os.Truncate(filepath.Join(f.mount, f.scope, "f_big"), 1000)
	syncfs(t, f)

	// --- captured bytes must equal the baseline -----------------------
	for rel, want := range f.oracle {
		e := byPath[rel]
		got, err := f.readAll(t, cs, row.CaptureID, e.Seq)
		if err != nil {
			t.Fatalf("%s: stream: %v", rel, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s: captured bytes diverge (len %d want %d)", rel, len(got), len(want))
		}
	}
	t.Log("post-mutation captured bytes == pre-mutation baseline for all files")
}

func syncfs(t *testing.T, f *capFixture) {
	t.Helper()
	// The mount flushes metadata into PG on each syscall; a short settle
	// covers async cleanup (writeback is off on the fixture mount).
	time.Sleep(250 * time.Millisecond)
}

func keys(m map[string]captureEntryRow) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestCaptureHTTPEndToEnd exercises the real HTTP surface: create via
// POST, manifest rows via GET entries, bytes via GET read, negative
// authority, release.
func TestCaptureHTTPEndToEnd(t *testing.T) {
	f := capEnv(t)
	root := t.TempDir()
	svc, st, cs := f.newService(t, root)
	defer st.Close()
	defer cs.Close()

	f.wr("a.txt", []byte("hello capture"))
	f.mkdir("d")
	f.wr("d/b.txt", []byte("nested"))
	syncfs(t, f)

	srv := httptest.NewServer(svc)
	defer srv.Close()

	auth := func(req *http.Request, tok string) {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	do := func(method, url, tok string) *http.Response {
		req, _ := http.NewRequest(method, srv.URL+url, nil)
		auth(req, tok)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	// Barrier must exist before any capture op — the lineage check
	// refuses requests for scopes with no durable authority.
	if err := st.SetScopeFrozen(context.Background(), f.scope, "sA", 7,
		"capture-test", true); err != nil {
		t.Fatalf("barrier: %v", err)
	}

	// scoped token cannot create (wildcard-only surface)
	if r := do("POST", "/v1/files/"+f.scope+"/capture?owner=sA&epoch=7", "sc"); r.StatusCode != 403 {
		r.Body.Close()
		t.Fatalf("scoped create: %d want 403", r.StatusCode)
	}
	// wildcard without lineage params → 400
	if r := do("POST", "/v1/files/"+f.scope+"/capture", "adm"); r.StatusCode != 400 {
		r.Body.Close()
		t.Fatalf("create without lineage: %d want 400", r.StatusCode)
	}
	// stale epoch (barrier says 7) → 403
	if r := do("POST", "/v1/files/"+f.scope+"/capture?owner=sA&epoch=3", "adm"); r.StatusCode != 403 {
		r.Body.Close()
		t.Fatalf("stale-epoch create: %d want 403", r.StatusCode)
	}
	// wildcard creates with matching lineage
	r := do("POST", "/v1/files/"+f.scope+"/capture?owner=sA&epoch=7", "adm")
	if r.StatusCode != 200 {
		b, _ := io.ReadAll(r.Body)
		t.Fatalf("create: %d %s", r.StatusCode, b)
	}
	var meta struct {
		CaptureID   string `json:"capture_id"`
		ManifestSHA string `json:"manifest_sha"`
		Entries     int64  `json:"entries"`
	}
	json.NewDecoder(r.Body).Decode(&meta)
	r.Body.Close()
	if meta.CaptureID == "" || meta.Entries == 0 {
		t.Fatalf("bad meta %+v", meta)
	}

	// non-wildcard tokens cannot read manifest or bytes — the whole
	// capture surface is internal-wildcard-only, scope tokens included
	if r := do("GET", "/v1/capture/"+meta.CaptureID+"/entries?owner=sA&epoch=7", "sc"); r.StatusCode != 403 {
		r.Body.Close()
		t.Fatalf("scoped entries: %d want 403", r.StatusCode)
	}
	if r := do("GET", "/v1/capture/"+meta.CaptureID+"/read?owner=sA&epoch=7&seq=1", "other"); r.StatusCode != 403 {
		r.Body.Close()
		t.Fatalf("cross-scope read: %d want 403", r.StatusCode)
	}
	// wildcard but wrong-generation lineage → 403 capture_stale
	if r := do("GET", "/v1/capture/"+meta.CaptureID+"/entries?owner=sA&epoch=9", "adm"); r.StatusCode != 403 {
		r.Body.Close()
		t.Fatalf("stale-epoch entries: %d want 403", r.StatusCode)
	}
	if r := do("GET", "/v1/capture/"+meta.CaptureID+"/entries?owner=other&epoch=7", "adm"); r.StatusCode != 403 {
		r.Body.Close()
		t.Fatalf("foreign-owner entries: %d want 403", r.StatusCode)
	}

	// entries stream → find a.txt
	r = do("GET", "/v1/capture/"+meta.CaptureID+"/entries?owner=sA&epoch=7", "adm")
	var lst struct {
		Entries []struct {
			Seq     int64  `json:"seq"`
			PathB64 string `json:"path_b64"`
			Type    string `json:"type"`
		} `json:"entries"`
	}
	json.NewDecoder(r.Body).Decode(&lst)
	r.Body.Close()
	var seq int64 = -1
	for _, e := range lst.Entries {
		p, _ := base64.StdEncoding.DecodeString(e.PathB64)
		if string(p) == "a.txt" {
			seq = e.Seq
		}
	}
	if seq < 0 {
		t.Fatal("a.txt not in manifest stream")
	}
	// byte stream == baseline
	r = do("GET", fmt.Sprintf("/v1/capture/%s/read?owner=sA&epoch=7&seq=%d", meta.CaptureID, seq), "adm")
	body, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if r.StatusCode != 200 || string(body) != "hello capture" {
		t.Fatalf("read: %d %q", r.StatusCode, body)
	}
	// release requires matching lineage → gone
	if r := do("DELETE", "/v1/capture/"+meta.CaptureID+"?owner=sA&epoch=9", "adm"); r.StatusCode != 403 {
		r.Body.Close()
		t.Fatalf("stale-epoch release: %d want 403", r.StatusCode)
	}
	if r := do("DELETE", "/v1/capture/"+meta.CaptureID+"?owner=sA&epoch=7", "adm"); r.StatusCode != 200 {
		r.Body.Close()
		t.Fatalf("release: %d", r.StatusCode)
	}
	if r := do("GET", "/v1/capture/"+meta.CaptureID+"/entries?owner=sA&epoch=7", "adm"); r.StatusCode != 410 {
		r.Body.Close()
		t.Fatalf("post-release entries: %d want 410", r.StatusCode)
	}
}

// TestCaptureNegatives: anchor/authority/lifecycle/format refusals and
// the pending path — all on real PG + real service.
func TestCaptureNegatives(t *testing.T) {
	f := capEnv(t)
	root := t.TempDir()
	svc, st, cs := f.newService(t, root)
	defer st.Close()
	defer cs.Close()

	// empty/missing scope anchor
	if _, err := cs.Capture(context.Background(), "", "s", 1, ""); !errors.Is(err, ErrCaptureRefused) {
		t.Fatalf("empty scope: %v", err)
	}
	if _, err := cs.Capture(context.Background(), "nosuchscope"+randHex(4), "s", 1, ""); !errors.Is(err, ErrCaptureRefused) {
		t.Fatalf("missing anchor: %v", err)
	}
	// .trash is never a capturable scope (it lives at volume root parent=1
	// only if present; a literal .trash scope name must not resolve)
	if _, err := cs.Capture(context.Background(), ".trash", "s", 1, ""); err == nil {
		t.Fatal(".trash capture accepted")
	}

	f.wr("victim", []byte("victim-bytes"))
	syncfs(t, f)
	row := f.doCapture(t, svc, st, "")
	byPath := f.entriesByPath(t, cs, row.CaptureID)
	e := byPath["victim"]

	// missing object → pending: remove the slice's object file
	slices, err := st.slicesFor(context.Background(), row.CaptureID, e.Ino)
	if err != nil || len(slices) == 0 {
		t.Fatalf("slices: %v", err)
	}
	// Derive the exact object key for the slice's only block.
	sl := slices[0]
	bsize := int(sl.Size)
	if bsize > row.Format.BlockBytes {
		bsize = row.Format.BlockBytes
	}
	objKey := objectKey(sl.SliceID, 0, bsize, row.Format.BlockBytes, row.Format.HashPrefix)
	objPath := filepath.Join(f.objroot, filepath.FromSlash(objKey))
	if _, err := os.Stat(objPath); err != nil {
		t.Fatalf("expected object %s: %v", objKey, err)
	}
	moved := objPath + ".hidden"
	if err := os.Rename(objPath, moved); err != nil {
		t.Fatal(err)
	}
	_, err = f.readAll(t, cs, row.CaptureID, e.Seq)
	if !errors.Is(err, ErrCapturePending) {
		t.Fatalf("missing object: %v want pending", err)
	}
	// fresh coherent capture replaces pending (object still missing →
	// new capture sees the live file — retake is mover-visible)
	row2 := f.doCapture(t, svc, st, "")
	if row2.ManifestSHA == row.ManifestSHA {
		// manifest identical is fine — retake may legitimately map the
		// same live content; identity equality is mover-decidable.
		t.Log("retake produced identical manifest (content unchanged)")
	}
	os.Rename(moved, objPath)

	// short object → pending
	if err := os.Truncate(objPath, 10); err != nil {
		t.Fatal(err)
	}
	_, err = f.readAll(t, cs, row.CaptureID, e.Seq)
	if !errors.Is(err, ErrCapturePending) {
		t.Fatalf("short object: %v want pending", err)
	}
}

// TestCaptureRestartSurvival: manifest + reads survive a fresh service
// instance (restart), not in-memory state.
func TestCaptureRestartSurvival(t *testing.T) {
	f := capEnv(t)
	root := t.TempDir()
	svc, st, cs := f.newService(t, root)
	defer st.Close()
	defer cs.Close()
	f.wr("restart.txt", []byte("durable"))
	syncfs(t, f)
	row := f.doCapture(t, svc, st, "")
	id, sha := row.CaptureID, row.ManifestSHA
	// Release the writer lock before binding st2; the deferred closes
	// above are then harmless no-ops if we fail before this point.
	cs.Close()
	st.Close()

	// restart: new pool + service, same durable rows
	st2, err := NewStore(context.Background(), pgDSN(t), root)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	cs2, err := NewCaptureService(context.Background(), CaptureConfig{
		MetaDSN: f.metaDSN, ObjKind: "file", ObjRoot: f.objroot,
	}, st2)
	if err != nil {
		t.Fatal(err)
	}
	defer cs2.Close()
	meta, err := cs2.Meta(context.Background(), id, f.owner, f.epoch)
	if err != nil {
		t.Fatalf("restart meta: %v", err)
	}
	if meta.ManifestSHA != sha {
		t.Fatalf("manifest identity changed across restart")
	}
	byPath := f.entriesByPath(t, cs2, id)
	got, err := f.readAll(t, cs2, id, byPath["restart.txt"].Seq)
	if err != nil {
		t.Fatalf("restart read: %v", err)
	}
	if string(got) != "durable" {
		t.Fatalf("restart bytes %q", got)
	}
	_ = svc
}

// jfs runs the real deployed juicefs CLI inside the fixture box.
func jfs(t *testing.T, args ...string) {
	t.Helper()
	out, err := exec.Command("/opt/jfs/juicefs", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("juicefs %v: %v\n%s", args, err, out)
	}
}

// TestCaptureCompactionRetention: force REAL compaction on the trash=1
// volume — old slice objects are retained under the delayed-cleanup
// edge, so the captured read still serves the pre-compaction baseline.
// This is the actual delayed-retention mechanism, not a synthetic
// missing-object test.
func TestCaptureCompactionRetention(t *testing.T) {
	f := capEnv(t)
	root := t.TempDir()
	svc, st, cs := f.newService(t, root)
	defer st.Close()
	defer cs.Close()

	base := make([]byte, 4<<20)
	io.ReadFull(rand.Reader, base)
	f.wr("f_c", base)
	syncfs(t, f)
	row := f.doCapture(t, svc, st, "")
	byPath := f.entriesByPath(t, cs, row.CaptureID)
	e := byPath["f_c"]

	// Accumulate >=5 slices in chunk 0 (compaction threshold), then
	// force compaction of the live file.
	p := filepath.Join(f.mount, f.scope, "f_c")
	for i := 0; i < 6; i++ {
		fh, err := os.OpenFile(p, os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fh.WriteAt([]byte{byte(i)}, int64(i*4096)); err != nil {
			t.Fatal(err)
		}
		fh.Close()
	}
	syncfs(t, f)
	jfs(t, "compact", p)
	syncfs(t, f)
	// gc --delete honors the trash edge (now - trashDays*86400): under
	// trash=1 the just-compacted slices are inside the window and must
	// be RETAINED — the captured read still serves the baseline.
	jfs(t, "gc", f.metaDSN, "--delete", "--threads", "4")

	got, err := f.readAll(t, cs, row.CaptureID, e.Seq)
	if err != nil {
		t.Fatalf("post-compaction+gc read: %v", err)
	}
	if !bytes.Equal(got, base) {
		t.Fatalf("post-compaction bytes diverge (len %d want %d)", len(got), len(base))
	}
	t.Log("trash=1: compacted-slice objects retained; captured baseline served")
}

// TestCaptureTrashZeroPending: on a TrashDays=0 volume the same real
// compaction + gc removes the captured objects — the manifest answers
// bounded pending, and a fresh coherent capture serves again.
func TestCaptureTrashZeroPending(t *testing.T) {
	f := capEnv(t)
	mount0 := os.Getenv("CAPTURE_TEST_MOUNT0")
	meta0 := os.Getenv("CAPTURE_TEST_META_DSN0")
	obj0 := os.Getenv("CAPTURE_TEST_OBJ_ROOT0")
	if mount0 == "" || meta0 == "" || obj0 == "" {
		t.Skip("CAPTURE_TEST_*0 env not set — trash=0 case skipped")
	}
	f.mount = mount0
	root := t.TempDir()
	svc, st, cs := f.newServiceVol(t, root, meta0, obj0)
	defer st.Close()
	defer cs.Close()

	base := make([]byte, 1<<20)
	io.ReadFull(rand.Reader, base)
	f.wr("f0", base)
	syncfs(t, f)
	row := f.doCapture(t, svc, st, "")
	e := f.entriesByPath(t, cs, row.CaptureID)["f0"]

	p := filepath.Join(f.mount, f.scope, "f0")
	for i := 0; i < 6; i++ {
		fh, _ := os.OpenFile(p, os.O_WRONLY, 0)
		fh.WriteAt([]byte{byte(i), byte(i)}, int64(i*2048))
		fh.Close()
	}
	syncfs(t, f)
	jfs(t, "compact", p)
	syncfs(t, f)
	// trash=0: cleanup edge = now → gc removes the compacted slices.
	jfs(t, "gc", meta0, "--delete", "--threads", "4")

	_, err := f.readAll(t, cs, row.CaptureID, e.Seq)
	if !errors.Is(err, ErrCapturePending) {
		t.Fatalf("trash=0 post-gc read: %v want pending", err)
	}
	// A fresh coherent capture of the (mutated) live file serves again —
	// retake is a NEW capture id, never a silent retarget of the old one.
	row2 := f.doCapture(t, svc, st, "")
	if row2.CaptureID == row.CaptureID {
		t.Fatal("retake reused capture id")
	}
	e2 := f.entriesByPath(t, cs, row2.CaptureID)["f0"]
	got, err := f.readAll(t, cs, row2.CaptureID, e2.Seq)
	if err != nil {
		t.Fatalf("retake read: %v", err)
	}
	if len(got) != len(base) {
		t.Fatalf("retake len %d want %d", len(got), len(base))
	}
	t.Log("trash=0: pending after gc; fresh coherent capture serves")
}

// TestCaptureFormatRefusal: an lz4 volume is refused at the format gate
// before any namespace trust — unsupported format is never decoded.
func TestCaptureFormatRefusal(t *testing.T) {
	f := capEnv(t)
	metaZ := os.Getenv("CAPTURE_TEST_META_DSN_LZ4")
	if metaZ == "" {
		t.Skip("CAPTURE_TEST_META_DSN_LZ4 not set")
	}
	root := t.TempDir()
	svc, st, cs := f.newServiceVol(t, root, metaZ, f.objroot)
	defer st.Close()
	defer cs.Close()
	_ = svc
	if _, err := cs.Capture(context.Background(), f.scope, "s", 1, ""); !errors.Is(err, ErrCaptureRefused) {
		t.Fatalf("lz4 volume: %v want refused", err)
	}
}

// TestCaptureRenameRecreate: renaming the scope dir out and recreating
// it under the same name changes scope_id — a stale grant cannot read
// an unrelated manifest through the same scope name.
func TestCaptureRenameRecreate(t *testing.T) {
	f := capEnv(t)
	root := t.TempDir()
	svc, st, cs := f.newService(t, root)
	defer st.Close()
	defer cs.Close()

	f.wr("orig.txt", []byte("original"))
	syncfs(t, f)
	row1 := f.doCapture(t, svc, st, "")

	// rename scope out, recreate same name with different content
	old := filepath.Join(f.mount, f.scope+".old")
	if err := os.Rename(filepath.Join(f.mount, f.scope), old); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(f.mount, f.scope), 0o755); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(f.mount, f.scope, "recreated.txt"), []byte("new"), 0o644)
	syncfs(t, f)
	row2 := f.doCapture(t, svc, st, "")

	if row1.ScopeID == row2.ScopeID {
		t.Fatal("rename+recreate produced identical scope_id — stale authority would validate")
	}
	if row1.ManifestSHA == row2.ManifestSHA {
		t.Fatal("rename+recreate produced identical manifest identity")
	}
	// old capture still serves its own immutable manifest (evidence
	// retention) — it does NOT retarget the new tree.
	byPath := f.entriesByPath(t, cs, row1.CaptureID)
	if _, ok := byPath["orig.txt"]; !ok {
		t.Fatal("old manifest lost its entries")
	}
}

// TestCaptureUnsupportedVisible: a fifo stays IN the manifest flagged
// unsupported — the tree is truthful, not silently smaller.
func TestCaptureUnsupportedVisible(t *testing.T) {
	f := capEnv(t)
	root := t.TempDir()
	svc, st, cs := f.newService(t, root)
	defer st.Close()
	defer cs.Close()

	f.wr("ok.txt", []byte("ok"))
	if err := syscall.Mkfifo(filepath.Join(f.mount, f.scope, "fifo1"), 0o644); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}
	syncfs(t, f)
	row := f.doCapture(t, svc, st, "")
	byPath := f.entriesByPath(t, cs, row.CaptureID)
	e, ok := byPath["fifo1"]
	if !ok {
		t.Fatal("fifo silently dropped from manifest")
	}
	if e.Supported {
		t.Fatal("fifo marked supported")
	}
	if row.Unsupported == 0 {
		t.Fatal("unsupported count not surfaced")
	}
	if _, err := f.readAll(t, cs, row.CaptureID, e.Seq); !errors.Is(err, ErrCaptureRefused) {
		t.Fatalf("fifo read: %v want refused", err)
	}
}
