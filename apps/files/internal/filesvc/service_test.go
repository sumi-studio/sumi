package filesvc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// fakeStore is an in-memory VersionStore for handler tests — it exercises the
// same CAS semantics (monotonic bump, conflict on stale if_version) without PG.
type fakeStore struct {
	vers   map[string]int64
	fps    map[string]string
	evs    []Event
	seq    int64 // global monotonic version mint — mirrors file_version_seq
	obsErr error // injected ObservedVersion failure (f33)
}

func newFakeStore() *fakeStore {
	return &fakeStore{vers: map[string]int64{}, fps: map[string]string{}}
}

// bump mirrors the real store's CAS rules, including the fingerprint gate:
// none/eq0 require no row AND nothing on disk; eq n requires the recorded
// fingerprint to still match the live file.
func (f *fakeStore) bump(scope, path string, iv IfVersion, probe FPProbe) (int64, error) {
	k := scope + "/" + path
	cur := f.vers[k]
	switch iv.Mode {
	case "none", "eq":
		if iv.Mode == "none" || iv.Version == 0 {
			if cur != 0 {
				return 0, ErrConflict
			}
			if _, exists, err := probe(); err != nil {
				return 0, err
			} else if exists {
				return 0, ErrExternalChange
			}
		} else {
			if cur == 0 {
				return 0, ErrNoSuchFile
			}
			if cur != iv.Version {
				return 0, ErrConflict
			}
			liveFP, exists, err := probe()
			if err != nil {
				return 0, err
			}
			if !exists || (f.fps[k] != "" && liveFP != f.fps[k]) {
				return 0, ErrExternalChange
			}
		}
	case "any":
	default:
		return 0, fmt.Errorf("bad mode")
	}
	// Global monotonic mint: versions never repeat at a path, so a stale
	// token can never collide with a later incarnation (f48).
	f.seq++
	f.vers[k] = f.seq
	return f.seq, nil
}

func (f *fakeStore) WithWrite(ctx context.Context, scope, path, op string, iv IfVersion, probe FPProbe, fn func() (FileInfo, error)) (int64, FileInfo, error) {
	k := scope + "/" + path
	prev, had := f.vers[k]
	ver, err := f.bump(scope, path, iv, probe)
	if err != nil {
		return 0, FileInfo{}, err
	}
	info, err := fn()
	if err != nil {
		if had {
			f.vers[k] = prev // roll back the row on FS failure (seq is consumed)
		} else {
			delete(f.vers, k)
		}
		return 0, FileInfo{}, err
	}
	f.fps[scope+"/"+path] = info.Fingerprint
	f.evs = append(f.evs, Event{Seq: int64(len(f.evs) + 1), Path: path, Op: op, Version: ver})
	return ver, info, nil
}

func (f *fakeStore) Rename(ctx context.Context, scope, from, to string, iv IfVersion, casProbe, fromProbe FPProbe, fn func() (FileInfo, error)) (int64, FileInfo, error) {
	k := scope + "/" + to
	prev, had := f.vers[k]
	ver, err := f.bump(scope, to, iv, casProbe)
	if err != nil {
		return 0, FileInfo{}, err
	}
	info, err := fn()
	if err != nil {
		if had {
			f.vers[k] = prev
		} else {
			delete(f.vers, k)
		}
		return 0, FileInfo{}, err
	}
	f.fps[scope+"/"+to] = info.Fingerprint
	// move descendants' rows like the real store
	pref := scope + "/" + from + "/"
	for k, v := range f.vers {
		if strings.HasPrefix(k, pref) {
			delete(f.vers, k)
			f.vers[scope+"/"+to+"/"+strings.TrimPrefix(k, pref)] = v
			f.fps[scope+"/"+to+"/"+strings.TrimPrefix(k, pref)] = f.fps[k]
			delete(f.fps, k)
		}
	}
	delete(f.vers, scope+"/"+from)
	delete(f.fps, scope+"/"+from)
	f.evs = append(f.evs, Event{Seq: int64(len(f.evs) + 1), Path: to, From: from, Op: "rename", Version: ver})
	return ver, info, nil
}

func (f *fakeStore) Remove(ctx context.Context, scope, path string, iv IfVersion, probe FPProbe, fn func() error) error {
	k := scope + "/" + path
	cur := f.vers[k]
	if cur == 0 && (iv.Mode == "eq" || iv.Mode == "none") {
		return ErrNoSuchFile
	}
	if iv.Mode == "eq" && cur != iv.Version {
		return ErrConflict
	}
	if iv.Mode == "eq" {
		liveFP, exists, err := probe()
		if err != nil {
			return err
		}
		if !exists || (f.fps[k] != "" && liveFP != f.fps[k]) {
			return ErrExternalChange
		}
	}
	if err := fn(); err != nil {
		return err
	}
	for kk := range f.vers {
		if kk == k || strings.HasPrefix(kk, k+"/") {
			delete(f.vers, kk)
			delete(f.fps, kk)
		}
	}
	f.seq++
	f.evs = append(f.evs, Event{Seq: int64(len(f.evs) + 1), Path: path, Op: "remove", Version: f.seq})
	return nil
}

func (f *fakeStore) ObservedVersion(ctx context.Context, scope, path string) (int64, string, error) {
	if f.obsErr != nil {
		return 0, "", f.obsErr
	}
	return f.vers[scope+"/"+path], f.fps[scope+"/"+path], nil
}

func (f *fakeStore) Changes(ctx context.Context, scope string, since int64, limit int) ([]Event, error) {
	out := []Event{}
	for _, e := range f.evs {
		if e.Seq > since {
			out = append(out, e)
		}
	}
	return out, nil
}

func testSvc(t *testing.T) (*Service, string) {
	t.Helper()
	dir := t.TempDir()
	svc, err := NewAt(dir, newFakeStore(), map[string]map[string]bool{
		"tok-a":   {"ws1": true},
		"tok-b":   {"ws2": true},
		"tok-all": {"*": true},
	})
	if err != nil {
		t.Fatal(err)
	}
	return svc, dir
}

func req(t *testing.T, svc *Service, method, target, tok string, body string, hdrs map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	var rdr *strings.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	} else {
		rdr = strings.NewReader("")
	}
	r := httptest.NewRequest(method, target, rdr)
	if tok != "" {
		r.Header.Set("Authorization", "Bearer "+tok)
	}
	for k, v := range hdrs {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	svc.ServeHTTP(w, r)
	return w
}

func TestWriteReadStatVersion(t *testing.T) {
	svc, _ := testSvc(t)
	w := req(t, svc, "PUT", "/v1/files/ws1/write?path=a.txt", "tok-a", "hello", map[string]string{"If-Version": "none"})
	if w.Code != 200 {
		t.Fatalf("write: %d %s", w.Code, w.Body)
	}
	if !strings.Contains(w.Body.String(), `"version":1`) {
		t.Fatalf("want version 1, got %s", w.Body)
	}
	w = req(t, svc, "GET", "/v1/files/ws1/read?path=a.txt", "tok-a", "", nil)
	if w.Code != 200 || w.Body.String() != "hello" {
		t.Fatalf("read: %d %q", w.Code, w.Body)
	}
	if w.Header().Get("X-File-Version") != "1" {
		t.Fatalf("missing version header: %v", w.Header())
	}
	w = req(t, svc, "GET", "/v1/files/ws1/stat?path=a.txt", "tok-a", "", nil)
	if !strings.Contains(w.Body.String(), `"version":1`) {
		t.Fatalf("stat: %s", w.Body)
	}
}

func TestIfVersionCASConflict(t *testing.T) {
	svc, _ := testSvc(t)
	req(t, svc, "PUT", "/v1/files/ws1/write?path=c.txt", "tok-a", "v1", map[string]string{"If-Version": "none"})
	// stale version 1 after a write to 2
	req(t, svc, "PUT", "/v1/files/ws1/write?path=c.txt", "tok-a", "v2", map[string]string{"If-Version": "1"})
	w := req(t, svc, "PUT", "/v1/files/ws1/write?path=c.txt", "tok-a", "v3", map[string]string{"If-Version": "1"})
	if w.Code != 409 {
		t.Fatalf("stale write should conflict, got %d", w.Code)
	}
	// create-if-absent on existing path
	w = req(t, svc, "PUT", "/v1/files/ws1/write?path=c.txt", "tok-a", "v4", map[string]string{"If-Version": "none"})
	if w.Code != 409 {
		t.Fatalf("none-on-existing should conflict, got %d", w.Code)
	}
}

func TestAuthScoping(t *testing.T) {
	svc, _ := testSvc(t)
	for _, tc := range []struct {
		name string
		tok  string
		want int
	}{
		{"no token", "", 403},
		{"wrong scope token", "tok-b", 403},
		{"scope token", "tok-a", 200},
		{"wildcard", "tok-all", 200},
	} {
		w := req(t, svc, "PUT", "/v1/files/ws1/write?path=p.txt", tc.tok, "x", map[string]string{"If-Version": "any"})
		if w.Code != tc.want {
			t.Fatalf("%s: got %d want %d", tc.name, w.Code, tc.want)
		}
	}
}

func TestContainment(t *testing.T) {
	svc, dir := testSvc(t)
	// path escape — ".." is rejected outright, not normalized into the root
	w := req(t, svc, "PUT", "/v1/files/ws1/write?path=../evil.txt", "tok-a", "x", map[string]string{"If-Version": "any"})
	if w.Code != 403 {
		t.Fatalf("dotdot write must be denied, got %d", w.Code)
	}
	if _, err := os.Stat(filepath.Join(dir, "..", "evil.txt")); err == nil {
		t.Fatal("escape wrote outside root")
	}
	// symlink escape: create a symlink inside root pointing outside
	outside := filepath.Join(dir, "..", "outside-target.txt")
	os.WriteFile(outside, []byte("secret"), 0o644)
	link := filepath.Join(dir, "link-out")
	os.Symlink(outside, link)
	w = req(t, svc, "GET", "/v1/files/ws1/read?path=link-out", "tok-a", "", nil)
	if w.Code == 200 {
		t.Fatalf("symlink escape read succeeded: %s", w.Body)
	}
}

func TestRenameAtomicAndRemove(t *testing.T) {
	svc, _ := testSvc(t)
	req(t, svc, "PUT", "/v1/files/ws1/write?path=r1.txt", "tok-a", "data", map[string]string{"If-Version": "none"})
	w := req(t, svc, "POST", "/v1/files/ws1/rename", "tok-a", `{"from":"r1.txt","to":"r2.txt","if_version":"any"}`, nil)
	if w.Code != 200 {
		t.Fatalf("rename: %d %s", w.Code, w.Body)
	}
	w = req(t, svc, "GET", "/v1/files/ws1/read?path=r2.txt", "tok-a", "", nil)
	if w.Body.String() != "data" {
		t.Fatalf("renamed content: %q", w.Body)
	}
	w = req(t, svc, "DELETE", "/v1/files/ws1/remove?path=r2.txt", "tok-a", "", map[string]string{"If-Version": "any"})
	if w.Code != 200 {
		t.Fatalf("remove: %d", w.Code)
	}
	w = req(t, svc, "GET", "/v1/files/ws1/read?path=r2.txt", "tok-a", "", nil)
	if w.Code != 404 {
		t.Fatalf("removed file readable: %d", w.Code)
	}
}

func TestExternalChangeFlag(t *testing.T) {
	svc, dir := testSvc(t)
	req(t, svc, "PUT", "/v1/files/ws1/write?path=x.txt", "tok-a", "v1", map[string]string{"If-Version": "none"})
	// executor-direct write bypasses the service (scope dir = dir/ws1)
	if err := os.WriteFile(filepath.Join(dir, "ws1", "x.txt"), []byte("executor-was-here"), 0o644); err != nil {
		t.Fatal(err)
	}
	w := req(t, svc, "GET", "/v1/files/ws1/read?path=x.txt", "tok-a", "", nil)
	if w.Body.String() != "executor-was-here" {
		t.Fatalf("read: %q", w.Body)
	}
	if w.Header().Get("X-External-Change") != "true" {
		t.Fatalf("expected X-External-Change header, got %v", w.Header())
	}
	w = req(t, svc, "GET", "/v1/files/ws1/stat?path=x.txt", "tok-a", "", nil)
	if !strings.Contains(w.Body.String(), `"external_change":true`) {
		t.Fatalf("stat should flag external change: %s", w.Body)
	}
}

// B1: a CAS write carrying the caller's last-seen version must NOT overwrite
// an executor-side edit — the fingerprint gate turns it into 409.
func TestCASDoesNotClobberExternalEdit(t *testing.T) {
	svc, dir := testSvc(t)
	req(t, svc, "PUT", "/v1/files/ws1/write?path=doc.md", "tok-a", "v1", map[string]string{"If-Version": "none"})
	os.WriteFile(filepath.Join(dir, "ws1", "doc.md"), []byte("human-edit"), 0o644)
	w := req(t, svc, "PUT", "/v1/files/ws1/write?path=doc.md", "tok-a", "stale", map[string]string{"If-Version": "1"})
	if w.Code != 409 {
		t.Fatalf("stale CAS over external edit must 409, got %d %s", w.Code, w.Body)
	}
	if !strings.Contains(w.Body.String(), "external_change") {
		t.Fatalf("want external_change code, got %s", w.Body)
	}
	data, _ := os.ReadFile(filepath.Join(dir, "ws1", "doc.md"))
	if string(data) != "human-edit" {
		t.Fatalf("executor edit was clobbered: %q", data)
	}
	// if_version=any is the explicit escape hatch
	w = req(t, svc, "PUT", "/v1/files/ws1/write?path=doc.md", "tok-a", "override", map[string]string{"If-Version": "any"})
	if w.Code != 200 {
		t.Fatalf("any write should pass: %d", w.Code)
	}
}

// B2a: create-only must not replace a file the service never saw.
func TestCreateOnlyOverExecutorFile(t *testing.T) {
	svc, dir := testSvc(t)
	os.MkdirAll(filepath.Join(dir, "ws1"), 0o755)
	os.WriteFile(filepath.Join(dir, "ws1", "linux.txt"), []byte("precious"), 0o644)
	w := req(t, svc, "PUT", "/v1/files/ws1/write?path=linux.txt", "tok-a", "new", map[string]string{"If-Version": "none"})
	if w.Code != 409 {
		t.Fatalf("none over executor file must 409, got %d", w.Code)
	}
	w = req(t, svc, "PUT", "/v1/files/ws1/write?path=linux.txt", "tok-a", "new", map[string]string{"If-Version": "0"})
	if w.Code != 409 {
		t.Fatalf("eq0 over executor file must 409, got %d", w.Code)
	}
	data, _ := os.ReadFile(filepath.Join(dir, "ws1", "linux.txt"))
	if string(data) != "precious" {
		t.Fatal("executor file clobbered")
	}
}

// B2b: rename requires a strict if_version; none must not replace existing.
func TestRenameStrictIfVersion(t *testing.T) {
	svc, dir := testSvc(t)
	req(t, svc, "PUT", "/v1/files/ws1/write?path=r1.txt", "tok-a", "a", map[string]string{"If-Version": "none"})
	req(t, svc, "PUT", "/v1/files/ws1/write?path=dst.txt", "tok-a", "precious", map[string]string{"If-Version": "none"})
	// missing if_version -> 400
	w := req(t, svc, "POST", "/v1/files/ws1/rename", "tok-a", `{"from":"r1.txt","to":"dst.txt"}`, nil)
	if w.Code != 400 {
		t.Fatalf("missing if_version must 400, got %d", w.Code)
	}
	// none over existing -> 409 and dst intact
	w = req(t, svc, "POST", "/v1/files/ws1/rename", "tok-a", `{"from":"r1.txt","to":"dst.txt","if_version":"none"}`, nil)
	if w.Code != 409 {
		t.Fatalf("rename none over existing must 409, got %d", w.Code)
	}
	if data, _ := os.ReadFile(filepath.Join(dir, "ws1", "dst.txt")); string(data) != "precious" {
		t.Fatal("rename clobbered existing dst")
	}
	// none over executor-created dst (no version row) -> 409 via NOREPLACE
	os.WriteFile(filepath.Join(dir, "ws1", "linux-dst.txt"), []byte("linux"), 0o644)
	w = req(t, svc, "POST", "/v1/files/ws1/rename", "tok-a", `{"from":"r1.txt","to":"linux-dst.txt","if_version":"none"}`, nil)
	if w.Code != 409 {
		t.Fatalf("rename none over executor dst must 409, got %d", w.Code)
	}
	if data, _ := os.ReadFile(filepath.Join(dir, "ws1", "linux-dst.txt")); string(data) != "linux" {
		t.Fatal("NOREPLACE rename clobbered executor file")
	}
	// numeric if_version parses strictly
	w = req(t, svc, "POST", "/v1/files/ws1/rename", "tok-a", `{"from":"r1.txt","to":"dst2.txt","if_version":0}`, nil)
	if w.Code != 200 {
		t.Fatalf("numeric if_version 0 should work, got %d %s", w.Code, w.Body)
	}
}

// B3: read range validation and bounded body.
func TestReadRangeBounds(t *testing.T) {
	svc, _ := testSvc(t)
	req(t, svc, "PUT", "/v1/files/ws1/write?path=b.bin", "tok-a", "0123456789", map[string]string{"If-Version": "none"})
	w := req(t, svc, "GET", "/v1/files/ws1/read?path=b.bin&len=36028797018963968", "tok-a", "", nil)
	if w.Code != 200 || w.Body.String() != "0123456789" {
		t.Fatalf("huge len must clamp to file, got %d %q", w.Code, w.Body)
	}
	w = req(t, svc, "GET", "/v1/files/ws1/read?path=b.bin&offset=4&len=3", "tok-a", "", nil)
	if w.Code != 200 || w.Body.String() != "456" {
		t.Fatalf("range read wrong: %d %q", w.Code, w.Body)
	}
	w = req(t, svc, "GET", "/v1/files/ws1/read?path=b.bin&offset=-1", "tok-a", "", nil)
	if w.Code != 400 {
		t.Fatalf("negative offset must 400, got %d", w.Code)
	}
	w = req(t, svc, "GET", "/v1/files/ws1/read?path=b.bin&len=-5", "tok-a", "", nil)
	if w.Code != 400 {
		t.Fatalf("negative len must 400, got %d", w.Code)
	}
}

// M1: renaming a dir moves descendants' version rows; events carry from.
func TestRenameDirMovesVersions(t *testing.T) {
	svc, _ := testSvc(t)
	req(t, svc, "PUT", "/v1/files/ws1/write?path=d/child.txt", "tok-a", "c", map[string]string{"If-Version": "none"})
	w := req(t, svc, "POST", "/v1/files/ws1/rename", "tok-a", `{"from":"d","to":"d2","if_version":"any"}`, nil)
	if w.Code != 200 {
		t.Fatalf("dir rename: %d %s", w.Code, w.Body)
	}
	w = req(t, svc, "GET", "/v1/files/ws1/stat?path=d2/child.txt", "tok-a", "", nil)
	if !strings.Contains(w.Body.String(), `"version":1`) {
		t.Fatalf("child version must follow the rename: %s", w.Body)
	}
	// recreating the old path with none must now succeed (row moved away)
	w = req(t, svc, "PUT", "/v1/files/ws1/write?path=d/fresh.txt", "tok-a", "x", map[string]string{"If-Version": "none"})
	if w.Code != 200 {
		t.Fatalf("fresh create under old dir must work, got %d %s", w.Code, w.Body)
	}
}

// M3: the staging prefix is reserved and a leftover staging file is swept.
func TestStagingHygiene(t *testing.T) {
	svc, dir := testSvc(t)
	w := req(t, svc, "PUT", "/v1/files/ws1/write?path=.filesv-tmp-user", "tok-a", "x", map[string]string{"If-Version": "any"})
	if w.Code != 400 {
		t.Fatalf("reserved name must 400, got %d", w.Code)
	}
	// a stale staging file older than the sweep cutoff is removed at startup
	os.MkdirAll(filepath.Join(dir, "ws1"), 0o755)
	stale := filepath.Join(dir, "ws1", stagingPrefix+"deadbeef")
	os.WriteFile(stale, []byte("partial"), 0o644)
	old := time.Now().Add(-time.Hour)
	os.Chtimes(stale, old, old)
	r, _ := newRoot(dir)
	r.sweepStaging()
	if _, err := os.Lstat(stale); !os.IsNotExist(err) {
		t.Fatal("stale staging file survived sweep")
	}
}

// L3: an in-scope symlink pointing outside is removable via the API.
func TestRemoveEscapingSymlink(t *testing.T) {
	svc, dir := testSvc(t)
	outside := filepath.Join(dir, "..", "target.txt")
	os.WriteFile(outside, []byte("x"), 0o644)
	os.MkdirAll(filepath.Join(dir, "ws1"), 0o755)
	os.Symlink(outside, filepath.Join(dir, "ws1", "bad-link"))
	w := req(t, svc, "DELETE", "/v1/files/ws1/remove?path=bad-link", "tok-a", "", map[string]string{"If-Version": "any"})
	if w.Code != 200 {
		t.Fatalf("remove symlink must succeed, got %d %s", w.Code, w.Body)
	}
	if _, err := os.Lstat(filepath.Join(dir, "ws1", "bad-link")); !os.IsNotExist(err) {
		t.Fatal("symlink not removed")
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatal("remove followed the symlink and deleted the target")
	}
}

// Cache-policy gate: a FUSE mount must prove zero metadata caching via the
// JuiceFS /.config control file; anything else mounted is refused.
func TestMountPolicyConfigCheck(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, ".config")
	good := `{"AttrTimeout":0,"EntryTimeout":0,"DirEntryTimeout":0,"NegEntryTimeout":0}`
	if err := checkZeroMetadataCache(cfg); !errors.Is(err, ErrMountPolicy) {
		t.Fatalf("missing config must fail closed, got %v", err)
	}
	os.WriteFile(cfg, []byte("not json"), 0o644)
	if err := checkZeroMetadataCache(cfg); !errors.Is(err, ErrMountPolicy) {
		t.Fatalf("unparseable config must fail, got %v", err)
	}
	os.WriteFile(cfg, []byte(good), 0o644)
	if err := checkZeroMetadataCache(cfg); err != nil {
		t.Fatalf("zero-cache config must pass, got %v", err)
	}
	os.WriteFile(cfg, []byte(`{"AttrTimeout":1e9,"EntryTimeout":0,"DirEntryTimeout":0,"NegEntryTimeout":0}`), 0o644)
	if err := checkZeroMetadataCache(cfg); !errors.Is(err, ErrMountPolicy) {
		t.Fatalf("1s attr cache must fail, got %v", err)
	}
	os.WriteFile(cfg, []byte(`{"AttrTimeout":0,"EntryTimeout":0}`), 0o644)
	if err := checkZeroMetadataCache(cfg); !errors.Is(err, ErrMountPolicy) {
		t.Fatalf("missing fields must fail, got %v", err)
	}
}

// A plain (unmounted) service root must be refused under RequireMount:
// statx resolves it on the parent mount whose mountpoint is not the root.
func TestRequireMountPlainDirRefused(t *testing.T) {
	r, err := newRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	svc, _ := NewAt(r.root, newFakeStore(), map[string]map[string]bool{"t": {"*": true}})
	svc.RequireMount()
	w := req(t, svc, "PUT", "/v1/files/ws1/write?path=x", "t", "x", map[string]string{"If-Version": "any"})
	if w.Code != 503 {
		t.Fatalf("unmounted root must 503, got %d", w.Code)
	}
}

// mountVerdict boundary cases — the mount the kernel resolves the service
// root to (identified by statx MNT_ID) must itself be a mountpoint at the
// root, and its filesystem must be provably fresh.
func TestMountVerdict(t *testing.T) {
	const root = "/data"

	t.Run("juicefs mount root passes to config check", func(t *testing.T) {
		e := mountInfoEntry{id: 10, root: "/", mountpoint: root, fstype: "fuse.juicefs"}
		if err := mountVerdict(e, root); err != nil {
			t.Fatalf("mount-root juicefs must pass verdict, got %v", err)
		}
	})

	// Opus F2: a JuiceFS subdirectory bind exposes a regular, user-writable
	// .config — forged zeros would pass the freshness check. Refuse it.
	t.Run("juicefs subdirectory bind refused", func(t *testing.T) {
		e := mountInfoEntry{id: 11, root: "/nsroot-a", mountpoint: root, fstype: "fuse.juicefs"}
		if err := mountVerdict(e, root); !errors.Is(err, ErrMountPolicy) {
			t.Fatalf("subdir bind must fail with mount_policy, got %v", err)
		}
	})

	// Opus F1 ancestor case: the visible mount covers an ancestor, so the
	// service root is not itself a mountpoint — writes would land on the
	// wrong filesystem. Unavailable, not policy: nothing was proved mounted.
	t.Run("ancestor overmount is unavailable", func(t *testing.T) {
		e := mountInfoEntry{id: 12, root: "/", mountpoint: "/data-parent", fstype: "tmpfs"}
		if err := mountVerdict(e, root); !errors.Is(err, ErrMountUnavailable) {
			t.Fatalf("ancestor mount must fail with mount_unavailable, got %v", err)
		}
	})

	t.Run("local coherent fs allowed", func(t *testing.T) {
		e := mountInfoEntry{id: 13, root: "/", mountpoint: root, fstype: "ext4"}
		if err := mountVerdict(e, root); err != nil {
			t.Fatalf("ext4 mount root must pass, got %v", err)
		}
	})

	t.Run("unknown and network fs refused", func(t *testing.T) {
		for _, fs := range []string{"nfs", "cifs", "9p", "fuse.somefs", "devpts"} {
			e := mountInfoEntry{id: 14, root: "/", mountpoint: root, fstype: fs}
			if err := mountVerdict(e, root); !errors.Is(err, ErrMountPolicy) {
				t.Fatalf("%s must fail with mount_policy, got %v", fs, err)
			}
		}
	})
}

// findMount selects by kernel mount ID, not by mountinfo order — Opus F1:
// a mount moved beneath the top (MOVE_MOUNT_BENEATH) is listed last, so
// last-match picks the hidden tmpfs and skips the visible JuiceFS check.
func TestFindMountSelectsVisibleByID(t *testing.T) {
	mi := `909 1 0:90 / /data rw - tmpfs tmpfs rw
910 1 0:91 / /data rw - fuse.juicefs jfs rw
943 1 0:92 / /data rw - tmpfs tmpfs rw`
	entries := parseMountInfo([]byte(mi))
	if len(entries) != 3 {
		t.Fatalf("want 3 entries, got %d", len(entries))
	}
	// statx at /data reports 910 (the visible top); the last-listed 943 is
	// the tmpfs hidden beneath. The verdict must see the JuiceFS entry.
	vis, ok := findMount(entries, 910)
	if !ok || vis.fstype != "fuse.juicefs" {
		t.Fatalf("visible mount must be juicefs entry 910, got %+v", vis)
	}
	if err := mountVerdict(vis, "/data"); err != nil {
		t.Fatalf("visible juicefs mount root must reach config check, got %v", err)
	}
}

// mountinfo escaping: space, tab, newline, and backslash appear as \040
// \011 \012 \134; all must decode for the mountpoint comparison (F3).
func TestMountInfoEscapes(t *testing.T) {
	mi := `800 1 0:80 / /data/with\040space rw - tmpfs tmpfs rw
801 1 0:81 / /data/with\011tab rw - tmpfs tmpfs rw
802 1 0:82 / /data/with\012nl rw - tmpfs tmpfs rw
803 1 0:83 / /data/with\134backslash rw - tmpfs tmpfs rw`
	entries := parseMountInfo([]byte(mi))
	want := map[uint64]string{
		800: "/data/with space",
		801: "/data/with\ttab",
		802: "/data/with\nnl",
		803: "/data/with\\backslash",
	}
	for id, mp := range want {
		e, ok := findMount(entries, id)
		if !ok || e.mountpoint != mp {
			t.Fatalf("id %d: want mountpoint %q, got %+v", id, mp, e)
		}
	}
}

// --- Operation-boundary tests (f-fabric-cloud-39/40/41, A F1/F2/F3) ---

// f33/A F2: an ObservedVersion failure must surface as a clear 503, never
// as version 0 / external_change:false.
func TestStatStoreOutageIs503NotZeroVersion(t *testing.T) {
	dir := t.TempDir()
	st := newFakeStore()
	svc, err := NewAt(dir, st, map[string]map[string]bool{"tok-a": {"ws1": true}})
	if err != nil {
		t.Fatal(err)
	}
	req(t, svc, "PUT", "/v1/files/ws1/write?path=a.txt", "tok-a", "hi", map[string]string{"If-Version": "any"})
	st.obsErr = errors.New("pg down")
	w := req(t, svc, "GET", "/v1/files/ws1/stat?path=a.txt", "tok-a", "", nil)
	if w.Code != 503 || !strings.Contains(w.Body.String(), "store_unavailable") {
		t.Fatalf("stat during outage: %d %s", w.Code, w.Body)
	}
	w = req(t, svc, "GET", "/v1/files/ws1/read?path=a.txt", "tok-a", "", nil)
	if w.Code != 503 {
		t.Fatalf("read during outage: %d %s", w.Code, w.Body)
	}
	st.obsErr = nil
	w = req(t, svc, "GET", "/v1/files/ws1/stat?path=a.txt", "tok-a", "", nil)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"version":1`) {
		t.Fatalf("stat after recovery: %d %s", w.Code, w.Body)
	}
	w = req(t, svc, "GET", "/v1/files/ws1/read?path=a.txt", "tok-a", "", nil)
	if w.Code != 200 || w.Body.String() != "hi" {
		t.Fatalf("read after recovery: %d %q", w.Code, w.Body)
	}
}

// f34/A F1: a FIFO (or any special file) must be refused fast, and a
// FIFO swapped in between mount check and open must not wedge the handler.
func TestSpecialFileRefused(t *testing.T) {
	svc, dir := testSvc(t)
	fifo := filepath.Join(dir, "ws1", "pipe")
	if err := os.MkdirAll(filepath.Dir(fifo), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(fifo, 0o644); err != nil {
		t.Skipf("mkfifo: %v", err)
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- req(t, svc, "GET", "/v1/files/ws1/read?path=pipe", "tok-a", "", nil)
	}()
	select {
	case w := <-done:
		if w.Code != 400 || !strings.Contains(w.Body.String(), "wrong_kind") {
			t.Fatalf("fifo read: %d %s", w.Code, w.Body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("fifo read wedged the handler")
	}
	w := req(t, svc, "GET", "/v1/files/ws1/stat?path=pipe", "tok-a", "", nil)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"kind":"special"`) {
		t.Fatalf("fifo stat should report special, got %d %s", w.Code, w.Body)
	}
}

// A F3: a dangling symlink is safe to overwrite — the publish rename
// replaces the link itself; it is not followed.
func TestWriteOverDanglingSymlink(t *testing.T) {
	svc, dir := testSvc(t)
	link := filepath.Join(dir, "ws1", "dangling")
	os.MkdirAll(filepath.Dir(link), 0o755)
	if err := os.Symlink("/nonexistent-target", link); err != nil {
		t.Fatal(err)
	}
	w := req(t, svc, "PUT", "/v1/files/ws1/write?path=dangling", "tok-a", "x", map[string]string{"If-Version": "any"})
	if w.Code != 200 {
		t.Fatalf("write over dangling symlink: %d %s", w.Code, w.Body)
	}
	if fi, err := os.Lstat(link); err != nil || fi.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("symlink should be replaced by a regular file: %v %+v", err, fi)
	}
	w = req(t, svc, "GET", "/v1/files/ws1/read?path=dangling", "tok-a", "", nil)
	if w.Code != 200 || w.Body.String() != "x" {
		t.Fatalf("read: %d %q", w.Code, w.Body)
	}
}

// f39/B F1: escaping symlinks are refused at use time by the kernel —
// RESOLVE_BENEATH cannot be raced by a directory/symlink swap.
func TestEscapingSymlinkRefused(t *testing.T) {
	svc, dir := testSvc(t)
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret"), []byte("s"), 0o644); err != nil {
		t.Fatal(err)
	}
	os.MkdirAll(filepath.Join(dir, "ws1"), 0o755)
	os.Symlink(outside, filepath.Join(dir, "ws1", "out"))
	for _, op := range []string{"stat", "read", "list"} {
		w := req(t, svc, "GET", "/v1/files/ws1/"+op+"?path=out/secret", "tok-a", "", nil)
		if w.Code != 403 {
			t.Fatalf("%s through escaping symlink: %d %s", op, w.Code, w.Body)
		}
	}
	w := req(t, svc, "PUT", "/v1/files/ws1/write?path=out/newfile", "tok-a", "x", map[string]string{"If-Version": "any"})
	if w.Code != 403 {
		t.Fatalf("write through escaping symlink: %d %s", w.Code, w.Body)
	}
	if _, err := os.Stat(filepath.Join(outside, "newfile")); !os.IsNotExist(err) {
		t.Fatal("write escaped the scope root")
	}
}

// f41/B F3: directory-over-empty-directory rename succeeds (kernel
// semantics — os.Rename's userspace pre-check refused this).
func TestRenameDirOverEmptyDir(t *testing.T) {
	svc, _ := testSvc(t)
	req(t, svc, "POST", "/v1/files/ws1/mkdir", "tok-a", `{"path":"src/inner"}`, nil)
	req(t, svc, "PUT", "/v1/files/ws1/write?path=src/inner/f", "tok-a", "c", map[string]string{"If-Version": "any"})
	req(t, svc, "POST", "/v1/files/ws1/mkdir", "tok-a", `{"path":"dst"}`, nil)
	w := req(t, svc, "POST", "/v1/files/ws1/rename", "tok-a",
		`{"from":"src","to":"dst","if_version":"any"}`, nil)
	if w.Code != 200 {
		t.Fatalf("dir-over-empty-dir rename: %d %s", w.Code, w.Body)
	}
	w = req(t, svc, "GET", "/v1/files/ws1/read?path=dst/inner/f", "tok-a", "", nil)
	if w.Code != 200 || w.Body.String() != "c" {
		t.Fatalf("moved content: %d %q", w.Code, w.Body)
	}
}

// f41/B F4: kind mismatches report wrong_kind, not a generic error.
func TestRenameKindMismatch(t *testing.T) {
	svc, _ := testSvc(t)
	req(t, svc, "PUT", "/v1/files/ws1/write?path=f", "tok-a", "x", map[string]string{"If-Version": "any"})
	req(t, svc, "POST", "/v1/files/ws1/mkdir", "tok-a", `{"path":"d"}`, nil)
	req(t, svc, "POST", "/v1/files/ws1/mkdir", "tok-a", `{"path":"nonempty/inner"}`, nil)
	for _, tc := range []struct{ from, to string }{
		{"f", "d"},        // file over dir
		{"d", "f"},        // dir over file
		{"d", "nonempty"}, // dir over non-empty dir
	} {
		w := req(t, svc, "POST", "/v1/files/ws1/rename", "tok-a",
			fmt.Sprintf(`{"from":%q,"to":%q,"if_version":"any"}`, tc.from, tc.to), nil)
		want := 400
		code := "wrong_kind"
		if tc.to == "nonempty" {
			want, code = 409, "dir_not_empty"
		}
		if w.Code != want || !strings.Contains(w.Body.String(), code) {
			t.Fatalf("rename %s -> %s: want %d %s, got %d %s", tc.from, tc.to, want, code, w.Code, w.Body)
		}
	}
}

// Traversal and absolute paths stay refused under the fd-relative boundary.
func TestTraversalRefused(t *testing.T) {
	svc, _ := testSvc(t)
	for _, p := range []string{"../x", "a/../../x", "..", "/etc/passwd"} {
		w := req(t, svc, "GET", "/v1/files/ws1/stat?path="+p, "tok-a", "", nil)
		if w.Code == 200 {
			t.Fatalf("path %q should not resolve", p)
		}
	}
}

// f48: a stale version token must never match a different object
// incarnation. Versions mint from one global sequence — a rename-overwrite
// or delete/recreate can never land on a value a client already holds.
func TestVersionABAOnRenameOverwrite(t *testing.T) {
	svc, _ := testSvc(t)
	req(t, svc, "PUT", "/v1/files/ws1/write?path=doc", "tok-a", "A", map[string]string{"If-Version": "none"})
	w := req(t, svc, "PUT", "/v1/files/ws1/write?path=doc", "tok-a", "A2", map[string]string{"If-Version": "1"})
	if w.Code != 200 {
		t.Fatalf("second write: %d %s", w.Code, w.Body)
	}
	// doc is incarnation A at version 2. Object B (tmp) gets version 3.
	req(t, svc, "PUT", "/v1/files/ws1/write?path=tmp", "tok-a", "B", map[string]string{"If-Version": "none"})
	w = req(t, svc, "POST", "/v1/files/ws1/rename", "tok-a", `{"from":"tmp","to":"doc","if_version":"any"}`, nil)
	if w.Code != 200 {
		t.Fatalf("rename-overwrite: %d %s", w.Code, w.Body)
	}
	// doc is now incarnation B at a fresh version (4). A's token (2) must
	// not apply — under path-local versioning the rename minted exactly 2.
	w = req(t, svc, "PUT", "/v1/files/ws1/write?path=doc", "tok-a", "stale", map[string]string{"If-Version": "2"})
	if w.Code != 409 {
		t.Fatalf("stale token against new incarnation must 409, got %d", w.Code)
	}
}

// f48: delete/recreate must not resurrect old tokens either.
func TestVersionABAOnDeleteRecreate(t *testing.T) {
	svc, _ := testSvc(t)
	req(t, svc, "PUT", "/v1/files/ws1/write?path=f", "tok-a", "one", map[string]string{"If-Version": "none"})
	w := req(t, svc, "DELETE", "/v1/files/ws1/remove?path=f", "tok-a", "", map[string]string{"If-Version": "any"})
	if w.Code != 200 {
		t.Fatalf("remove: %d %s", w.Code, w.Body)
	}
	w = req(t, svc, "PUT", "/v1/files/ws1/write?path=f", "tok-a", "two", map[string]string{"If-Version": "none"})
	if w.Code != 200 {
		t.Fatalf("recreate: %d %s", w.Code, w.Body)
	}
	// A client holding version 1 of the deleted file must not CAS the new one.
	w = req(t, svc, "PUT", "/v1/files/ws1/write?path=f", "tok-a", "stale", map[string]string{"If-Version": "1"})
	if w.Code != 409 {
		t.Fatalf("stale token after delete/recreate must 409, got %d", w.Code)
	}
}

// f54: a scope name that is a symlink to another scope must be refused —
// token separation survives; ordinary in-scope symlinks still resolve.
func TestScopeNameSymlinkRefused(t *testing.T) {
	svc, dir := testSvc(t)
	os.MkdirAll(filepath.Join(dir, "ws1"), 0o755)
	os.WriteFile(filepath.Join(dir, "ws1", "secret.txt"), []byte("s"), 0o644)
	if err := os.Symlink("ws1", filepath.Join(dir, "ws2")); err != nil {
		t.Fatal(err)
	}
	// tok-b legitimately grants ws2 — the alias must not expose ws1 files.
	w := req(t, svc, "GET", "/v1/files/ws2/read?path=secret.txt", "tok-b", "", nil)
	if w.Code == 200 {
		t.Fatal("scope-name symlink granted cross-scope access")
	}
	// Ordinary in-scope symlinks still work.
	os.Symlink("secret.txt", filepath.Join(dir, "ws1", "link.txt"))
	w = req(t, svc, "GET", "/v1/files/ws1/read?path=link.txt", "tok-a", "", nil)
	if w.Code != 200 || w.Body.String() != "s" {
		t.Fatalf("in-scope symlink must resolve: %d %q", w.Code, w.Body)
	}
}

// f49: list must report true kinds (dir/file/symlink/special) — the raw
// Stat_t.Mode cast used to misclassify everything.
func TestListKinds(t *testing.T) {
	svc, dir := testSvc(t)
	os.MkdirAll(filepath.Join(dir, "ws1", "d"), 0o755)
	os.WriteFile(filepath.Join(dir, "ws1", "f"), []byte("x"), 0o644)
	if err := syscall.Mkfifo(filepath.Join(dir, "ws1", "p"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("f", filepath.Join(dir, "ws1", "l")); err != nil {
		t.Fatal(err)
	}
	w := req(t, svc, "GET", "/v1/files/ws1/list?path=", "tok-a", "", nil)
	if w.Code != 200 {
		t.Fatalf("list: %d %s", w.Code, w.Body)
	}
	var body struct {
		Entries []ListEntry `json:"entries"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	kinds := map[string]string{}
	for _, e := range body.Entries {
		kinds[e.Name] = e.Kind
	}
	want := map[string]string{"d": "dir", "f": "file", "p": "special", "l": "symlink"}
	for n, k := range want {
		if kinds[n] != k {
			t.Fatalf("kind of %s: want %q got %q (all: %v)", n, k, kinds[n], kinds)
		}
	}
}

// f53: reading a socket must be 400 wrong_kind, not a generic 500.
func TestSocketReadWrongKind(t *testing.T) {
	svc, dir := testSvc(t)
	os.MkdirAll(filepath.Join(dir, "ws1"), 0o755)
	fd, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Bind(fd, &syscall.SockaddrUnix{Name: filepath.Join(dir, "ws1", "sock")}); err != nil {
		t.Fatal(err)
	}
	syscall.Close(fd)
	w := req(t, svc, "GET", "/v1/files/ws1/read?path=sock", "tok-a", "", nil)
	if w.Code != 400 || !strings.Contains(w.Body.String(), "wrong_kind") {
		t.Fatalf("socket read: want 400 wrong_kind, got %d %s", w.Code, w.Body)
	}
	w = req(t, svc, "GET", "/v1/files/ws1/stat?path=sock", "tok-a", "", nil)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"kind":"special"`) {
		t.Fatalf("socket stat: want kind=special, got %d %s", w.Code, w.Body)
	}
}

// f49: staging sweep must descend into subdirectories — the mode bug left
// nested leftover staging files behind.
func TestSweepNestedStaging(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "ws1", "sub")
	os.MkdirAll(sub, 0o755)
	stale := filepath.Join(sub, stagingPrefix+"abc")
	os.WriteFile(stale, []byte("x"), 0o644)
	old := time.Now().Add(-time.Hour)
	os.Chtimes(stale, old, old)
	if _, err := NewAt(dir, newFakeStore(), map[string]map[string]bool{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("nested staging file not swept: %v", err)
	}
}
