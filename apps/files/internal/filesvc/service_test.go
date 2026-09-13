package filesvc

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeStore is an in-memory VersionStore for handler tests — it exercises the
// same CAS semantics (monotonic bump, conflict on stale if_version) without PG.
type fakeStore struct {
	vers map[string]int64
	fps  map[string]string
	evs  []Event
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
	f.vers[k] = cur + 1
	return cur + 1, nil
}

func (f *fakeStore) WithWrite(ctx context.Context, scope, path, op string, iv IfVersion, probe FPProbe, fn func() (FileInfo, error)) (int64, FileInfo, error) {
	ver, err := f.bump(scope, path, iv, probe)
	if err != nil {
		return 0, FileInfo{}, err
	}
	info, err := fn()
	if err != nil {
		f.vers[scope+"/"+path]-- // roll back the bump on FS failure
		return 0, FileInfo{}, err
	}
	f.fps[scope+"/"+path] = info.Fingerprint
	f.evs = append(f.evs, Event{Seq: int64(len(f.evs) + 1), Path: path, Op: op, Version: ver})
	return ver, info, nil
}

func (f *fakeStore) Rename(ctx context.Context, scope, from, to string, iv IfVersion, probe FPProbe, fn func() (FileInfo, error)) (int64, FileInfo, error) {
	ver, err := f.bump(scope, to, iv, probe)
	if err != nil {
		return 0, FileInfo{}, err
	}
	info, err := fn()
	if err != nil {
		f.vers[scope+"/"+to]--
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
	f.evs = append(f.evs, Event{Seq: int64(len(f.evs) + 1), Path: path, Op: "remove", Version: cur + 1})
	return nil
}

func (f *fakeStore) ObservedVersion(ctx context.Context, scope, path string) (int64, string, error) {
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
