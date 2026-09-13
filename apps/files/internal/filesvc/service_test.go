package filesvc

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
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

func (f *fakeStore) bump(scope, path string, iv IfVersion) (int64, error) {
	k := scope + "/" + path
	cur := f.vers[k]
	switch iv.Mode {
	case "none":
		if cur != 0 {
			return 0, ErrConflict
		}
	case "eq":
		if cur == 0 && iv.Version != 0 {
			return 0, ErrNoSuchFile
		}
		if cur != iv.Version {
			return 0, ErrConflict
		}
	case "any":
	default:
		return 0, fmt.Errorf("bad mode")
	}
	f.vers[k] = cur + 1
	return cur + 1, nil
}

func (f *fakeStore) WithWrite(ctx context.Context, scope, path, op string, iv IfVersion, fn func() (FileInfo, error)) (int64, FileInfo, error) {
	ver, err := f.bump(scope, path, iv)
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

func (f *fakeStore) Rename(ctx context.Context, scope, from, to string, iv IfVersion, fn func() (FileInfo, error)) (int64, FileInfo, error) {
	ver, err := f.bump(scope, to, iv)
	if err != nil {
		return 0, FileInfo{}, err
	}
	info, err := fn()
	if err != nil {
		f.vers[scope+"/"+to]--
		return 0, FileInfo{}, err
	}
	f.fps[scope+"/"+to] = info.Fingerprint
	delete(f.vers, scope+"/"+from)
	delete(f.fps, scope+"/"+from)
	f.evs = append(f.evs, Event{Seq: int64(len(f.evs) + 1), Path: to, Op: "rename", Version: ver})
	return ver, info, nil
}

func (f *fakeStore) Remove(ctx context.Context, scope, path string, iv IfVersion, fn func() error) error {
	k := scope + "/" + path
	cur := f.vers[k]
	if cur == 0 && (iv.Mode == "eq" || iv.Mode == "none") {
		return ErrNoSuchFile
	}
	if iv.Mode == "eq" && cur != iv.Version {
		return ErrConflict
	}
	if err := fn(); err != nil {
		return err
	}
	delete(f.vers, k)
	delete(f.fps, k)
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
