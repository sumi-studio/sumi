package main

// File-mode coverage for the return mover over the real session/HTTP
// path. The storage backend is a fixture implementing the filesvc wire
// contract (list/stat/read/write/freeze) — the real filesvc's own PG
// tests cover its internals; what these tests prove is the mover's
// copy/credential behavior through the real grant-authorized routes.

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sumi-studio/sumi/apps/api/internal/fileaccess"
	"github.com/sumi-studio/sumi/apps/api/internal/returnsession/returnsessiontest"
)

// fakeFilesvc answers the subset of the filesvc API the return flow
// uses: list/stat/read for enumeration and copy, freeze/unfreeze for the
// source fence, and a mutation refusal while frozen. Its bytes are real
// — the copier's per-file digest check runs against actual content.
type fakeFilesvc struct {
	t       *testing.T
	files   map[string][]byte // "scope/path" -> content
	dirs    map[string]bool   // "scope/path" -> dir marker
	extras  map[string]string // "scope/path" -> reported kind (symlink, ...)
	sizes   map[string]int64  // "scope/path" -> reported list size override
	frozen  map[string]bool   // scope -> frozen
	freezes []string          // recorded fence transitions
	failOps map[string]int    // "op:scope/path" -> remaining 503s
	caps    map[string]*fakeCapture
	capN    int
}

// fakeCapRow is one manifest row of a fake immutable capture — the
// snapshot of the scope's namespace taken at create time.
type fakeCapRow struct {
	seq    int64
	path   string
	typ    string
	sup    bool
	data   []byte
	length int64
}

// fakeCapture is a durable manifest binding: the rows are frozen at
// create; reads answer the snapshot even if the store mutates.
type fakeCapture struct {
	id, scope, owner string
	epoch            int64
	scopeID, sha     string
	rows             []fakeCapRow
	active           bool
}

// captureManifest snapshots the scope's namespace into capture rows:
// regular files and directories supported; extras carry their recorded
// kind as an UNSUPPORTED type so the copy refuses them visibly.
func (f *fakeFilesvc) captureManifest(scope string) []fakeCapRow {
	var rows []fakeCapRow
	for k, data := range f.files {
		rel := strings.TrimPrefix(k, scope+"/")
		if rel == k {
			continue
		}
		length := int64(len(data))
		if sz, ok := f.sizes[k]; ok {
			length = sz
		}
		rows = append(rows, fakeCapRow{path: rel, typ: "file", sup: true, data: data, length: length})
	}
	for k := range f.dirs {
		rel := strings.TrimPrefix(k, scope+"/")
		if rel == k {
			continue
		}
		rows = append(rows, fakeCapRow{path: rel, typ: "dir", sup: true})
	}
	for k, kind := range f.extras {
		rel := strings.TrimPrefix(k, scope+"/")
		if rel == k {
			continue
		}
		rows = append(rows, fakeCapRow{path: rel, typ: kind, sup: false})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].path < rows[j].path })
	for i := range rows {
		rows[i].seq = int64(i + 1)
	}
	return rows
}

func fakeScopeID(scope string) string {
	sum := sha256.Sum256([]byte("scope:" + scope))
	return "si:" + hex.EncodeToString(sum[:16])
}

func capMeta(c *fakeCapture) map[string]any {
	return map[string]any{
		"capture_id": c.id, "scope": c.scope, "scope_id": c.scopeID,
		"owner": c.owner, "owner_epoch": c.epoch, "manifest_sha": c.sha,
		"status":     map[bool]string{true: "active", false: "released"}[c.active],
		"entries":    len(c.rows), "unsupported": 0,
		"created_at": "2026-01-01T00:00:00Z", "expires_at": "2027-01-01T00:00:00Z",
	}
}

func newFakeFilesvc(t *testing.T) *fakeFilesvc {
	return &fakeFilesvc{
		t: t, files: map[string][]byte{},
		dirs: map[string]bool{}, extras: map[string]string{},
		sizes: map[string]int64{}, frozen: map[string]bool{},
		failOps: map[string]int{}, caps: map[string]*fakeCapture{}}
}

func (f *fakeFilesvc) put(scope, path, content string) {
	f.files[scope+"/"+path] = []byte(content)
}

// failNext makes the next n calls of op on path answer 503 — the mover
// sees unreachable and the copy must wait, never fake progress.
func (f *fakeFilesvc) failNext(op, scope, path string, n int) {
	f.failOps[op+":"+scope+"/"+path] = n
}

func (f *fakeFilesvc) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// /v1/files/{scope}/{op} and /v1/capture/{id}[/op]
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) >= 3 && parts[0] == "v1" && parts[1] == "capture" {
			f.serveCapture(w, r, parts[2:])
			return
		}
		if len(parts) != 4 || parts[0] != "v1" || parts[1] != "files" {
			http.Error(w, "bad path", http.StatusNotFound)
			return
		}
		scope, op := parts[2], parts[3]
		q := r.URL.Query()
		path := strings.Trim(q.Get("path"), "/")
		write := func(code int, v any) {
			raw, _ := json.Marshal(v)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			_, _ = w.Write(raw)
		}
		if n := f.failOps[op+":"+scope+"/"+path]; n > 0 {
			f.failOps[op+":"+scope+"/"+path] = n - 1
			write(503, map[string]any{"error": "fixture failure"})
			return
		}
		switch op {
		case "capture":
			if r.Method != http.MethodPost {
				write(405, map[string]any{"error": "method"})
				return
			}
			sid := fakeScopeID(scope)
			if es := q.Get("expected_scope_id"); es != "" && es != sid {
				write(422, map[string]any{
					"error": "scope anchor does not match expected scope identity",
					"code":  "scope_changed"})
				return
			}
			rows := f.captureManifest(scope)
			h := sha256.New()
			for _, row := range rows {
				fmt.Fprintf(h, "%d|%s|%s|%t|%d\n", row.seq, row.path, row.typ, row.sup, row.length)
			}
			f.capN++
			c := &fakeCapture{
				id: fmt.Sprintf("cap-fake-%d", f.capN), scope: scope,
				owner: q.Get("owner"), scopeID: sid,
				sha: hex.EncodeToString(h.Sum(nil)), rows: rows, active: true}
			fmt.Sscan(q.Get("epoch"), &c.epoch)
			f.caps[c.id] = c
			write(200, capMeta(c))
			return
		case "freeze", "unfreeze":
			f.frozen[scope] = op == "freeze"
			f.freezes = append(f.freezes, scope+"="+op)
			write(200, map[string]any{"scope": scope, "frozen": op == "freeze"})
			return
		}
		if f.frozen[scope] && (op == "write" || op == "mkdir" || op == "remove" || op == "rename") {
			write(409, map[string]any{"error": "scope_frozen"})
			return
		}
		switch op {
		case "list":
			base := strings.Trim(q.Get("path"), "/")
			type entry struct {
				Name    string `json:"name"`
				Kind    string `json:"kind"`
				Size    int64  `json:"size"`
				MtimeNs int64  `json:"mtime_ns"`
			}
			var names []string
			seen := map[string]bool{}
			add := func(full string, dir bool) {
				rel := strings.TrimPrefix(strings.TrimPrefix(full, scope), "/")
				if base != "" && !strings.HasPrefix(rel, base+"/") && rel != base {
					return
				}
				rest := strings.TrimPrefix(rel, base)
				rest = strings.TrimPrefix(rest, "/")
				if rest == "" {
					return
				}
				seg := strings.SplitN(rest, "/", 2)[0]
				name := seg
				if seen[name] {
					return
				}
				seen[name] = true
				if dir || strings.Contains(rest, "/") {
					names = append(names, name+":dir")
				} else {
					names = append(names, name+":file")
				}
			}
			for k := range f.files {
				add(k, false)
			}
			for k := range f.dirs {
				add(k, true)
			}
			for k, kind := range f.extras {
				rel := strings.TrimPrefix(strings.TrimPrefix(k, scope), "/")
				if base != "" && !strings.HasPrefix(rel, base+"/") && rel != base {
					continue
				}
				rest := strings.TrimPrefix(rel, base)
				rest = strings.TrimPrefix(rest, "/")
				if rest == "" || strings.Contains(rest, "/") || seen[rest] {
					continue
				}
				seen[rest] = true
				names = append(names, rest+":"+kind)
			}
			sort.Strings(names)
			entries := []entry{}
			for _, n := range names {
				parts := strings.SplitN(n, ":", 2)
				e := entry{Name: parts[0], Kind: parts[1]}
				if e.Kind == "file" {
					full := scope + "/" + filepath.Join(base, e.Name)
					if sz, ok := f.sizes[full]; ok {
						e.Size = sz
					} else {
						e.Size = int64(len(f.files[full]))
					}
				}
				entries = append(entries, e)
			}
			write(200, map[string]any{"entries": entries})
		case "stat":
			key := scope + "/" + strings.Trim(q.Get("path"), "/")
			content, ok := f.files[key]
			if !ok {
				write(404, map[string]any{"error": "not_found"})
				return
			}
			sum := sha256.Sum256(content)
			write(200, map[string]any{
				"kind": "file", "size": len(content), "version": 1,
				"content_sha": hex.EncodeToString(sum[:]),
			})
		case "read":
			key := scope + "/" + strings.Trim(q.Get("path"), "/")
			content, ok := f.files[key]
			if !ok {
				write(404, map[string]any{"error": "not_found"})
				return
			}
			w.WriteHeader(200)
			_, _ = w.Write(content)
		case "write":
			// Unused by the copy (it never writes the source) but the
			// surface exists so a stray write is visible, not silently
			// dropped.
			body := make([]byte, r.ContentLength)
			_, _ = r.Body.Read(body)
			f.files[scope+"/"+strings.Trim(q.Get("path"), "/")] = body
			write(200, map[string]any{"version": 1})
		default:
			http.Error(w, "unknown op", http.StatusNotFound)
		}
	})
}

// serveCapture answers /v1/capture/{id}[/op]: durable manifest metadata,
// paged entries, captured row bytes and release. Reads honour failOps
// keyed "read:<scope>/<path>" like the live-read fixture did.
func (f *fakeFilesvc) serveCapture(w http.ResponseWriter, r *http.Request, rest []string) {
	write := func(code int, v any) {
		raw, _ := json.Marshal(v)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_, _ = w.Write(raw)
	}
	c := f.caps[rest[0]]
	if c == nil {
		write(404, map[string]any{"error": "capture_not_found", "code": "capture_not_found"})
		return
	}
	op := ""
	if len(rest) > 1 {
		op = rest[1]
	}
	if !c.active && op != "" {
		write(410, map[string]any{"error": "capture_gone", "code": "capture_gone"})
		return
	}
	q := r.URL.Query()
	switch {
	case op == "" && r.Method == http.MethodGet:
		write(200, capMeta(c))
	case op == "" && r.Method == http.MethodDelete:
		c.active = false
		write(200, map[string]any{"released": true})
	case op == "entries" && r.Method == http.MethodGet:
		cursor, _ := strconv.ParseInt(q.Get("cursor"), 10, 64)
		limit, _ := strconv.Atoi(q.Get("limit"))
		if limit < 1 {
			limit = 500
		}
		var entries []map[string]any
		var next int64
		for _, row := range c.rows {
			if row.seq <= cursor || len(entries) >= limit {
				continue
			}
			base := row.path
			if i := strings.LastIndexByte(base, '/'); i >= 0 {
				base = base[i+1:]
			}
			entries = append(entries, map[string]any{
				"seq": row.seq,
				"path_b64": base64.StdEncoding.EncodeToString([]byte(row.path)),
				"name_b64": base64.StdEncoding.EncodeToString([]byte(base)),
				"path": row.path, "name": base,
				"type": row.typ, "supported": row.sup,
				"length": row.length, "mode": 0o100644,
			})
			next = row.seq
		}
		write(200, map[string]any{
			"entries": entries, "has_more": len(entries) == limit, "next_cursor": next})
	case op == "read" && r.Method == http.MethodGet:
		seq, _ := strconv.ParseInt(q.Get("seq"), 10, 64)
		var row *fakeCapRow
		for i := range c.rows {
			if c.rows[i].seq == seq {
				row = &c.rows[i]
				break
			}
		}
		if row == nil || row.typ != "file" {
			write(404, map[string]any{"error": "capture_pending", "code": "capture_pending"})
			return
		}
		if n := f.failOps["read:"+c.scope+"/"+row.path]; n > 0 {
			f.failOps["read:"+c.scope+"/"+row.path] = n - 1
			write(503, map[string]any{"error": "fixture failure"})
			return
		}
		data := row.data
		if off, _ := strconv.ParseInt(q.Get("offset"), 10, 64); off > 0 {
			if off >= int64(len(data)) {
				data = nil
			} else {
				data = data[off:]
			}
		}
		if ln, _ := strconv.ParseInt(q.Get("len"), 10, 64); ln > 0 && ln < int64(len(data)) {
			data = data[:ln]
		}
		w.WriteHeader(200)
		_, _ = w.Write(data)
	default:
		http.Error(w, "bad capture op", http.StatusNotFound)
	}
}

// wireFiles points the session service + server at a fixture file
// service: the grant file ops, the freeze fence and the token proxy
// all run over real HTTP against it.
func (h *retHarness) wireFiles(t *testing.T, f *fakeFilesvc) *httptest.Server {
	t.Helper()
	fsrv := httptest.NewServer(f.handler())
	t.Cleanup(fsrv.Close)
	client, err := fileaccess.NewClient(fsrv.URL, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	h.sessions.SetFileStore(client)
	h.sessions.SetCaptureStore(client)
	h.server.SetFiles(client)
	h.server.RegisterFileProxy(h.mux)
	return fsrv
}

func (h *retHarness) newReturnMode(mode string) (sessionID, returnURL string) {
	h.t.Helper()
	req, err := http.NewRequestWithContext(h.ctx, http.MethodPost,
		h.srv.URL+"/api/secretary-return/sessions",
		strings.NewReader(fmt.Sprintf(`{"file_mode":%q}`, mode)))
	if err != nil {
		h.t.Fatal(err)
	}
	req.Header.Set("Cookie", returnsessiontest.Cookie+"=owner-cookie")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	var body struct {
		ReturnURL string `json:"return_url"`
		Session   struct {
			SessionID string `json:"session_id"`
		} `json:"session"`
	}
	if res.StatusCode != http.StatusCreated || json.Unmarshal(raw, &body) != nil || body.ReturnURL == "" {
		h.t.Fatalf("create mode=%s: %d %s", mode, res.StatusCode, raw)
	}
	return body.Session.SessionID, body.ReturnURL
}

// --- cloud mode ---------------------------------------------------------------

// A completed cloud-mode return mints the scoped credential and writes
// the install's file client config — the running service picks it up on
// its next start through config.env.
func TestReturnCloudModeConfiguresScopedCredential(t *testing.T) {
	h := setupReturn(t)
	h.wireFiles(t, newFakeFilesvc(t))
	_, returnURL := h.newReturnMode("cloud")
	config := writeConfig(t, h.home, h.slot)

	m, out := h.mover()
	code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false)
	if code != exitDone {
		t.Fatalf("return: %d\n%s", code, out)
	}
	cfg, err := os.ReadFile(config)
	if err != nil {
		t.Fatal(err)
	}
	text := string(cfg)
	if !strings.Contains(text, "SUMI_FILESVC_URL=") ||
		!strings.Contains(text, "/api/secretary-files") {
		t.Fatalf("config missing cloud file URL:\n%s", text)
	}
	if !strings.Contains(text, "SUMI_FILESVC_TOKEN='sft_") {
		t.Fatalf("config missing scoped token:\n%s", text)
	}
	// The evidence file records where the working store lives.
	ev, err := os.ReadFile(filepath.Join(h.home, "return", "files.json"))
	if err != nil {
		t.Fatalf("files evidence: %v", err)
	}
	if !strings.Contains(string(ev), `"file_mode": "cloud"`) {
		t.Fatalf("evidence: %s", ev)
	}
	// And the minted token actually authorizes on the Cloud side.
	var tok string
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "SUMI_FILESVC_TOKEN=") {
			tok = strings.Trim(strings.TrimPrefix(line, "SUMI_FILESVC_TOKEN="), "'")
		}
	}
	if _, scope, err := h.sessions.AuthorizeFileToken(h.ctx, tok); err != nil {
		t.Fatalf("minted token does not authorize: %v", err)
	} else {
		want, _ := fileaccess.ScopeForPersona(h.pid)
		if scope != want {
			t.Fatalf("token scope %s, want %s", scope, want)
		}
	}
}

// --- local mode ---------------------------------------------------------------

// A completed local-mode return copies the frozen Cloud workspace into
// the install's workspace root, byte-verified, and leaves the Cloud
// bytes retained.
func TestReturnLocalModeCopiesVerifiedFiles(t *testing.T) {
	h := setupReturn(t)
	f := newFakeFilesvc(t)
	h.wireFiles(t, f)
	scope, _ := fileaccess.ScopeForPersona(h.pid)
	f.put(scope, "notes/today.txt", "会議メモ")
	f.put(scope, "hello.txt", "hello world")
	f.dirs[scope+"/empty-dir"] = true
	ws := t.TempDir()
	config := writeConfig(t, h.home, h.slot)

	_, returnURL := h.newReturnMode("local")
	m, out := h.mover()
	m.wsRoot = ws
	code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false)
	if code != exitDone {
		t.Fatalf("return: %d\n%s", code, out)
	}
	got, err := os.ReadFile(filepath.Join(ws, strings.ReplaceAll(h.pid, "-", ""), "notes/today.txt"))
	if err != nil || string(got) != "会議メモ" {
		t.Fatalf("nested file: %v %q", err, got)
	}
	got, err = os.ReadFile(filepath.Join(ws, strings.ReplaceAll(h.pid, "-", ""), "hello.txt"))
	if err != nil || string(got) != "hello world" {
		t.Fatalf("root file: %v %q", err, got)
	}
	if fi, err := os.Stat(filepath.Join(ws, strings.ReplaceAll(h.pid, "-", ""), "empty-dir")); err != nil || !fi.IsDir() {
		t.Fatalf("empty dir not carried: %v", err)
	}
	// Source untouched — retained, not synced or deleted.
	if string(f.files[scope+"/hello.txt"]) != "hello world" {
		t.Fatal("source file changed")
	}
	// The fence actually ran against the source scope — and stays: a
	// completed local return's barrier is what makes the retained Cloud
	// copy read-only (it clears only when a newer return takes over).
	joined := strings.Join(f.freezes, ",")
	if !strings.Contains(joined, scope+"=freeze") {
		t.Fatalf("fence transitions %v — want freeze of %s", f.freezes, scope)
	}
	if !f.frozen[scope] {
		t.Fatalf("retained Cloud copy lost its read-only barrier: %v", f.freezes)
	}
	// Evidence marks Cloud's copy retained, never synced/backup.
	ev, _ := os.ReadFile(filepath.Join(h.home, "return", "files.json"))
	if !strings.Contains(string(ev), "retained read-only") {
		t.Fatalf("evidence: %s", ev)
	}
	// No staging debris remains.
	matches, _ := filepath.Glob(filepath.Join(ws, ".sumi-return-staging-*"))
	for _, m := range matches {
		if _, err := os.Stat(m); err == nil {
			t.Fatalf("staging left behind: %s", m)
		}
	}
}

// An existing Local file with different content is never overwritten:
// it moves to quarantine and the Cloud version lands.
func TestReturnLocalModeQuarantinesConflicts(t *testing.T) {
	h := setupReturn(t)
	f := newFakeFilesvc(t)
	h.wireFiles(t, f)
	scope, _ := fileaccess.ScopeForPersona(h.pid)
	f.put(scope, "keep.txt", "cloud version")
	ws := t.TempDir()
	if err := os.MkdirAll(filepath.Join(ws, strings.ReplaceAll(h.pid, "-", "")), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ws, strings.ReplaceAll(h.pid, "-", ""), "keep.txt"), []byte("local authored"), 0o600); err != nil {
		t.Fatal(err)
	}
	config := writeConfig(t, h.home, h.slot)

	_, returnURL := h.newReturnMode("local")
	m, out := h.mover()
	m.wsRoot = ws
	code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false)
	if code != exitDone {
		t.Fatalf("return: %d\n%s", code, out)
	}
	got, err := os.ReadFile(filepath.Join(ws, strings.ReplaceAll(h.pid, "-", ""), "keep.txt"))
	if err != nil || string(got) != "cloud version" {
		t.Fatalf("cloud file did not land: %v %q", err, got)
	}
	// The authored local bytes are preserved somewhere under the
	// recorded quarantine — find them.
	ev, _ := os.ReadFile(filepath.Join(h.home, "return", "files.json"))
	var evj map[string]any
	if err := json.Unmarshal(ev, &evj); err != nil {
		t.Fatal(err)
	}
	qdir, _ := evj["quarantine_dir"].(string)
	if qdir == "" {
		t.Fatalf("no quarantine recorded: %s", ev)
	}
	qgot, err := os.ReadFile(filepath.Join(qdir, "keep.txt"))
	if err != nil || string(qgot) != "local authored" {
		t.Fatalf("quarantined content lost: %v %q", err, qgot)
	}
}

// --- copy negative + recovery coverage -----------------------------------------

// The copy stalls on an unreachable Cloud, the journal keeps what was
// proven, and a resume re-verifies rather than trusting it: a staged
// blob whose bytes no longer match its journal entry is refetched.
func TestReturnLocalModeResumeAfterUnreachableCopy(t *testing.T) {
	h := setupReturn(t)
	f := newFakeFilesvc(t)
	h.wireFiles(t, f)
	scope, _ := fileaccess.ScopeForPersona(h.pid)
	f.put(scope, "a.txt", "first carried")
	f.put(scope, "b.txt", "second carried")
	f.failNext("read", scope, "b.txt", 50)
	ws := t.TempDir()
	config := writeConfig(t, h.home, h.slot)

	sessID, returnURL := h.newReturnMode("local")
	m, out := h.mover()
	m.wsRoot = ws
	code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false)
	if code != exitPending {
		t.Fatalf("expected pending on unreachable copy, got %d\n%s", code, out)
	}

	// The journal holds a.txt verified; corrupt its staged blob so the
	// resume must prove the bytes again instead of skipping the fetch.
	jraw, err := os.ReadFile(filepath.Join(h.home, "return", "files-"+sessID+".json"))
	if err != nil {
		t.Fatalf("no copy journal after interruption: %v", err)
	}
	var j struct {
		Staging string `json:"staging_dir"`
	}
	if err := json.Unmarshal(jraw, &j); err != nil || j.Staging == "" {
		t.Fatalf("journal: %v %s", err, jraw)
	}
	blob := filepath.Join(j.Staging, "blobs", "a.txt")
	if err := os.WriteFile(blob, []byte("torn write"), 0o600); err != nil {
		t.Fatalf("staged blob a.txt missing: %v", err)
	}

	f.failOps["read:"+scope+"/b.txt"] = 0
	m2, out2 := h.mover()
	m2.wsRoot = ws
	code = m2.ReturnResume(h.ctx, h.local.pool, config, false)
	if code != exitDone {
		t.Fatalf("resume: %d\n%s", code, out2)
	}
	got, err := os.ReadFile(filepath.Join(ws, strings.ReplaceAll(h.pid, "-", ""), "a.txt"))
	if err != nil || string(got) != "first carried" {
		t.Fatalf("a.txt after resume: %v %q", err, got)
	}
	got, err = os.ReadFile(filepath.Join(ws, strings.ReplaceAll(h.pid, "-", ""), "b.txt"))
	if err != nil || string(got) != "second carried" {
		t.Fatalf("b.txt after resume: %v %q", err, got)
	}
}

// Entries that are not regular files or directories refuse the copy
// loudly — nothing is silently dropped from the carried workspace.
func TestReturnLocalModeRefusesUnsupportedEntries(t *testing.T) {
	h := setupReturn(t)
	f := newFakeFilesvc(t)
	h.wireFiles(t, f)
	scope, _ := fileaccess.ScopeForPersona(h.pid)
	f.put(scope, "ok.txt", "plain")
	f.extras[scope+"/link"] = "symlink"
	ws := t.TempDir()
	config := writeConfig(t, h.home, h.slot)

	_, returnURL := h.newReturnMode("local")
	m, out := h.mover()
	m.wsRoot = ws
	code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false)
	if code == exitDone || code == exitPending {
		t.Fatalf("unsupported entry must refuse, got %d\n%s", code, out)
	}
	if !strings.Contains(out.String(), "cannot carry") {
		t.Fatalf("refusal should name the unsupported entry:\n%s", out)
	}
	if _, err := os.Stat(filepath.Join(ws, strings.ReplaceAll(h.pid, "-", ""), "ok.txt")); !os.IsNotExist(err) {
		t.Fatalf("refused copy still landed files: %v", err)
	}
}

// When the destination filesystem cannot hold the carried bytes the
// copy refuses before a single byte is staged.
func TestReturnLocalModeRefusesWhenCapacityShort(t *testing.T) {
	h := setupReturn(t)
	f := newFakeFilesvc(t)
	h.wireFiles(t, f)
	scope, _ := fileaccess.ScopeForPersona(h.pid)
	f.put(scope, "huge.bin", "x")
	f.sizes[scope+"/huge.bin"] = 1 << 60 // list reports more than any disk
	ws := t.TempDir()
	config := writeConfig(t, h.home, h.slot)

	_, returnURL := h.newReturnMode("local")
	m, out := h.mover()
	m.wsRoot = ws
	code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false)
	if code == exitDone || code == exitPending {
		t.Fatalf("short capacity must refuse, got %d\n%s", code, out)
	}
	if !strings.Contains(out.String(), "bytes free") {
		t.Fatalf("refusal should explain capacity:\n%s", out)
	}
}

// A lost credential response is recovered by resume: the mint is
// retried and the config still lands the scoped URL + token.
func TestReturnCloudModeRemintsAfterLostResponse(t *testing.T) {
	h := setupReturn(t)
	f := newFakeFilesvc(t)
	h.wireFiles(t, f)
	config := writeConfig(t, h.home, h.slot)

	var dropped atomic.Int32
	h.setIntercept(func(w http.ResponseWriter, r *http.Request) bool {
		if strings.Contains(r.URL.Path, "/files-credential") && dropped.Add(1) == 1 {
			http.Error(w, "lost", http.StatusBadGateway)
			return true
		}
		return false
	})
	defer h.setIntercept(nil)

	_, returnURL := h.newReturnMode("cloud")
	m, out := h.mover()
	code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false)
	if code != exitPending {
		t.Fatalf("expected pending after dropped credential response, got %d\n%s", code, out)
	}
	m2, out2 := h.mover()
	code = m2.ReturnResume(h.ctx, h.local.pool, config, false)
	if code != exitDone {
		t.Fatalf("resume: %d\n%s", code, out2)
	}
	tok, err := configValue(config, "SUMI_FILESVC_TOKEN")
	if err != nil || tok == "" {
		t.Fatalf("no token after re-mint: %v %q", err, tok)
	}
}

// A crash between promote and activation re-proves the tree: the copy
// phase rehashes every promoted file against its journal, finds the
// edited one, and refetches it from the still-sealed source — "done" is
// never trusted from a marker or a flag.
func TestReturnLocalModeReverifiesPromotedTree(t *testing.T) {
	h := setupReturn(t)
	f := newFakeFilesvc(t)
	h.wireFiles(t, f)
	scope, _ := fileaccess.ScopeForPersona(h.pid)
	f.put(scope, "a.txt", "carried a")
	f.put(scope, "b.txt", "carried b")
	ws := t.TempDir()
	config := writeConfig(t, h.home, h.slot)
	scopeDir := filepath.Join(ws, strings.ReplaceAll(h.pid, "-", ""))

	// The activation report dies once — the copy already promoted.
	var dropped atomic.Int32
	h.setIntercept(func(w http.ResponseWriter, r *http.Request) bool {
		if strings.HasSuffix(r.URL.Path, "/activated") && dropped.Add(1) == 1 {
			http.Error(w, "lost", http.StatusBadGateway)
			return true
		}
		return false
	})
	defer h.setIntercept(nil)

	_, returnURL := h.newReturnMode("local")
	m, out := h.mover()
	m.wsRoot = ws
	code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false)
	if code != exitPending {
		t.Fatalf("expected pending after dropped activation, got %d\n%s", code, out)
	}
	if _, err := os.Stat(filepath.Join(scopeDir, "a.txt")); err != nil {
		t.Fatalf("a.txt not promoted yet: %v", err)
	}
	// The crash-window edit: bytes at the destination no longer match
	// the journal's proof. The file phase must refetch, not trust it —
	// driven directly, the way a resume re-enters the same code.
	if err := os.WriteFile(filepath.Join(scopeDir, "a.txt"), []byte("edited mid-crash"), 0o600); err != nil {
		t.Fatal(err)
	}
	r := m.newReturner(h.local.pool, config, false)
	st, err := r.load()
	if err != nil || st == nil {
		t.Fatalf("state: %v", err)
	}
	if err := r.copyFilesLocal(h.ctx, st); err != nil {
		t.Fatalf("re-verify pass: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(scopeDir, "a.txt"))
	if err != nil || string(got) != "carried a" {
		t.Fatalf("edited file not repaired: %v %q", err, got)
	}
	// The edit was preserved in quarantine, not destroyed.
	matches, _ := filepath.Glob(filepath.Join(ws, ".sumi-return-quarantine-*", "a.txt"))
	if len(matches) != 1 {
		t.Fatalf("edited file not quarantined: %v", matches)
	}
}

// A destination path held by a symlink is moved aside whole — never
// followed (which could write outside the workspace), never overwritten.
func TestReturnLocalModeQuarantinesSymlinkOccupant(t *testing.T) {
	h := setupReturn(t)
	f := newFakeFilesvc(t)
	h.wireFiles(t, f)
	scope, _ := fileaccess.ScopeForPersona(h.pid)
	f.put(scope, "keep.txt", "cloud version")
	ws := t.TempDir()
	scopeDir := filepath.Join(ws, strings.ReplaceAll(h.pid, "-", ""))
	if err := os.MkdirAll(scopeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "elsewhere.txt")
	if err := os.WriteFile(target, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(scopeDir, "keep.txt")); err != nil {
		t.Fatal(err)
	}
	config := writeConfig(t, h.home, h.slot)

	_, returnURL := h.newReturnMode("local")
	m, out := h.mover()
	m.wsRoot = ws
	code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false)
	if code != exitDone {
		t.Fatalf("return: %d\n%s", code, out)
	}
	got, err := os.ReadFile(filepath.Join(scopeDir, "keep.txt"))
	if err != nil || string(got) != "cloud version" {
		t.Fatalf("cloud file did not land: %v %q", err, got)
	}
	// The outside file was never touched through the link.
	if b, _ := os.ReadFile(target); string(b) != "outside" {
		t.Fatal("symlink target was written through the link")
	}
	// The symlink itself survives, moved aside whole.
	matches, _ := filepath.Glob(filepath.Join(ws, ".sumi-return-quarantine-*", "keep.txt"))
	if len(matches) != 1 {
		t.Fatalf("symlink not quarantined: %v", matches)
	}
	fi, err := os.Lstat(matches[0])
	if err != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("quarantined object is not the symlink: %v %v", fi, err)
	}
}

// A cancelled copy unwinds what it placed: carried files are removed,
// displaced Local-authored files come back out of quarantine, and the
// journal records the restore as evidence.
func TestReturnLocalModeCancelRestoresWorkspace(t *testing.T) {
	h := setupReturn(t)
	f := newFakeFilesvc(t)
	h.wireFiles(t, f)
	scope, _ := fileaccess.ScopeForPersona(h.pid)
	f.put(scope, "keep.txt", "cloud version")
	f.put(scope, "extra.txt", "extra carried")
	ws := t.TempDir()
	scopeDir := filepath.Join(ws, strings.ReplaceAll(h.pid, "-", ""))
	if err := os.MkdirAll(scopeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(scopeDir, "keep.txt"), []byte("local authored"), 0o600); err != nil {
		t.Fatal(err)
	}
	config := writeConfig(t, h.home, h.slot)

	// Hold the drive before activation — a failing service retarget is
	// pending, so the copy has promoted and the session is still
	// cancellable when the owner gives up.
	runDir := filepath.Join(h.home, "run")
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "service.pid"), []byte("424242"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctl := filepath.Join(t.TempDir(), "sumi-local")
	if err := os.WriteFile(ctl, []byte("#!/bin/sh\nexit 3\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SUMI_LOCAL_CTL", ctl)

	sessID, returnURL := h.newReturnMode("local")
	m, out := h.mover()
	m.wsRoot = ws
	code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false)
	if code != exitPending {
		t.Fatalf("expected held pending, got %d\n%s", code, out)
	}
	m2, out2 := h.mover()
	m2.wsRoot = ws
	code = m2.ReturnCancel(h.ctx, h.local.pool, config)
	if code != exitDone {
		t.Fatalf("cancel: %d\n%s", code, out2)
	}
	// The authored file is back; the carried tree is gone.
	got, err := os.ReadFile(filepath.Join(scopeDir, "keep.txt"))
	if err != nil || string(got) != "local authored" {
		t.Fatalf("authored file not restored: %v %q", err, got)
	}
	if _, err := os.Lstat(filepath.Join(scopeDir, "extra.txt")); !os.IsNotExist(err) {
		t.Fatalf("carried file left behind: %v", err)
	}
	if matches, _ := filepath.Glob(filepath.Join(ws, ".sumi-return-staging-*")); len(matches) != 0 {
		t.Fatalf("staging left behind: %v", matches)
	}
	jraw, err := os.ReadFile(filepath.Join(h.home, "return", "files-"+sessID+".json"))
	if err != nil {
		t.Fatalf("journal missing: %v", err)
	}
	var j struct {
		Phase string `json:"phase"`
	}
	if err := json.Unmarshal(jraw, &j); err != nil || j.Phase != "restored" {
		t.Fatalf("journal phase %q: %s", j.Phase, jraw)
	}
}

// Cloud mode retargets the running service inside the seal window:
// sumi-local start is invoked through SUMI_LOCAL_CTL and the recorded
// files-env must read "cloud" before the secretary may activate.
func TestReturnCloudModeRetargetsServiceBeforeActivation(t *testing.T) {
	h := setupReturn(t)
	f := newFakeFilesvc(t)
	h.wireFiles(t, f)
	config := writeConfig(t, h.home, h.slot)

	runDir := filepath.Join(h.home, "run")
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "service.pid"), []byte("424242"), 0o600); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(runDir, "files-env")
	if err := os.WriteFile(marker, []byte("local"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctlLog := filepath.Join(t.TempDir(), "ctl.log")
	ctl := filepath.Join(t.TempDir(), "sumi-local")
	// The fake ctl mimics the real script: stop drops the pidfile, start
	// writes the fingerprinted cloud marker derived from config.env.
	script := "#!/bin/sh\n" +
		"echo \"$1\" >> \"$FAKE_CTL_LOG\"\n" +
		"case \"$1\" in\n" +
		"stop) rm -f \"$FAKE_RUN/service.pid\" ;;\n" +
		"start) touch \"$FAKE_RUN/service.pid\"\n" +
		"  url=$(grep '^SUMI_FILESVC_URL=' \"$FAKE_CONFIG\" | cut -d\"'\" -f2)\n" +
		"  tok=$(grep '^SUMI_FILESVC_TOKEN=' \"$FAKE_CONFIG\" | cut -d\"'\" -f2)\n" +
		"  printf 'cloud:%s' \"$(printf '%s\\n%s' \"$url\" \"$tok\" | sha256sum | cut -c1-12)\" > \"$FAKE_MARKER\" ;;\n" +
		"esac\n"
	if err := os.WriteFile(ctl, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SUMI_LOCAL_CTL", ctl)
	t.Setenv("FAKE_MARKER", marker)
	t.Setenv("FAKE_CTL_LOG", ctlLog)
	t.Setenv("FAKE_RUN", runDir)
	t.Setenv("FAKE_CONFIG", config)

	_, returnURL := h.newReturnMode("cloud")
	m, out := h.mover()
	code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false)
	if code != exitDone {
		t.Fatalf("return: %d\n%s", code, out)
	}
	b, _ := os.ReadFile(marker)
	if !strings.HasPrefix(string(b), "cloud:") {
		t.Fatalf("service never retargeted: files-env=%q", b)
	}
	if b, _ := os.ReadFile(ctlLog); !strings.Contains(string(b), "stop") || !strings.Contains(string(b), "start") {
		t.Fatalf("expected stop+start through the ctl, got %q", b)
	}
}

// A failed retarget is pending, not "configured": CredDone stays false
// so the resume re-mints and retargets rather than reporting a store
// the running service does not use.
func TestReturnCloudModeRetargetFailureRecoversOnResume(t *testing.T) {
	h := setupReturn(t)
	f := newFakeFilesvc(t)
	h.wireFiles(t, f)
	config := writeConfig(t, h.home, h.slot)

	runDir := filepath.Join(h.home, "run")
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "service.pid"), []byte("424242"), 0o600); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(runDir, "files-env")
	ctl := filepath.Join(t.TempDir(), "sumi-local")
	if err := os.WriteFile(ctl, []byte("#!/bin/sh\nexit 3\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SUMI_LOCAL_CTL", ctl)

	sessID, returnURL := h.newReturnMode("cloud")
	m, out := h.mover()
	code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false)
	if code == exitDone {
		t.Fatalf("return completed while the service retarget failed:\n%s", out)
	}
	sraw, err := os.ReadFile(filepath.Join(h.home, "return", "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var st struct {
		CredDone bool `json:"cred_done"`
	}
	if err := json.Unmarshal(sraw, &st); err != nil || st.CredDone {
		t.Fatalf("cred_done marked without a retarget: %s", sraw)
	}
	// Resume with a working ctl — the credential step re-enters, the
	// owed stop+start lands, and the marker records the fingerprint.
	script := "#!/bin/sh\n" +
		"case \"$1\" in\n" +
		"stop) rm -f \"$FAKE_RUN/service.pid\" ;;\n" +
		"start) touch \"$FAKE_RUN/service.pid\"\n" +
		"  url=$(grep '^SUMI_FILESVC_URL=' \"$FAKE_CONFIG\" | cut -d\"'\" -f2)\n" +
		"  tok=$(grep '^SUMI_FILESVC_TOKEN=' \"$FAKE_CONFIG\" | cut -d\"'\" -f2)\n" +
		"  printf 'cloud:%s' \"$(printf '%s\\n%s' \"$url\" \"$tok\" | sha256sum | cut -c1-12)\" > \"$FAKE_MARKER\" ;;\n" +
		"esac\n"
	if err := os.WriteFile(ctl, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FAKE_MARKER", marker)
	t.Setenv("FAKE_RUN", runDir)
	t.Setenv("FAKE_CONFIG", config)
	m2, out2 := h.mover()
	code = m2.ReturnResume(h.ctx, h.local.pool, config, false)
	if code != exitDone {
		t.Fatalf("resume: %d\n%s", code, out2)
	}
	if b, _ := os.ReadFile(marker); !strings.HasPrefix(string(b), "cloud:") {
		t.Fatalf("files-env=%q after recovery", b)
	}
	_ = sessID
}

// A journal that exists but cannot be read is not the same as no journal:
// cancellation must not announce a restore it could not perform. The
// explicit return-cancel fails recoverably, leaves the quarantined
// authored bytes, the carried tree and the journal exactly as they were.
func TestReturnCancelUnreadableJournalStaysRecoverable(t *testing.T) {
	h := setupReturn(t)
	f := newFakeFilesvc(t)
	h.wireFiles(t, f)
	scope, _ := fileaccess.ScopeForPersona(h.pid)
	f.put(scope, "keep.txt", "cloud version")
	ws := t.TempDir()
	scopeDir := filepath.Join(ws, strings.ReplaceAll(h.pid, "-", ""))
	if err := os.MkdirAll(scopeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(scopeDir, "keep.txt"), []byte("local authored"), 0o600); err != nil {
		t.Fatal(err)
	}
	config := writeConfig(t, h.home, h.slot)
	runDir := filepath.Join(h.home, "run")
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "service.pid"), []byte("424242"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctl := filepath.Join(t.TempDir(), "sumi-local")
	if err := os.WriteFile(ctl, []byte("#!/bin/sh\nexit 3\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SUMI_LOCAL_CTL", ctl)

	sessID, returnURL := h.newReturnMode("local")
	m, out := h.mover()
	m.wsRoot = ws
	code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false)
	if code != exitPending {
		t.Fatalf("expected held pending, got %d\n%s", code, out)
	}
	jpath := filepath.Join(h.home, "return", "files-"+sessID+".json")
	if _, err := os.Stat(jpath); err != nil {
		t.Fatalf("journal missing before corruption: %v", err)
	}
	if err := os.WriteFile(jpath, []byte("{not-json"), 0o600); err != nil {
		t.Fatal(err)
	}

	m2, out2 := h.mover()
	m2.wsRoot = ws
	code = m2.ReturnCancel(h.ctx, h.local.pool, config)
	if code == exitDone {
		t.Fatalf("cancel announced completion with an unreadable journal\n%s", out2)
	}
	// Nothing may be unwound or reported: the journal is preserved as-is,
	// the authored bytes stay in quarantine, the carried file stays placed.
	raw, err := os.ReadFile(jpath)
	if err != nil || string(raw) != "{not-json" {
		t.Fatalf("journal not preserved: %v %q", err, raw)
	}
	if got, err := os.ReadFile(filepath.Join(scopeDir, "keep.txt")); err != nil || string(got) != "cloud version" {
		t.Fatalf("placed file changed during failed cancel: %v %q", err, got)
	}
	q, _ := filepath.Glob(filepath.Join(ws, ".sumi-return-quarantine-*", "keep.txt"))
	if len(q) != 1 {
		t.Fatalf("quarantined authored bytes lost: %v", q)
	}
	if b, err := os.ReadFile(q[0]); err != nil || string(b) != "local authored" {
		t.Fatalf("authored bytes corrupted: %v %q", err, b)
	}
}

// The same unreadable journal on the owner-cancel path observed through
// return-resume: retireStaged must refuse to report the retire, keeping
// the return recoverable instead of settling over unrestored files.
func TestReturnResumeOwnerCancelUnreadableJournalStaysRecoverable(t *testing.T) {
	h := setupReturn(t)
	f := newFakeFilesvc(t)
	h.wireFiles(t, f)
	scope, _ := fileaccess.ScopeForPersona(h.pid)
	f.put(scope, "keep.txt", "cloud version")
	ws := t.TempDir()
	scopeDir := filepath.Join(ws, strings.ReplaceAll(h.pid, "-", ""))
	if err := os.MkdirAll(scopeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(scopeDir, "keep.txt"), []byte("local authored"), 0o600); err != nil {
		t.Fatal(err)
	}
	config := writeConfig(t, h.home, h.slot)
	runDir := filepath.Join(h.home, "run")
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "service.pid"), []byte("424242"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctl := filepath.Join(t.TempDir(), "sumi-local")
	if err := os.WriteFile(ctl, []byte("#!/bin/sh\nexit 3\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SUMI_LOCAL_CTL", ctl)

	sessID, returnURL := h.newReturnMode("local")
	m, out := h.mover()
	m.wsRoot = ws
	code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false)
	if code != exitPending {
		t.Fatalf("expected held pending, got %d\n%s", code, out)
	}
	jpath := filepath.Join(h.home, "return", "files-"+sessID+".json")
	if err := os.WriteFile(jpath, []byte("{not-json"), 0o600); err != nil {
		t.Fatal(err)
	}

	// The owner cancels through the real route, then the operator resumes.
	req, err := http.NewRequestWithContext(h.ctx, http.MethodPost,
		h.srv.URL+"/api/secretary-return/sessions/"+sessID+"/cancel", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Cookie", returnsessiontest.Cookie+"=owner-cookie")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("owner cancel: HTTP %d", res.StatusCode)
	}

	m2, out2 := h.mover()
	m2.wsRoot = ws
	code = m2.ReturnResume(h.ctx, h.local.pool, config, false)
	if code == exitDone {
		t.Fatalf("resume settled the cancel over an unreadable journal\n%s", out2)
	}
	raw, err := os.ReadFile(jpath)
	if err != nil || string(raw) != "{not-json" {
		t.Fatalf("journal not preserved: %v %q", err, raw)
	}
	q, _ := filepath.Glob(filepath.Join(ws, ".sumi-return-quarantine-*", "keep.txt"))
	if len(q) != 1 {
		t.Fatalf("quarantined authored bytes lost: %v", q)
	}
}
