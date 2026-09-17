package fileaccess

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// fakeFilesvc is a wire-faithful stand-in for filesvc in unit tests: per-scope
// trees, a global monotonic version counter with per-path recorded versions,
// real If-Version semantics (none|eq|any), filesvc's {"error","code"} error
// shape, and the X-File-Version response headers. The fixture suite still runs
// the slice against the real service; this exists so the effect and proxy
// logic can be exercised deterministically (including injected faults).
type fakeFilesvc struct {
	t     *testing.T
	mu    sync.Mutex
	next  int64
	files map[string]map[string]fakeEntry // scope -> path -> entry
	token string
	// receipts models file_receipt: (scope, op_key) -> the committed
	// request hash + recorded result. Consulted before CAS, written after
	// the mutation commits — the durable identity the real service stores
	// in Postgres.
	receipts map[string]fakeReceipt
	// dropResponse, when set, hijacks the connection after the handler ran —
	// simulating a transport failure after a committed upstream mutation.
	dropResponse bool

	seen []seenReq
}

type fakeReceipt struct {
	reqHash string
	op      string
	version int64
	// verdict mirrors file_receipt.verdict: "" is an applied receipt;
	// "diverged" is a settle whose declared effect could not be confirmed —
	// the replay answers outcome_uncertain, never a silent success.
	verdict string
}

type fakeEntry struct {
	body    []byte
	version int64
	dir     bool
}

type seenReq struct {
	Scope string
	Op    string
	Path  string
	IfVer string
	Auth  string
}

func newFakeFilesvc(t *testing.T) *fakeFilesvc {
	return &fakeFilesvc{
		t:        t,
		files:    map[string]map[string]fakeEntry{},
		token:    "svc-token",
		receipts: map[string]fakeReceipt{},
	}
}

// fakeReqHash mirrors filesvc's service.go reqHash: sha256 over
// length-prefixed canonical request parts.
func fakeReqHash(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(strconv.Itoa(len(p))))
		h.Write([]byte{0})
		h.Write([]byte(p))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// fakeIVCanon mirrors filesvc's ivCanon.
func fakeIVCanon(iv string) string {
	switch iv {
	case "any":
		return "any:0"
	case "none":
		return "none:0"
	default:
		return "eq:" + iv
	}
}

// fakeReceiptCheck is the declare-time receipt gate: same key + same
// canonical request answers from the recorded receipt; same key +
// different request is an idempotency_conflict. Returns true when a
// response was written (the caller must return).
func (f *fakeFilesvc) fakeReceiptCheck(w http.ResponseWriter, scope, opKey, reqH string) bool {
	if opKey == "" {
		return false
	}
	rec, ok := f.receipts[scope+"\x00"+opKey]
	if !ok {
		return false
	}
	if rec.reqHash != reqH {
		writeErrJSON(w, 409, "idempotency_conflict", "idempotency key was already used for a different request")
		return true
	}
	if rec.verdict == "diverged" {
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(409)
		json.NewEncoder(w).Encode(map[string]any{
			"error":    "operation outcome could not be confirmed: " + rec.op,
			"code":     "outcome_uncertain",
			"replayed": true,
			"op":       rec.op,
			"version":  rec.version,
		})
		return true
	}
	w.Header().Set("content-type", "application/json")
	out := map[string]any{"version": rec.version, "replayed": true}
	if rec.op == "remove" {
		out["removed"] = true
	}
	json.NewEncoder(w).Encode(out)
	return true
}

// fakeReceiptRecord commits the operation's receipt after the mutation —
// the same ordering the real apply transaction guarantees.
func (f *fakeFilesvc) fakeReceiptRecord(scope, opKey, reqH, op string, version int64) {
	if opKey == "" {
		return
	}
	k := scope + "\x00" + opKey
	if _, ok := f.receipts[k]; !ok {
		f.receipts[k] = fakeReceipt{reqHash: reqH, op: op, version: version}
	}
}

func (f *fakeFilesvc) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/v1/files/"), "/", 2)
		if len(parts) != 2 {
			writeErrJSON(w, 404, "not_found", "bad route")
			return
		}
		f.mu.Lock()
		f.seen = append(f.seen, seenReq{
			Scope: parts[0], Op: parts[1], Path: r.URL.Query().Get("path"),
			IfVer: r.Header.Get("If-Version"), Auth: r.Header.Get("Authorization"),
		})
		drop := f.dropResponse
		f.mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer "+f.token {
			writeErrJSON(w, 401, "unauthorized", "bad token")
			return
		}
		rw := w
		if drop {
			rw = &swallowWriter{header: http.Header{}}
		}
		f.dispatch(rw, r, parts[0], parts[1])
		if drop {
			f.mu.Lock()
			f.dropResponse = false
			f.mu.Unlock()
			if hj, ok := w.(http.Hijacker); ok {
				if conn, _, err := hj.Hijack(); err == nil {
					conn.Close()
				}
			}
		}
	})
}

// swallowWriter absorbs a committed response so the connection can then be
// cut, simulating "mutation landed, response never arrived".
type swallowWriter struct {
	header http.Header
	status int
}

func (s *swallowWriter) Header() http.Header { return s.header }
func (s *swallowWriter) Write(b []byte) (int, error) {
	return len(b), nil
}
func (s *swallowWriter) WriteHeader(code int) { s.status = code }

func writeErrJSON(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg, "code": code})
}

func (f *fakeFilesvc) scopeTree(scope string) map[string]fakeEntry {
	if f.files[scope] == nil {
		f.files[scope] = map[string]fakeEntry{"/": {dir: true}}
	}
	return f.files[scope]
}

func (f *fakeFilesvc) mint() int64 {
	f.next++
	return f.next
}

func (f *fakeFilesvc) dispatch(w http.ResponseWriter, r *http.Request, scope, op string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	tree := f.scopeTree(scope)
	path := r.URL.Query().Get("path")
	if strings.Contains(path, "..") || strings.HasPrefix(path, "/") || strings.HasPrefix(path, "\\") {
		writeErrJSON(w, 400, "bad_path", "escapes scope")
		return
	}
	opKey := r.Header.Get("X-Idempotency-Key")
	switch op {
	case "write":
		body, _ := io.ReadAll(r.Body)
		iv := r.Header.Get("If-Version")
		if iv == "" {
			writeErrJSON(w, 400, "bad_if_version", "missing")
			return
		}
		sum := sha256.Sum256(body)
		reqH := fakeReqHash("write", path, fakeIVCanon(iv), hex.EncodeToString(sum[:]))
		if f.fakeReceiptCheck(w, scope, opKey, reqH) {
			return
		}
		ent, exists := tree[path]
		exists = exists && !ent.dir
		switch {
		case iv == "none":
			if exists {
				writeErrJSON(w, 409, "version_conflict", "exists")
				return
			}
		case iv == "any":
		default:
			n, err := strconv.ParseInt(iv, 10, 64)
			if err != nil {
				writeErrJSON(w, 400, "bad_if_version", "bad")
				return
			}
			if !exists {
				writeErrJSON(w, 404, "not_found", "missing")
				return
			}
			if n != ent.version {
				writeErrJSON(w, 409, "version_conflict", "stale")
				return
			}
		}
		v := f.mint()
		tree[path] = fakeEntry{body: body, version: v}
		f.fakeReceiptRecord(scope, opKey, reqH, "write", v)
		w.Header().Set("content-type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"version": v})
	case "mkdir":
		var b struct {
			Path string `json:"path"`
		}
		json.NewDecoder(r.Body).Decode(&b)
		if b.Path == "" {
			writeErrJSON(w, 400, "bad_request", "path required")
			return
		}
		if strings.Contains(b.Path, "..") || strings.HasPrefix(b.Path, "/") {
			writeErrJSON(w, 400, "bad_path", "escapes scope")
			return
		}
		reqH := fakeReqHash("mkdir", b.Path)
		if f.fakeReceiptCheck(w, scope, opKey, reqH) {
			return
		}
		v := f.mint()
		tree[b.Path] = fakeEntry{dir: true, version: v}
		f.fakeReceiptRecord(scope, opKey, reqH, "mkdir", v)
		json.NewEncoder(w).Encode(map[string]any{"version": v})
	case "read":
		ent, ok := tree[path]
		if !ok || ent.dir {
			writeErrJSON(w, 404, "not_found", "missing")
			return
		}
		var off, length int64 = 0, -1
		if v := r.URL.Query().Get("offset"); v != "" {
			off, _ = strconv.ParseInt(v, 10, 64)
		}
		if v := r.URL.Query().Get("len"); v != "" {
			length, _ = strconv.ParseInt(v, 10, 64)
		}
		if off > int64(len(ent.body)) {
			writeErrJSON(w, 416, "out_of_range", "past eof")
			return
		}
		n := int64(len(ent.body)) - off
		if length >= 0 && length < n {
			n = length
		}
		w.Header().Set("X-File-Version", strconv.FormatInt(ent.version, 10))
		w.Header().Set("content-type", "application/octet-stream")
		w.Write(ent.body[off : off+n])
	case "stat":
		ent, ok := tree[path]
		if !ok {
			writeErrJSON(w, 404, "not_found", "missing")
			return
		}
		kind := "file"
		if ent.dir {
			kind = "dir"
		}
		json.NewEncoder(w).Encode(map[string]any{
			"kind": kind, "size": len(ent.body), "mtime_ns": 1,
			"version": ent.version, "fingerprint": "fp", "external_change": false,
		})
	case "list":
		prefix := strings.TrimSuffix(path, "/")
		var entries []map[string]any
		for p, ent := range tree {
			if p == "/" {
				continue
			}
			if prefix == "" || strings.HasPrefix(p, prefix+"/") || p == prefix {
				kind := "file"
				if ent.dir {
					kind = "dir"
				}
				entries = append(entries, map[string]any{"path": p, "kind": kind})
			}
		}
		if entries == nil {
			entries = []map[string]any{}
		}
		json.NewEncoder(w).Encode(map[string]any{"entries": entries, "next_cursor": ""})
	case "remove":
		iv := r.Header.Get("If-Version")
		reqH := fakeReqHash("remove", path, fakeIVCanon(iv))
		if f.fakeReceiptCheck(w, scope, opKey, reqH) {
			return
		}
		ent, exists := tree[path]
		if !exists {
			writeErrJSON(w, 404, "not_found", "missing")
			return
		}
		if iv != "any" && iv != "none" {
			n, perr := strconv.ParseInt(iv, 10, 64)
			if perr != nil || iv == "" {
				writeErrJSON(w, 400, "bad_if_version", "bad")
				return
			}
			if n != ent.version {
				writeErrJSON(w, 409, "version_conflict", "stale")
				return
			}
		} else if iv == "none" {
			writeErrJSON(w, 409, "version_conflict", "exists")
			return
		}
		delete(tree, path)
		f.fakeReceiptRecord(scope, opKey, reqH, "remove", f.mint())
		json.NewEncoder(w).Encode(map[string]any{"removed": true})
	default:
		writeErrJSON(w, 404, "not_found", "unknown op")
	}
}

// clientFor builds a Client against a running fake server.
func (f *fakeFilesvc) client(t *testing.T) (*Client, *httptest.Server) {
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	c, err := NewClient(srv.URL, f.token)
	if err != nil {
		t.Fatal(err)
	}
	return c, srv
}
