package filesvc

// PG-backed coverage for the durable operation receipt: keyed mutations
// commit a receipt atomically with the settled effect, replays are
// answered from it without a second mutation, mismatched reuse conflicts,
// and dropped intents leave no receipt. Gated on FILESV_TEST_DSN like the
// other store tests; each uses the real service handler, real posixRoot
// on a tempdir, and a real Store on real Postgres.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func pgReceiptSvc(t *testing.T) (*Service, *Store, string) {
	t.Helper()
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()
	st := newPGStore(t, dsn, dir)
	svc, err := NewAt(dir, st, map[string]map[string]bool{"svc": {"*": true}})
	if err != nil {
		t.Fatal(err)
	}
	return svc, st, dir
}

func keyHdr(key string) map[string]string {
	return map[string]string{"X-Idempotency-Key": key}
}

func bodyVersion(t *testing.T, w *httptest.ResponseRecorder) int64 {
	t.Helper()
	var v struct {
		Version int64 `json:"version"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &v); err != nil || v.Version == 0 {
		t.Fatalf("no version in response: %s", w.Body)
	}
	return v.Version
}

func withIfV(h map[string]string, iv string) map[string]string {
	h["If-Version"] = iv
	return h
}

// A committed keyed write answers a retry from the receipt — same version,
// replayed flag, no second mutation — even after the path was deleted by
// an independent operation.
func TestPGReceiptWriteReplayAndDelete(t *testing.T) {
	svc, _, dir := pgReceiptSvc(t)

	w := req(t, svc, "PUT", "/v1/files/ws/write?path=r.txt", "svc", "v1",
		withIfV(keyHdr("op:1"), "none"))
	if w.Code != 200 {
		t.Fatalf("keyed write: %d %s", w.Code, w.Body)
	}
	ver := bodyVersion(t, w)
	// Replay of the identical request: receipt answers, no new version.
	w = req(t, svc, "PUT", "/v1/files/ws/write?path=r.txt", "svc", "v1",
		withIfV(keyHdr("op:1"), "none"))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"replayed":true`) ||
		bodyVersion(t, w) != ver {
		t.Fatalf("replay must come from the receipt: %d %s", w.Code, w.Body)
	}
	// An independent operation deletes the file, then the retry still
	// receipts — and the file must NOT be recreated.
	w = req(t, svc, "DELETE", "/v1/files/ws/remove?path=r.txt", "svc", "",
		map[string]string{"If-Version": "any"})
	if w.Code != 200 {
		t.Fatalf("intervening delete: %d %s", w.Code, w.Body)
	}
	w = req(t, svc, "PUT", "/v1/files/ws/write?path=r.txt", "svc", "v1",
		withIfV(keyHdr("op:1"), "none"))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"replayed":true`) {
		t.Fatalf("replay after delete must still receipt: %d %s", w.Code, w.Body)
	}
	if _, err := os.Stat(filepath.Join(dir, "ws", "r.txt")); !os.IsNotExist(err) {
		t.Fatal("replayed write recreated a deleted file")
	}
}

// A keyed remove whose ledger reply was lost must not delete a replacement
// another operation created after the remove committed.
func TestPGReceiptRemovePreservesReplacement(t *testing.T) {
	svc, _, dir := pgReceiptSvc(t)

	req(t, svc, "PUT", "/v1/files/ws/write?path=x.txt", "svc", "old",
		map[string]string{"If-Version": "none"})
	w := req(t, svc, "DELETE", "/v1/files/ws/remove?path=x.txt", "svc", "",
		withIfV(keyHdr("op:rm"), "any"))
	if w.Code != 200 {
		t.Fatalf("keyed remove: %d %s", w.Code, w.Body)
	}
	// Independent replacement.
	req(t, svc, "PUT", "/v1/files/ws/write?path=x.txt", "svc", "new",
		map[string]string{"If-Version": "none"})
	w = req(t, svc, "DELETE", "/v1/files/ws/remove?path=x.txt", "svc", "",
		withIfV(keyHdr("op:rm"), "any"))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"replayed":true`) {
		t.Fatalf("replayed remove must come from the receipt: %d %s", w.Code, w.Body)
	}
	data, err := os.ReadFile(filepath.Join(dir, "ws", "x.txt"))
	if err != nil || string(data) != "new" {
		t.Fatalf("replayed remove deleted the replacement: %q %v", data, err)
	}
}

// Same key + different request is a deterministic refusal — a receipt is
// never borrowed. Body, path, version condition, and op all bind.
func TestPGReceiptKeyReuseMismatch(t *testing.T) {
	svc, _, _ := pgReceiptSvc(t)

	req(t, svc, "PUT", "/v1/files/ws/write?path=k.txt", "svc", "original",
		withIfV(keyHdr("op:k"), "none"))
	for _, tc := range []struct {
		name   string
		method string
		target string
		body   string
		hdrs   map[string]string
	}{
		{"different body", "PUT", "/v1/files/ws/write?path=k.txt", "changed", withIfV(keyHdr("op:k"), "none")},
		{"different path", "PUT", "/v1/files/ws/write?path=other.txt", "original", withIfV(keyHdr("op:k"), "none")},
		{"different if-version", "PUT", "/v1/files/ws/write?path=k.txt", "original", withIfV(keyHdr("op:k"), "any")},
		{"different op", "DELETE", "/v1/files/ws/remove?path=k.txt", "", withIfV(keyHdr("op:k"), "any")},
		{"mkdir with same key", "POST", "/v1/files/ws/mkdir", `{"path":"k.txt"}`, keyHdr("op:k")},
	} {
		w := req(t, svc, tc.method, tc.target, "svc", tc.body, tc.hdrs)
		if w.Code != 409 || !strings.Contains(w.Body.String(), "idempotency_conflict") {
			t.Fatalf("%s: want 409 idempotency_conflict, got %d %s", tc.name, w.Code, w.Body)
		}
	}
	// The file is untouched by every refused attempt.
	w := req(t, svc, "GET", "/v1/files/ws/read?path=k.txt", "svc", "", nil)
	if w.Code != 200 || w.Body.String() != "original" {
		t.Fatalf("receipt borrow mutated the file: %d %q", w.Code, w.Body)
	}
}

// A new operation key over bytes identical to another writer's is a
// genuine conflict — identical content is not an identity.
func TestPGReceiptIdenticalForeignBytesConflict(t *testing.T) {
	svc, _, _ := pgReceiptSvc(t)

	req(t, svc, "PUT", "/v1/files/ws/write?path=same.txt", "svc", "same",
		map[string]string{"If-Version": "none"})
	w := req(t, svc, "PUT", "/v1/files/ws/write?path=same.txt", "svc", "same",
		withIfV(keyHdr("op:new"), "none"))
	if w.Code != 409 || !strings.Contains(w.Body.String(), "version_conflict") {
		t.Fatalf("new create over identical foreign bytes must conflict: %d %s", w.Code, w.Body)
	}
}

// An intent that never landed leaves no receipt: a keyed declare followed
// by a drop frees the key for a genuine retry.
func TestPGReceiptDroppedIntentRetriesForReal(t *testing.T) {
	svc, st, dir := pgReceiptSvc(t)
	ctx := context.Background()

	// Declare a keyed intent directly (committed to file_op, fs never ran).
	it, err := st.declare(ctx, "ws", "write", "d.txt", "", IfVersion{Mode: "none"},
		sha("bytes"), OpIdentity{Key: "op:dropped", ReqHash: "h1"},
		func() (FileInfo, bool, error) { return FileInfo{}, false, nil },
		func() (FileInfo, bool, error) { return FileInfo{}, false, nil })
	if err != nil {
		t.Fatalf("declare: %v", err)
	}
	st.inflight.Delete(it.id)
	if !st.dropIntent(ctx, it) {
		t.Fatal("drop intent")
	}
	// The same key retries the operation for real — no phantom receipt.
	w := req(t, svc, "PUT", "/v1/files/ws/write?path=d.txt", "svc", "bytes",
		withIfV(keyHdr("op:dropped"), "none"))
	if w.Code != 200 || strings.Contains(w.Body.String(), `"replayed"`) {
		t.Fatalf("dropped intent must not receipt; the retry must run for real: %d %s", w.Code, w.Body)
	}
	data, err := os.ReadFile(filepath.Join(dir, "ws", "d.txt"))
	if err != nil || string(data) != "bytes" {
		t.Fatalf("retry after drop did not land: %q %v", data, err)
	}
}

// A second keyed declare while the first is still pending is a truthful
// in-flight refusal — never a second pending intent under the same key.
func TestPGReceiptInFlightConflict(t *testing.T) {
	_, st, _ := pgReceiptSvc(t)
	ctx := context.Background()

	noProbe := func() (FileInfo, bool, error) { return FileInfo{}, false, nil }
	it, err := st.declare(ctx, "ws", "write", "p.txt", "", IfVersion{Mode: "none"},
		sha("a"), OpIdentity{Key: "op:race", ReqHash: "h1"}, noProbe, noProbe)
	if err != nil {
		t.Fatalf("first declare: %v", err)
	}
	defer st.inflight.Delete(it.id)
	// Same key, DIFFERENT path — only the pending-op-key index can stop it.
	_, err = st.declare(ctx, "ws", "write", "q.txt", "", IfVersion{Mode: "none"},
		sha("b"), OpIdentity{Key: "op:race", ReqHash: "h2"}, noProbe, noProbe)
	if !errors.Is(err, ErrIdemInFlight) {
		t.Fatalf("second keyed declare must refuse in-flight, got %v", err)
	}
}

// A keyed mkdir receipts like any other mutation: replay after the dir
// was removed answers from the receipt without recreating it.
func TestPGReceiptMkdirReplay(t *testing.T) {
	svc, _, dir := pgReceiptSvc(t)

	w := req(t, svc, "POST", "/v1/files/ws/mkdir", "svc", `{"path":"d/sub"}`, keyHdr("op:mk"))
	if w.Code != 200 {
		t.Fatalf("keyed mkdir: %d %s", w.Code, w.Body)
	}
	ver := bodyVersion(t, w)
	if err := os.RemoveAll(filepath.Join(dir, "ws", "d")); err != nil {
		t.Fatal(err)
	}
	w = req(t, svc, "POST", "/v1/files/ws/mkdir", "svc", `{"path":"d/sub"}`, keyHdr("op:mk"))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"replayed":true`) {
		t.Fatalf("mkdir replay must receipt: %d %s", w.Code, w.Body)
	}
	if bodyVersion(t, w) != ver {
		t.Fatalf("receipt must carry the original version: %s vs %d", w.Body, ver)
	}
	if _, err := os.Stat(filepath.Join(dir, "ws", "d")); !os.IsNotExist(err) {
		t.Fatal("replayed mkdir recreated a deleted directory")
	}
}

// A keyed remove refused as dir_not_empty is a deterministic rejection,
// not an accepted operation: no receipt row, no pending intent, the key
// stays free. After the member is removed, the same key+request retries
// for real and commits its own receipt.
func TestPGReceiptDirNotEmptyIsNotAccepted(t *testing.T) {
	svc, st, dir := pgReceiptSvc(t)
	ctx := context.Background()

	w := req(t, svc, "POST", "/v1/files/ws/mkdir", "svc", `{"path":"ne"}`, nil)
	if w.Code != 200 {
		t.Fatalf("mkdir: %d %s", w.Code, w.Body)
	}
	w = req(t, svc, "PUT", "/v1/files/ws/write?path=ne/f.txt", "svc", "child",
		map[string]string{"If-Version": "none"})
	if w.Code != 200 {
		t.Fatalf("member write: %d %s", w.Code, w.Body)
	}
	w = req(t, svc, "DELETE", "/v1/files/ws/remove?path=ne", "svc", "",
		withIfV(keyHdr("op:ne"), "any"))
	if w.Code != 409 || !strings.Contains(w.Body.String(), "dir_not_empty") {
		t.Fatalf("keyed non-empty dir remove: want 409 dir_not_empty, got %d %s",
			w.Code, w.Body)
	}
	if strings.Contains(w.Body.String(), "external_change") {
		t.Fatalf("refusal claims a false external change: %s", w.Body)
	}
	// No receipt and no lingering intent: the refusal is not an accepted
	// operation and must not pin the key.
	var n int
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM file_receipt WHERE scope='ws' AND op_key='op:ne'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("refused op left a receipt: rows=%d err=%v", n, err)
	}
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM file_op WHERE scope='ws' AND op_key='op:ne'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("refused op left a pending intent: rows=%d err=%v", n, err)
	}
	// The dir and member are intact on disk.
	if got, ok := authReadOpt(dir, "ws/ne/f.txt"); !ok || got != "child" {
		t.Fatalf("member after refusal: %q present=%v", got, ok)
	}
	// Same key + same request after emptying retries for real — the
	// refusal did not borrow or create a receipt.
	w = req(t, svc, "DELETE", "/v1/files/ws/remove?path=ne/f.txt", "svc", "",
		map[string]string{"If-Version": "any"})
	if w.Code != 200 {
		t.Fatalf("member remove: %d %s", w.Code, w.Body)
	}
	w = req(t, svc, "DELETE", "/v1/files/ws/remove?path=ne", "svc", "",
		withIfV(keyHdr("op:ne"), "any"))
	if w.Code != 200 || strings.Contains(w.Body.String(), `"replayed"`) {
		t.Fatalf("retry after refusal must run for real: %d %s", w.Code, w.Body)
	}
	if durExists(t, dir, "ws/ne") {
		t.Fatal("dir survived a real keyed remove")
	}
	// And now the key is consumed: a replay receipts, a different request
	// conflicts.
	w = req(t, svc, "DELETE", "/v1/files/ws/remove?path=ne", "svc", "",
		withIfV(keyHdr("op:ne"), "any"))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"replayed":true`) {
		t.Fatalf("accepted remove must receipt: %d %s", w.Code, w.Body)
	}
	w = req(t, svc, "DELETE", "/v1/files/ws/remove?path=other", "svc", "",
		withIfV(keyHdr("op:ne"), "any"))
	if w.Code != 409 || !strings.Contains(w.Body.String(), "idempotency_conflict") {
		t.Fatalf("different request on consumed key must conflict: %d %s", w.Code, w.Body)
	}
}

// Receipts are durable across store instances: a new Store on the same
// DB+root answers the replay from the committed row.
func TestPGReceiptSurvivesStoreRestart(t *testing.T) {
	dsn := pgDSN(t)
	resetTables(t, dsn)
	dir := t.TempDir()

	st := newPGStore(t, dsn, dir)
	svc, err := NewAt(dir, st, map[string]map[string]bool{"svc": {"*": true}})
	if err != nil {
		t.Fatal(err)
	}
	w := req(t, svc, "PUT", "/v1/files/ws/write?path=durable.txt", "svc", "data",
		withIfV(keyHdr("op:durable"), "none"))
	if w.Code != 200 {
		t.Fatalf("keyed write: %d %s", w.Code, w.Body)
	}
	ver := bodyVersion(t, w)
	st.Close()

	// New owner on the same DB+root — the receipt answers across restart.
	st2 := newPGStore(t, dsn, dir)
	svc2, err := NewAt(dir, st2, map[string]map[string]bool{"svc": {"*": true}})
	if err != nil {
		t.Fatal(err)
	}
	w = req(t, svc2, "PUT", "/v1/files/ws/write?path=durable.txt", "svc", "data",
		withIfV(keyHdr("op:durable"), "none"))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"replayed":true`) ||
		bodyVersion(t, w) != ver {
		t.Fatalf("post-restart replay must receipt: %d %s", w.Code, w.Body)
	}
}

// An unkeyed mutation keeps the pre-receipt behavior: no receipt row, and
// a second identical unkeyed write under "any" mutates again.
func TestPGUnkeyedOpsUnchanged(t *testing.T) {
	svc, _, dir := pgReceiptSvc(t)
	_ = dir

	w := req(t, svc, "PUT", "/v1/files/ws/write?path=u.txt", "svc", "a",
		map[string]string{"If-Version": "none"})
	if w.Code != 200 {
		t.Fatalf("unkeyed write: %d", w.Code)
	}
	v1 := bodyVersion(t, w)
	w = req(t, svc, "PUT", "/v1/files/ws/write?path=u.txt", "svc", "b",
		map[string]string{"If-Version": "any"})
	if w.Code != 200 || bodyVersion(t, w) <= v1 {
		t.Fatalf("unkeyed overwrite must mint a later version: %d %s", w.Code, w.Body)
	}
}
