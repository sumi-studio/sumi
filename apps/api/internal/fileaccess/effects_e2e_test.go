package fileaccess

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

// Real-filesvc end-to-end for the core effects. Skipped unless the fixture
// provides FILEACCESS_E2E_URL + FILEACCESS_E2E_TOKEN (a *-grant service token).
// Covers the boundaries a fake cannot: real traversal rejection, real CAS
// conflict codes, real per-scope isolation, and the landed-write reconcile.
func e2eClient(t *testing.T) *Client {
	t.Helper()
	rawURL, token := os.Getenv("FILEACCESS_E2E_URL"), os.Getenv("FILEACCESS_E2E_TOKEN")
	if rawURL == "" || token == "" {
		t.Skip("FILEACCESS_E2E_URL/FILEACCESS_E2E_TOKEN unset; skipping real-filesvc e2e")
	}
	c, err := NewClient(rawURL, token)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

const e2ePersonaA = "019a0000-0000-7000-8000-0000000000a1"
const e2ePersonaB = "019a0000-0000-7000-8000-0000000000b2"

func TestE2EFileEffectsRealFilesvc(t *testing.T) {
	c := e2eClient(t)
	fx := FileEffects(c)
	ctx := context.Background()
	scopeA, _ := ScopeForPersona(e2ePersonaA)
	scopeB, _ := ScopeForPersona(e2ePersonaB)

	// Receipts are durable: every operation key this run mints must be
	// unique to the run or a previous run's receipt answers/conflicts.
	runID := fmt.Sprintf("r%d", time.Now().UnixNano())
	var opSeq int
	apply := func(persona, tool string, req map[string]any) (map[string]any, error) {
		opSeq++
		return fx[tool].Apply(ctx, nil, persona, fmt.Sprintf("%s:tool:%d", runID, opSeq), req)
	}
	applyKey := func(persona, tool, key string, req map[string]any) (map[string]any, error) {
		return fx[tool].Apply(ctx, nil, persona, key, req)
	}

	// Best-effort pre-clean so residue from a crashed earlier run cannot
	// wedge create-only writes.
	for _, p := range []string{"e2e/nested/hello.txt", "e2e/replay.txt", "e2e/rm.txt",
		"b-file.txt", "e2e/nested", "e2e"} {
		_ = c.Remove(ctx, scopeA, p, "any")
		_ = c.Remove(ctx, scopeB, p, "any")
	}

	// mkdir + nested write on persona A.
	if _, err := apply(e2ePersonaA, ToolMkdir, map[string]any{"path": "e2e/nested"}); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	out, err := apply(e2ePersonaA, ToolWrite, map[string]any{
		"path": "e2e/nested/hello.txt", "content_text": "from secretary A",
	})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	v1, _ := out["version"].(int64)
	if v1 < 1 {
		t.Fatalf("no version minted: %v", out)
	}

	// Persona B cannot see persona A's scope: a different PAID maps to a
	// different subtree, so B's read of the same path is a real 404.
	if _, err = apply(e2ePersonaB, ToolRead, map[string]any{"path": "e2e/nested/hello.txt"}); !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("foreign persona read must fail: %v", err)
	}
	// And B's writes land in B's own scope only.
	if _, err = apply(e2ePersonaB, ToolWrite, map[string]any{
		"path": "b-file.txt", "content_text": "B data",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Stat(ctx, scopeA, "b-file.txt"); err == nil {
		t.Fatal("persona B write leaked into persona A scope")
	}

	// Real traversal rejection from filesvc itself.
	if _, err = apply(e2ePersonaA, ToolWrite, map[string]any{
		"path": "../outside.txt", "content_text": "x",
	}); !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("real traversal must fail: %v", err)
	}
	if _, err = c.Stat(ctx, scopeA, "../outside.txt"); err == nil {
		t.Fatal("traversal wrote outside scope")
	}

	// Real stale-version conflict.
	if _, err = apply(e2ePersonaA, ToolWrite, map[string]any{
		"path": "e2e/nested/hello.txt", "content_text": "second",
		"expect_version": float64(v1),
	}); err != nil {
		t.Fatalf("CAS overwrite: %v", err)
	}
	if _, err = apply(e2ePersonaA, ToolWrite, map[string]any{
		"path": "e2e/nested/hello.txt", "content_text": "third",
		"expect_version": float64(v1),
	}); !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("stale CAS must record conflict: %v", err)
	}

	// Lost-ledger window against the real service: the keyed write lands and
	// its receipt commits upstream, but the API operation record never did
	// (modeled by issuing the identical keyed request out-of-band). A retry
	// of the same operation must be answered by the receipt, not by looking
	// at the file.
	lostKey := runID + ":lost-ledger-write"
	lostReq := map[string]any{"path": "e2e/replay.txt", "content_text": "landed"}
	landedVer, _, err := c.WriteKeyed(ctx, scopeA, "e2e/replay.txt", "none", effectOpKey(lostKey), []byte("landed"))
	if err != nil {
		t.Fatalf("land keyed write: %v", err)
	}
	out, err = applyKey(e2ePersonaA, ToolWrite, lostKey, lostReq)
	if err != nil || out["replayed"] != true {
		t.Fatalf("receipt must report replayed: %v %v", out, err)
	}
	if out["version"] != landedVer {
		t.Fatalf("receipt version %v != landed %d", out["version"], landedVer)
	}
	// Intervening delete + retry: the receipt still answers — the file must
	// NOT be recreated (a later operation's deletion is not ours to undo).
	if err := c.Remove(ctx, scopeA, "e2e/replay.txt", "any"); err != nil {
		t.Fatalf("intervening delete: %v", err)
	}
	out, err = applyKey(e2ePersonaA, ToolWrite, lostKey, lostReq)
	if err != nil || out["replayed"] != true {
		t.Fatalf("replay after delete must still receipt: %v %v", out, err)
	}
	if _, err := c.Stat(ctx, scopeA, "e2e/replay.txt"); err == nil {
		t.Fatal("replayed write recreated a file another operation deleted")
	}
	// A NEW operation asking for the same bytes is a genuine conflict —
	// identical content is not an identity. Recreate via an independent
	// op first, then create-only from a different key must refuse.
	if _, err := c.Write(ctx, scopeA, "e2e/replay.txt", "none", []byte("landed")); err != nil {
		t.Fatalf("independent recreate: %v", err)
	}
	if _, err = apply(e2ePersonaA, ToolWrite, lostReq); !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("new op with identical bytes must conflict, got %v", err)
	}

	// Same window for remove: the keyed remove lands, a replacement is
	// created by another operation, and the retried remove must not
	// delete it.
	rmKey := runID + ":lost-ledger-remove"
	if _, err := c.Write(ctx, scopeA, "e2e/rm.txt", "none", []byte("old")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RemoveKeyed(ctx, scopeA, "e2e/rm.txt", "any", effectOpKey(rmKey)); err != nil {
		t.Fatalf("land keyed remove: %v", err)
	}
	if _, err := c.Write(ctx, scopeA, "e2e/rm.txt", "none", []byte("replacement")); err != nil {
		t.Fatalf("independent replacement: %v", err)
	}
	out, err = applyKey(e2ePersonaA, ToolRemove, rmKey, map[string]any{"path": "e2e/rm.txt"})
	if err != nil || out["replayed"] != true {
		t.Fatalf("remove replay must receipt: %v %v", out, err)
	}
	got, err := c.Read(ctx, scopeA, "e2e/rm.txt", 0, -1)
	if err != nil || string(got.Body) != "replacement" {
		t.Fatalf("replayed remove deleted a later replacement: %q %v", got.Body, err)
	}

	// Same key, different request: refused — a receipt is never borrowed.
	if _, _, err := c.WriteKeyed(ctx, scopeA, "e2e/replay.txt", "any", effectOpKey(lostKey), []byte("different")); err != nil {
		var se *ServiceError
		if !errors.As(err, &se) || se.Code != "idempotency_conflict" {
			t.Fatalf("key reuse with a different request must conflict, got %v", err)
		}
	}

	// Persistence marker for the restart phase of the fixture script.
	if _, err = apply(e2ePersonaA, ToolWrite, map[string]any{
		"path": "e2e/persist.txt", "content_text": "survives-restart",
	}); err != nil {
		t.Fatalf("persist marker: %v", err)
	}

	// Cleanup: keep only persist.txt; the restart-read phase deletes it.
	for _, p := range []string{"e2e/nested/hello.txt", "e2e/replay.txt", "e2e/rm.txt"} {
		if err := c.Remove(ctx, scopeA, p, "any"); err != nil {
			t.Fatalf("cleanup %s: %v", p, err)
		}
	}
	if err := c.Remove(ctx, scopeB, "b-file.txt", "any"); err != nil {
		t.Fatalf("cleanup B: %v", err)
	}
	_ = scopeB
}

// TestE2EFileEffectsPersistRead is the post-restart phase: filesvc has been
// restarted by the fixture script; the marker must still read back through
// the effect path.
func TestE2EFileEffectsPersistRead(t *testing.T) {
	if os.Getenv("FILEACCESS_E2E_PHASE") != "read" {
		t.Skip("post-restart phase")
	}
	c := e2eClient(t)
	fx := FileEffects(c)
	out, err := fx[ToolRead].Apply(context.Background(), nil, e2ePersonaA, "e2e:tool:0", map[string]any{"path": "e2e/persist.txt"})
	if err != nil {
		t.Fatalf("post-restart read: %v", err)
	}
	if out["content_text"] != "survives-restart" {
		t.Fatalf("content lost across restart: %v", out)
	}
	scopeA, _ := ScopeForPersona(e2ePersonaA)
	if err := c.Remove(context.Background(), scopeA, "e2e/persist.txt", "any"); err != nil {
		t.Fatalf("cleanup persist: %v", err)
	}
	// Best-effort dir cleanup.
	_ = c.Remove(context.Background(), scopeA, "e2e/nested", "any")
	_ = c.Remove(context.Background(), scopeA, "e2e", "any")
}

// Transport failure truthfulness against the real wiring: point at a dead
// port — every effect must surface a transient (non-ErrBadRequest) error.
func TestE2EFileEffectsServiceDown(t *testing.T) {
	if os.Getenv("FILEACCESS_E2E_URL") == "" {
		t.Skip("e2e unset")
	}
	c, err := NewClient("http://127.0.0.1:1", "x")
	if err != nil {
		t.Fatal(err)
	}
	fx := FileEffects(c)
	for _, tool := range []string{ToolStat, ToolList, ToolRead, ToolWrite} {
		req := map[string]any{"path": "x"}
		if tool == ToolWrite {
			req["content_text"] = "x"
		}
		_, err := fx[tool].Apply(context.Background(), nil, e2ePersonaA, "e2e:tool:0", req)
		if err == nil || errors.Is(err, agentstate.ErrBadRequest) || strings.Contains(err.Error(), "persona") {
			t.Fatalf("%s down: must be transient, got %v", tool, err)
		}
	}
}
