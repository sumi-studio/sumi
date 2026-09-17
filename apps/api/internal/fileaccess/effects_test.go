package fileaccess

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

const testPersona = "018f47a2-9b3c-7def-8abc-0123456789ab"
const testScope = "018f47a29b3c7def8abc0123456789ab"

// Every Apply carries the operation ledger's stable identity. Distinct
// operations get distinct keys — the same key is only ever retried with
// the identical request, which is what the durable receipt answers.
var testOpSeq atomic.Int64

func applyTool(t *testing.T, fx map[string]agentstate.ToolEffect, tool string, req map[string]any) (map[string]any, error) {
	t.Helper()
	return applyToolKey(t, fx, tool, fmt.Sprintf("in:tool:%d", testOpSeq.Add(1)), req)
}

func applyToolKey(t *testing.T, fx map[string]agentstate.ToolEffect, tool, opKey string, req map[string]any) (map[string]any, error) {
	t.Helper()
	e, ok := fx[tool]
	if !ok {
		t.Fatalf("tool %s not registered", tool)
	}
	return e.Apply(context.Background(), nil, testPersona, opKey, req)
}

func TestFileEffectsScopePinnedToPersona(t *testing.T) {
	f := newFakeFilesvc(t)
	c, _ := f.client(t)
	fx := FileEffects(c)

	// A caller-supplied scope/persona must never steer the request: the
	// effect derives its scope from the claiming persona only.
	out, err := applyTool(t, fx, ToolWrite, map[string]any{
		"path":         "a.txt",
		"content_text": "hello",
		"scope":        "00000000000000000000000000000000",
		"persona_id":   "00000000-0000-7000-8000-000000000000",
	})
	if err != nil {
		t.Fatal(err)
	}
	if out["written"] != true {
		t.Fatalf("unexpected response %v", out)
	}
	for _, s := range f.seen {
		if s.Scope != testScope {
			t.Fatalf("effect touched foreign scope %q", s.Scope)
		}
	}
	// The foreign scope's tree was never created.
	if _, ok := f.files["00000000000000000000000000000000"]; ok {
		t.Fatal("foreign scope materialized")
	}
}

func TestFileWriteCreateThenCASOverwrite(t *testing.T) {
	f := newFakeFilesvc(t)
	c, _ := f.client(t)
	fx := FileEffects(c)

	out, err := applyTool(t, fx, ToolWrite, map[string]any{
		"path": "docs/a.txt", "content_text": "v1",
	})
	if err != nil || out["written"] != true {
		t.Fatalf("create: %v %v", out, err)
	}
	v1 := out["version"].(int64)

	// A second create-only write must conflict (file exists).
	_, err = applyTool(t, fx, ToolWrite, map[string]any{
		"path": "docs/a.txt", "content_text": "v2",
	})
	if !errors.Is(err, agentstate.ErrBadRequest) || !strings.Contains(err.Error(), "version_conflict") {
		t.Fatalf("expected recorded version_conflict, got %v", err)
	}

	// CAS with the right version succeeds; CAS with a stale one fails.
	out, err = applyTool(t, fx, ToolWrite, map[string]any{
		"path": "docs/a.txt", "content_text": "v2", "expect_version": float64(v1),
	})
	if err != nil {
		t.Fatalf("CAS overwrite: %v", err)
	}
	if _, err = applyTool(t, fx, ToolWrite, map[string]any{
		"path": "docs/a.txt", "content_text": "v3", "expect_version": float64(v1),
	}); !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("stale CAS must be a recorded failure, got %v", err)
	}
}

// The crash-window case: the filesvc write commits, the response never
// arrives (process died before the ledger commit). The retried claim runs
// the effect again under the same operation key; filesvc's durable receipt
// answers with the recorded version — no second mutation, and nothing
// depends on the file's current bytes.
func TestFileWriteReplayAfterLandedWrite(t *testing.T) {
	f := newFakeFilesvc(t)
	c, _ := f.client(t)
	fx := FileEffects(c)

	f.mu.Lock()
	f.dropResponse = true
	f.mu.Unlock()

	opKey := "lost-commit:write:0"
	req := map[string]any{"path": "crash.txt", "content_text": "committed bytes"}
	_, err := applyToolKey(t, fx, ToolWrite, opKey, req)
	if err == nil || errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("first attempt must fail transiently, got %v", err)
	}
	// The write landed upstream even though the caller saw a transport error.
	if got := string(f.files[testScope]["crash.txt"].body); got != "committed bytes" {
		t.Fatalf("write did not land: %q", got)
	}

	landedVersion := f.files[testScope]["crash.txt"].version

	out, err := applyToolKey(t, fx, ToolWrite, opKey, req)
	if err != nil {
		t.Fatalf("replay must reconcile, got %v", err)
	}
	if out["written"] != true || out["replayed"] != true {
		t.Fatalf("expected replayed reconciliation, got %v", out)
	}
	if out["version"] != landedVersion {
		t.Fatalf("receipt must carry the landed version, got %v want %d", out["version"], landedVersion)
	}
	// The retry was answered from the receipt — no second mutation was
	// minted, so the file's recorded version is unchanged.
	if f.files[testScope]["crash.txt"].version != landedVersion {
		t.Fatal("replay minted a second version")
	}
}

// Accepted-operation identity: a brand-new create-only operation must NOT
// treat another writer's identical bytes as its own receipt. Content
// equality is not operation identity — the conflict stays a real conflict.
func TestFileWriteNewOpNeverBorrowsIdenticalBytes(t *testing.T) {
	f := newFakeFilesvc(t)
	c, _ := f.client(t)
	fx := FileEffects(c)

	// An unrelated writer (no operation key) creates the file first.
	if _, err := c.Write(context.Background(), testScope, "same.txt", "none", []byte("same")); err != nil {
		t.Fatal(err)
	}
	out, err := applyToolKey(t, fx, ToolWrite, "brand-new-op", map[string]any{
		"path": "same.txt", "content_text": "same"})
	if !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("new create borrowed foreign bytes as its receipt: out=%v err=%v", out, err)
	}
}

// Lost-ledger + intervening actor: the keyed write commits upstream and
// its ledger commit is lost, then another operation deletes the file.
// Replaying the same operation must report the receipt, never recreate.
func TestFileWriteLostLedgerRetryDoesNotRecreateDeleted(t *testing.T) {
	f := newFakeFilesvc(t)
	c, _ := f.client(t)
	fx := FileEffects(c)
	ctx := context.Background()

	key := "lost-ledger-write"
	req := map[string]any{"path": "gone.txt", "content_text": "old"}
	out, err := applyToolKey(t, fx, ToolWrite, key, req)
	if err != nil || out["written"] != true {
		t.Fatalf("first apply: %v %v", out, err)
	}
	landed := f.files[testScope]["gone.txt"].version

	// Intervening independent operation deletes the file.
	if err := c.Remove(ctx, testScope, "gone.txt", "any"); err != nil {
		t.Fatal(err)
	}

	out, err = applyToolKey(t, fx, ToolWrite, key, req)
	if err != nil || out["replayed"] != true {
		t.Fatalf("replay must be answered by the receipt: %v %v", out, err)
	}
	if out["version"] != landed {
		t.Fatalf("receipt version %v != landed %d", out["version"], landed)
	}
	if _, err := c.Stat(ctx, testScope, "gone.txt"); err == nil {
		t.Fatal("replayed write recreated a file another operation deleted")
	}
}

// Lost-ledger remove + intervening replacement: replay must not delete a
// file another operation created after the remove committed.
func TestFileRemoveLostLedgerRetryPreservesReplacement(t *testing.T) {
	f := newFakeFilesvc(t)
	c, _ := f.client(t)
	fx := FileEffects(c)
	ctx := context.Background()

	if _, err := c.Write(ctx, testScope, "replace.txt", "none", []byte("old")); err != nil {
		t.Fatal(err)
	}
	key := "lost-ledger-remove"
	req := map[string]any{"path": "replace.txt"}
	if _, err := applyToolKey(t, fx, ToolRemove, key, req); err != nil {
		t.Fatal(err)
	}
	// Intervening independent operation creates the replacement.
	if _, err := c.Write(ctx, testScope, "replace.txt", "none", []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	out, err := applyToolKey(t, fx, ToolRemove, key, req)
	if err != nil || out["removed"] != true || out["replayed"] != true {
		t.Fatalf("replayed remove must report the receipt: %v %v", out, err)
	}
	got, err := c.Read(ctx, testScope, "replace.txt", 0, -1)
	if err != nil || string(got.Body) != "replacement" {
		t.Fatalf("replayed remove deleted a later replacement: %q %v", got.Body, err)
	}
}

// The receipt binds the canonical request, not just the key: reusing a
// committed key for a different request is a deterministic refusal.
func TestFileWriteKeyReuseDifferentRequestConflicts(t *testing.T) {
	f := newFakeFilesvc(t)
	c, _ := f.client(t)
	fx := FileEffects(c)

	key := "reused-key"
	req := map[string]any{"path": "k.txt", "content_text": "original"}
	if _, err := applyToolKey(t, fx, ToolWrite, key, req); err != nil {
		t.Fatal(err)
	}
	// Same key, different bytes.
	_, err := applyToolKey(t, fx, ToolWrite, key, map[string]any{
		"path": "k.txt", "content_text": "changed", "expect_version": "none"})
	if !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("key reuse with different request must conflict, got %v", err)
	}
	// Same key, different path.
	_, err = applyToolKey(t, fx, ToolWrite, key, map[string]any{
		"path": "other.txt", "content_text": "original"})
	if !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("key reuse with different path must conflict, got %v", err)
	}
	// Same key, different op.
	_, err = applyToolKey(t, fx, ToolRemove, key, map[string]any{"path": "k.txt"})
	if !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("key reuse across ops must conflict, got %v", err)
	}
}

// A diverged receipt (filesvc settled the keyed op but could not confirm
// its declared effect — F338) is a deterministic failure for the ledger,
// never a bare success: the effect surfaces outcome_uncertain as a
// recorded-failure error, does not mutate, and repeats identically.
func TestFileWriteDivergedReceiptIsDeterministicFailure(t *testing.T) {
	f := newFakeFilesvc(t)
	c, _ := f.client(t)
	fx := FileEffects(c)

	key := "diverged-op"
	req := map[string]any{"path": "d.txt", "content_text": "ours"}
	// Seed the receipt the real filesvc writes when a keyed write settles
	// unconfirmed (foreign bytes at the path, landing unproven).
	sum := sha256.Sum256([]byte("ours"))
	f.receipts[testScope+"\x00"+effectOpKey(key)] = fakeReceipt{
		reqHash: fakeReqHash("write", "d.txt", fakeIVCanon("none"), hex.EncodeToString(sum[:])),
		op:      "write", version: 7, verdict: "diverged",
	}

	_, err := applyToolKey(t, fx, ToolWrite, key, req)
	if !errors.Is(err, agentstate.ErrBadRequest) || !strings.Contains(err.Error(), "outcome_uncertain") {
		t.Fatalf("diverged replay must be a deterministic outcome_uncertain failure, got %v", err)
	}
	if _, ok := f.files[testScope]["d.txt"]; ok {
		t.Fatal("diverged replay mutated the tree")
	}
	// Same verdict on a second replay — no endless retry, no success.
	if _, err := applyToolKey(t, fx, ToolWrite, key, req); !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("second diverged replay must still fail deterministically, got %v", err)
	}
	// A diverged remove receipt propagates the same way.
	f.files[testScope] = map[string]fakeEntry{"/": {dir: true}, "v.txt": {body: []byte("v"), version: 9}}
	rmKey := "diverged-rm"
	f.receipts[testScope+"\x00"+effectOpKey(rmKey)] = fakeReceipt{
		reqHash: fakeReqHash("remove", "v.txt", fakeIVCanon("any")),
		op:      "remove", version: 9, verdict: "diverged",
	}
	_, err = applyToolKey(t, fx, ToolRemove, rmKey, map[string]any{"path": "v.txt"})
	if !errors.Is(err, agentstate.ErrBadRequest) || !strings.Contains(err.Error(), "outcome_uncertain") {
		t.Fatalf("diverged remove replay must fail deterministically, got %v", err)
	}
	if got := string(f.files[testScope]["v.txt"].body); got != "v" {
		t.Fatalf("diverged remove replay deleted content: %q", got)
	}
}

// mkdir is a mutation with the same identity contract: a committed keyed
// mkdir replays from its receipt even after the dir was removed.
func TestFileMkdirReplayFromReceipt(t *testing.T) {
	f := newFakeFilesvc(t)
	c, _ := f.client(t)
	fx := FileEffects(c)
	ctx := context.Background()

	key := "mkdir-op"
	req := map[string]any{"path": "made/dir"}
	out, err := applyToolKey(t, fx, ToolMkdir, key, req)
	if err != nil {
		t.Fatal(err)
	}
	landed := out["version"].(int64)
	if err := c.Remove(ctx, testScope, "made/dir", "any"); err != nil {
		t.Fatal(err)
	}
	out, err = applyToolKey(t, fx, ToolMkdir, key, req)
	if err != nil || out["replayed"] != true || out["version"] != landed {
		t.Fatalf("mkdir replay must receipt: %v %v", out, err)
	}
	// The dir was NOT recreated by the replay.
	if _, err := c.Stat(ctx, testScope, "made/dir"); err == nil {
		t.Fatal("replayed mkdir recreated a deleted directory")
	}
	// A fresh mkdir of a recreated path is a different operation — it
	// lands its own version.
	if _, err := c.Mkdir(ctx, testScope, "made/dir"); err != nil {
		t.Fatal(err)
	}
	out, err = applyTool(t, fx, ToolMkdir, req)
	if err != nil || out["replayed"] == true {
		t.Fatalf("fresh mkdir must run for real: %v %v", out, err)
	}
}

// A committed keyed write followed by another operation's overwrite still
// answers its own recorded version — never the later file's state.
func TestFileWriteReplayAfterInterveningOverwrite(t *testing.T) {
	f := newFakeFilesvc(t)
	c, _ := f.client(t)
	fx := FileEffects(c)
	ctx := context.Background()

	key := "write-then-overwritten"
	req := map[string]any{"path": "ov.txt", "content_text": "mine"}
	out, err := applyToolKey(t, fx, ToolWrite, key, req)
	if err != nil {
		t.Fatal(err)
	}
	mine := out["version"].(int64)
	// Another operation overwrites with different bytes.
	if _, err := c.Write(ctx, testScope, "ov.txt", "any", []byte("theirs")); err != nil {
		t.Fatal(err)
	}
	out, err = applyToolKey(t, fx, ToolWrite, key, req)
	if err != nil || out["replayed"] != true || out["version"] != mine {
		t.Fatalf("replay must report the recorded version: %v %v", out, err)
	}
	got, _ := c.Read(ctx, testScope, "ov.txt", 0, -1)
	if string(got.Body) != "theirs" {
		t.Fatalf("replay touched the later writer's bytes: %q", got.Body)
	}
}

// A 409 whose path holds different bytes is a genuine conflict — the
// recorded failure must not masquerade as a replay.
func TestFileWriteReplayConflictDifferentContent(t *testing.T) {
	f := newFakeFilesvc(t)
	c, _ := f.client(t)
	fx := FileEffects(c)

	if _, err := applyTool(t, fx, ToolWrite, map[string]any{
		"path": "x.txt", "content_text": "theirs",
	}); err != nil {
		t.Fatal(err)
	}
	_, err := applyTool(t, fx, ToolWrite, map[string]any{
		"path": "x.txt", "content_text": "ours",
	})
	if !errors.Is(err, agentstate.ErrBadRequest) || !strings.Contains(err.Error(), "version_conflict") {
		t.Fatalf("different-content conflict must record failure, got %v", err)
	}
	if got := string(f.files[testScope]["x.txt"].body); got != "theirs" {
		t.Fatalf("conflict clobbered foreign content: %q", got)
	}
}

func TestFileWriteRejectsUnconditionalAny(t *testing.T) {
	f := newFakeFilesvc(t)
	c, _ := f.client(t)
	fx := FileEffects(c)
	_, err := applyTool(t, fx, ToolWrite, map[string]any{
		"path": "a.txt", "content_text": "x", "expect_version": "any",
	})
	if !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("any must be refused (replay-unsafe), got %v", err)
	}
	for _, s := range f.seen {
		if s.Op == "write" {
			t.Fatal("refused write still reached filesvc")
		}
	}
}

func TestFileReadTextBinaryAndPaging(t *testing.T) {
	f := newFakeFilesvc(t)
	c, _ := f.client(t)
	fx := FileEffects(c)

	if _, err := applyTool(t, fx, ToolWrite, map[string]any{
		"path": "t.txt", "content_text": "hello world",
	}); err != nil {
		t.Fatal(err)
	}
	bin := []byte{0x00, 0x01, 0xfe}
	if _, err := applyTool(t, fx, ToolWrite, map[string]any{
		"path": "b.bin", "content_base64": base64.StdEncoding.EncodeToString(bin),
	}); err != nil {
		t.Fatal(err)
	}

	out, err := applyTool(t, fx, ToolRead, map[string]any{"path": "t.txt"})
	if err != nil {
		t.Fatal(err)
	}
	if out["content_text"] != "hello world" || out["has_more"] != false {
		t.Fatalf("text read: %v", out)
	}
	out, err = applyTool(t, fx, ToolRead, map[string]any{"path": "t.txt", "offset": float64(6)})
	if err != nil || out["content_text"] != "world" {
		t.Fatalf("paged read: %v %v", out, err)
	}
	out, err = applyTool(t, fx, ToolRead, map[string]any{"path": "b.bin"})
	if err != nil || out["content_base64"] != base64.StdEncoding.EncodeToString(bin) {
		t.Fatalf("binary read must come back base64: %v %v", out, err)
	}
	if _, err = applyTool(t, fx, ToolRead, map[string]any{"path": "missing.txt"}); !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("missing read must be a recorded failure, got %v", err)
	}
}

func TestFileStatListMkdirRemove(t *testing.T) {
	f := newFakeFilesvc(t)
	c, _ := f.client(t)
	fx := FileEffects(c)

	if _, err := applyTool(t, fx, ToolMkdir, map[string]any{"path": "sub/dir"}); err != nil {
		t.Fatal(err)
	}
	out, err := applyTool(t, fx, ToolStat, map[string]any{"path": "sub/dir"})
	if err != nil || out["kind"] != "dir" {
		t.Fatalf("stat dir: %v %v", out, err)
	}
	if _, err = applyTool(t, fx, ToolWrite, map[string]any{
		"path": "sub/dir/f.txt", "content_text": "x",
	}); err != nil {
		t.Fatal(err)
	}
	out, err = applyTool(t, fx, ToolList, map[string]any{"path": "sub/dir"})
	if err != nil {
		t.Fatal(err)
	}
	entries, _ := out["entries"].([]any)
	found := false
	for _, e := range entries {
		if m, _ := e.(map[string]any); m["path"] == "sub/dir/f.txt" {
			found = true
		}
	}
	if !found {
		t.Fatalf("list missing file: %v", out)
	}
	out, err = applyTool(t, fx, ToolRemove, map[string]any{"path": "sub/dir/f.txt"})
	if err != nil || out["removed"] != true {
		t.Fatalf("remove: %v %v", out, err)
	}
	// A replayed remove finds the path absent — the honest idempotent answer.
	out, err = applyTool(t, fx, ToolRemove, map[string]any{"path": "sub/dir/f.txt"})
	if err != nil || out["removed"] != true || out["already_absent"] != true {
		t.Fatalf("remove replay: %v %v", out, err)
	}
}

func TestFileEffectTraversalIsRefused(t *testing.T) {
	f := newFakeFilesvc(t)
	c, _ := f.client(t)
	fx := FileEffects(c)
	for _, p := range []string{"../escape", "a/../../b", "/abs/path"} {
		_, err := applyTool(t, fx, ToolWrite, map[string]any{
			"path": p, "content_text": "x",
		})
		if !errors.Is(err, agentstate.ErrBadRequest) {
			t.Fatalf("path %q must fail deterministically, got %v", p, err)
		}
	}
}

func TestFileEffectTransientFailureIsNotRecorded(t *testing.T) {
	f := newFakeFilesvc(t)
	c, _ := f.client(t)
	fx := FileEffects(c)
	// A transport failure must surface as a plain (transient) error so the
	// claim rolls back and retries — never a durable "failed".
	f.mu.Lock()
	f.dropResponse = true
	f.mu.Unlock()
	_, err := applyTool(t, fx, ToolRead, map[string]any{"path": "x"})
	if err == nil || errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("transport failure must stay transient, got %v", err)
	}
}

func TestFileEffectBadArgs(t *testing.T) {
	f := newFakeFilesvc(t)
	c, _ := f.client(t)
	fx := FileEffects(c)
	cases := []map[string]any{
		{}, // no path
		{"path": ""},
		{"path": strings.Repeat("x", toolPathMaxBytes+1)},
		{"path": "a", "content_base64": "!!!not-base64!!!"},
		{"path": "a"}, // write with no content
	}
	for i, req := range cases {
		tool := ToolWrite
		if i < 3 {
			tool = ToolStat
		}
		if _, err := applyTool(t, fx, tool, req); !errors.Is(err, agentstate.ErrBadRequest) {
			t.Fatalf("case %d (%v) must be ErrBadRequest, got %v", i, req, err)
		}
	}
}

func TestFileEffectsReadOnlyClassification(t *testing.T) {
	f := newFakeFilesvc(t)
	c, _ := f.client(t)
	fx := FileEffects(c)
	for _, tool := range []string{ToolStat, ToolList, ToolRead} {
		if !fx[tool].ReadOnly(map[string]any{}) {
			t.Fatalf("%s must be read-only", tool)
		}
	}
	for _, tool := range []string{ToolWrite, ToolMkdir, ToolRemove} {
		if fx[tool].ReadOnly(map[string]any{}) {
			t.Fatalf("%s must not be read-only", tool)
		}
	}
}

func TestScopeForPersonaRejectsGarbage(t *testing.T) {
	if s, err := ScopeForPersona(testPersona); err != nil || s != testScope {
		t.Fatalf("scope %q %v", s, err)
	}
	for _, bad := range []string{"", "*", "..", "not-a-uuid", "018F47A2-9B3C-7DEF-8ABC-0123456789AB/extra"} {
		if _, err := ScopeForPersona(bad); err == nil {
			t.Fatalf("persona %q must not map to a scope", bad)
		}
	}
	// Uppercase canonical UUIDs compact correctly.
	if s, err := ScopeForPersona("018F47A2-9B3C-7DEF-8ABC-0123456789AB"); err != nil || s != testScope {
		t.Fatalf("uppercase persona must compact: %q %v", s, err)
	}
}

func TestFromEnvBothOrNeither(t *testing.T) {
	if c, err := FromEnv(func(string) string { return "" }); c != nil || err != nil {
		t.Fatalf("unset env must disable cleanly: %v %v", c, err)
	}
	env := map[string]string{"SUMI_FILESVC_URL": "http://127.0.0.1:8780"}
	if _, err := FromEnv(func(k string) string { return env[k] }); err == nil {
		t.Fatal("URL without token must fail")
	}
	env = map[string]string{"SUMI_FILESVC_TOKEN": "x"}
	if _, err := FromEnv(func(k string) string { return env[k] }); err == nil {
		t.Fatal("token without URL must fail")
	}
}
