package agentevents

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestCommandStore_OpenClose(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCommandStore_AppendAllocatesSeqAndCommandID(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	cmd := json.RawMessage(`{"type":"user_message","text":"hello","attachments":[]}`)
	env, err := store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "", cmd)
	if err != nil {
		t.Fatal(err)
	}
	if env.Seq != 1 {
		t.Fatalf("expected first seq 1, got %d", env.Seq)
	}
	if env.CommandID == "" {
		t.Fatal("expected non-empty command_id")
	}
	if !isCanonicalUUID(env.CommandID) {
		t.Fatalf("command_id %q is not a canonical UUID", env.CommandID)
	}

	env2, err := store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "", cmd)
	if err != nil {
		t.Fatal(err)
	}
	if env2.Seq != 2 {
		t.Fatalf("expected second seq 2, got %d", env2.Seq)
	}
	if env2.CommandID == env.CommandID {
		t.Fatal("second append reused command_id")
	}
}

func TestCommandStore_ConcurrentAppendsNoDuplicateOrGap(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	const workers = 50
	cmd := json.RawMessage(`{"type":"user_message","text":"concurrent","attachments":[]}`)

	var wg sync.WaitGroup
	envelopes := make([]CommandEnvelope, workers)
	errs := make([]error, workers)

	for i := range workers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			envelopes[idx], errs[idx] = store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "", cmd)
		}(i)
	}
	wg.Wait()

	seqs := make(map[uint64]bool)
	ids := make(map[string]bool)
	for i, env := range envelopes {
		if errs[i] != nil {
			t.Fatalf("concurrent append %d failed: %v", i, errs[i])
		}
		if env.Seq < 1 || env.Seq > workers {
			t.Fatalf("seq %d out of range [1,%d]", env.Seq, workers)
		}
		if seqs[env.Seq] {
			t.Fatalf("duplicate seq %d", env.Seq)
		}
		seqs[env.Seq] = true
		if ids[env.CommandID] {
			t.Fatalf("duplicate command_id %s", env.CommandID)
		}
		ids[env.CommandID] = true
	}
	for i := 1; i <= workers; i++ {
		if !seqs[uint64(i)] {
			t.Fatalf("missing seq %d", i)
		}
	}
}

func TestCommandStore_RestartPreservesLogAndNextSeq(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	cmd1 := json.RawMessage(`{"type":"user_message","text":"first","attachments":[]}`)
	env1, err := store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "", cmd1)
	if err != nil {
		t.Fatal(err)
	}

	cmd2 := json.RawMessage(`{"type":"user_message","text":"second","attachments":[]}`)
	env2, err := store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "", cmd2)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store2, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	next, err := store2.NextCommandSeq(context.Background(), "018f47a2-9b3c-7def-8abc-0123456789ab")
	if err != nil {
		t.Fatal(err)
	}
	if next != env2.Seq+1 {
		t.Fatalf("expected next seq %d after restart, got %d", env2.Seq+1, next)
	}

	caught, err := store2.CatchUp(context.Background(), "018f47a2-9b3c-7def-8abc-0123456789ab", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(caught) != 2 {
		t.Fatalf("expected 2 commands after restart, got %d", len(caught))
	}
	if caught[0].Seq != env1.Seq || caught[1].Seq != env2.Seq {
		t.Fatalf("restart changed seqs: got %+v", caught)
	}
	if string(caught[0].Command) != string(cmd1) || string(caught[1].Command) != string(cmd2) {
		t.Fatal("restart corrupted command bytes")
	}

	env3, err := store2.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "", json.RawMessage(`{"type":"user_message","text":"third","attachments":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	if env3.Seq != env2.Seq+1 {
		t.Fatalf("expected next seq %d after restart, got %d", env2.Seq+1, env3.Seq)
	}
}

func TestCommandStore_IdempotencyKey(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	cmd := json.RawMessage(`{"type":"user_message","text":"idem","attachments":[]}`)
	env1, err := store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "key-1", cmd)
	if err != nil {
		t.Fatal(err)
	}

	env2, err := store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "key-1", cmd)
	if err != nil {
		t.Fatal(err)
	}
	if env2.Seq != env1.Seq || env2.CommandID != env1.CommandID {
		t.Fatal("retried submission with same key allocated a second command")
	}

	cmd3 := json.RawMessage(`{"type":"user_message","text":"different","attachments":[]}`)
	_, err = store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "key-1", cmd3)
	if err == nil {
		t.Fatal("expected idempotency key conflict for different command body")
	}
	if !isIdempotencyConflict(err) {
		t.Fatalf("expected idempotency conflict, got %v", err)
	}
}

func TestCommandStore_PerPersonalityAgentSeq(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	cmd := json.RawMessage(`{"type":"user_message","text":"x","attachments":[]}`)
	a, err := store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-012345678990"), "", cmd)
	if err != nil {
		t.Fatal(err)
	}
	b, err := store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-012345678991"), "", cmd)
	if err != nil {
		t.Fatal(err)
	}
	if a.Seq != 1 || b.Seq != 1 {
		t.Fatalf("expected first seq 1 for each personality agent, got %d and %d", a.Seq, b.Seq)
	}
}

func TestCommandStore_CatchUpFromSeq(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	for i := 0; i < 5; i++ {
		cmd := json.RawMessage(`{"type":"user_message","text":"x","attachments":[]}`)
		if _, err := store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "", cmd); err != nil {
			t.Fatal(err)
		}
	}

	caught, err := store.CatchUp(context.Background(), "018f47a2-9b3c-7def-8abc-0123456789ab", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(caught) != 4 {
		t.Fatalf("expected 4 commands from seq 2, got %d", len(caught))
	}
	if caught[0].Seq != 2 || caught[3].Seq != 5 {
		t.Fatalf("expected seq [2,3,4,5], got [%d,%d,%d,%d]", caught[0].Seq, caught[1].Seq, caught[2].Seq, caught[3].Seq)
	}
}

func isCanonicalUUID(s string) bool {
	return regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`).MatchString(s)
}

func TestCommandStore_ContextCancellation(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cmd := json.RawMessage(`{"type":"user_message","text":"x","attachments":[]}`)
	_, err = store.Append(ctx, testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "", cmd)
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

func TestCommandStoreBlockedPersonalityAgentDoesNotBlockOtherPersonalityAgent(t *testing.T) {
	skipIfNoFlock(t)
	dir := t.TempDir()
	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	const blocked = "018f47a2-9b3c-7def-8abc-012345678992"
	if _, err := store.Append(context.Background(), testDirectChatProvenance(blocked), "", json.RawMessage(`{"type":"user_message","text":"blocked","attachments":[]}`)); err != nil {
		t.Fatal(err)
	}
	blocker, err := os.OpenFile(commandLogPath(dir, blocked), os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	if err := syscall.Flock(int(blocker.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	defer syscall.Flock(int(blocker.Fd()), syscall.LOCK_UN)

	store.mu.Lock()
	st := store.states[blocked]
	store.mu.Unlock()
	blockedDone := make(chan error, 1)
	go func() { _, err := store.NextCommandSeq(context.Background(), blocked); blockedDone <- err }()
	deadline := time.Now().Add(250 * time.Millisecond)
	for st.mu.TryLock() {
		st.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("blocked personality agent never reached its flock wait")
		}
		runtime.Gosched()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if _, err := store.Append(ctx, testDirectChatProvenance("018f47a2-9b3c-7def-8abc-012345678993"), "", json.RawMessage(`{"type":"user_message","text":"progress","attachments":[]}`)); err != nil {
		t.Fatalf("blocked personality agent serialized unrelated append: %v", err)
	}
	if err := syscall.Flock(int(blocker.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	if err := <-blockedDone; err != nil {
		t.Fatalf("blocked personality agent did not recover: %v", err)
	}
}

func TestCommandStoreFirstLoadFlockHonorsCancellation(t *testing.T) {
	skipIfNoFlock(t)
	dir := t.TempDir()
	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-012345678971"
	blocker, err := os.OpenFile(commandLogPath(dir, personalityAgentID), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	if err := syscall.Flock(int(blocker.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if _, err := store.NextCommandSeq(ctx, personalityAgentID); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first load must propagate deadline, got %v", err)
	}
	if err := syscall.Flock(int(blocker.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	if next, err := store.NextCommandSeq(context.Background(), personalityAgentID); err != nil || next != 1 {
		t.Fatalf("cancelled load must not leak partial state: next=%d err=%v", next, err)
	}
}

func TestCommandStore_DurableCommitBeforeSuccess(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	cmd := json.RawMessage(`{"type":"user_message","text":"durable","attachments":[]}`)
	env, err := store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "", cmd)
	if err != nil {
		t.Fatal(err)
	}

	// Close, reopen, and verify the just-committed command is visible.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store2, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	caught, err := store2.CatchUp(context.Background(), "018f47a2-9b3c-7def-8abc-0123456789ab", env.Seq)
	if err != nil {
		t.Fatal(err)
	}
	if len(caught) != 1 || caught[0].Seq != env.Seq {
		t.Fatal("committed command not visible after reopen")
	}
}

func TestCommandStore_RaceNoDuplicateSeqUnderPressure(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	const workers = 100
	var wg sync.WaitGroup
	var mu sync.Mutex
	seqs := make(map[uint64]bool)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Duration(i) * time.Microsecond) // jitter
			cmd := json.RawMessage(`{"type":"user_message","text":"race","attachments":[]}`)
			env, err := store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "", cmd)
			if err != nil {
				t.Errorf("append failed: %v", err)
				return
			}
			mu.Lock()
			if seqs[env.Seq] {
				t.Errorf("duplicate seq %d", env.Seq)
			}
			seqs[env.Seq] = true
			mu.Unlock()
		}()
	}
	wg.Wait()

	if len(seqs) != workers {
		t.Fatalf("expected %d unique seqs, got %d", workers, len(seqs))
	}
}

func TestCommandStore_IdempotencyKeyPersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	cmd := json.RawMessage(`{"type":"user_message","text":"idem","attachments":[]}`)
	env1, err := store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "key-1", cmd)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store2, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	env2, err := store2.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "key-1", cmd)
	if err != nil {
		t.Fatal(err)
	}
	if env2.Seq != env1.Seq || env2.CommandID != env1.CommandID {
		t.Fatalf("restart lost idempotency: got %+v vs %+v", env2, env1)
	}

	cmd3 := json.RawMessage(`{"type":"user_message","text":"different","attachments":[]}`)
	_, err = store2.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "key-1", cmd3)
	if err == nil {
		t.Fatal("expected idempotency conflict after restart for different command body")
	}
	if !isIdempotencyConflict(err) {
		t.Fatalf("expected idempotency conflict, got %v", err)
	}
}

func TestCommandStore_SameSizeReplacementIsRescanned(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// The original command has a 40-character text so that a same-length
	// replacement with a 7-character text plus an idempotency_key field has
	// exactly the same JSON size.
	originalCmd := json.RawMessage(`{"type":"user_message","text":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx","attachments":[]}`)
	_, err = store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "", originalCmd)
	if err != nil {
		t.Fatal(err)
	}

	replacementCmd := json.RawMessage(`{"type":"user_message","text":"yyyyyyy","attachments":[]}`)
	replacementID := "00000000-0000-4000-8000-000000000002"
	replacementRecord := testLogRecord(1, replacementID, replacementCmd, "018f47a2-9b3c-7def-8abc-0123456789ab")
	replacementRecord.IdempotencyKey = "replaced-key"
	replacementLine, err := json.Marshal(replacementRecord)
	if err != nil {
		t.Fatal(err)
	}
	replacementLine = append(replacementLine, '\n')

	path := commandLogPath(dir, "018f47a2-9b3c-7def-8abc-0123456789ab")
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(replacementLine) != len(original) {
		t.Fatalf("test setup: replacement length %d != original length %d", len(replacementLine), len(original))
	}
	if err := os.WriteFile(path, replacementLine, 0o600); err != nil {
		t.Fatal(err)
	}

	// A same-size rewrite should be detected by refreshStateLocked and the
	// idempotency key must be visible on the next Append.
	env, err := store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "replaced-key", replacementCmd)
	if err != nil {
		t.Fatalf("append after replacement: %v", err)
	}
	if env.Seq != 1 {
		t.Fatalf("expected existing seq 1 after rescan, got %d", env.Seq)
	}
	if env.CommandID != replacementID {
		t.Fatalf("expected replacement command_id %q, got %q", replacementID, env.CommandID)
	}
}

func TestCommandStore_PartialTailRecovery(t *testing.T) {
	dir := t.TempDir()
	env := func(seq uint64) string {
		b, _ := json.Marshal(testLogRecord(
			seq,
			"00000000-0000-4000-8000-00000000000"+string(rune('1'+seq-1)),
			json.RawMessage(`{"type":"user_message","text":"x","attachments":[]}`),
			"018f47a2-9b3c-7def-8abc-0123456789ab",
		))
		return string(b)
	}

	// Two complete records followed by a partial third (no trailing newline).
	contents := env(1) + "\n" + env(2) + "\n" + env(3)[:len(env(3))-3]
	path := commandLogPath(dir, "018f47a2-9b3c-7def-8abc-0123456789ab")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	caught, err := store.CatchUp(context.Background(), "018f47a2-9b3c-7def-8abc-0123456789ab", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(caught) != 2 || caught[0].Seq != 1 || caught[1].Seq != 2 {
		t.Fatalf("expected seq [1,2], got %+v", caught)
	}

	next, err := store.NextCommandSeq(context.Background(), "018f47a2-9b3c-7def-8abc-0123456789ab")
	if err != nil {
		t.Fatal(err)
	}
	if next != 3 {
		t.Fatalf("expected next seq 3 after tail truncation, got %d", next)
	}

	env3, err := store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "", json.RawMessage(`{"type":"user_message","text":"third","attachments":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	if env3.Seq != 3 {
		t.Fatalf("expected seq 3 after recovery, got %d", env3.Seq)
	}
}

func TestCommandStore_InteriorMalformedRecordIsRejected(t *testing.T) {
	dir := t.TempDir()
	env := func(seq uint64) string {
		b, _ := json.Marshal(testLogRecord(
			seq,
			"00000000-0000-4000-8000-00000000000"+string(rune('1'+seq-1)),
			json.RawMessage(`{"type":"user_message","text":"x","attachments":[]}`),
			"018f47a2-9b3c-7def-8abc-0123456789ab",
		))
		return string(b)
	}

	// A complete record, then a malformed complete interior record, then a
	// partial final record. Only the final tail may be truncated; the interior
	// malformed record must fail the load.
	contents := env(1) + "\n" + `{"not":"valid"` + "\n" + env(2)[:len(env(2))-3]
	path := commandLogPath(dir, "018f47a2-9b3c-7def-8abc-0123456789ab")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := OpenCommandStore(dir)
	if err == nil {
		t.Fatal("expected OpenCommandStore to reject malformed interior record")
	}
	if !strings.Contains(err.Error(), "decode command log") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCommandStore_FirstCommandSeqAndCatchUp(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	first, err := store.FirstCommandSeq(context.Background(), "018f47a2-9b3c-7def-8abc-0123456789ab")
	if err != nil {
		t.Fatal(err)
	}
	if first != 1 {
		t.Fatalf("expected FirstCommandSeq 1 for empty log, got %d", first)
	}

	for i := 1; i <= 3; i++ {
		cmd := json.RawMessage(fmt.Sprintf(`{"type":"user_message","text":"msg %d","attachments":[]}`, i))
		if _, err := store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "", cmd); err != nil {
			t.Fatal(err)
		}
	}

	first, err = store.FirstCommandSeq(context.Background(), "018f47a2-9b3c-7def-8abc-0123456789ab")
	if err != nil {
		t.Fatal(err)
	}
	if first != 1 {
		t.Fatalf("expected FirstCommandSeq 1, got %d", first)
	}

	// Catch-up from LastAppliedCommandSeq+1 (2) must return seq 2 and 3.
	caught, err := store.CatchUp(context.Background(), "018f47a2-9b3c-7def-8abc-0123456789ab", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(caught) != 2 || caught[0].Seq != 2 || caught[1].Seq != 3 {
		t.Fatalf("expected catch-up [2,3], got %+v", caught)
	}
}

func TestCommandStore_RejectsSymlinkedLogFile(t *testing.T) {
	dir := t.TempDir()
	outside, err := os.MkdirTemp("", "sumi-command-escape")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(outside)

	target := filepath.Join(outside, "real.jsonl")
	if err := os.WriteFile(target, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	link := commandLogPath(dir, "018f47a2-9b3c-7def-8abc-0123456789ab")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}

	_, err = OpenCommandStore(dir)
	if err == nil {
		t.Fatal("expected OpenCommandStore to reject symlinked log file")
	}
	if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("expected symlink error, got %v", err)
	}
}

func TestCommandStore_DuplicateSeqOnDiskIsRejected(t *testing.T) {
	dir := t.TempDir()
	rec := testLogRecord(1, "00000000-0000-4000-8000-000000000001", json.RawMessage(`{"type":"user_message","text":"x","attachments":[]}`), "018f47a2-9b3c-7def-8abc-0123456789ab")
	line, _ := json.Marshal(rec)
	// Write the same seq twice, simulating a crash where a committed line was
	// appended again before nextSeq advanced.
	contents := string(line) + "\n" + string(line) + "\n"
	path := commandLogPath(dir, "018f47a2-9b3c-7def-8abc-0123456789ab")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := OpenCommandStore(dir)
	if err == nil {
		t.Fatal("expected OpenCommandStore to reject duplicate seq on disk")
	}
	if !strings.Contains(err.Error(), "duplicate seq") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// failingFile wraps *os.File and can fail on a specific 1-indexed call to a
// method, giving tests deterministic control over write/sync/truncate failures.
type failingFile struct {
	*os.File
	mu             sync.Mutex
	writeCalls     int
	failWriteOn    int
	syncCalls      int
	failSyncOn     int
	truncateCalls  int
	failTruncateOn int
	seekCalls      int
	failSeekOn     int
}

func (f *failingFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	f.writeCalls++
	fail := f.writeCalls == f.failWriteOn
	f.mu.Unlock()
	if fail {
		return 0, errors.New("injected write failure")
	}
	return f.File.Write(p)
}

func (f *failingFile) Sync() error {
	f.mu.Lock()
	f.syncCalls++
	fail := f.syncCalls == f.failSyncOn
	f.mu.Unlock()
	if fail {
		return errors.New("injected sync failure")
	}
	return f.File.Sync()
}

func (f *failingFile) Truncate(size int64) error {
	f.mu.Lock()
	f.truncateCalls++
	fail := f.truncateCalls == f.failTruncateOn
	f.mu.Unlock()
	if fail {
		return errors.New("injected truncate failure")
	}
	return f.File.Truncate(size)
}

func (f *failingFile) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	f.seekCalls++
	fail := f.seekCalls == f.failSeekOn
	f.mu.Unlock()
	if fail {
		return 0, errors.New("injected seek failure")
	}
	return f.File.Seek(offset, whence)
}

func (f *failingFile) Fd() uintptr {
	return f.File.Fd()
}

func injectFailingFile(t *testing.T, store *CommandStore, personalityAgentID string) *failingFile {
	t.Helper()
	st := store.states[personalityAgentID]
	if st == nil {
		t.Fatalf("no state for personality agent %q", personalityAgentID)
	}
	ff := &failingFile{File: st.file.(*os.File)}
	st.file = ff
	return ff
}

func TestCommandStore_PoisonOnWriteRollbackFailure(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Seed the file so the append path seeks to end and then fails.
	if _, err := store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "", json.RawMessage(`{"type":"abort"}`)); err != nil {
		t.Fatal(err)
	}

	ff := injectFailingFile(t, store, "018f47a2-9b3c-7def-8abc-0123456789ab")
	ff.failWriteOn = 1
	ff.failTruncateOn = 1

	_, err = store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "", json.RawMessage(`{"type":"abort"}`))
	if err == nil {
		t.Fatal("expected append to fail")
	}
	if !strings.Contains(err.Error(), "rollback could not be confirmed") {
		t.Fatalf("expected compound rollback error, got %v", err)
	}

	_, err = store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "", json.RawMessage(`{"type":"abort"}`))
	if err == nil || !strings.Contains(err.Error(), "poisoned") {
		t.Fatalf("expected poisoned state error, got %v", err)
	}
}

func TestCommandStoreAppendRejectsInvalidCommandBeforeWrite(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if _, err := store.Append(
		context.Background(),
		testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"),
		"",
		json.RawMessage(`{"type":"abort","extra":true}`),
	); err == nil {
		t.Fatal("direct append must reject an invalid command")
	}

	env, err := store.Append(
		context.Background(),
		testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"),
		"",
		json.RawMessage(`{"type":"abort"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	if env.Seq != 1 {
		t.Fatalf("invalid command must not allocate sequence space, got %d", env.Seq)
	}
	raw, err := os.ReadFile(commandLogPath(dir, "018f47a2-9b3c-7def-8abc-0123456789ab"))
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(string(raw), "\n"); lines != 1 {
		t.Fatalf("expected only the valid command record, got %q", raw)
	}
}

func TestCommandStore_PoisonOnSyncRollbackFailure(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if _, err := store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "", json.RawMessage(`{"type":"abort"}`)); err != nil {
		t.Fatal(err)
	}

	ff := injectFailingFile(t, store, "018f47a2-9b3c-7def-8abc-0123456789ab")
	ff.failSyncOn = 1
	ff.failTruncateOn = 1

	_, err = store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "", json.RawMessage(`{"type":"abort"}`))
	if err == nil {
		t.Fatal("expected append to fail")
	}
	if !strings.Contains(err.Error(), "rollback could not be confirmed") {
		t.Fatalf("expected compound rollback error, got %v", err)
	}

	_, err = store.NextCommandSeq(context.Background(), "018f47a2-9b3c-7def-8abc-0123456789ab")
	if err == nil || !strings.Contains(err.Error(), "poisoned") {
		t.Fatalf("expected poisoned state error, got %v", err)
	}
}

func TestCommandStore_NoPoisonOnRollbackSuccess(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if _, err := store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "", json.RawMessage(`{"type":"abort"}`)); err != nil {
		t.Fatal(err)
	}

	ff := injectFailingFile(t, store, "018f47a2-9b3c-7def-8abc-0123456789ab")
	ff.failSyncOn = 1

	_, err = store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "", json.RawMessage(`{"type":"abort"}`))
	if err == nil {
		t.Fatal("expected append to fail")
	}
	if strings.Contains(err.Error(), "poisoned") {
		t.Fatalf("expected non-poisoning sync error, got %v", err)
	}

	next, err := store.NextCommandSeq(context.Background(), "018f47a2-9b3c-7def-8abc-0123456789ab")
	if err != nil {
		t.Fatal(err)
	}
	if next != 2 {
		t.Fatalf("expected next seq to remain 2 after rollback, got %d", next)
	}

	env, err := store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "", json.RawMessage(`{"type":"abort"}`))
	if err != nil {
		t.Fatal(err)
	}
	if env.Seq != 2 {
		t.Fatalf("expected seq 2 after successful retry, got %d", env.Seq)
	}
}

func TestCommandStore_LoadIncompleteTailWithoutNewline(t *testing.T) {
	dir := t.TempDir()
	rec := func(seq uint64) []byte {
		r := testLogRecord(seq, fmt.Sprintf("00000000-0000-4000-8000-%012d", seq), json.RawMessage(`{"type":"user_message","text":"x","attachments":[]}`), "018f47a2-9b3c-7def-8abc-0123456789ab")
		b, _ := json.Marshal(r)
		return b
	}

	partial := string(rec(2))[:len(rec(2))-7]
	contents := string(rec(1)) + "\n" + partial
	path := commandLogPath(dir, "018f47a2-9b3c-7def-8abc-0123456789ab")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	first, err := store.FirstCommandSeq(context.Background(), "018f47a2-9b3c-7def-8abc-0123456789ab")
	if err != nil {
		t.Fatal(err)
	}
	if first != 1 {
		t.Fatalf("expected first seq 1, got %d", first)
	}
	next, err := store.NextCommandSeq(context.Background(), "018f47a2-9b3c-7def-8abc-0123456789ab")
	if err != nil {
		t.Fatal(err)
	}
	if next != 2 {
		t.Fatalf("expected next seq 2 after truncating incomplete tail, got %d", next)
	}
}

func TestCommandStore_LoadMalformedFinalRecordWithoutNewlineFails(t *testing.T) {
	dir := t.TempDir()
	rec := testLogRecord(1, "00000000-0000-4000-8000-000000000001", json.RawMessage(`{"type":"user_message","text":"x","attachments":[]}`), "018f47a2-9b3c-7def-8abc-0123456789ab")
	line, _ := json.Marshal(rec)
	// A complete (all braces present) but syntactically invalid final record with no newline.
	malformed := `{"seq":2,"command_id":"00000000-0000-4000-8000-000000000002","command":{}}}`
	contents := string(line) + "\n" + malformed
	path := commandLogPath(dir, "018f47a2-9b3c-7def-8abc-0123456789ab")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := OpenCommandStore(dir)
	if err == nil {
		t.Fatal("expected OpenCommandStore to reject complete-but-malformed final record")
	}
	if !strings.Contains(err.Error(), "malformed but complete") {
		t.Fatalf("expected malformed-but-complete error, got %v", err)
	}
}

func TestCommandStore_LoadValidFinalRecordWithoutNewline(t *testing.T) {
	dir := t.TempDir()
	rec := func(seq uint64) []byte {
		r := testLogRecord(seq, fmt.Sprintf("00000000-0000-4000-8000-%012d", seq), json.RawMessage(`{"type":"user_message","text":"x","attachments":[]}`), "018f47a2-9b3c-7def-8abc-0123456789ab")
		b, _ := json.Marshal(r)
		return b
	}

	contents := string(rec(1)) + "\n" + string(rec(2))
	path := commandLogPath(dir, "018f47a2-9b3c-7def-8abc-0123456789ab")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	next, err := store.NextCommandSeq(context.Background(), "018f47a2-9b3c-7def-8abc-0123456789ab")
	if err != nil {
		t.Fatal(err)
	}
	if next != 3 {
		t.Fatalf("expected next seq 3, got %d", next)
	}
}

func TestCommandStore_RepairMissingTrailingNewlineEndToEnd(t *testing.T) {
	dir := t.TempDir()
	rec := func(seq uint64, text string) []byte {
		r := testLogRecord(seq, fmt.Sprintf("00000000-0000-4000-8000-%012d", seq), json.RawMessage(fmt.Sprintf(`{"type":"user_message","text":%q,"attachments":[]}`, text)), "018f47a2-9b3c-7def-8abc-0123456789ab")
		b, _ := json.Marshal(r)
		return b
	}

	// Two valid records; the second is missing its trailing newline.
	contents := string(rec(1, "first")) + "\n" + string(rec(2, "second"))
	path := commandLogPath(dir, "018f47a2-9b3c-7def-8abc-0123456789ab")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	env3, err := store.Append(context.Background(), testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"), "", json.RawMessage(`{"type":"user_message","text":"third","attachments":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	if env3.Seq != 3 {
		t.Fatalf("expected seq 3, got %d", env3.Seq)
	}

	all, err := store.CatchUp(context.Background(), "018f47a2-9b3c-7def-8abc-0123456789ab", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 || all[0].Seq != 1 || all[1].Seq != 2 || all[2].Seq != 3 {
		t.Fatalf("expected [1,2,3], got %+v", all)
	}

	texts := []string{"first", "second", "third"}
	for i, env := range all {
		if !strings.Contains(string(env.Command), texts[i]) {
			t.Fatalf("command %d does not contain %q: %s", env.Seq, texts[i], env.Command)
		}
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store2, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	all2, err := store2.CatchUp(context.Background(), "018f47a2-9b3c-7def-8abc-0123456789ab", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(all2) != 3 || all2[0].Seq != 1 || all2[1].Seq != 2 || all2[2].Seq != 3 {
		t.Fatalf("after reopen expected [1,2,3], got %+v", all2)
	}

	seen := make(map[string]bool)
	for _, env := range all2 {
		if seen[env.CommandID] {
			t.Fatalf("duplicate command_id %q", env.CommandID)
		}
		seen[env.CommandID] = true
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	trimmed := strings.TrimSuffix(string(raw), "\n")
	lines := strings.Split(trimmed, "\n")
	if len(lines) != 3 {
		t.Fatalf("expected exactly 3 JSONL lines, got %d: %s", len(lines), string(raw))
	}
	for i, l := range lines {
		if len(l) == 0 {
			t.Fatalf("line %d is empty", i)
		}
		var lr LogRecord
		if err := json.Unmarshal([]byte(l), &lr); err != nil {
			t.Fatalf("line %d is not valid JSON: %v: %q", i, err, l)
		}
	}
}

// TestCommandStore_MultiProcessWorker is invoked as a sub-process by the
// multi-process tests below. It is skipped when run as a normal test.
func TestCommandStore_MultiProcessWorker(t *testing.T) {
	if os.Getenv("SUMI_MP_WORKER") == "" {
		t.Skip("sub-process worker only")
	}

	dir := os.Getenv("SUMI_MP_DIR")
	conv := os.Getenv("SUMI_MP_CONV")
	mode := os.Getenv("SUMI_MP_MODE")

	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	switch mode {
	case "append":
		count, err := strconv.Atoi(os.Getenv("SUMI_MP_COUNT"))
		if err != nil {
			t.Fatalf("invalid count: %v", err)
		}
		id := os.Getenv("SUMI_MP_ID")
		for i := 0; i < count; i++ {
			text := fmt.Sprintf("child-%s-%d", id, i)
			cmd := json.RawMessage(fmt.Sprintf(`{"type":"user_message","text":%q,"attachments":[]}`, text))
			env, err := store.Append(context.Background(), testDirectChatProvenance(conv), "", cmd)
			if err != nil {
				t.Fatalf("append %d: %v", i, err)
			}
			fmt.Println(env.Seq)
		}
	case "rollback":
		st := store.states[conv]
		if st == nil {
			t.Fatalf("no state for personality agent %q", conv)
		}
		ff := &failingFile{File: st.file.(*os.File), failSyncOn: 1}
		st.file = ff
		cmd := json.RawMessage(`{"type":"user_message","text":"rollback-child","attachments":[]}`)
		_, err := store.Append(context.Background(), testDirectChatProvenance(conv), "", cmd)
		if err == nil {
			t.Fatal("expected append to fail with injected sync failure")
		}
	case "keyed":
		cmd := json.RawMessage(`{"type":"user_message","text":"global-idempotency","attachments":[]}`)
		_, err := store.Append(context.Background(), testDirectChatProvenance(conv), "global-key", cmd)
		switch {
		case err == nil:
			fmt.Println("KEYED_ACCEPTED")
		case errors.Is(err, errIdempotencyConflict):
			fmt.Println("KEYED_CONFLICT")
		default:
			t.Fatalf("keyed append: %v", err)
		}
	default:
		t.Fatalf("unknown mode %q", mode)
	}
}

func skipIfNoFlock(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("flock is not available on windows")
	}
}

func runMPWorker(t *testing.T, dir, conv, mode, id, count string) ([]byte, error) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCommandStore_MultiProcessWorker$", "-test.v")
	cmd.Env = append(
		os.Environ(),
		"SUMI_MP_WORKER=1",
		"SUMI_MP_DIR="+dir,
		"SUMI_MP_CONV="+conv,
		"SUMI_MP_MODE="+mode,
		"SUMI_MP_ID="+id,
		"SUMI_MP_COUNT="+count,
	)
	return cmd.CombinedOutput()
}

func TestCommandStore_MultiProcessNoDuplicateSeqOrLostRecord(t *testing.T) {
	skipIfNoFlock(t)

	dir := t.TempDir()
	conv := "018f47a2-9b3c-7def-8abc-012345678994"
	const children = 3
	const count = 20

	var wg sync.WaitGroup
	errs := make(chan error, children)
	for i := 0; i < children; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out, err := runMPWorker(t, dir, conv, "append", strconv.Itoa(i), strconv.Itoa(count))
			if err != nil {
				errs <- fmt.Errorf("child %d failed: %w\n%s", i, err, out)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	all, err := store.CatchUp(context.Background(), conv, 1)
	if err != nil {
		t.Fatal(err)
	}
	want := children * count
	if len(all) != want {
		t.Fatalf("expected %d commands, got %d", want, len(all))
	}

	seen := make(map[uint64]bool)
	for i, env := range all {
		if env.Seq != uint64(i+1) {
			t.Fatalf("non-contiguous seq at index %d: got %d", i, env.Seq)
		}
		if seen[env.Seq] {
			t.Fatalf("duplicate seq %d", env.Seq)
		}
		seen[env.Seq] = true
		if !strings.Contains(string(env.Command), "child-") {
			t.Fatalf("command %d missing child marker: %s", env.Seq, env.Command)
		}
	}

	next, err := store.NextCommandSeq(context.Background(), conv)
	if err != nil {
		t.Fatal(err)
	}
	if next != uint64(want+1) {
		t.Fatalf("expected next seq %d, got %d", want+1, next)
	}
}

func TestCommandStore_MultiProcessIdempotencyIsGlobalAcrossTargets(t *testing.T) {
	skipIfNoFlock(t)

	dir := t.TempDir()
	targets := []string{
		"018f47a2-9b3c-7def-8abc-012345678996",
		"018f47a2-9b3c-7def-9abc-012345678997",
	}
	outputs := make(chan []byte, len(targets))
	errs := make(chan error, len(targets))
	var wg sync.WaitGroup
	for _, target := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := runMPWorker(t, dir, target, "keyed", "", "")
			if err != nil {
				errs <- fmt.Errorf("target %s failed: %w\n%s", target, err, out)
				return
			}
			outputs <- out
		}()
	}
	wg.Wait()
	close(outputs)
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	var accepted, conflicts int
	for out := range outputs {
		switch {
		case bytes.Contains(out, []byte("KEYED_ACCEPTED")):
			accepted++
		case bytes.Contains(out, []byte("KEYED_CONFLICT")):
			conflicts++
		default:
			t.Fatalf("worker returned no keyed result:\n%s", out)
		}
	}
	if accepted != 1 || conflicts != 1 {
		t.Fatalf("global idempotency race accepted=%d conflicts=%d", accepted, conflicts)
	}
}

func TestCommandStore_MultiProcessRollbackDoesNotDestroyPeerRecord(t *testing.T) {
	skipIfNoFlock(t)

	dir := t.TempDir()
	conv := "018f47a2-9b3c-7def-8abc-012345678995"

	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	parentCmd := json.RawMessage(`{"type":"user_message","text":"parent","attachments":[]}`)
	env1, err := store.Append(context.Background(), testDirectChatProvenance(conv), "", parentCmd)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	out, err := runMPWorker(t, dir, conv, "rollback", "", "")
	if err != nil {
		t.Fatalf("rollback worker failed: %v\n%s", err, out)
	}

	store2, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	all, err := store2.CatchUp(context.Background(), conv, env1.Seq)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 || all[0].Seq != env1.Seq {
		t.Fatalf("peer record destroyed by rollback: got %+v", all)
	}

	next, err := store2.NextCommandSeq(context.Background(), conv)
	if err != nil {
		t.Fatal(err)
	}
	if next != env1.Seq+1 {
		t.Fatalf("expected next seq %d after rollback, got %d", env1.Seq+1, next)
	}
}

func TestCommandStore_RejectsCorruptNonContiguousLogsOnOpen(t *testing.T) {
	for _, seqs := range [][]uint64{{2}, {1, 3}, {2, 1}, {1, 1}} {
		t.Run(fmt.Sprintf("seqs_%v", seqs), func(t *testing.T) {
			dir := t.TempDir()
			conv := "018f47a2-9b3c-7def-8abc-012345678996"
			var log bytes.Buffer
			for i, seq := range seqs {
				record := testLogRecord(seq, fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1), json.RawMessage(`{"type":"user_message","text":"x","attachments":[]}`), conv)
				line, err := json.Marshal(record)
				if err != nil {
					t.Fatal(err)
				}
				log.Write(line)
				log.WriteByte('\n')
			}
			if err := os.WriteFile(commandLogPath(dir, conv), log.Bytes(), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := OpenCommandStore(dir); err == nil || (!strings.Contains(err.Error(), "non-contiguous") && !strings.Contains(err.Error(), "duplicate seq")) {
				t.Fatalf("expected corrupt log rejection, got %v", err)
			}
		})
	}
}

func TestCommandStore_RejectLogWithSeqExceedingJsonSafeInteger(t *testing.T) {
	dir := t.TempDir()
	conv := "018f47a2-9b3c-7def-8abc-012345678998"

	seed := testLogRecord(maxJSONSafeInteger+1, "00000000-0000-4000-8000-000000000000", json.RawMessage(`{"type":"user_message","text":"bad","attachments":[]}`), conv)
	seedLine, _ := json.Marshal(seed)
	path := commandLogPath(dir, conv)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(seedLine, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := OpenCommandStore(dir)
	if err == nil {
		t.Fatal("expected open to reject log with out-of-range seq")
	}
	if !strings.Contains(err.Error(), "JSON-safe integer") {
		t.Fatalf("expected JSON-safe integer error, got %v", err)
	}
}

func TestCommandStore_ReopenRejectsInvalidPersistedCommandVocabulary(t *testing.T) {
	for name, command := range map[string]string{
		"null":                  `null`,
		"non_object":            `[]`,
		"non_string_type":       `{"type":1}`,
		"unknown_type":          `{"type":"future_command"}`,
		"invalid_variant_shape": `{"type":"user_message","text":"x","attachments":[{}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			record := testLogRecord(
				1,
				"00000000-0000-4000-8000-000000000001",
				json.RawMessage(command),
				"018f47a2-9b3c-7def-8abc-0123456789ab",
			)
			line, marshalErr := json.Marshal(record)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			if err := os.WriteFile(commandLogPath(dir, "018f47a2-9b3c-7def-8abc-0123456789ab"), append(line, '\n'), 0o600); err != nil {
				t.Fatal(err)
			}
			store, err := OpenCommandStore(dir)
			if err == nil {
				defer store.Close()
				t.Fatalf("reopen accepted persisted %s command", name)
			}
			if !strings.Contains(err.Error(), "invalid persisted command") {
				t.Fatalf("unexpected reopen error: %v", err)
			}
		})
	}
}

func TestCommandStore_ReopenCatchUpAcceptsCurrentCommandVocabulary(t *testing.T) {
	dir := t.TempDir()
	record := testLogRecord(1, "00000000-0000-4000-8000-000000000001", json.RawMessage(`{"type":"abort"}`), "018f47a2-9b3c-7def-8abc-0123456789ab")
	line, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(commandLogPath(dir, "018f47a2-9b3c-7def-8abc-0123456789ab"), append(line, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	commands, err := store.CatchUp(context.Background(), "018f47a2-9b3c-7def-8abc-0123456789ab", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 {
		t.Fatalf("unexpected recovered commands: %+v", commands)
	}
	commandType, err := commands[0].CommandType()
	if err != nil || commandType != "abort" {
		t.Fatalf("unexpected recovered commands: %+v", commands)
	}
}
