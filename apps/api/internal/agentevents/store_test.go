package agentevents

import (
	"context"
	"encoding/json"
	"regexp"
	"sync"
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
	env, err := store.Append(context.Background(), "conv-1", "", cmd)
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

	env2, err := store.Append(context.Background(), "conv-1", "", cmd)
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
			envelopes[idx], errs[idx] = store.Append(context.Background(), "conv-1", "", cmd)
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
	env1, err := store.Append(context.Background(), "conv-1", "", cmd1)
	if err != nil {
		t.Fatal(err)
	}

	cmd2 := json.RawMessage(`{"type":"user_message","text":"second","attachments":[]}`)
	env2, err := store.Append(context.Background(), "conv-1", "", cmd2)
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

	next, err := store2.NextCommandSeq(context.Background(), "conv-1")
	if err != nil {
		t.Fatal(err)
	}
	if next != env2.Seq+1 {
		t.Fatalf("expected next seq %d after restart, got %d", env2.Seq+1, next)
	}

	caught, err := store2.CatchUp(context.Background(), "conv-1", 0)
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

	env3, err := store2.Append(context.Background(), "conv-1", "", json.RawMessage(`{"type":"user_message","text":"third","attachments":[]}`))
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
	env1, err := store.Append(context.Background(), "conv-1", "key-1", cmd)
	if err != nil {
		t.Fatal(err)
	}

	env2, err := store.Append(context.Background(), "conv-1", "key-1", cmd)
	if err != nil {
		t.Fatal(err)
	}
	if env2.Seq != env1.Seq || env2.CommandID != env1.CommandID {
		t.Fatal("retried submission with same key allocated a second command")
	}

	cmd3 := json.RawMessage(`{"type":"user_message","text":"different","attachments":[]}`)
	_, err = store.Append(context.Background(), "conv-1", "key-1", cmd3)
	if err == nil {
		t.Fatal("expected idempotency key conflict for different command body")
	}
	if !isIdempotencyConflict(err) {
		t.Fatalf("expected idempotency conflict, got %v", err)
	}
}

func TestCommandStore_PerConversationSeq(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	cmd := json.RawMessage(`{"type":"user_message","text":"x","attachments":[]}`)
	a, err := store.Append(context.Background(), "conv-a", "", cmd)
	if err != nil {
		t.Fatal(err)
	}
	b, err := store.Append(context.Background(), "conv-b", "", cmd)
	if err != nil {
		t.Fatal(err)
	}
	if a.Seq != 1 || b.Seq != 1 {
		t.Fatalf("expected first seq 1 for each conversation, got %d and %d", a.Seq, b.Seq)
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
		if _, err := store.Append(context.Background(), "conv-1", "", cmd); err != nil {
			t.Fatal(err)
		}
	}

	caught, err := store.CatchUp(context.Background(), "conv-1", 2)
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
	_, err = store.Append(ctx, "conv-1", "", cmd)
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

func TestCommandStore_DurableCommitBeforeSuccess(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	cmd := json.RawMessage(`{"type":"user_message","text":"durable","attachments":[]}`)
	env, err := store.Append(context.Background(), "conv-1", "", cmd)
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

	caught, err := store2.CatchUp(context.Background(), "conv-1", env.Seq)
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
			env, err := store.Append(context.Background(), "conv-1", "", cmd)
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
