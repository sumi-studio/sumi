package agentevents

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// blockingMirror holds the first Sync of one command log until released, as
// a mirror waiting on a slow or unreachable database does.
type blockingMirror struct {
	path    string
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (m *blockingMirror) Wrap(path string, _ int, file JournalFile) (JournalFile, error) {
	if path != m.path {
		return file, nil
	}
	return &blockingFile{JournalFile: file, mirror: m}, nil
}

func (m *blockingMirror) WrapAtomicWrite(write func(string, []byte, os.FileMode) error) func(string, []byte, os.FileMode) error {
	return write
}

type blockingFile struct {
	JournalFile
	mirror *blockingMirror
}

func (f *blockingFile) Sync() error {
	if err := f.JournalFile.Sync(); err != nil {
		return err
	}
	blocked := false
	f.mirror.once.Do(func() { blocked = true })
	if blocked {
		close(f.mirror.entered)
		<-f.mirror.release
	}
	return nil
}

// keyOutsideStripe returns a key whose idempotency lock differs from key's.
func keyOutsideStripe(t *testing.T, key string) string {
	t.Helper()
	for i := 0; i < 1000; i++ {
		other := key + "-other-" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		if idempotencyLockName(other) != idempotencyLockName(key) {
			return other
		}
	}
	t.Fatal("no key outside the stripe")
	return ""
}

// N-3: a keyed append blocked in the mirror delays neither another persona's
// keyed append nor a lookup; the same key stays exactly-once.
func TestKeyedAppendBlockedInMirrorDoesNotDelayOtherPersonas(t *testing.T) {
	const personaA, personaB = "018f47a2-9b3c-7def-8abc-0123456789ab", "018f47a2-9b3c-7def-8abc-0123456789ac"
	dir := t.TempDir()
	abs, err := filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	mirror := &blockingMirror{path: commandLogPath(abs, personaA), entered: make(chan struct{}), release: make(chan struct{})}
	store, err := OpenMirroredCommandStore(dir, mirror)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	release := sync.OnceFunc(func() { close(mirror.release) })
	defer release() // before Close, which waits for the blocked append
	command := json.RawMessage(`{"type":"abort"}`)
	ctx := context.Background()

	const slowKey = "slow-key"
	slow := make(chan error, 1)
	go func() {
		_, err := store.Append(ctx, testDirectChatProvenance(personaA), slowKey, command)
		slow <- err
	}()
	select {
	case <-mirror.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("persona A's append did not reach the mirror")
	}

	// Another persona, another key: admitted while A is still blocked.
	quick, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	otherKey := keyOutsideStripe(t, slowKey)
	started := time.Now()
	env, err := store.Append(quick, testDirectChatProvenance(personaB), otherKey, command)
	if err != nil {
		t.Fatalf("persona B's keyed append while A is blocked in the mirror: %v", err)
	}
	if env.Seq != 1 {
		t.Fatalf("persona B seq = %d", env.Seq)
	}
	if _, found, err := store.Lookup(quick, testDirectChatProvenance(personaB), otherKey, command); err != nil || !found {
		t.Fatalf("lookup while A is blocked = %v %v", found, err)
	}
	t.Logf("persona B admitted in %v while persona A's append was held in the mirror", time.Since(started).Round(time.Microsecond))

	// The same key for another persona waits for A's admission instead of
	// admitting it twice.
	same := make(chan error, 1)
	go func() {
		_, err := store.Append(ctx, testDirectChatProvenance(personaB), slowKey, command)
		same <- err
	}()
	select {
	case err := <-same:
		t.Fatalf("same key admitted while its first admission is in flight: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	release()
	if err := <-slow; err != nil {
		t.Fatalf("persona A's append: %v", err)
	}
	if err := <-same; !errors.Is(err, errIdempotencyConflict) {
		t.Fatalf("same key for another persona = %v, want the conflict", err)
	}
	if seq, err := store.NextCommandSeq(ctx, personaB); err != nil || seq != 2 {
		t.Fatalf("persona B next seq = %d, %v; the conflicting key must not have been appended", seq, err)
	}
}

// N-3: with per-key locks another key's record may be half written while a
// keyed admission scans the logs. A last line without its newline is not an
// admission and does not fail the scan.
func TestIdempotencyScanSkipsARecordStillBeingWritten(t *testing.T) {
	const personaA, personaB = "018f47a2-9b3c-7def-8abc-0123456789ab", "018f47a2-9b3c-7def-8abc-0123456789ac"
	dir := t.TempDir()
	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	command := json.RawMessage(`{"type":"abort"}`)
	ctx := context.Background()
	if _, err := store.Append(ctx, testDirectChatProvenance(personaA), "done-key", command); err != nil {
		t.Fatal(err)
	}
	abs, _ := filepath.Abs(dir)
	f, err := os.OpenFile(commandLogPath(abs, personaA), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"seq":2,"command_id":"x","personality_agent_id":"` + personaA + `","idempotency_key":"in-flight-key","comm`); err != nil {
		t.Fatal(err)
	}
	f.Close()

	env, err := store.Append(ctx, testDirectChatProvenance(personaB), "in-flight-key", command)
	if err != nil {
		t.Fatalf("keyed append while another record is half written: %v", err)
	}
	if env.PersonalityAgentID != personaB || env.Seq != 1 {
		t.Fatalf("admitted %+v", env)
	}
	if _, found, err := store.Lookup(ctx, testDirectChatProvenance(personaA), "done-key", command); err != nil || !found {
		t.Fatalf("complete record not found: %v %v", found, err)
	}
}
