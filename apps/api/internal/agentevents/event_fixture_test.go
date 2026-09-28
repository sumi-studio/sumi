package agentevents

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

var testSecret = []byte("test-secret-32bytes-long-string!!")

func openRuntimeGateway(t *testing.T) *BrowserJournal {
	t.Helper()
	store, err := OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	gateway, err := OpenBrowserJournal(privateRuntimeDir(t), store)
	if err != nil {
		t.Fatal(err)
	}
	gateway.PollInterval = 5 * time.Millisecond
	return gateway
}
func openGatewayAt(t *testing.T, storeDir, journalDir string) (*CommandStore, *BrowserJournal, error) {
	t.Helper()
	store, err := OpenCommandStore(storeDir)
	if err != nil {
		return nil, nil, err
	}
	gateway, err := OpenBrowserJournal(journalDir, store)
	if err != nil {
		_ = store.Close()
		return nil, nil, err
	}
	gateway.PollInterval = 5 * time.Millisecond
	return store, gateway, nil
}
func failingOpener(ff *failingFile) func(string, int, os.FileMode) (durableFileHandle, error) {
	return func(name string, flag int, perm os.FileMode) (durableFileHandle, error) {
		f, err := os.OpenFile(name, flag, perm)
		if err != nil {
			return nil, err
		}
		ff.File = f
		return ff, nil
	}
}
func realOpener() func(string, int, os.FileMode) (durableFileHandle, error) {
	return func(name string, flag int, perm os.FileMode) (durableFileHandle, error) {
		return os.OpenFile(name, flag, perm)
	}
}

func currentRuntimeClaims(t testing.TB, g *BrowserJournal, id string) JournalScope {
	return JournalScope{PersonalityAgentID: id}
}

// appendFixtureEvent exercises the current event file and browser delivery boundaries.
func (g *BrowserJournal) Receive(ctx context.Context, claims JournalScope, e Envelope) error {
	if err := validateEnvelope(e); err != nil {
		return err
	}
	if e.PersonalityAgentID != claims.PersonalityAgentID {
		return errors.New("persona mismatch")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if e.Seq == nil {
		g.publishVolatileLocked(e.PersonalityAgentID, e)
		return nil
	}
	if err := g.appendDurableEventLocked(ctx, e.PersonalityAgentID, durableEventRecord{Seq: *e.Seq, Event: e}); err != nil {
		return err
	}
	g.updateAgentSessionStateLocked(e.PersonalityAgentID, e.Event)
	return nil
}

type countingDurableFile struct {
	durableFileHandle
	readBytes *atomic.Int64
}

func (f *countingDurableFile) Read(p []byte) (int, error) {
	n, err := f.durableFileHandle.Read(p)
	f.readBytes.Add(int64(n))
	return n, err
}
func attentionTestProvenance() IncomingProvenance {
	const id = "018f47a2-9b3c-7def-8abc-0123456789ab"
	return IncomingProvenance{Version: 2, TenantID: "test-tenant", PersonalityAgentID: id,
		Actor: ProvenanceActor{Kind: "human", PrincipalID: "human-author", DisplayName: "Actual author"},
		Source: ProvenanceSource{Surface: "messaging", EventID: id, Kind: "messaging_mention", WorkspaceID: id, InstallationID: id, AuthorityEpoch: 1,
			Place: &ProvenancePlace{ID: id, Kind: "channel", Name: "Shared place"}, MessageID: id, MessageRevision: 1, MessageSeq: 1, OccurredAt: "2026-09-08T12:00:00Z"}}
}

func mustJSON(t testing.TB, v any) []byte {
	t.Helper()
	b, e := json.Marshal(v)
	if e != nil {
		t.Fatal(e)
	}
	return b
}

type DirectChatSpawner interface {
	EnsureRunning(context.Context, string) error
	Touch(string)
}
