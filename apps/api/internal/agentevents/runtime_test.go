package agentevents

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type countingDurableFile struct {
	durableFileHandle
	readBytes *atomic.Int64
}

func (f *countingDurableFile) Read(p []byte) (int, error) {
	n, err := f.durableFileHandle.Read(p)
	f.readBytes.Add(int64(n))
	return n, err
}

type blockingDurableFile struct {
	durableFileHandle
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (f *blockingDurableFile) Read(p []byte) (int, error) {
	f.once.Do(func() { close(f.entered) })
	<-f.release
	return f.durableFileHandle.Read(p)
}

func openRuntimeGateway(t *testing.T) *DurableGateway {
	t.Helper()
	store, err := OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	gateway, err := OpenDurableGateway(t.TempDir(), store)
	if err != nil {
		t.Fatal(err)
	}
	gateway.PollInterval = 5 * time.Millisecond
	return gateway
}

func currentRuntimeClaims(t testing.TB, gateway *DurableGateway, personalityAgentID string) TokenClaims {
	t.Helper()
	const generation = uint64(1)
	if err := gateway.PublishRuntimeState(personalityAgentID, generation, nil); err != nil {
		t.Fatalf("publish current runtime generation: %v", err)
	}
	return TokenClaims{PersonalityAgentID: personalityAgentID, Generation: generation}
}

func openGatewayAt(t *testing.T, storeDir, runtimeDir string) (*CommandStore, *DurableGateway, error) {
	t.Helper()
	store, err := OpenCommandStore(storeDir)
	if err != nil {
		return nil, nil, err
	}
	gateway, err := OpenDurableGateway(runtimeDir, store)
	if err != nil {
		_ = store.Close()
		return nil, nil, err
	}
	gateway.PollInterval = 5 * time.Millisecond
	return store, gateway, nil
}

func TestDurableGatewayMissingStateFailsGenerationVerificationUntilPublished(t *testing.T) {
	gateway := openRuntimeGateway(t)
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	const generation = uint64(7)

	if err := gateway.VerifyGeneration(context.Background(), personalityAgentID, generation); err == nil {
		t.Fatal("missing runtime state must fail generation verification")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- gateway.WaitFor(ctx, TokenClaims{PersonalityAgentID: personalityAgentID}, generation)
	}()

	if err := gateway.publishRuntimeState(personalityAgentID, runtimeState{
		Generation:               7,
		HydrationReceiptIdentity: nil,
	}); err != nil {
		t.Fatal(err)
	}
	if err := gateway.VerifyGeneration(context.Background(), personalityAgentID, generation); err != nil {
		t.Fatalf("published matching runtime state rejected generation: %v", err)
	}
	if err := gateway.VerifyGeneration(context.Background(), personalityAgentID, generation+1); err == nil {
		t.Fatal("mismatched runtime generation must fail verification")
	}
	select {
	case err := <-done:
		t.Fatalf("not-ready state released the latch: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	receipt := "receipt-7"
	if err := gateway.publishRuntimeState(personalityAgentID, runtimeState{
		Generation:               7,
		HydrationReceiptIdentity: &receipt,
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("ready state did not release the latch: %v", err)
	}
}

func TestDurableGatewayFencesStaleAckDurableAndVolatileEvents(t *testing.T) {
	gateway := openRuntimeGateway(t)
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	stale := currentRuntimeClaims(t, gateway, personalityAgentID)
	command, err := gateway.commands.Append(
		context.Background(),
		testDirectChatProvenance(personalityAgentID),
		"",
		json.RawMessage(`{"type":"user_message","text":"hello","attachments":[]}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	ack := CommandAck{
		PersonalityAgentID: personalityAgentID,
		Seq:                command.Seq,
		CommandID:          command.CommandID,
		Status:             "applied",
	}
	volatile, unsubscribe := gateway.SubscribeBrowserVolatile(personalityAgentID)
	defer unsubscribe()

	current := stale
	current.Generation++
	if err := gateway.PublishRuntimeState(personalityAgentID, current.Generation, nil); err != nil {
		t.Fatal(err)
	}
	if err := gateway.ApplyAck(context.Background(), stale, ack); err == nil {
		t.Fatal("stale generation ACK reached the durable gateway")
	}
	seq := uint64(1)
	durableEvent := Envelope{
		Seq:                &seq,
		PersonalityAgentID: personalityAgentID,
		Event:              json.RawMessage(`{"type":"agent_start"}`),
	}
	if err := gateway.Receive(context.Background(), stale, durableEvent); err == nil {
		t.Fatal("stale generation durable event reached the durable gateway")
	}
	volatileEvent := Envelope{
		PersonalityAgentID: personalityAgentID,
		Event:              json.RawMessage(`{"type":"message_update","message_id":"00000000-0000-4000-8000-000000000001","event":{"type":"text_delta","content_index":0,"delta":"stale"}}`),
	}
	if err := gateway.Receive(context.Background(), stale, volatileEvent); err == nil {
		t.Fatal("stale generation volatile event reached the durable gateway")
	}
	select {
	case event := <-volatile:
		t.Fatalf("stale volatile event was published: %+v", event)
	default:
	}
	if _, err := os.Stat(gateway.ackPath(personalityAgentID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale ACK created durable state: %v", err)
	}
	last, err := gateway.LastReceivedEventSeq(context.Background(), current)
	if err != nil || last != 0 {
		t.Fatalf("stale durable event changed cursor: last=%d err=%v", last, err)
	}

	if err := gateway.ApplyAck(context.Background(), current, ack); err != nil {
		t.Fatalf("current generation ACK rejected: %v", err)
	}
	if err := gateway.Receive(context.Background(), current, durableEvent); err != nil {
		t.Fatalf("current generation durable event rejected: %v", err)
	}
	volatileEvent.Event = json.RawMessage(`{"type":"message_update","message_id":"00000000-0000-4000-8000-000000000001","event":{"type":"text_delta","content_index":0,"delta":"current"}}`)
	if err := gateway.Receive(context.Background(), current, volatileEvent); err != nil {
		t.Fatalf("current generation volatile event rejected: %v", err)
	}
	select {
	case event := <-volatile:
		if !bytes.Contains(event.Event, []byte(`"delta":"current"`)) {
			t.Fatalf("unexpected current volatile event: %s", event.Event)
		}
	case <-time.After(time.Second):
		t.Fatal("current volatile event was not published")
	}
	if next, err := gateway.NextCommandSeq(context.Background(), current); err != nil || next != command.Seq+1 {
		t.Fatalf("current terminal ACK did not advance replay: next=%d err=%v", next, err)
	}
}

func TestDurableGatewayRolloverCommitSerializesWithAdmittedSideEffect(t *testing.T) {
	gateway := openRuntimeGateway(t)
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	stale := currentRuntimeClaims(t, gateway, personalityAgentID)
	sideEffectEntered := make(chan struct{})
	releaseSideEffect := make(chan struct{})
	sideEffectDone := make(chan error, 1)
	go func() {
		sideEffectDone <- gateway.withCurrentGeneration(
			context.Background(),
			stale,
			func() error {
				close(sideEffectEntered)
				<-releaseSideEffect
				return nil
			},
		)
	}()
	select {
	case <-sideEffectEntered:
	case <-time.After(time.Second):
		t.Fatal("side effect did not enter generation boundary")
	}

	publishDone := make(chan error, 1)
	go func() {
		publishDone <- gateway.PublishRuntimeState(personalityAgentID, stale.Generation+1, nil)
	}()
	select {
	case err := <-publishDone:
		t.Fatalf("rollover committed across admitted side effect: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseSideEffect)
	if err := <-sideEffectDone; err != nil {
		t.Fatalf("admitted side effect failed: %v", err)
	}
	if err := <-publishDone; err != nil {
		t.Fatalf("rollover failed after admitted side effect: %v", err)
	}

	called := false
	if err := gateway.withCurrentGeneration(context.Background(), stale, func() error {
		called = true
		return nil
	}); err == nil || called {
		t.Fatalf("old generation side effect admitted after rollover: called=%v err=%v", called, err)
	}
}

func TestConnectionLeaseRolloverSerializesWithCommandDelivery(t *testing.T) {
	gateway := openRuntimeGateway(t)
	claims := currentRuntimeClaims(t, gateway, "018f47a2-9b3c-7def-8abc-0123456789ab")
	lease, err := gateway.ClaimConnectionLease(context.Background(), claims)
	if err != nil {
		t.Fatal(err)
	}
	deliveryEntered := make(chan struct{})
	releaseDelivery := make(chan struct{})
	deliveryDone := make(chan error, 1)
	go func() {
		deliveryDone <- gateway.WithConnectionLease(
			context.Background(),
			claims,
			lease,
			func() error {
				close(deliveryEntered)
				<-releaseDelivery
				return nil
			},
		)
	}()
	select {
	case <-deliveryEntered:
	case <-time.After(time.Second):
		t.Fatal("command delivery did not enter authoritative lease boundary")
	}

	publishDone := make(chan error, 1)
	go func() {
		publishDone <- gateway.PublishRuntimeState(
			claims.PersonalityAgentID,
			claims.Generation+1,
			nil,
		)
	}()
	select {
	case err := <-publishDone:
		t.Fatalf("rollover committed during command delivery: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseDelivery)
	if err := <-deliveryDone; err != nil {
		t.Fatalf("admitted command delivery failed: %v", err)
	}
	if err := <-publishDone; err != nil {
		t.Fatalf("rollover after command delivery failed: %v", err)
	}

	called := false
	err = gateway.WithConnectionLease(context.Background(), claims, lease, func() error {
		called = true
		return nil
	})
	if err == nil || called {
		t.Fatalf("old command delivery admitted after rollover: called=%v err=%v", called, err)
	}
}

func TestConnectionLeaseLockIsIsolatedPerPersonalityAgent(t *testing.T) {
	gateway := openRuntimeGateway(t)
	first := currentRuntimeClaims(t, gateway, "018f47a2-9b3c-7def-8abc-0123456789ab")
	second := currentRuntimeClaims(t, gateway, "018f47a2-9b3c-7def-9abc-0123456789ac")
	firstLease, err := gateway.ClaimConnectionLease(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- gateway.WithConnectionLease(
			context.Background(),
			first,
			firstLease,
			func() error {
				close(firstEntered)
				<-releaseFirst
				return nil
			},
		)
	}()
	select {
	case <-firstEntered:
	case <-time.After(time.Second):
		t.Fatal("first PAID lease boundary did not enter")
	}

	secondDone := make(chan error, 1)
	go func() {
		_, err := gateway.ClaimConnectionLease(context.Background(), second)
		secondDone <- err
	}()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second PAID lease claim failed: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("first PAID lease operation stalled a different PAID")
	}
	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}

func TestDurableGatewayRejectsMutableDirectoryAndReplacedPinnedLock(t *testing.T) {
	store, err := OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	publicDir := t.TempDir()
	if err := os.Chmod(publicDir, 0o777); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDurableGateway(publicDir, store); err == nil {
		t.Fatal("gateway accepted a group/world-writable runtime directory")
	}

	symlinkParent := t.TempDir()
	symlinkPath := filepath.Join(symlinkParent, "runtime-link")
	if err := os.Symlink(t.TempDir(), symlinkPath); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDurableGateway(symlinkPath, store); err == nil {
		t.Fatal("gateway accepted a symlink runtime directory")
	}
	realParent := t.TempDir()
	symlinkedParent := filepath.Join(t.TempDir(), "linked-parent")
	if err := os.Symlink(realParent, symlinkedParent); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDurableGateway(filepath.Join(symlinkedParent, "runtime"), store); err == nil {
		t.Fatal("gateway accepted a runtime directory beneath a symlink path component")
	}

	runtimeDir := t.TempDir()
	gateway, err := OpenDurableGateway(runtimeDir, store)
	if err != nil {
		t.Fatal(err)
	}
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	claims := currentRuntimeClaims(t, gateway, personalityAgentID)
	firstLease, err := gateway.ClaimConnectionLease(context.Background(), claims)
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(gateway.connectionLeasePath(personalityAgentID))
	if err != nil {
		t.Fatal(err)
	}

	lockPath := gateway.localControlLockPath(personalityAgentID)
	displacedLockPath := lockPath + ".displaced"
	if err := os.Chmod(runtimeDir, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(lockPath, displacedLockPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(lockPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.ClaimConnectionLease(context.Background(), claims); err == nil {
		t.Fatal("gateway claimed through a runtime directory made mutable after open")
	}
	if err := os.Chmod(runtimeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.ClaimConnectionLease(context.Background(), claims); err == nil {
		t.Fatal("gateway accepted a replacement for its pinned PAID lock inode")
	}
	after, err := os.ReadFile(gateway.connectionLeasePath(personalityAgentID))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("refused split-lock claims changed the authoritative lease record")
	}
	if err := gateway.ValidateConnectionLease(context.Background(), claims, firstLease); err == nil {
		t.Fatal("gateway continued through the replaced lock instead of failing closed")
	}
}

func TestConnectionLeaseStateUsesLocalControlIntegrityWithoutIdentityLeakage(t *testing.T) {
	gateway := openRuntimeGateway(t)
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	key := deriveLocalControlIntegrityKey([]byte("connection-lease-integrity-secret-32-bytes"))
	if err := gateway.installLocalControlIntegrityKey(key, []string{personalityAgentID}); err != nil {
		t.Fatal(err)
	}
	record := connectionLeaseState{
		Version:    connectionLeaseStateVersion,
		Generation: 7,
		Sequence:   1,
		LeaseID:    base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x42}, connectionLeaseIDBytes)),
		Active:     true,
	}
	if err := gateway.writeConnectionLeaseState(personalityAgentID, record); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(gateway.connectionLeasePath(personalityAgentID))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte(personalityAgentID)) {
		t.Fatal("connection lease content leaked personality agent identity")
	}
	if !bytes.Contains(raw, []byte(`"integrity"`)) {
		t.Fatal("local-control-owned connection lease was not integrity protected")
	}
	if _, err := gateway.connectionLeaseState(personalityAgentID); err != nil {
		t.Fatalf("signed connection lease did not verify: %v", err)
	}
	reopened, err := OpenDurableGateway(gateway.dir, gateway.commands)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.connectionLeaseState(personalityAgentID); err == nil {
		t.Fatal("gateway without local-control integrity key accepted signed connection lease")
	}
	if err := reopened.installLocalControlIntegrityKey(key, []string{personalityAgentID}); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.connectionLeaseState(personalityAgentID); err != nil {
		t.Fatalf("gateway with matching key rejected shared signed lease: %v", err)
	}

	tampered := bytes.Replace(raw, []byte(`"sequence":1`), []byte(`"sequence":2`), 1)
	if bytes.Equal(tampered, raw) {
		t.Fatal("test did not alter signed connection lease")
	}
	if err := os.WriteFile(gateway.connectionLeasePath(personalityAgentID), tampered, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.connectionLeaseState(personalityAgentID); err == nil {
		t.Fatal("tampered connection lease passed local-control integrity verification")
	}
}

func TestConnectionLeaseIntegrityKeyRotationResignsConcurrently(t *testing.T) {
	store, err := OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runtimeDir := t.TempDir()
	oldGateway, err := OpenDurableGateway(runtimeDir, store)
	if err != nil {
		t.Fatal(err)
	}
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	claims := currentRuntimeClaims(t, oldGateway, personalityAgentID)
	oldKey := deriveLocalControlIntegrityKey([]byte("old-connection-integrity-secret-32-bytes"))
	currentKey := deriveLocalControlIntegrityKey([]byte("new-connection-integrity-secret-32-bytes"))
	if err := oldGateway.installLocalControlIntegrityKey(oldKey, []string{personalityAgentID}); err != nil {
		t.Fatal(err)
	}
	lease, err := oldGateway.ClaimConnectionLease(context.Background(), claims)
	if err != nil {
		t.Fatal(err)
	}

	// Model a pre-key-ID record from the previous deployment. Integrity covers
	// the lease payload, not the metadata wrapper, so removing the ID preserves
	// the valid legacy MAC.
	raw, err := os.ReadFile(oldGateway.connectionLeasePath(personalityAgentID))
	if err != nil {
		t.Fatal(err)
	}
	var legacy connectionLeaseState
	if err := json.Unmarshal(raw, &legacy); err != nil {
		t.Fatal(err)
	}
	legacy.Integrity.KeyID = ""
	raw, err = json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(oldGateway.connectionLeasePath(personalityAgentID), raw, 0o600); err != nil {
		t.Fatal(err)
	}

	rotated, err := OpenDurableGateway(runtimeDir, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := rotated.installLocalControlIntegrityKeyring(
		currentKey,
		[][]byte{oldKey},
		[]string{personalityAgentID},
	); err != nil {
		t.Fatal(err)
	}

	const readers = 16
	start := make(chan struct{})
	results := make(chan error, readers)
	for range readers {
		go func() {
			<-start
			results <- rotated.ValidateConnectionLease(context.Background(), claims, lease)
		}()
	}
	close(start)
	for range readers {
		if err := <-results; err != nil {
			t.Fatalf("concurrent rotation validation failed: %v", err)
		}
	}

	resigned, err := rotated.connectionLeaseState(personalityAgentID)
	if err != nil {
		t.Fatal(err)
	}
	if resigned.Integrity == nil ||
		resigned.Integrity.KeyID != deriveLocalControlIntegrityKeyID(currentKey) {
		t.Fatalf("lease was not re-signed with current key ID: %+v", resigned.Integrity)
	}
	if resigned.Generation != lease.Generation ||
		resigned.Sequence != lease.Sequence ||
		resigned.LeaseID != lease.ID ||
		!resigned.Active {
		t.Fatalf("rotation changed lease authority: %+v", resigned)
	}

	currentOnly, err := OpenDurableGateway(runtimeDir, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := currentOnly.installLocalControlIntegrityKey(currentKey, []string{personalityAgentID}); err != nil {
		t.Fatal(err)
	}
	if err := currentOnly.ValidateConnectionLease(context.Background(), claims, lease); err != nil {
		t.Fatalf("current-only verifier rejected re-signed lease: %v", err)
	}

	resigned.Integrity.KeyID = strings.Repeat("f", 32)
	unknownKeyRecord, err := json.Marshal(resigned)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rotated.connectionLeasePath(personalityAgentID), unknownKeyRecord, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := rotated.connectionLeaseState(personalityAgentID); err == nil ||
		!strings.Contains(err.Error(), "unknown") {
		t.Fatalf("unknown integrity key ID did not fail closed: %v", err)
	}

	duplicateGateway := openRuntimeGateway(t)
	if err := duplicateGateway.installLocalControlIntegrityKeyring(
		currentKey,
		[][]byte{oldKey, oldKey},
		[]string{personalityAgentID},
	); err == nil {
		t.Fatal("ambiguous duplicate previous integrity keys were accepted")
	}
}

func TestDurableGatewayRejectsLegacyReadyBoolean(t *testing.T) {
	gateway := openRuntimeGateway(t)
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	if err := os.WriteFile(
		gateway.statePath(personalityAgentID),
		[]byte(`{"generation":7,"ready":true}`),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.state(context.Background(), personalityAgentID); err == nil {
		t.Fatal("legacy ready boolean must fail strict runtime-state decoding")
	}
}

func TestDurableGatewayRuntimeStateGenerationBoundary(t *testing.T) {
	gateway := openRuntimeGateway(t)
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"

	if err := gateway.publishRuntimeState(personalityAgentID, runtimeState{Generation: maxProcessGeneration}); err != nil {
		t.Fatalf("publish max process generation failed: %v", err)
	}
	st, err := gateway.state(context.Background(), personalityAgentID)
	if err != nil || st.Generation != maxProcessGeneration {
		t.Fatalf("max generation must be accepted in recovery: err=%v gen=%d", err, st.Generation)
	}

	if err := gateway.publishRuntimeState(personalityAgentID, runtimeState{Generation: maxProcessGeneration + 1}); err == nil {
		t.Fatal("publish must reject generation max+1")
	}

	raw, err := json.Marshal(runtimeState{Generation: maxProcessGeneration + 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(gateway.statePath(personalityAgentID), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.state(context.Background(), personalityAgentID); err == nil {
		t.Fatal("recovery must reject generation max+1")
	}
}

func TestDurableGatewayRejectsEmptyReceiptIdentity(t *testing.T) {
	gateway := openRuntimeGateway(t)
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	empty := ""
	if err := gateway.publishRuntimeState(personalityAgentID, runtimeState{
		Generation:               7,
		HydrationReceiptIdentity: &empty,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.state(context.Background(), personalityAgentID); err == nil {
		t.Fatal("empty hydration receipt identity must fail closed")
	}
}

func TestDurableGatewaySerializesEventSequenceAcrossInstances(t *testing.T) {
	commandStore, err := OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer commandStore.Close()
	runtimeDir := t.TempDir()
	first, err := OpenDurableGateway(runtimeDir, commandStore)
	if err != nil {
		t.Fatal(err)
	}
	second, err := OpenDurableGateway(runtimeDir, commandStore)
	if err != nil {
		t.Fatal(err)
	}
	first.PollInterval = 5 * time.Millisecond
	second.PollInterval = 5 * time.Millisecond
	claims := currentRuntimeClaims(t, first, "018f47a2-9b3c-7def-8abc-0123456789ab")
	seq := uint64(1)
	event := Envelope{
		Seq:                &seq,
		PersonalityAgentID: claims.PersonalityAgentID,
		Event:              json.RawMessage(`{"type":"agent_start"}`),
	}
	results := make(chan error, 2)
	go func() { results <- first.Receive(context.Background(), claims, event) }()
	go func() { results <- second.Receive(context.Background(), claims, event) }()
	var successes int
	for range 2 {
		if err := <-results; err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("exactly one cross-process seq=1 append must succeed, got %d", successes)
	}

	seq = 2
	event.Seq = &seq
	if err := second.Receive(context.Background(), claims, event); err != nil {
		t.Fatalf("seq=2 append after contention failed: %v", err)
	}
	last, err := first.LastReceivedEventSeq(context.Background(), claims)
	if err != nil {
		t.Fatal(err)
	}
	if last != 2 {
		t.Fatalf("expected last event seq 2, got %d", last)
	}
}

func TestDurableGatewayCorrelatesAndDeduplicatesCommandAcks(t *testing.T) {
	gateway := openRuntimeGateway(t)
	claims := currentRuntimeClaims(t, gateway, "018f47a2-9b3c-7def-8abc-0123456789ab")
	command, err := gateway.commands.Append(
		context.Background(),
		testDirectChatProvenance(claims.PersonalityAgentID),
		"",
		json.RawMessage(`{"type":"user_message","text":"hello","attachments":[]}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	wrong := CommandAck{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
		Seq:       command.Seq,
		CommandID: "00000000-0000-4000-8000-000000000001",
		Status:    "received",
	}
	if err := gateway.ApplyAck(context.Background(), claims, wrong); err == nil {
		t.Fatal("ack with a mismatched command_id must be rejected")
	}

	received := CommandAck{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Seq: command.Seq, CommandID: command.CommandID, Status: "received"}
	if err := gateway.ApplyAck(context.Background(), claims, received); err != nil {
		t.Fatal(err)
	}
	if err := gateway.ApplyAck(context.Background(), claims, received); err != nil {
		t.Fatalf("exact duplicate ack must be idempotent: %v", err)
	}
	applied := CommandAck{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Seq: command.Seq, CommandID: command.CommandID, Status: "applied"}
	if err := gateway.ApplyAck(context.Background(), claims, applied); err != nil {
		t.Fatal(err)
	}
	reason := "schema_violation"
	rejected := CommandAck{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
		Seq:          command.Seq,
		CommandID:    command.CommandID,
		Status:       "rejected",
		RejectReason: &reason,
	}
	if err := gateway.ApplyAck(context.Background(), claims, rejected); err == nil {
		t.Fatal("terminal applied ack must reject a conflicting terminal ack")
	}
	raw, err := os.ReadFile(gateway.ackPath(claims.PersonalityAgentID))
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(string(raw), "\n"); lines != 2 {
		t.Fatalf("expected received+applied records without duplicates, got %d lines", lines)
	}
}

func TestDurableGatewayNextCommandSeqUsesDurableTerminalAckStateAcrossRestart(t *testing.T) {
	storeDir, runtimeDir := t.TempDir(), t.TempDir()
	store, gateway, err := openGatewayAt(t, storeDir, runtimeDir)
	if err != nil {
		t.Fatal(err)
	}
	claims := currentRuntimeClaims(t, gateway, "018f47a2-9b3c-7def-8abc-012345678982")
	first, err := store.Append(context.Background(), testDirectChatProvenance(claims.PersonalityAgentID), "", json.RawMessage(`{"type":"user_message","text":"one","attachments":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Append(context.Background(), testDirectChatProvenance(claims.PersonalityAgentID), "", json.RawMessage(`{"type":"user_message","text":"two","attachments":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := gateway.ApplyAck(context.Background(), claims, CommandAck{PersonalityAgentID: claims.PersonalityAgentID, Seq: first.Seq, CommandID: first.CommandID, Status: "applied"}); err != nil {
		t.Fatal(err)
	}
	if err := gateway.ApplyAck(context.Background(), claims, CommandAck{PersonalityAgentID: claims.PersonalityAgentID, Seq: second.Seq, CommandID: second.CommandID, Status: "received"}); err != nil {
		t.Fatal(err)
	}
	if next, err := gateway.NextCommandSeq(context.Background(), claims); err != nil || next != second.Seq {
		t.Fatalf("nonterminal durable ACK must replay seq %d: next=%d err=%v", second.Seq, next, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedStore, reopened, err := openGatewayAt(t, storeDir, runtimeDir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedStore.Close()
	if next, err := reopened.NextCommandSeq(context.Background(), claims); err != nil || next != second.Seq {
		t.Fatalf("restart lost nonterminal durable ACK gap: next=%d err=%v", next, err)
	}
	if err := reopened.ApplyAck(context.Background(), claims, CommandAck{PersonalityAgentID: claims.PersonalityAgentID, Seq: second.Seq, CommandID: second.CommandID, Status: "applied"}); err != nil {
		t.Fatal(err)
	}
	if next, err := reopened.NextCommandSeq(context.Background(), claims); err != nil || next != second.Seq+1 {
		t.Fatalf("terminal ACK prefix must advance to %d: next=%d err=%v", second.Seq+1, next, err)
	}
}

func TestDurableGatewayNextCommandSeqSupportsOutOfOrderTerminalAcks(t *testing.T) {
	gateway := openRuntimeGateway(t)
	claims := currentRuntimeClaims(t, gateway, "018f47a2-9b3c-7def-8abc-012345678986")
	commands := make([]CommandEnvelope, 0, 3)
	for i := 0; i < 3; i++ {
		command, err := gateway.commands.Append(context.Background(), testDirectChatProvenance(claims.PersonalityAgentID), "", json.RawMessage(`{"type":"user_message","text":"hello","attachments":[]}`))
		if err != nil {
			t.Fatal(err)
		}
		commands = append(commands, command)
	}
	for _, ack := range []CommandAck{
		{PersonalityAgentID: claims.PersonalityAgentID, Seq: commands[2].Seq, CommandID: commands[2].CommandID, Status: "applied"},
		{PersonalityAgentID: claims.PersonalityAgentID, Seq: commands[0].Seq, CommandID: commands[0].CommandID, Status: "applied"},
		{PersonalityAgentID: claims.PersonalityAgentID, Seq: commands[1].Seq, CommandID: commands[1].CommandID, Status: "received"},
	} {
		if err := gateway.ApplyAck(context.Background(), claims, ack); err != nil {
			t.Fatal(err)
		}
	}
	if next, err := gateway.NextCommandSeq(context.Background(), claims); err != nil || next != commands[1].Seq {
		t.Fatalf("first nonterminal gap = %d, want %d (err=%v)", next, commands[1].Seq, err)
	}
	if err := gateway.ApplyAck(context.Background(), claims, CommandAck{PersonalityAgentID: claims.PersonalityAgentID, Seq: commands[1].Seq, CommandID: commands[1].CommandID, Status: "applied"}); err != nil {
		t.Fatal(err)
	}
	if next, err := gateway.NextCommandSeq(context.Background(), claims); err != nil || next != commands[2].Seq+1 {
		t.Fatalf("completed out-of-order history next = %d, want %d (err=%v)", next, commands[2].Seq+1, err)
	}
}

func TestDurableGatewayNextCommandSeqRestartScanIsLinearAndBounded(t *testing.T) {
	const commandCount = 4096
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-012345678972"
	storeDir, runtimeDir := t.TempDir(), t.TempDir()
	var commandLog, ackLog bytes.Buffer
	for i := 1; i <= commandCount; i++ {
		commandID := fmt.Sprintf("00000000-0000-4000-8000-%012d", i)
		record := testLogRecord(uint64(i), commandID, json.RawMessage(`{"type":"user_message","text":"history","attachments":[]}`), personalityAgentID)
		line, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		commandLog.Write(line)
		commandLog.WriteByte('\n')
		line, err = json.Marshal(CommandAck{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Seq: uint64(i), CommandID: commandID, Status: "applied"})
		if err != nil {
			t.Fatal(err)
		}
		ackLog.Write(line)
		ackLog.WriteByte('\n')
	}
	if err := os.WriteFile(commandLogPath(storeDir, personalityAgentID), commandLog.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	store, gateway, err := openGatewayAt(t, storeDir, runtimeDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(gateway.ackPath(personalityAgentID), ackLog.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedStore, reopened, err := openGatewayAt(t, storeDir, runtimeDir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedStore.Close()
	var readBytes atomic.Int64
	open := reopened.newFile
	ackPath := reopened.ackPath(personalityAgentID)
	reopened.newFile = func(name string, flag int, perm os.FileMode) (durableFileHandle, error) {
		file, err := open(name, flag, perm)
		if err != nil || name != ackPath {
			return file, err
		}
		return &countingDurableFile{durableFileHandle: file, readBytes: &readBytes}, nil
	}
	next, err := reopened.NextCommandSeq(context.Background(), TokenClaims{PersonalityAgentID: personalityAgentID})
	if err != nil {
		t.Fatal(err)
	}
	if next != commandCount+1 {
		t.Fatalf("next command seq = %d, want %d", next, commandCount+1)
	}
	if got, limit := readBytes.Load(), int64(ackLog.Len()+4096); got > limit {
		t.Fatalf("restart cursor read %d bytes from a %d-byte ACK log; scan must remain linear", got, ackLog.Len())
	}
}

func TestDurableGatewayNextCommandSeqDoesNotBlockOtherPersonalityAgentAck(t *testing.T) {
	gateway := openRuntimeGateway(t)
	firstClaims := currentRuntimeClaims(t, gateway, "018f47a2-9b3c-7def-8abc-012345678980")
	secondClaims := currentRuntimeClaims(t, gateway, "018f47a2-9b3c-7def-8abc-012345678981")
	first, err := gateway.commands.Append(context.Background(), testDirectChatProvenance(firstClaims.PersonalityAgentID), "", json.RawMessage(`{"type":"user_message","text":"first","attachments":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	second, err := gateway.commands.Append(context.Background(), testDirectChatProvenance(secondClaims.PersonalityAgentID), "", json.RawMessage(`{"type":"user_message","text":"second","attachments":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := gateway.ApplyAck(context.Background(), firstClaims, CommandAck{PersonalityAgentID: firstClaims.PersonalityAgentID, Seq: first.Seq, CommandID: first.CommandID, Status: "applied"}); err != nil {
		t.Fatal(err)
	}
	open := gateway.newFile
	blockedPath := gateway.ackPath(firstClaims.PersonalityAgentID)
	entered, release := make(chan struct{}), make(chan struct{})
	gateway.newFile = func(name string, flag int, perm os.FileMode) (durableFileHandle, error) {
		file, err := open(name, flag, perm)
		if err != nil || name != blockedPath {
			return file, err
		}
		return &blockingDurableFile{durableFileHandle: file, entered: entered, release: release}, nil
	}
	cursorDone := make(chan error, 1)
	go func() { _, err := gateway.NextCommandSeq(context.Background(), firstClaims); cursorDone <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("slow ACK scan did not start")
	}
	ackDone := make(chan error, 1)
	go func() {
		ackDone <- gateway.ApplyAck(context.Background(), secondClaims, CommandAck{PersonalityAgentID: secondClaims.PersonalityAgentID, Seq: second.Seq, CommandID: second.CommandID, Status: "received"})
	}()
	select {
	case err := <-ackDone:
		if err != nil {
			t.Fatalf("independent ACK failed: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("one personality agent's ACK scan blocked ApplyAck for another personality agent")
	}
	close(release)
	if err := <-cursorDone; err != nil {
		t.Fatalf("cursor scan failed after release: %v", err)
	}
}

func TestFoldAckCursorRecordRejectsInvalidTransitionWithoutStateChange(t *testing.T) {
	const commandID = "00000000-0000-4000-8000-000000000001"
	snapshot := commandLogSnapshot{commands: []CommandEnvelope{{Seq: 1, CommandID: commandID}}, nextSeq: 2}
	states, err := newCompactAckStates(1)
	if err != nil {
		t.Fatal(err)
	}
	err = foldAckCursorRecord(snapshot, states, CommandAck{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Seq: 1, CommandID: commandID, Status: "completed"})
	if err == nil || !strings.Contains(err.Error(), `status "completed" is not valid`) {
		t.Fatalf("constructed unknown ACK status must fail closed, got %v", err)
	}
	if got := states.get(0); got != ackStateAbsent {
		t.Fatalf("invalid ACK changed state to %d", got)
	}
}

func TestDurableGatewayNextCommandSeqFlockHonorsCancellation(t *testing.T) {
	gateway := openRuntimeGateway(t)
	claims := currentRuntimeClaims(t, gateway, "018f47a2-9b3c-7def-8abc-012345678983")
	if _, err := gateway.commands.Append(context.Background(), testDirectChatProvenance(claims.PersonalityAgentID), "", json.RawMessage(`{"type":"user_message","text":"hello","attachments":[]}`)); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(gateway.ackPath(claims.PersonalityAgentID), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	defer syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if _, err := gateway.NextCommandSeq(ctx, claims); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("locked replay cursor must honor cancellation, got %v", err)
	}
}

func TestDurableGatewayNextCommandSeqRejectsContradictoryRestartAckHistory(t *testing.T) {
	for _, tc := range []struct {
		name  string
		lines func(CommandEnvelope) []CommandAck
	}{
		{
			name: "multiple terminal transitions",
			lines: func(command CommandEnvelope) []CommandAck {
				reason := "schema_violation"
				return []CommandAck{
					{PersonalityAgentID: command.PersonalityAgentID, Seq: command.Seq, CommandID: command.CommandID, Status: "received"},
					{PersonalityAgentID: command.PersonalityAgentID, Seq: command.Seq, CommandID: command.CommandID, Status: "applied"},
					{PersonalityAgentID: command.PersonalityAgentID, Seq: command.Seq, CommandID: command.CommandID, Status: "rejected", RejectReason: &reason},
				}
			},
		},
		{
			name: "identity changes then returns",
			lines: func(command CommandEnvelope) []CommandAck {
				return []CommandAck{
					{PersonalityAgentID: command.PersonalityAgentID, Seq: command.Seq, CommandID: "00000000-0000-4000-8000-000000000099", Status: "received"},
					{PersonalityAgentID: command.PersonalityAgentID, Seq: command.Seq, CommandID: command.CommandID, Status: "applied"},
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storeDir, runtimeDir := t.TempDir(), t.TempDir()
			store, gateway, err := openGatewayAt(t, storeDir, runtimeDir)
			if err != nil {
				t.Fatal(err)
			}
			claims := currentRuntimeClaims(t, gateway, "018f47a2-9b3c-7def-8abc-012345678984")
			command, err := store.Append(context.Background(), testDirectChatProvenance(claims.PersonalityAgentID), "", json.RawMessage(`{"type":"user_message","text":"hello","attachments":[]}`))
			if err != nil {
				t.Fatal(err)
			}
			var log bytes.Buffer
			encoder := json.NewEncoder(&log)
			for _, ack := range tc.lines(command) {
				if err := encoder.Encode(ack); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(gateway.ackPath(claims.PersonalityAgentID), log.Bytes(), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopenedStore, reopened, err := openGatewayAt(t, storeDir, runtimeDir)
			if err != nil {
				t.Fatal(err)
			}
			defer reopenedStore.Close()
			if _, err := reopened.NextCommandSeq(context.Background(), claims); err == nil {
				t.Fatal("individually valid but contradictory ACK records must fail closed")
			}
		})
	}
}

func TestDurableGatewayNextCommandSeqRejectsMalformedRestartAck(t *testing.T) {
	tests := []struct {
		name    string
		ackJSON func(CommandEnvelope) string
	}{
		{name: "empty status", ackJSON: func(command CommandEnvelope) string {
			return fmt.Sprintf(`{"seq":%d,"command_id":%q,"status":""}`, command.Seq, command.CommandID)
		}},
		{name: "unknown status", ackJSON: func(command CommandEnvelope) string {
			return fmt.Sprintf(`{"seq":%d,"command_id":%q,"status":"completed"}`, command.Seq, command.CommandID)
		}},
		{name: "terminal status with reject reason", ackJSON: func(command CommandEnvelope) string {
			return fmt.Sprintf(`{"seq":%d,"command_id":%q,"status":"applied","reject_reason":"schema_violation"}`, command.Seq, command.CommandID)
		}},
		{name: "rejected without reason", ackJSON: func(command CommandEnvelope) string {
			return fmt.Sprintf(`{"seq":%d,"command_id":%q,"status":"rejected"}`, command.Seq, command.CommandID)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			storeDir, runtimeDir := t.TempDir(), t.TempDir()
			store, gateway, err := openGatewayAt(t, storeDir, runtimeDir)
			if err != nil {
				t.Fatal(err)
			}
			claims := currentRuntimeClaims(t, gateway, "018f47a2-9b3c-7def-8abc-012345678985")
			command, err := store.Append(context.Background(), testDirectChatProvenance(claims.PersonalityAgentID), "", json.RawMessage(`{"type":"user_message","text":"hello","attachments":[]}`))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(gateway.ackPath(claims.PersonalityAgentID), append([]byte(tc.ackJSON(command)), '\n'), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopenedStore, reopened, err := openGatewayAt(t, storeDir, runtimeDir)
			if err != nil {
				t.Fatal(err)
			}
			defer reopenedStore.Close()
			if next, err := reopened.NextCommandSeq(context.Background(), claims); err == nil {
				t.Fatalf("malformed durable ACK advanced replay cursor to %d", next)
			} else if !strings.Contains(err.Error(), "command_ack") {
				t.Fatalf("malformed durable ACK returned unexpected error: %v", err)
			}
		})
	}
}

func TestDurableGatewayReceiveRejectsPersonalityAgentClaimMismatch(t *testing.T) {
	gateway := openRuntimeGateway(t)
	claims := currentRuntimeClaims(t, gateway, "018f47a2-9b3c-7def-8abc-0123456789ab")
	seq := uint64(1)
	err := gateway.Receive(context.Background(), claims, Envelope{
		Seq:                &seq,
		PersonalityAgentID: "018f47a2-9b3c-7def-9abc-0123456789ac",
		Event:              json.RawMessage(`{"type":"agent_start"}`),
	})
	if err == nil || !strings.Contains(err.Error(), "does not match token claim") {
		t.Fatalf("expected personality agent claim mismatch, got %v", err)
	}
	last, err := gateway.LastReceivedEventSeq(context.Background(), claims)
	if err != nil {
		t.Fatal(err)
	}
	if last != 0 {
		t.Fatalf("mismatched event must not be persisted, got seq %d", last)
	}
}

func TestDurableGatewayDetectsSameSizeEventReplacement(t *testing.T) {
	gateway := openRuntimeGateway(t)
	claims := currentRuntimeClaims(t, gateway, "018f47a2-9b3c-7def-8abc-0123456789ab")
	seq := uint64(1)
	if err := gateway.Receive(context.Background(), claims, Envelope{
		Seq:                &seq,
		PersonalityAgentID: claims.PersonalityAgentID,
		Event:              json.RawMessage(`{"type":"agent_start"}`),
	}); err != nil {
		t.Fatal(err)
	}

	path := gateway.eventPath(claims.PersonalityAgentID)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	replaced := bytes.Replace(original, []byte("agent_start"), []byte("agent_stXrt"), 1)
	if len(replaced) != len(original) || bytes.Equal(replaced, original) {
		t.Fatal("test replacement must change content without changing file size")
	}
	if err := os.WriteFile(path, replaced, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.LastReceivedEventSeq(context.Background(), claims); err == nil {
		t.Fatal("same-size corrupt event replacement must invalidate the cached tail")
	}
}

func TestDurableGatewayDetectsSameSizeAckReplacement(t *testing.T) {
	gateway := openRuntimeGateway(t)
	claims := currentRuntimeClaims(t, gateway, "018f47a2-9b3c-7def-8abc-0123456789ab")
	command, err := gateway.commands.Append(
		context.Background(),
		testDirectChatProvenance(claims.PersonalityAgentID),
		"",
		json.RawMessage(`{"type":"user_message","text":"hello","attachments":[]}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	received := CommandAck{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Seq: command.Seq, CommandID: command.CommandID, Status: "received"}
	if err := gateway.ApplyAck(context.Background(), claims, received); err != nil {
		t.Fatal(err)
	}

	path := gateway.ackPath(claims.PersonalityAgentID)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	replaced := bytes.Replace(original, []byte("received"), []byte("receivXd"), 1)
	if len(replaced) != len(original) || bytes.Equal(replaced, original) {
		t.Fatal("test replacement must change content without changing file size")
	}
	if err := os.WriteFile(path, replaced, 0o600); err != nil {
		t.Fatal(err)
	}
	applied := CommandAck{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Seq: command.Seq, CommandID: command.CommandID, Status: "applied"}
	if err := gateway.ApplyAck(context.Background(), claims, applied); err == nil {
		t.Fatal("same-size corrupt ack replacement must invalidate the cached tail")
	}
}

func TestDurableGatewayEvictsInactiveTailsAndReloadsDurableState(t *testing.T) {
	gateway := openRuntimeGateway(t)
	gateway.MaxPersonalityAgentTails = 2
	gateway.MaxAckTail = 1

	for _, personalityAgentID := range []string{"018f47a2-9b3c-7def-8abc-0123456789ab", "018f47a2-9b3c-7def-9abc-0123456789ac", "018f47a2-9b3c-7def-aabc-0123456789ad"} {
		claims := currentRuntimeClaims(t, gateway, personalityAgentID)
		seq := uint64(1)
		if err := gateway.Receive(context.Background(), claims, Envelope{
			Seq:                &seq,
			PersonalityAgentID: personalityAgentID,
			Event:              json.RawMessage(`{"type":"agent_start"}`),
		}); err != nil {
			t.Fatal(err)
		}
	}
	gateway.mu.Lock()
	if len(gateway.tails) > gateway.MaxPersonalityAgentTails {
		gateway.mu.Unlock()
		t.Fatalf("retained %d personality agent tails, limit is %d", len(gateway.tails), gateway.MaxPersonalityAgentTails)
	}
	gateway.mu.Unlock()

	claims := currentRuntimeClaims(t, gateway, "018f47a2-9b3c-7def-8abc-0123456789ab")
	last, err := gateway.LastReceivedEventSeq(context.Background(), claims)
	if err != nil || last != 1 {
		t.Fatalf("evicted event tail did not reload: last=%d err=%v", last, err)
	}
	seq := uint64(2)
	if err := gateway.Receive(context.Background(), claims, Envelope{
		Seq:                &seq,
		PersonalityAgentID: claims.PersonalityAgentID,
		Event:              json.RawMessage(`{"type":"agent_end"}`),
	}); err != nil {
		t.Fatalf("event append after reload: %v", err)
	}

	for i := 0; i < 2; i++ {
		if _, err := gateway.commands.Append(context.Background(), testDirectChatProvenance(claims.PersonalityAgentID), "", json.RawMessage(`{"type":"user_message","text":"hello","attachments":[]}`)); err != nil {
			t.Fatal(err)
		}
	}
	commands, err := gateway.commands.CatchUp(context.Background(), claims.PersonalityAgentID, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, command := range commands {
		if err := gateway.ApplyAck(context.Background(), claims, CommandAck{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Seq: command.Seq, CommandID: command.CommandID, Status: "received"}); err != nil {
			t.Fatal(err)
		}
	}
	// seq=1 is outside the one-entry cache, but its durable received ACK must
	// still allow the one legal terminal transition.
	first := commands[0]
	if err := gateway.ApplyAck(context.Background(), claims, CommandAck{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Seq: first.Seq, CommandID: first.CommandID, Status: "applied"}); err != nil {
		t.Fatalf("evicted ACK did not reload for terminal transition: %v", err)
	}
	gateway.mu.Lock()
	ackEntries := len(gateway.stateFor(claims.PersonalityAgentID).acks)
	gateway.mu.Unlock()
	if ackEntries > gateway.MaxAckTail {
		t.Fatalf("retained %d ACK cache entries, limit is %d", ackEntries, gateway.MaxAckTail)
	}
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

func TestDurableGatewayEventAppendRollsBackOnWriteFailure(t *testing.T) {
	gateway := openRuntimeGateway(t)
	claims := currentRuntimeClaims(t, gateway, "018f47a2-9b3c-7def-8abc-0123456789ab")
	seq := uint64(1)
	event := Envelope{
		Seq:                &seq,
		PersonalityAgentID: claims.PersonalityAgentID,
		Event:              json.RawMessage(`{"type":"agent_start"}`),
	}

	ff := &failingFile{failWriteOn: 1}
	gateway.newFile = failingOpener(ff)
	if err := gateway.Receive(context.Background(), claims, event); err == nil {
		t.Fatal("expected write failure")
	}

	gateway.newFile = realOpener()
	last, err := gateway.LastReceivedEventSeq(context.Background(), claims)
	if err != nil {
		t.Fatal(err)
	}
	if last != 0 {
		t.Fatalf("expected empty log after rollback, got seq %d", last)
	}
	if err := gateway.Receive(context.Background(), claims, event); err != nil {
		t.Fatalf("retry after rollback failed: %v", err)
	}
	last, err = gateway.LastReceivedEventSeq(context.Background(), claims)
	if err != nil {
		t.Fatal(err)
	}
	if last != 1 {
		t.Fatalf("expected seq 1, got %d", last)
	}
}

func TestDurableGatewayEventAppendRollsBackOnSyncFailure(t *testing.T) {
	gateway := openRuntimeGateway(t)
	claims := currentRuntimeClaims(t, gateway, "018f47a2-9b3c-7def-8abc-0123456789ab")
	seq := uint64(1)
	event := Envelope{
		Seq:                &seq,
		PersonalityAgentID: claims.PersonalityAgentID,
		Event:              json.RawMessage(`{"type":"agent_start"}`),
	}

	ff := &failingFile{failSyncOn: 1}
	gateway.newFile = failingOpener(ff)
	if err := gateway.Receive(context.Background(), claims, event); err == nil {
		t.Fatal("expected sync failure")
	}
	last, err := gateway.LastReceivedEventSeq(context.Background(), claims)
	if err != nil {
		t.Fatal(err)
	}
	if last != 0 {
		t.Fatalf("expected empty log after sync rollback, got seq %d", last)
	}

	gateway.newFile = realOpener()
	if err := gateway.Receive(context.Background(), claims, event); err != nil {
		t.Fatalf("retry after rollback failed: %v", err)
	}
	last, err = gateway.LastReceivedEventSeq(context.Background(), claims)
	if err != nil {
		t.Fatal(err)
	}
	if last != 1 {
		t.Fatalf("expected seq 1, got %d", last)
	}
}

func TestDurableGatewayEventRecoversFromIncompleteFinalRecord(t *testing.T) {
	gateway := openRuntimeGateway(t)
	claims := currentRuntimeClaims(t, gateway, "018f47a2-9b3c-7def-8abc-0123456789ab")
	seq := uint64(1)
	event := Envelope{
		Seq:                &seq,
		PersonalityAgentID: claims.PersonalityAgentID,
		Event:              json.RawMessage(`{"type":"agent_start"}`),
	}
	if err := gateway.Receive(context.Background(), claims, event); err != nil {
		t.Fatal(err)
	}
	path := gateway.eventPath(claims.PersonalityAgentID)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// Truncate into the final JSON record (not just the trailing newline) so
	// the next append must truncate the partial tail and recover.
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatal(err)
	}
	if err := gateway.Receive(context.Background(), claims, event); err != nil {
		t.Fatalf("recovery append failed: %v", err)
	}
	last, err := gateway.LastReceivedEventSeq(context.Background(), claims)
	if err != nil {
		t.Fatal(err)
	}
	if last != 1 {
		t.Fatalf("expected seq 1 after recovery, got %d", last)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(string(raw), "\n"); lines != 1 {
		t.Fatalf("expected one complete line, got %q", raw)
	}
}

func TestDurableGatewayEventLogRejectsCorruptButCompleteRecords(t *testing.T) {
	gateway := openRuntimeGateway(t)
	claims := currentRuntimeClaims(t, gateway, "018f47a2-9b3c-7def-8abc-0123456789ab")
	seq := uint64(1)
	event := Envelope{
		Seq:                &seq,
		PersonalityAgentID: claims.PersonalityAgentID,
		Event:              json.RawMessage(`{"type":"agent_start"}`),
	}
	if err := gateway.Receive(context.Background(), claims, event); err != nil {
		t.Fatal(err)
	}

	corrupt := []string{
		`{"seq":2,"event":{"seq":2,"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"agent_end"}},"seq":2}` + "\n",
		`{"seq":2,"event":{"seq":2,"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"agent_end"}},"extra":true}` + "\n",
		`{"seq":9007199254740992,"event":{"seq":9007199254740992,"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"agent_end"}}}` + "\n",
	}
	for index, line := range corrupt {
		if err := os.WriteFile(gateway.eventPath(claims.PersonalityAgentID), append([]byte(nil), []byte(line)...), 0o600); err != nil {
			t.Fatal(err)
		}
		seq = 2
		event.Seq = &seq
		event.Event = json.RawMessage(`{"type":"agent_end"}`)
		err := gateway.Receive(context.Background(), claims, event)
		if err == nil {
			t.Fatalf("corrupt event log line must be rejected: %s", line)
		}
		if index == 2 && !strings.Contains(err.Error(), "JSON-safe integer") {
			t.Fatalf("overflow record must fail for its sequence range, got %v", err)
		}
	}
}

func TestDurableGatewayAckLogRejectsCorruptButCompleteRecords(t *testing.T) {
	gateway := openRuntimeGateway(t)
	claims := currentRuntimeClaims(t, gateway, "018f47a2-9b3c-7def-8abc-0123456789ab")
	command, err := gateway.commands.Append(
		context.Background(),
		testDirectChatProvenance(claims.PersonalityAgentID),
		"",
		json.RawMessage(`{"type":"user_message","text":"hello","attachments":[]}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	ack := CommandAck{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Seq: command.Seq, CommandID: command.CommandID, Status: "received"}
	if err := gateway.ApplyAck(context.Background(), claims, ack); err != nil {
		t.Fatal(err)
	}

	corrupt := []string{
		fmt.Sprintf(`{"seq":%d,"command_id":"%s","status":"applied","command_id":"%s"}`+"\n", command.Seq, command.CommandID, command.CommandID),
		fmt.Sprintf(`{"seq":%d,"command_id":"%s","status":"applied","extra":true}`+"\n", command.Seq, command.CommandID),
		`{"seq":9007199254740992,"command_id":"00000000-0000-4000-8000-000000000001","status":"applied"}` + "\n",
	}
	for _, line := range corrupt {
		if err := os.WriteFile(gateway.ackPath(claims.PersonalityAgentID), []byte(line), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := gateway.ApplyAck(context.Background(), claims, CommandAck{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Seq: command.Seq, CommandID: command.CommandID, Status: "applied"}); err == nil {
			t.Fatalf("corrupt ack log line must be rejected: %s", line)
		}
	}
}

func TestDurableGatewayAckLogRejectsCorruptRecordOnFindAckLookup(t *testing.T) {
	gateway := openRuntimeGateway(t)
	claims := currentRuntimeClaims(t, gateway, "018f47a2-9b3c-7def-8abc-0123456789ab")
	command, err := gateway.commands.Append(
		context.Background(),
		testDirectChatProvenance(claims.PersonalityAgentID),
		"",
		json.RawMessage(`{"type":"user_message","text":"hello","attachments":[]}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	ack := CommandAck{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Seq: command.Seq, CommandID: command.CommandID, Status: "received"}
	if err := gateway.ApplyAck(context.Background(), claims, ack); err != nil {
		t.Fatal(err)
	}

	// Evict the tail so findAckLocked must read the durable log.
	gateway.mu.Lock()
	gateway.tails = make(map[string]*personalityAgentLogState)
	gateway.mu.Unlock()

	corrupt := fmt.Sprintf(`{"seq":%d,"command_id":"%s","status":"received","status":"received"}`+"\n", command.Seq, command.CommandID)
	if err := os.WriteFile(gateway.ackPath(claims.PersonalityAgentID), []byte(corrupt), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := gateway.ApplyAck(context.Background(), claims, CommandAck{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Seq: command.Seq, CommandID: command.CommandID, Status: "applied"}); err == nil {
		t.Fatal("findAckLocked must reject corrupt ack log")
	}
}

func TestDurableGatewayAckAppendRollsBackOnWriteFailure(t *testing.T) {
	gateway := openRuntimeGateway(t)
	claims := currentRuntimeClaims(t, gateway, "018f47a2-9b3c-7def-8abc-0123456789ab")
	command, err := gateway.commands.Append(
		context.Background(),
		testDirectChatProvenance(claims.PersonalityAgentID),
		"",
		json.RawMessage(`{"type":"user_message","text":"hello","attachments":[]}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	ack := CommandAck{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Seq: command.Seq, CommandID: command.CommandID, Status: "received"}

	ff := &failingFile{failWriteOn: 1}
	gateway.newFile = failingOpener(ff)
	if err := gateway.ApplyAck(context.Background(), claims, ack); err == nil {
		t.Fatal("expected write failure")
	}

	gateway.newFile = realOpener()
	if err := gateway.ApplyAck(context.Background(), claims, ack); err != nil {
		t.Fatalf("retry after rollback failed: %v", err)
	}
	raw, err := os.ReadFile(gateway.ackPath(claims.PersonalityAgentID))
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(string(raw), "\n"); lines != 1 {
		t.Fatalf("expected one ack line, got %q", raw)
	}
}

func TestDurableGatewayAckAppendRollsBackOnSyncFailure(t *testing.T) {
	gateway := openRuntimeGateway(t)
	claims := currentRuntimeClaims(t, gateway, "018f47a2-9b3c-7def-8abc-0123456789ab")
	command, err := gateway.commands.Append(
		context.Background(),
		testDirectChatProvenance(claims.PersonalityAgentID),
		"",
		json.RawMessage(`{"type":"user_message","text":"hello","attachments":[]}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	ack := CommandAck{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Seq: command.Seq, CommandID: command.CommandID, Status: "received"}

	ff := &failingFile{failSyncOn: 1}
	gateway.newFile = failingOpener(ff)
	if err := gateway.ApplyAck(context.Background(), claims, ack); err == nil {
		t.Fatal("expected sync failure")
	}

	gateway.newFile = realOpener()
	if err := gateway.ApplyAck(context.Background(), claims, ack); err != nil {
		t.Fatalf("retry after rollback failed: %v", err)
	}
	raw, err := os.ReadFile(gateway.ackPath(claims.PersonalityAgentID))
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(string(raw), "\n"); lines != 1 {
		t.Fatalf("expected one ack line, got %q", raw)
	}
}

func TestDurableGatewayAckRecoversFromIncompleteFinalRecord(t *testing.T) {
	gateway := openRuntimeGateway(t)
	claims := currentRuntimeClaims(t, gateway, "018f47a2-9b3c-7def-8abc-0123456789ab")
	command, err := gateway.commands.Append(
		context.Background(),
		testDirectChatProvenance(claims.PersonalityAgentID),
		"",
		json.RawMessage(`{"type":"user_message","text":"hello","attachments":[]}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	ack := CommandAck{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Seq: command.Seq, CommandID: command.CommandID, Status: "received"}
	if err := gateway.ApplyAck(context.Background(), claims, ack); err != nil {
		t.Fatal(err)
	}
	path := gateway.ackPath(claims.PersonalityAgentID)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatal(err)
	}
	if err := gateway.ApplyAck(context.Background(), claims, ack); err != nil {
		t.Fatalf("recovery ack failed: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(string(raw), "\n"); lines != 1 {
		t.Fatalf("expected one complete ack line, got %q", raw)
	}
}

func TestDurableGatewayLogsRejectSymlinkTargets(t *testing.T) {
	tests := []struct {
		name string
		path func(*DurableGateway, string) string
		run  func(*testing.T, *DurableGateway, TokenClaims) error
	}{
		{
			name: "event",
			path: func(g *DurableGateway, personalityAgentID string) string {
				return g.eventPath(personalityAgentID)
			},
			run: func(t *testing.T, g *DurableGateway, claims TokenClaims) error {
				t.Helper()
				seq := uint64(1)
				return g.Receive(context.Background(), claims, Envelope{
					Seq:                &seq,
					PersonalityAgentID: claims.PersonalityAgentID,
					Event:              json.RawMessage(`{"type":"agent_start"}`),
				})
			},
		},
		{
			name: "ack",
			path: func(g *DurableGateway, personalityAgentID string) string {
				return g.ackPath(personalityAgentID)
			},
			run: func(t *testing.T, g *DurableGateway, claims TokenClaims) error {
				t.Helper()
				command, err := g.commands.Append(
					context.Background(),
					testDirectChatProvenance(claims.PersonalityAgentID),
					"",
					json.RawMessage(`{"type":"abort"}`),
				)
				if err != nil {
					t.Fatal(err)
				}
				return g.ApplyAck(context.Background(), claims, CommandAck{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
					Seq:       command.Seq,
					CommandID: command.CommandID,
					Status:    "received",
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gateway := openRuntimeGateway(t)
			claims := currentRuntimeClaims(t, gateway, "018f47a2-9b3c-7def-8abc-0123456789ab")
			target := filepath.Join(t.TempDir(), "redirected.log")
			const original = "sentinel"
			if err := os.WriteFile(target, []byte(original), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, test.path(gateway, claims.PersonalityAgentID)); err != nil {
				t.Fatal(err)
			}
			if err := test.run(t, gateway, claims); err == nil {
				t.Fatal("expected symlink-backed durable log to be rejected")
			}
			raw, err := os.ReadFile(target)
			if err != nil {
				t.Fatal(err)
			}
			if string(raw) != original {
				t.Fatalf("symlink target changed: %q", raw)
			}
		})
	}
}

func TestDurableGatewayLiveDoesNotLoseConcurrentAppend(t *testing.T) {
	gateway := openRuntimeGateway(t)
	claims := currentRuntimeClaims(t, gateway, "018f47a2-9b3c-7def-8abc-0123456789ab")
	raw := json.RawMessage(`{"type":"abort"}`)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	live, errs, err := gateway.Live(ctx, claims, 1)
	if err != nil {
		t.Fatalf("live: %v", err)
	}

	// Append a command after Live has returned its channels. Because Live starts
	// polling from fromSeq, the next tick must deliver it rather than advance
	// past it.
	env, err := gateway.commands.Append(ctx, testDirectChatProvenance(claims.PersonalityAgentID), "key-1", raw)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	select {
	case cmd := <-live:
		if cmd.Seq != env.Seq {
			t.Fatalf("expected seq %d, got %d", env.Seq, cmd.Seq)
		}
	case err := <-errs:
		t.Fatalf("live error: %v", err)
	case <-ctx.Done():
		t.Fatal("timeout waiting for live command")
	}
}

func TestDurableGatewayEventCatchUpRejectsCorruptRecords(t *testing.T) {
	personalityAgentID := "018f47a2-9b3c-7def-8abc-0123456789ab"

	cases := []struct {
		name     string
		contents []byte
		wantErr  string
	}{
		{
			name:     "outer/inner seq mismatch",
			contents: []byte(`{"seq":1,"event":{"seq":2,"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"agent_start"}}}` + "\n"),
			wantErr:  "seq mismatch",
		},
		{
			name:     "personality agent mismatch",
			contents: []byte(`{"seq":1,"event":{"seq":1,"personality_agent_id":"018f47a2-9b3c-7def-9abc-0123456789ac","event":{"type":"agent_start"}}}` + "\n"),
			wantErr:  "personality agent mismatch",
		},
		{
			name:     "volatile event with seq",
			contents: []byte(`{"seq":1,"event":{"seq":1,"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"message_update"}}}` + "\n"),
			wantErr:  "volatile event",
		},
		{
			name:     "durable event missing inner seq",
			contents: []byte(`{"seq":1,"event":{"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"agent_start"}}}` + "\n"),
			wantErr:  "requires seq",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := openRuntimeGateway(t)
			if err := os.WriteFile(g.eventPath(personalityAgentID), tc.contents, 0o600); err != nil {
				t.Fatalf("write corrupt log: %v", err)
			}
			_, err := g.EventCatchUp(context.Background(), personalityAgentID, 0)
			if err == nil {
				t.Fatal("expected corrupt event log to be rejected")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestDurableGatewayEventCatchUpAcceptsValidRecords(t *testing.T) {
	gateway := openRuntimeGateway(t)
	personalityAgentID := "018f47a2-9b3c-7def-8abc-0123456789ab"
	claims := currentRuntimeClaims(t, gateway, personalityAgentID)

	seq := uint64(1)
	if err := gateway.Receive(context.Background(), claims, Envelope{
		Seq:                &seq,
		PersonalityAgentID: personalityAgentID,
		Event:              json.RawMessage(`{"type":"agent_start"}`),
	}); err != nil {
		t.Fatalf("receive event: %v", err)
	}

	caught, err := gateway.EventCatchUp(context.Background(), personalityAgentID, 0)
	if err != nil {
		t.Fatalf("catch-up: %v", err)
	}
	if len(caught) != 1 {
		t.Fatalf("expected 1 event, got %d", len(caught))
	}
	if caught[0].PersonalityAgentID != personalityAgentID {
		t.Fatalf("expected personality agent %q, got %q", personalityAgentID, caught[0].PersonalityAgentID)
	}
	if caught[0].Seq == nil || *caught[0].Seq != 1 {
		t.Fatalf("expected seq 1, got %v", caught[0].Seq)
	}
}

func TestDurableGatewayAppendRejectsInnerOuterSeqMismatch(t *testing.T) {
	gateway := openRuntimeGateway(t)
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	inner := uint64(2)
	err := gateway.appendDurableEventLocked(context.Background(), personalityAgentID, durableEventRecord{
		Seq: 1,
		Event: Envelope{
			Seq:                &inner,
			PersonalityAgentID: personalityAgentID,
			Event:              json.RawMessage(`{"type":"agent_start"}`),
		},
	})
	if err == nil {
		t.Fatal("expected inner/outer seq mismatch to be rejected")
	}
	if !strings.Contains(err.Error(), "seq mismatch") {
		t.Fatalf("expected seq mismatch error, got %v", err)
	}

	err = gateway.appendDurableEventLocked(context.Background(), personalityAgentID, durableEventRecord{
		Seq: 1,
		Event: Envelope{
			Seq:                nil,
			PersonalityAgentID: personalityAgentID,
			Event:              json.RawMessage(`{"type":"agent_start"}`),
		},
	})
	if err == nil {
		t.Fatal("expected missing inner seq to be rejected")
	}
	if !strings.Contains(err.Error(), "seq mismatch") {
		t.Fatalf("expected seq mismatch error, got %v", err)
	}
}

func TestDurableGatewayTracksRunLifecycleAcrossTurnBoundaries(t *testing.T) {
	gateway := openRuntimeGateway(t)
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	claims := TokenClaims{TenantID: "tenant-1", PersonalityAgentID: personalityAgentID, Generation: 1}
	if err := gateway.PublishRuntimeState(personalityAgentID, claims.Generation, nil); err != nil {
		t.Fatal(err)
	}
	var seq uint64
	receive := func(raw string) {
		t.Helper()
		seq++
		if err := gateway.Receive(context.Background(), claims, Envelope{
			Seq:                &seq,
			PersonalityAgentID: personalityAgentID,
			Event:              json.RawMessage(raw),
		}); err != nil {
			t.Fatalf("receive event %d: %v", seq, err)
		}
	}
	assertInFlight := func(want bool) {
		t.Helper()
		if got := gateway.IsRunInFlight(personalityAgentID); got != want {
			t.Fatalf("in-flight state after event %d = %v, want %v", seq, got, want)
		}
	}

	assertInFlight(false)
	receive(`{"type":"agent_start"}`)
	assertInFlight(true)
	receive(`{"type":"turn_start"}`)
	assertInFlight(true)
	receive(`{"type":"message_start","message_id":"00000000-0000-4000-8000-000000000001","message":{"role":"user","content":[{"type":"text","text":"hello"}],"timestamp":"2026-07-28T00:00:00Z"}}`)
	assertInFlight(true)
	receive(`{"type":"message_end","message_id":"00000000-0000-4000-8000-000000000001","message":{"role":"user","content":[{"type":"text","text":"hello"}],"timestamp":"2026-07-28T00:00:00Z"}}`)
	assertInFlight(true)
	receive(`{"type":"message_start","message_id":"00000000-0000-4000-8000-000000000003","message":{"role":"assistant","content":[],"model":"m","provider":"p","origin":{"provider_instance_id":"x","protocol":"open_ai_responses","model":"m"},"usage":{"input":0,"output":0,"cache_read":0,"cache_write":0,"reasoning":0,"total_tokens":0},"stop_reason":"stop","error_message":null,"provider_code":null,"interrupted":false,"timestamp":"2026-07-28T00:00:00Z"}}`)
	assertInFlight(true)
	receive(`{"type":"message_end","message_id":"00000000-0000-4000-8000-000000000003","message":{"role":"assistant","content":[],"model":"m","provider":"p","origin":{"provider_instance_id":"x","protocol":"open_ai_responses","model":"m"},"usage":{"input":0,"output":0,"cache_read":0,"cache_write":0,"reasoning":0,"total_tokens":0},"stop_reason":"tool_use","error_message":null,"provider_code":null,"interrupted":false,"timestamp":"2026-07-28T00:00:00Z"}}`)
	assertInFlight(true)
	receive(`{"type":"message_start","message_id":"00000000-0000-4000-8000-000000000002","message":{"role":"tool_result","tool_call_id":"call-1","tool_name":"read_file","content":[{"type":"text","text":"ok"}],"details":{},"is_error":false,"timestamp":"2026-07-28T00:00:00Z"}}`)
	assertInFlight(true)
	receive(`{"type":"message_end","message_id":"00000000-0000-4000-8000-000000000002","message":{"role":"tool_result","tool_call_id":"call-1","tool_name":"read_file","content":[{"type":"text","text":"ok"}],"details":{},"is_error":false,"timestamp":"2026-07-28T00:00:00Z"}}`)
	assertInFlight(true)
	receive(`{"type":"turn_end","message":null,"tool_results":[]}`)
	assertInFlight(true)
	receive(`{"type":"turn_start"}`)
	assertInFlight(true)
	receive(`{"type":"agent_end"}`)
	assertInFlight(false)
}

func TestDurableGatewayReconstructsCommandGuardStateAcrossRestart(t *testing.T) {
	tmp := t.TempDir()
	storeDir := filepath.Join(tmp, "commands")
	runtimeDir := filepath.Join(tmp, "runtime")

	store, gateway, err := openGatewayAt(t, storeDir, runtimeDir)
	if err != nil {
		t.Fatalf("open first gateway: %v", err)
	}

	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	claims := TokenClaims{TenantID: "tenant-1", PersonalityAgentID: personalityAgentID, Generation: 1}
	if err := gateway.PublishRuntimeState(personalityAgentID, claims.Generation, nil); err != nil {
		t.Fatal(err)
	}

	seq := uint64(1)
	if err := gateway.Receive(context.Background(), claims, Envelope{
		Seq:                &seq,
		PersonalityAgentID: personalityAgentID,
		Event:              json.RawMessage(`{"type":"agent_start"}`),
	}); err != nil {
		t.Fatalf("receive agent_start: %v", err)
	}

	seq = 2
	if err := gateway.Receive(context.Background(), claims, Envelope{
		Seq:                &seq,
		PersonalityAgentID: personalityAgentID,
		Event:              json.RawMessage(`{"type":"turn_start"}`),
	}); err != nil {
		t.Fatalf("receive turn_start: %v", err)
	}

	seq = 3
	if err := gateway.Receive(context.Background(), claims, Envelope{
		Seq:                &seq,
		PersonalityAgentID: personalityAgentID,
		Event:              json.RawMessage(`{"type":"message_end","message_id":"00000000-0000-4000-8000-000000000002","message":{"role":"user","content":[{"type":"text","text":"do not clear assistant state"}],"timestamp":"2026-07-28T00:00:00Z"}}`),
	}); err != nil {
		t.Fatalf("receive user message_end: %v", err)
	}

	seq = 4
	if err := gateway.Receive(context.Background(), claims, Envelope{
		Seq:                &seq,
		PersonalityAgentID: personalityAgentID,
		Event:              json.RawMessage(`{"type":"approval_requested","request":{"id":"request-1","tool_call_id":"call-1","tool_name":"read_file","action":{"reviewable":"read"},"args_summary":"read"}}`),
	}); err != nil {
		t.Fatalf("receive approval_requested: %v", err)
	}

	seq = 5
	if err := gateway.Receive(context.Background(), claims, Envelope{
		Seq:                &seq,
		PersonalityAgentID: personalityAgentID,
		Event:              json.RawMessage(`{"type":"approval_requested","request":{"id":"request-2","tool_call_id":"call-2","tool_name":"read_file","action":{"reviewable":"read"},"args_summary":"read"}}`),
	}); err != nil {
		t.Fatalf("receive second approval_requested: %v", err)
	}

	seq = 6
	if err := gateway.Receive(context.Background(), claims, Envelope{
		Seq:                &seq,
		PersonalityAgentID: personalityAgentID,
		Event:              json.RawMessage(`{"type":"approval_resolved","request_id":"request-2","resolution":{"decision":{"type":"approve_once"}}}`),
	}); err != nil {
		t.Fatalf("receive approval_resolved: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close command store: %v", err)
	}

	store, gateway, err = openGatewayAt(t, storeDir, runtimeDir)
	if err != nil {
		t.Fatalf("reopen gateway: %v", err)
	}
	defer store.Close()

	// Before reconstruction the guard state must appear empty. This is safe
	// only because EnsureAgentSessionStateRebuilt is invoked before command
	// admission in the browser WebSocket path.
	if gateway.IsRunInFlight(personalityAgentID) {
		t.Fatal("expected no in-flight run before state rebuild")
	}
	if gateway.IsApprovalPending(personalityAgentID, "request-1") {
		t.Fatal("expected no pending approvals before state rebuild")
	}

	if err := gateway.EnsureAgentSessionStateRebuilt(context.Background(), personalityAgentID); err != nil {
		t.Fatalf("rebuild agent session state: %v", err)
	}

	if !gateway.IsRunInFlight(personalityAgentID) {
		t.Fatal("expected in-flight run after state rebuild")
	}
	if !gateway.IsApprovalPending(personalityAgentID, "request-1") {
		t.Fatal("expected request-1 to be pending after state rebuild")
	}
	if gateway.IsApprovalPending(personalityAgentID, "request-2") {
		t.Fatal("expected resolved request-2 not to be pending")
	}
	if gateway.IsApprovalPending(personalityAgentID, "request-unknown") {
		t.Fatal("expected unknown request not to be pending")
	}
}

func TestDurableGatewayReconstructionFailsClosedOnCorruptState(t *testing.T) {
	tmp := t.TempDir()
	storeDir := filepath.Join(tmp, "commands")
	runtimeDir := filepath.Join(tmp, "runtime")

	store, gateway, err := openGatewayAt(t, storeDir, runtimeDir)
	if err != nil {
		t.Fatalf("open gateway: %v", err)
	}

	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	// A non-contiguous durable event log must fail reconstruction rather than
	// defaulting to an empty "no run / no approval" state.
	if err := os.WriteFile(
		gateway.eventPath(personalityAgentID),
		[]byte(`{"seq":2,"event":{"seq":2,"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"agent_start"}}}`+"\n"),
		0o600,
	); err != nil {
		t.Fatalf("write corrupt event log: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close command store: %v", err)
	}

	store, gateway, err = openGatewayAt(t, storeDir, runtimeDir)
	if err != nil {
		t.Fatalf("reopen gateway: %v", err)
	}
	defer store.Close()

	if err := gateway.EnsureAgentSessionStateRebuilt(context.Background(), personalityAgentID); err == nil {
		t.Fatal("expected corrupt durable state to fail reconstruction")
	} else if !strings.Contains(err.Error(), "non-contiguous") {
		t.Fatalf("expected non-contiguous error, got %v", err)
	}

	// The guard must remain closed after failed reconstruction; it must not
	// silently default to an empty state that would admit abort or
	// approval_decision commands.
	if gateway.IsRunInFlight(personalityAgentID) {
		t.Fatal("expected in-flight flag to remain false after failed reconstruction")
	}
	if gateway.IsApprovalPending(personalityAgentID, "request-1") {
		t.Fatal("expected pending approvals to remain empty after failed reconstruction")
	}
}
