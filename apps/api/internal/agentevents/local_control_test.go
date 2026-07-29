package agentevents

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	localControlTestPAID     = "0198f0f4-9b72-7000-8000-000000000001"
	localControlOtherPAID    = "0198f0f4-9b72-7000-8000-000000000002"
	localControlTestBearer   = "runtime-a-control-bearer-32-bytes-minimum"
	localControlNextBearer   = "runtime-b-control-bearer-32-bytes-minimum"
	localControlOtherBearer  = "runtime-c-control-bearer-32-bytes-minimum"
	localControlTestAudience = "sumi:agent:events"
	localControlTestTenant   = "tenant-local"
)

var localControlTestSigningSecret = []byte("local-control-signing-secret-32-bytes-minimum")

func localControlAuthorization(
	bearer, personalityAgentID string,
	generation uint64,
	nonce string,
) LocalRuntimeAuthorization {
	return LocalRuntimeAuthorization{
		BearerToken:           bearer,
		TenantID:              localControlTestTenant,
		PersonalityAgentID:    personalityAgentID,
		Generation:            generation,
		RPCBootNonce:          nonce,
		Audience:              localControlTestAudience,
		DeliveryAuthorization: LocalDeliveryRaw,
	}
}

func openLocalControlTestGateway(t *testing.T, runtimeDir string) (*CommandStore, *DurableGateway) {
	t.Helper()
	store, err := OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	gateway, err := OpenDurableGateway(runtimeDir, store)
	if err != nil {
		t.Fatal(err)
	}
	return store, gateway
}

func newLocalControlHTTPServer(
	t *testing.T,
	gateway *DurableGateway,
	authorizations ...LocalRuntimeAuthorization,
) (*LocalControlServer, *httptest.Server) {
	t.Helper()
	control, err := NewLocalControlServer(gateway, localControlTestSigningSecret, authorizations)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	if err := control.RegisterRoutes(mux); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return control, server
}

func postLocalControl(
	t *testing.T,
	serverURL, path, bearer string,
	value any,
) (*http.Response, []byte) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return postLocalControlRaw(t, serverURL, path, bearer, raw)
}

func postLocalControlRaw(
	t *testing.T,
	serverURL, path, bearer string,
	raw []byte,
) (*http.Response, []byte) {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, serverURL+path, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		request.Header.Set("Authorization", "Bearer "+bearer)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	return response, body
}

func decodeLocalControlResponse[T any](t *testing.T, raw []byte) T {
	t.Helper()
	var value T
	if err := unmarshalStrict(raw, &value); err != nil {
		t.Fatalf("decode local control response: %v; body=%s", err, raw)
	}
	return value
}

func revision(value uint64) *uint64 {
	return &value
}

func receipt(value string) *string {
	return &value
}

func startupPublication(id, personalityAgentID string, generation uint64, nonce string) LocalRuntimeStatePublication {
	return LocalRuntimeStatePublication{
		PublicationID:            id,
		PersonalityAgentID:       personalityAgentID,
		Generation:               generation,
		RPCBootNonce:             nonce,
		ExpectedRevision:         nil,
		State:                    LocalRuntimeNotReady,
		HydrationReceiptIdentity: nil,
		Reason:                   LocalRuntimeStartup,
	}
}

func readyPublication(
	id, personalityAgentID string,
	generation uint64,
	nonce string,
	expectedRevision uint64,
	receiptIdentity string,
) LocalRuntimeStatePublication {
	return LocalRuntimeStatePublication{
		PublicationID:            id,
		PersonalityAgentID:       personalityAgentID,
		Generation:               generation,
		RPCBootNonce:             nonce,
		ExpectedRevision:         revision(expectedRevision),
		State:                    LocalRuntimeReady,
		HydrationReceiptIdentity: receipt(receiptIdentity),
		Reason:                   LocalRuntimeHydrated,
	}
}

func credentialRequest(id, personalityAgentID string, generation uint64, nonce string) LocalCredentialIssueRequest {
	return LocalCredentialIssueRequest{
		RequestID:          id,
		PersonalityAgentID: personalityAgentID,
		Generation:         generation,
		RPCBootNonce:       nonce,
		Audience:           localControlTestAudience,
	}
}

func TestLocalControlLifecycleIssuesStrictCredentialAndLatchesReady(t *testing.T) {
	_, gateway := openLocalControlTestGateway(t, t.TempDir())
	control, server := newLocalControlHTTPServer(
		t,
		gateway,
		localControlAuthorization(localControlTestBearer, localControlTestPAID, 7, "boot-a"),
	)
	now := time.Now().UTC().Truncate(time.Second)
	control.now = func() time.Time { return now }

	credential := credentialRequest("credential-1", localControlTestPAID, 7, "boot-a")
	response, _ := postLocalControl(t, server.URL, LocalCredentialIssuePath, localControlTestBearer, credential)
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("credential before startup: got %d, want 409", response.StatusCode)
	}

	startup := startupPublication("publication-startup", localControlTestPAID, 7, "boot-a")
	response, body := postLocalControl(t, server.URL, LocalRuntimeStatePublishPath, localControlTestBearer, startup)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("startup: got %d, want 200; body=%s", response.StatusCode, body)
	}
	startupAck := decodeLocalControlResponse[LocalRuntimeStateAck](t, body)
	if startupAck.PublicationID != startup.PublicationID ||
		startupAck.PersonalityAgentID != startup.PersonalityAgentID ||
		startupAck.Generation != startup.Generation ||
		startupAck.RPCBootNonce != startup.RPCBootNonce ||
		startupAck.Revision != 1 ||
		startupAck.State != LocalRuntimeNotReady ||
		startupAck.HydrationReceiptIdentity != nil {
		t.Fatalf("startup ack did not exactly echo epoch/state: %+v", startupAck)
	}

	response, duplicateStartupBody := postLocalControl(
		t,
		server.URL,
		LocalRuntimeStatePublishPath,
		localControlTestBearer,
		startup,
	)
	if response.StatusCode != http.StatusOK || !bytes.Equal(body, duplicateStartupBody) {
		t.Fatalf("duplicate-same startup was not idempotent: status=%d first=%s second=%s",
			response.StatusCode, body, duplicateStartupBody)
	}

	conflictingStartup := readyPublication(
		startup.PublicationID,
		localControlTestPAID,
		7,
		"boot-a",
		1,
		"receipt-conflict",
	)
	response, _ = postLocalControl(
		t,
		server.URL,
		LocalRuntimeStatePublishPath,
		localControlTestBearer,
		conflictingStartup,
	)
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate-different publication: got %d, want 409", response.StatusCode)
	}

	response, body = postLocalControl(t, server.URL, LocalCredentialIssuePath, localControlTestBearer, credential)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("issue credential: got %d, want 200; body=%s", response.StatusCode, body)
	}
	issued := decodeLocalControlResponse[LocalCredentialIssueResponse](t, body)
	if issued.RequestID != credential.RequestID ||
		issued.PersonalityAgentID != credential.PersonalityAgentID ||
		issued.Generation != credential.Generation ||
		issued.RPCBootNonce != credential.RPCBootNonce ||
		issued.Audience != credential.Audience ||
		issued.ExpiresAtUnix != now.Add(defaultLocalCredentialTTL).Unix() ||
		issued.DeliveryAuthorization != LocalDeliveryRaw ||
		issued.Token == "" {
		t.Fatalf("credential response did not exactly echo/bind request: %+v", issued)
	}
	verifier, err := NewHMACTokenVerifier(localControlTestSigningSecret, localControlTestAudience)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := verifier.Verify(context.Background(), issued.Token)
	if err != nil {
		t.Fatalf("issued token failed strict verifier: %v", err)
	}
	if claims.TenantID != localControlTestTenant ||
		claims.PersonalityAgentID != localControlTestPAID ||
		claims.Generation != 7 {
		t.Fatalf("issued token claims were not authorization-bound: %+v", claims)
	}

	control.now = func() time.Time { return now.Add(defaultLocalCredentialTTL + time.Second) }
	response, duplicateCredentialBody := postLocalControl(
		t,
		server.URL,
		LocalCredentialIssuePath,
		localControlTestBearer,
		credential,
	)
	if response.StatusCode != http.StatusOK || !bytes.Equal(body, duplicateCredentialBody) {
		t.Fatalf("credential duplicate-same was not durable/idempotent: status=%d first=%s second=%s",
			response.StatusCode, body, duplicateCredentialBody)
	}
	duplicateIssued := decodeLocalControlResponse[LocalCredentialIssueResponse](t, duplicateCredentialBody)
	if duplicateIssued.ExpiresAtUnix > control.now().Unix() {
		t.Fatal("test did not exercise an expired idempotent credential response")
	}
	durableRaw, err := os.ReadFile(gateway.statePath(localControlTestPAID))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(durableRaw, []byte(issued.Token)) || bytes.Contains(durableRaw, []byte(`"token"`)) {
		t.Fatal("local control persisted a plaintext runtime credential")
	}
	response, refreshedBody := postLocalControl(
		t,
		server.URL,
		LocalCredentialIssuePath,
		localControlTestBearer,
		credentialRequest("credential-2", localControlTestPAID, 7, "boot-a"),
	)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("fresh request_id did not refresh an expired credential: status=%d body=%s",
			response.StatusCode, refreshedBody)
	}
	refreshed := decodeLocalControlResponse[LocalCredentialIssueResponse](t, refreshedBody)
	if refreshed.ExpiresAtUnix <= duplicateIssued.ExpiresAtUnix || refreshed.Token == duplicateIssued.Token {
		t.Fatalf("fresh request_id did not produce a fresh short-lived credential: old=%+v new=%+v",
			duplicateIssued, refreshed)
	}

	ready := readyPublication("publication-ready", localControlTestPAID, 7, "boot-a", 1, "receipt-a")
	response, body = postLocalControl(t, server.URL, LocalRuntimeStatePublishPath, localControlTestBearer, ready)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("ready: got %d, want 200; body=%s", response.StatusCode, body)
	}
	readyAck := decodeLocalControlResponse[LocalRuntimeStateAck](t, body)
	if readyAck.Revision != 2 ||
		readyAck.State != LocalRuntimeReady ||
		!stringPointerEqual(readyAck.HydrationReceiptIdentity, ready.HydrationReceiptIdentity) {
		t.Fatalf("ready ack mismatch: %+v", readyAck)
	}
	if ready, err := gateway.IsPersonalityAgentReady(context.Background(), localControlTestPAID); err != nil || !ready {
		t.Fatalf("Gateway did not observe authoritative Ready: ready=%v err=%v", ready, err)
	}

	secondReady := readyPublication("publication-ready-again", localControlTestPAID, 7, "boot-a", 2, "receipt-a")
	response, _ = postLocalControl(
		t,
		server.URL,
		LocalRuntimeStatePublishPath,
		localControlTestBearer,
		secondReady,
	)
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("Ready was not immutable/one-shot: got %d, want 409", response.StatusCode)
	}

	shutdown := LocalRuntimeStatePublication{
		PublicationID:            "publication-shutdown",
		PersonalityAgentID:       localControlTestPAID,
		Generation:               7,
		RPCBootNonce:             "boot-a",
		ExpectedRevision:         revision(2),
		State:                    LocalRuntimeNotReady,
		HydrationReceiptIdentity: nil,
		Reason:                   LocalRuntimeShutdown,
	}
	response, body = postLocalControl(t, server.URL, LocalRuntimeStatePublishPath, localControlTestBearer, shutdown)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("shutdown: got %d, want 200; body=%s", response.StatusCode, body)
	}
	shutdownAck := decodeLocalControlResponse[LocalRuntimeStateAck](t, body)
	if shutdownAck.Revision != 3 || shutdownAck.State != LocalRuntimeNotReady ||
		shutdownAck.HydrationReceiptIdentity != nil {
		t.Fatalf("shutdown ack mismatch: %+v", shutdownAck)
	}
	if ready, err := gateway.IsPersonalityAgentReady(context.Background(), localControlTestPAID); err != nil || ready {
		t.Fatalf("shutdown did not clear Ready: ready=%v err=%v", ready, err)
	}
	response, _ = postLocalControl(
		t,
		server.URL,
		LocalCredentialIssuePath,
		localControlTestBearer,
		credentialRequest("credential-after-shutdown", localControlTestPAID, 7, "boot-a"),
	)
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("shutdown epoch still issued credentials: got %d, want 409", response.StatusCode)
	}
}

func TestLocalControlRolloverAtomicallyFencesOldEpochAndSurvivesRestart(t *testing.T) {
	runtimeDir := t.TempDir()
	store, gateway := openLocalControlTestGateway(t, runtimeDir)
	oldAuthorization := localControlAuthorization(localControlTestBearer, localControlTestPAID, 7, "boot-a")
	newAuthorization := localControlAuthorization(localControlNextBearer, localControlTestPAID, 8, "boot-b")
	_, server := newLocalControlHTTPServer(t, gateway, oldAuthorization, newAuthorization)

	oldStartup := startupPublication("old-startup", localControlTestPAID, 7, "boot-a")
	response, _ := postLocalControl(t, server.URL, LocalRuntimeStatePublishPath, localControlTestBearer, oldStartup)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("old startup: got %d", response.StatusCode)
	}
	oldReady := readyPublication("old-ready", localControlTestPAID, 7, "boot-a", 1, "receipt-old")
	response, _ = postLocalControl(t, server.URL, LocalRuntimeStatePublishPath, localControlTestBearer, oldReady)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("old ready: got %d", response.StatusCode)
	}
	oldCredential := credentialRequest("old-credential", localControlTestPAID, 7, "boot-a")
	response, _ = postLocalControl(t, server.URL, LocalCredentialIssuePath, localControlTestBearer, oldCredential)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("old credential: got %d", response.StatusCode)
	}

	newStartup := startupPublication("new-startup", localControlTestPAID, 8, "boot-b")
	response, body := postLocalControl(t, server.URL, LocalRuntimeStatePublishPath, localControlNextBearer, newStartup)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("rollover startup: got %d, want 200; body=%s", response.StatusCode, body)
	}
	rolloverAck := decodeLocalControlResponse[LocalRuntimeStateAck](t, body)
	if rolloverAck.Revision != 3 || rolloverAck.State != LocalRuntimeNotReady {
		t.Fatalf("rollover did not publish next NotReady revision: %+v", rolloverAck)
	}
	if err := gateway.VerifyGeneration(context.Background(), localControlTestPAID, 7); err == nil {
		t.Fatal("old generation remained admissible after rollover")
	}
	if err := gateway.VerifyGeneration(context.Background(), localControlTestPAID, 8); err != nil {
		t.Fatalf("new generation was not current after rollover: %v", err)
	}
	if ready, err := gateway.IsPersonalityAgentReady(context.Background(), localControlTestPAID); err != nil || ready {
		t.Fatalf("rollover did not invalidate old Ready: ready=%v err=%v", ready, err)
	}

	response, _ = postLocalControl(t, server.URL, LocalCredentialIssuePath, localControlTestBearer, oldCredential)
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("old idempotent credential escaped stale-epoch fence: got %d", response.StatusCode)
	}
	response, _ = postLocalControl(t, server.URL, LocalRuntimeStatePublishPath, localControlTestBearer, oldReady)
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("old idempotent Ready escaped stale-epoch fence: got %d", response.StatusCode)
	}
	lateOldReady := readyPublication("late-old-ready", localControlTestPAID, 7, "boot-a", 3, "receipt-old")
	response, _ = postLocalControl(t, server.URL, LocalRuntimeStatePublishPath, localControlTestBearer, lateOldReady)
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("late old Ready was accepted: got %d", response.StatusCode)
	}

	newReady := readyPublication("new-ready", localControlTestPAID, 8, "boot-b", 3, "receipt-new")
	response, body = postLocalControl(t, server.URL, LocalRuntimeStatePublishPath, localControlNextBearer, newReady)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("new ready: got %d, want 200; body=%s", response.StatusCode, body)
	}
	newReadyBody := append([]byte(nil), body...)
	newCredential := credentialRequest("new-credential", localControlTestPAID, 8, "boot-b")
	response, body = postLocalControl(t, server.URL, LocalCredentialIssuePath, localControlNextBearer, newCredential)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("new credential: got %d, want 200; body=%s", response.StatusCode, body)
	}
	newCredentialBody := append([]byte(nil), body...)

	restartedGateway, err := OpenDurableGateway(runtimeDir, store)
	if err != nil {
		t.Fatal(err)
	}
	_, restartedServer := newLocalControlHTTPServer(t, restartedGateway, oldAuthorization, newAuthorization)
	response, body = postLocalControl(
		t,
		restartedServer.URL,
		LocalRuntimeStatePublishPath,
		localControlNextBearer,
		newReady,
	)
	if response.StatusCode != http.StatusOK || !bytes.Equal(newReadyBody, body) {
		t.Fatalf("publication idempotency did not survive restart: status=%d old=%s new=%s",
			response.StatusCode, newReadyBody, body)
	}
	response, body = postLocalControl(
		t,
		restartedServer.URL,
		LocalCredentialIssuePath,
		localControlNextBearer,
		newCredential,
	)
	if response.StatusCode != http.StatusOK || !bytes.Equal(newCredentialBody, body) {
		t.Fatalf("credential idempotency did not survive restart: status=%d old=%s new=%s",
			response.StatusCode, newCredentialBody, body)
	}
	if ready, err := restartedGateway.IsPersonalityAgentReady(context.Background(), localControlTestPAID); err != nil || !ready {
		t.Fatalf("restart lost authoritative Ready: ready=%v err=%v", ready, err)
	}
}

func TestLocalControlExpectedRevisionIsSerializedAcrossConcurrentPublications(t *testing.T) {
	_, gateway := openLocalControlTestGateway(t, t.TempDir())
	_, server := newLocalControlHTTPServer(
		t,
		gateway,
		localControlAuthorization(localControlTestBearer, localControlTestPAID, 7, "boot-a"),
	)
	startup := startupPublication("startup", localControlTestPAID, 7, "boot-a")
	response, _ := postLocalControl(t, server.URL, LocalRuntimeStatePublishPath, localControlTestBearer, startup)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("startup: got %d", response.StatusCode)
	}

	publications := []LocalRuntimeStatePublication{
		readyPublication("ready-a", localControlTestPAID, 7, "boot-a", 1, "receipt-a"),
		readyPublication("ready-b", localControlTestPAID, 7, "boot-a", 1, "receipt-b"),
	}
	statuses := make([]int, len(publications))
	var wg sync.WaitGroup
	for index := range publications {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			response, _ := postLocalControl(
				t,
				server.URL,
				LocalRuntimeStatePublishPath,
				localControlTestBearer,
				publications[index],
			)
			statuses[index] = response.StatusCode
		}(index)
	}
	wg.Wait()
	if !((statuses[0] == http.StatusOK && statuses[1] == http.StatusConflict) ||
		(statuses[1] == http.StatusOK && statuses[0] == http.StatusConflict)) {
		t.Fatalf("CAS admitted other than exactly one concurrent Ready: %v", statuses)
	}
	state, err := gateway.state(context.Background(), localControlTestPAID)
	if err != nil {
		t.Fatal(err)
	}
	if state.LocalControl == nil || state.LocalControl.Revision != 2 ||
		state.LocalControl.State != LocalRuntimeReady || state.HydrationReceiptIdentity == nil {
		t.Fatalf("unexpected durable state after concurrent CAS: %+v", state)
	}
}

func TestLocalControlDurableHistoryTamperingFailsClosed(t *testing.T) {
	_, gateway := openLocalControlTestGateway(t, t.TempDir())
	oldAuthorization := localControlAuthorization(localControlTestBearer, localControlTestPAID, 7, "boot-a")
	newAuthorization := localControlAuthorization(localControlNextBearer, localControlTestPAID, 8, "boot-b")
	_, server := newLocalControlHTTPServer(t, gateway, oldAuthorization, newAuthorization)

	for _, step := range []struct {
		bearer      string
		path        string
		publication any
	}{
		{localControlTestBearer, LocalRuntimeStatePublishPath, startupPublication("old-startup", localControlTestPAID, 7, "boot-a")},
		{localControlTestBearer, LocalRuntimeStatePublishPath, readyPublication("old-ready", localControlTestPAID, 7, "boot-a", 1, "receipt-old")},
		{localControlNextBearer, LocalRuntimeStatePublishPath, startupPublication("new-startup", localControlTestPAID, 8, "boot-b")},
	} {
		response, body := postLocalControl(t, server.URL, step.path, step.bearer, step.publication)
		if response.StatusCode != http.StatusOK {
			t.Fatalf("seed durable history: status=%d body=%s", response.StatusCode, body)
		}
	}
	response, body := postLocalControl(
		t,
		server.URL,
		LocalCredentialIssuePath,
		localControlNextBearer,
		credentialRequest("new-credential", localControlTestPAID, 8, "boot-b"),
	)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("seed durable credential: status=%d body=%s", response.StatusCode, body)
	}

	path := gateway.statePath(localControlTestPAID)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*runtimeState)
	}{
		{
			name: "historical generation with wrong nonce",
			mutate: func(state *runtimeState) {
				record := state.LocalControl.Publications["old-startup"]
				record.Ack.Generation = 8
				state.LocalControl.Publications["old-startup"] = record
			},
		},
		{
			name: "reused revision",
			mutate: func(state *runtimeState) {
				record := state.LocalControl.Publications["old-startup"]
				record.Ack.Revision = 2
				state.LocalControl.Publications["old-startup"] = record
			},
		},
		{
			name: "ack no longer echoes request",
			mutate: func(state *runtimeState) {
				record := state.LocalControl.Publications["old-startup"]
				record.Ack.State = LocalRuntimeReady
				state.LocalControl.Publications["old-startup"] = record
			},
		},
		{
			name: "credential moved into current generation with stale nonce",
			mutate: func(state *runtimeState) {
				record := state.LocalControl.CredentialRequests["new-credential"]
				record.Request.RPCBootNonce = "boot-stale"
				state.LocalControl.CredentialRequests["new-credential"] = record
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var state runtimeState
			if err := unmarshalStrict(original, &state); err != nil {
				t.Fatal(err)
			}
			test.mutate(&state)
			tampered, err := json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, tampered, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := gateway.state(context.Background(), localControlTestPAID); err == nil {
				t.Fatal("tampered durable registry was accepted")
			}
			if err := os.WriteFile(path, original, 0o600); err != nil {
				t.Fatal(err)
			}
		})
	}

	var credentialTamper runtimeState
	if err := unmarshalStrict(original, &credentialTamper); err != nil {
		t.Fatal(err)
	}
	record := credentialTamper.LocalControl.CredentialRequests["new-credential"]
	record.ExpiresAtUnix += int64((24 * time.Hour) / time.Second)
	credentialTamper.LocalControl.CredentialRequests["new-credential"] = record
	tampered, err := json.Marshal(credentialTamper)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, tampered, 0o600); err != nil {
		t.Fatal(err)
	}
	response, body = postLocalControl(
		t,
		server.URL,
		LocalCredentialIssuePath,
		localControlNextBearer,
		credentialRequest("new-credential", localControlTestPAID, 8, "boot-b"),
	)
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("tampered credential metadata was re-signed: status=%d body=%s",
			response.StatusCode, body)
	}
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestLocalControlRejectsPublicStateDirectoryAndSymlinkLock(t *testing.T) {
	runtimeDir := t.TempDir()
	_, gateway := openLocalControlTestGateway(t, runtimeDir)
	authorization := localControlAuthorization(localControlTestBearer, localControlTestPAID, 7, "boot-a")

	if err := os.Chmod(runtimeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := NewLocalControlServer(gateway, localControlTestSigningSecret, []LocalRuntimeAuthorization{authorization}); err == nil {
		t.Fatal("local control accepted a group/world-accessible state directory")
	}
	if err := os.Chmod(runtimeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	control, err := NewLocalControlServer(
		gateway,
		localControlTestSigningSecret,
		[]LocalRuntimeAuthorization{authorization},
	)
	if err != nil {
		t.Fatal(err)
	}
	target := gateway.localControlLockPath(localControlTestPAID) + ".target"
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, gateway.localControlLockPath(localControlTestPAID)); err != nil {
		t.Fatal(err)
	}
	if _, err := control.publishRuntimeState(
		context.Background(),
		startupPublication("startup", localControlTestPAID, 7, "boot-a"),
	); err == nil {
		t.Fatal("local control followed a symlink registry lock")
	}
}

func TestLocalControlRejectsNonLoopbackWrongScopeAndNonStrictJSON(t *testing.T) {
	_, gateway := openLocalControlTestGateway(t, t.TempDir())
	control, server := newLocalControlHTTPServer(
		t,
		gateway,
		localControlAuthorization(localControlTestBearer, localControlTestPAID, 7, "boot-a"),
	)
	startup := startupPublication("startup", localControlTestPAID, 7, "boot-a")

	directRequest := httptest.NewRequest(
		http.MethodPost,
		LocalRuntimeStatePublishPath,
		bytes.NewReader(mustJSON(t, startup)),
	)
	directRequest.RemoteAddr = "203.0.113.10:12345"
	directRequest.Header.Set("Content-Type", "application/json")
	directRequest.Header.Set("Authorization", "Bearer "+localControlTestBearer)
	recorder := httptest.NewRecorder()
	control.handleRuntimeStatePublish(recorder, directRequest)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("non-loopback request: got %d, want 403", recorder.Code)
	}

	response, _ := postLocalControl(t, server.URL, LocalRuntimeStatePublishPath, "", startup)
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("missing bearer: got %d, want 401", response.StatusCode)
	}
	response, _ = postLocalControl(
		t,
		server.URL,
		LocalRuntimeStatePublishPath,
		localControlTestBearer,
		startupPublication("wrong-target", localControlOtherPAID, 7, "boot-a"),
	)
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-PAID request: got %d, want 403", response.StatusCode)
	}

	unknown := []byte(`{"publication_id":"startup","personality_agent_id":"` + localControlTestPAID +
		`","generation":7,"rpc_boot_nonce":"boot-a","expected_revision":null,"state":"not_ready",` +
		`"hydration_receipt_identity":null,"reason":"startup","agent_id":"legacy"}`)
	response, _ = postLocalControlRaw(
		t,
		server.URL,
		LocalRuntimeStatePublishPath,
		localControlTestBearer,
		unknown,
	)
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("unknown field: got %d, want 400", response.StatusCode)
	}

	duplicate := []byte(`{"publication_id":"startup","publication_id":"startup","personality_agent_id":"` +
		localControlTestPAID + `","generation":7,"rpc_boot_nonce":"boot-a","expected_revision":null,` +
		`"state":"not_ready","hydration_receipt_identity":null,"reason":"startup"}`)
	response, _ = postLocalControlRaw(
		t,
		server.URL,
		LocalRuntimeStatePublishPath,
		localControlTestBearer,
		duplicate,
	)
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("duplicate JSON key: got %d, want 400", response.StatusCode)
	}

	trailing := append(mustJSON(t, startup), []byte(` {}`)...)
	response, _ = postLocalControlRaw(
		t,
		server.URL,
		LocalRuntimeStatePublishPath,
		localControlTestBearer,
		trailing,
	)
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("trailing JSON: got %d, want 400", response.StatusCode)
	}

	missingGeneration := []byte(`{"publication_id":"startup","personality_agent_id":"` +
		localControlTestPAID + `","rpc_boot_nonce":"boot-a","expected_revision":null,` +
		`"state":"not_ready","hydration_receipt_identity":null,"reason":"startup"}`)
	response, _ = postLocalControlRaw(
		t,
		server.URL,
		LocalRuntimeStatePublishPath,
		localControlTestBearer,
		missingGeneration,
	)
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing valid-zero generation: got %d, want 400", response.StatusCode)
	}

	request, err := http.NewRequest(
		http.MethodPost,
		server.URL+LocalRuntimeStatePublishPath,
		bytes.NewReader(mustJSON(t, startup)),
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "text/plain")
	request.Header.Set("Authorization", "Bearer "+localControlTestBearer)
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("non-JSON media type: got %d, want 415", response.StatusCode)
	}
}

func TestLocalControlIsNeverRegisteredByProductionMux(t *testing.T) {
	store, gateway := openLocalControlTestGateway(t, t.TempDir())
	mux, _, _, err := NewProductionMux(store, gateway, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{LocalCredentialIssuePath, LocalRuntimeStatePublishPath} {
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
		request.RemoteAddr = "127.0.0.1:12345"
		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("%s was exposed by NewProductionMux: got %d, want 404", path, recorder.Code)
		}
	}
}

func TestNewLocalControlServerRejectsAmbiguousOrWeakAuthorization(t *testing.T) {
	_, gateway := openLocalControlTestGateway(t, t.TempDir())
	valid := localControlAuthorization(localControlTestBearer, localControlTestPAID, 7, "boot-a")

	weak := valid
	weak.BearerToken = "short"
	if _, err := NewLocalControlServer(gateway, localControlTestSigningSecret, []LocalRuntimeAuthorization{weak}); err == nil {
		t.Fatal("weak per-runtime bearer was accepted")
	}
	if _, err := NewLocalControlServer(gateway, []byte("short"), []LocalRuntimeAuthorization{valid}); err == nil {
		t.Fatal("weak issuer signing key was accepted")
	}
	duplicateBearer := localControlAuthorization(localControlTestBearer, localControlOtherPAID, 1, "boot-other")
	if _, err := NewLocalControlServer(
		gateway,
		localControlTestSigningSecret,
		[]LocalRuntimeAuthorization{valid, duplicateBearer},
	); err == nil {
		t.Fatal("one control bearer was allowed to authorize multiple runtimes")
	}
	duplicateEpoch := valid
	duplicateEpoch.BearerToken = localControlOtherBearer
	if _, err := NewLocalControlServer(
		gateway,
		localControlTestSigningSecret,
		[]LocalRuntimeAuthorization{valid, duplicateEpoch},
	); err == nil {
		t.Fatal("one runtime epoch was allowed multiple ambiguous authorizations")
	}
}
