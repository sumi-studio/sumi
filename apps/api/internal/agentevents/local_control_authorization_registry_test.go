package agentevents

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestLocalRuntimeAuthorizationCanBeInstalledReplacedAndRemoved(t *testing.T) {
	_, gateway := openLocalControlTestGateway(t, t.TempDir())
	control, err := NewLocalControlServer(gateway, localControlTestSigningSecret, nil)
	if err != nil {
		t.Fatalf("construct empty dynamic local control: %v", err)
	}
	keyringBefore, ok := gateway.localControlIntegrityKeyringSnapshot()
	if !ok {
		t.Fatal("dynamic local control did not install its process-owned integrity keyring")
	}
	mux := http.NewServeMux()
	if err := control.RegisterRoutes(mux); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	oldAuthorization := localControlAuthorization(localControlTestBearer, localControlTestPAID, 7, "boot-a")
	newAuthorization := localControlAuthorization(localControlNextBearer, localControlTestPAID, 8, "boot-b")
	oldStartup := startupPublication("old-startup", localControlTestPAID, 7, "boot-a")
	response, _ := postLocalControl(
		t,
		server.URL,
		LocalRuntimeStatePublishPath,
		localControlTestBearer,
		oldStartup,
	)
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("uninstalled bearer was accepted: got %d", response.StatusCode)
	}
	if err := control.InstallLocalRuntimeAuthorization(context.Background(), oldAuthorization); err != nil {
		t.Fatalf("install initial authorization: %v", err)
	}
	duplicateBearer := localControlAuthorization(
		localControlTestBearer,
		localControlOtherPAID,
		3,
		"boot-other",
	)
	if err := control.InstallLocalRuntimeAuthorization(context.Background(), duplicateBearer); err == nil {
		t.Fatal("dynamic install allowed one bearer to authorize two PAIDs")
	}
	if gateway.localControlOwns(localControlOtherPAID) {
		t.Fatal("rejected authorization partially installed durable ownership")
	}
	response, body := postLocalControl(
		t,
		server.URL,
		LocalRuntimeStatePublishPath,
		localControlTestBearer,
		oldStartup,
	)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("installed authorization failed: status=%d body=%s", response.StatusCode, body)
	}

	if err := control.InstallLocalRuntimeAuthorization(context.Background(), newAuthorization); err != nil {
		t.Fatalf("replace authorization: %v", err)
	}
	response, _ = postLocalControl(
		t,
		server.URL,
		LocalRuntimeStatePublishPath,
		localControlTestBearer,
		oldStartup,
	)
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("replaced bearer remained authorized: got %d", response.StatusCode)
	}
	newStartup := startupPublication("new-startup", localControlTestPAID, 8, "boot-b")
	response, body = postLocalControl(
		t,
		server.URL,
		LocalRuntimeStatePublishPath,
		localControlNextBearer,
		newStartup,
	)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("replacement authorization failed: status=%d body=%s", response.StatusCode, body)
	}

	if err := control.RemoveLocalRuntimeAuthorization(localControlTestPAID); err != nil {
		t.Fatalf("remove authorization: %v", err)
	}
	if err := control.RemoveLocalRuntimeAuthorization(localControlTestPAID); err != nil {
		t.Fatalf("idempotent removal: %v", err)
	}
	response, _ = postLocalControl(
		t,
		server.URL,
		LocalRuntimeStatePublishPath,
		localControlNextBearer,
		newStartup,
	)
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("removed bearer remained authorized: got %d", response.StatusCode)
	}
	if !gateway.localControlOwns(localControlTestPAID) {
		t.Fatal("authorization removal retired the process-owned durable integrity fence")
	}
	keyringAfter, ok := gateway.localControlIntegrityKeyringSnapshot()
	if !ok || !localControlIntegrityKeyringsEqual(
		keyringBefore.Current,
		keyringBefore.Previous,
		keyringAfter.Current,
		keyringAfter.Previous,
	) {
		t.Fatal("dynamic authorization mutation changed the process signing/integrity keyring")
	}
}

func TestFenceLocalRuntimeAuthorizationClearsExactReadyEpochEvenAfterAPIRestart(t *testing.T) {
	_, gateway := openLocalControlTestGateway(t, t.TempDir())
	control, server := newLocalControlHTTPServer(
		t,
		gateway,
		localControlAuthorization(localControlTestBearer, localControlTestPAID, 7, "boot-a"),
	)
	for _, publication := range []LocalRuntimeStatePublication{
		startupPublication("startup", localControlTestPAID, 7, "boot-a"),
		readyPublication("ready", localControlTestPAID, 7, "boot-a", 1, "receipt-a"),
	} {
		response, body := postLocalControl(
			t,
			server.URL,
			LocalRuntimeStatePublishPath,
			localControlTestBearer,
			publication,
		)
		if response.StatusCode != http.StatusOK {
			t.Fatalf("publish %s: status=%d body=%s", publication.PublicationID, response.StatusCode, body)
		}
	}
	ready, err := gateway.IsPersonalityAgentReady(context.Background(), localControlTestPAID)
	if err != nil || !ready {
		t.Fatalf("ready before fence=%v err=%v", ready, err)
	}
	// Simulate a restarted API whose in-memory authorization registry is empty
	// while the durable Ready record and root-managed runtime survive.
	control.authorizations.remove(localControlTestPAID)
	if err := control.FenceLocalRuntimeAuthorization(
		context.Background(),
		localControlTestPAID,
		7,
		"boot-a",
	); err != nil {
		t.Fatal(err)
	}
	ready, err = gateway.IsPersonalityAgentReady(context.Background(), localControlTestPAID)
	if err != nil || ready {
		t.Fatalf("ready after fence=%v err=%v", ready, err)
	}
}

func TestAuthorizationReplacementWaitsForCoherentCredentialSnapshot(t *testing.T) {
	_, gateway := openLocalControlTestGateway(t, t.TempDir())
	oldAuthorization := localControlAuthorization(localControlTestBearer, localControlTestPAID, 7, "boot-a")
	control, server := newLocalControlHTTPServer(t, gateway, oldAuthorization)
	startup := startupPublication("startup", localControlTestPAID, 7, "boot-a")
	response, body := postLocalControl(
		t,
		server.URL,
		LocalRuntimeStatePublishPath,
		localControlTestBearer,
		startup,
	)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("startup: status=%d body=%s", response.StatusCode, body)
	}

	issueEntered := make(chan struct{})
	releaseIssue := make(chan struct{})
	control.now = func() time.Time {
		close(issueEntered)
		<-releaseIssue
		return time.Unix(1_800_000_000, 0)
	}
	type result struct {
		status int
		body   []byte
	}
	issued := make(chan result, 1)
	go func() {
		response, body := postLocalControl(
			t,
			server.URL,
			LocalCredentialIssuePath,
			localControlTestBearer,
			credentialRequest("old-snapshot", localControlTestPAID, 7, "boot-a"),
		)
		issued <- result{status: response.StatusCode, body: body}
	}()
	<-issueEntered

	newAuthorization := localControlAuthorization(localControlNextBearer, localControlTestPAID, 8, "boot-b")
	replaced := make(chan error, 1)
	go func() {
		replaced <- control.InstallLocalRuntimeAuthorization(context.Background(), newAuthorization)
	}()
	select {
	case err := <-replaced:
		t.Fatalf("replacement returned while an old-epoch issuance was active: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseIssue)
	issueResult := <-issued
	if issueResult.status != http.StatusOK {
		t.Fatalf("linearized old credential issuance failed: status=%d body=%s", issueResult.status, issueResult.body)
	}
	if err := <-replaced; err != nil {
		t.Fatalf("replace authorization: %v", err)
	}

	response, _ = postLocalControl(
		t,
		server.URL,
		LocalCredentialIssuePath,
		localControlTestBearer,
		credentialRequest("after-fence", localControlTestPAID, 7, "boot-a"),
	)
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("old bearer succeeded after replacement returned: got %d", response.StatusCode)
	}
}

func TestConcurrentAuthorizationSnapshotsNeverMixEpochFields(t *testing.T) {
	registry := localRuntimeAuthorizationRegistry{
		byPAID: make(map[string]LocalRuntimeAuthorization),
	}
	first := localControlAuthorization(localControlTestBearer, localControlTestPAID, 7, "boot-a")
	second := localControlAuthorization(localControlNextBearer, localControlTestPAID, 8, "boot-b")
	if err := registry.install(first); err != nil {
		t.Fatal(err)
	}

	const iterations = 2_000
	errCh := make(chan string, 8)
	done := make(chan struct{})
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				for _, candidate := range []LocalRuntimeAuthorization{first, second} {
					snapshot, release, ok := registry.acquire(candidate.BearerToken, localControlTestPAID)
					if !ok {
						continue
					}
					coherent := snapshot == first || snapshot == second
					release()
					if !coherent {
						errCh <- "authorization snapshot mixed fields from different epochs"
						return
					}
				}
			}
		}()
	}
	for i := 0; i < iterations; i++ {
		if err := registry.install(first); err != nil {
			t.Fatal(err)
		}
		if err := registry.install(second); err != nil {
			t.Fatal(err)
		}
	}
	close(done)
	workers.Wait()
	select {
	case message := <-errCh:
		t.Fatal(message)
	default:
	}
}
