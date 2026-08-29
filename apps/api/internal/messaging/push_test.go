package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

const (
	testPushP256dh = "BDCBECfmPtnXzdwJgF_TdPNsC6ZfDnFm31D8x6HqxOJ7zq3tJamfSlIVx2cDwsSUKwGiiuucupkLMDwJFObrclE"
	testPushAuth   = "lRvaLEgVKoLGOxRND8ZCTA"
)

type testPushSessionAuthorizer struct {
	allow bool
}

func (a testPushSessionAuthorizer) AuthorizeBrowserSessionIdentity(
	_ context.Context,
	_ agentevents.BrowserSessionIdentity,
	operation func() error,
) error {
	if !a.allow {
		return errors.New("retired browser session")
	}
	return operation()
}

type recordingPushClient struct {
	mu        sync.Mutex
	endpoints []string
}

func (c *recordingPushClient) Do(request *http.Request) (*http.Response, error) {
	c.mu.Lock()
	c.endpoints = append(c.endpoints, request.URL.String())
	c.mu.Unlock()
	return &http.Response{
		StatusCode: http.StatusCreated,
		Body:       io.NopCloser(strings.NewReader("")),
	}, nil
}

func (c *recordingPushClient) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.endpoints)
}

func configureTestPushEgress(store *Store) {
	store.egress = &pushEgress{
		resolve: func(context.Context, string) ([]net.IP, error) {
			return []net.IP{net.ParseIP("8.8.8.8")}, nil
		},
	}
}

func TestPushEndpointEgressRejectsPrivateAndMixedResolution(t *testing.T) {
	policy := &pushEgress{resolve: func(_ context.Context, host string) ([]net.IP, error) {
		switch host {
		case "public.example.test":
			return []net.IP{net.ParseIP("8.8.8.8")}, nil
		case "mixed.example.test":
			return []net.IP{net.ParseIP("8.8.8.8"), net.ParseIP("127.0.0.1")}, nil
		default:
			return nil, errors.New("unexpected host")
		}
	}}
	if err := policy.allowEndpoint(context.Background(), "https://public.example.test/push"); err != nil {
		t.Fatalf("public push endpoint rejected: %v", err)
	}
	for _, endpoint := range []string{
		"http://public.example.test/push",
		"https://127.0.0.1/push",
		"https://mixed.example.test/push",
	} {
		if err := policy.allowEndpoint(context.Background(), endpoint); !errors.Is(err, ErrInvalidPushSubscription) {
			t.Fatalf("endpoint %q = %v, want invalid subscription", endpoint, err)
		}
	}
}

func testPushSession(id string) agentevents.BrowserSessionIdentity {
	return agentevents.BrowserSessionIdentity{
		ID:        id,
		ExpiresAt: time.Now().Add(time.Hour),
	}
}

func TestGenericPushPayloadContainsOnlyRoutingPointer(t *testing.T) {
	payloadType := reflect.TypeOf(PushPayload{})
	if payloadType.NumField() != 3 {
		t.Fatalf("generic payload has %d fields, want only three routing fields", payloadType.NumField())
	}
	payload, err := json.Marshal(PushPayload{
		WorkspaceID: "workspace-pointer",
		PlaceID:     "place-pointer",
		PlaceKind:   PlaceChannel,
	})
	if err != nil {
		t.Fatal(err)
	}
	got := string(payload)
	want := `{"workspace_id":"workspace-pointer","place_id":"place-pointer","place_kind":"channel"}`
	if got != want {
		t.Fatalf("payload = %s, want %s", got, want)
	}
	for _, forbidden := range []string{
		"body", "content", "attachment", "filename", "author", "participant",
		"display_name", "title", "reason", "seq",
	} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("generic payload exposes %q: %s", forbidden, got)
		}
	}
}

func TestPushSubscriptionOwnershipAndSessionCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, _ := w.workspaceWithChannel(t, ctx)
	configureTestPushEgress(w.store.Store)
	endpoint := "https://push.example.test/device"
	firstSession := testPushSession(strings.Repeat("A", 43))
	secondSession := testPushSession(strings.Repeat("B", 43))

	first := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	saved, err := first.SavePushSubscription(
		ctx, firstSession, endpoint, testPushP256dh, testPushAuth,
	)
	if err != nil {
		t.Fatal(err)
	}
	if saved.OwnerGeneration != 1 {
		t.Fatalf("initial owner generation = %d", saved.OwnerGeneration)
	}

	second := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB)
	if _, err := second.SavePushSubscription(
		ctx, secondSession, endpoint, "different-key", "different-auth",
	); !errors.Is(err, ErrPushSubscriptionOwned) {
		t.Fatalf("endpoint-only takeover = %v, want owned conflict", err)
	}
	transferred, err := second.SavePushSubscription(
		ctx, secondSession, endpoint, testPushP256dh, testPushAuth,
	)
	if err != nil {
		t.Fatalf("same physical subscription transfer: %v", err)
	}
	if transferred.OwnerGeneration != 2 || transferred.Human != w.humanB {
		t.Fatalf("transferred subscription = %+v", transferred)
	}
	staleClient := &recordingPushClient{}
	stale, err := NewPushDispatcher(
		ctx, w.store.Store, testPushSessionAuthorizer{allow: true}, "mailto:test@example.com",
	)
	if err != nil {
		t.Fatal(err)
	}
	stale.client = staleClient
	stale.send(ctx, pushDelivery{subscription: saved, payload: []byte(`{"workspace_id":"stale"}`)})
	if staleClient.count() != 0 {
		t.Fatal("pre-transfer delivery reached the endpoint's new owner")
	}

	if err := first.DeletePushSubscription(ctx, firstSession, endpoint); err != nil {
		t.Fatal(err)
	}
	var ownerID, sessionID string
	if err := w.store.pool.QueryRow(ctx, `
		SELECT human_id, browser_session_id FROM push_subscriptions
		WHERE endpoint = $1`, endpoint).Scan(&ownerID, &sessionID); err != nil {
		t.Fatalf("stale owner deleted current subscription: %v", err)
	}
	if ownerID != w.humanB.ID || sessionID != secondSession.ID {
		t.Fatalf("current owner/session = %s/%s", ownerID, sessionID)
	}

	w.store.CloseBrowserSession(secondSession.ID)
	var remaining int
	if err := w.store.pool.QueryRow(ctx,
		"SELECT count(*) FROM push_subscriptions WHERE endpoint = $1", endpoint).
		Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("retired session left %d subscriptions", remaining)
	}
}

func TestPushSessionCleanupTimeoutNeverWedgesLogout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, _ := w.workspaceWithChannel(t, ctx)
	configureTestPushEgress(w.store.Store)
	endpoint := "https://push.example.test/blocked-cleanup"
	session := testPushSession(strings.Repeat("E", 43))
	owner := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	if _, err := owner.SavePushSubscription(
		ctx, session, endpoint, testPushP256dh, testPushAuth,
	); err != nil {
		t.Fatal(err)
	}

	blocker, err := w.store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(context.Background()) }()
	if _, err := blocker.Exec(
		ctx, "SELECT pg_advisory_xact_lock(hashtext($1))", endpoint,
	); err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	w.store.CloseBrowserSession(session.ID)
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("blocked session cleanup held logout for %v", elapsed)
	}
	var remaining int
	if err := w.store.pool.QueryRow(ctx,
		"SELECT count(*) FROM push_subscriptions WHERE endpoint = $1", endpoint,
	).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 1 {
		t.Fatalf("timed-out cleanup partially mutated subscriptions: %d", remaining)
	}

	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	w.store.CloseBrowserSession(session.ID)
	if err := w.store.pool.QueryRow(ctx,
		"SELECT count(*) FROM push_subscriptions WHERE endpoint = $1", endpoint,
	).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("retry left %d subscriptions after the lock cleared", remaining)
	}
}

func TestPushReauthorizesAudienceAndBrowserSessionBeforeSend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, place := w.workspaceWithChannel(t, ctx)
	configureTestPushEgress(w.store.Store)
	scope := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB)
	_, err := scope.SavePushSubscription(
		ctx,
		testPushSession(strings.Repeat("C", 43)),
		"https://push.example.test/recipient",
		testPushP256dh,
		testPushAuth,
	)
	if err != nil {
		t.Fatal(err)
	}
	decision := []NotificationDecision{{Participant: w.humanB, Reason: NotifyReasonAll}}

	rejectedClient := &recordingPushClient{}
	rejected, err := NewPushDispatcher(
		ctx, w.store.Store, testPushSessionAuthorizer{allow: false}, "mailto:test@example.com",
	)
	if err != nil {
		t.Fatal(err)
	}
	rejected.client = rejectedClient
	rejected.deliver(ctx, scope.Scope, place, decision)
	if rejectedClient.count() != 0 {
		t.Fatal("retired browser session reached push transport")
	}

	if err := w.store.RemoveWorkspaceMember(ctx, workspace.WorkspaceID, w.humanB); err != nil {
		t.Fatal(err)
	}
	audienceClient := &recordingPushClient{}
	audience, err := NewPushDispatcher(
		ctx, w.store.Store, testPushSessionAuthorizer{allow: true}, "mailto:test@example.com",
	)
	if err != nil {
		t.Fatal(err)
	}
	audience.client = audienceClient
	audience.deliver(ctx, scope.Scope, place, decision)
	if audienceClient.count() != 0 {
		t.Fatal("removed audience member received a push")
	}
}

func TestPublishMessageCreatedDeliversPushWithoutWebSocketHub(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, place := w.workspaceWithChannel(t, ctx)
	configureTestPushEgress(w.store.Store)
	recipient := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB)
	if _, err := recipient.SavePushSubscription(
		ctx,
		testPushSession(strings.Repeat("D", 43)),
		"https://push.example.test/closed-tab",
		testPushP256dh,
		testPushAuth,
	); err != nil {
		t.Fatal(err)
	}

	client := &recordingPushClient{}
	dispatcher, err := NewPushDispatcher(
		ctx,
		w.store.Store,
		testPushSessionAuthorizer{allow: true},
		"mailto:test@example.com",
	)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.client = client
	w.store.UsePush(dispatcher)

	message := w.send(t, ctx, place.PlaceID, w.humanA, "closed tab delivery")
	sender := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	publishMessageCreated(ctx, sender, nil, place, message)

	deadline := time.Now().Add(5 * time.Second)
	for client.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if client.count() != 1 {
		t.Fatalf("hubless publish sent %d pushes, want one", client.count())
	}
}
