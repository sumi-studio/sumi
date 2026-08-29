package messaging

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

type pushClientFunc func(*http.Request) (*http.Response, error)

func (f pushClientFunc) Do(request *http.Request) (*http.Response, error) {
	return f(request)
}

type blockingPushClient struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockingPushClient) Do(request *http.Request) (*http.Response, error) {
	c.once.Do(func() { close(c.started) })
	select {
	case <-c.release:
		return &http.Response{
			StatusCode: http.StatusCreated,
			Body:       io.NopCloser(strings.NewReader("")),
		}, nil
	case <-request.Context().Done():
		return nil, request.Context().Err()
	}
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

func notificationDecisionFor(
	t *testing.T,
	ctx context.Context,
	store *ScopedStore,
	messageID string,
	recipient ParticipantRef,
) NotificationDecision {
	t.Helper()
	decisions, err := store.NotificationIntentsForMessage(ctx, messageID)
	if err != nil {
		t.Fatalf("load notification intents: %v", err)
	}
	for _, decision := range decisions {
		if decision.Participant == recipient {
			return decision
		}
	}
	t.Fatalf("notification intent for %s was not found", recipient.Key())
	return NotificationDecision{}
}

func pushDeliveryFor(
	scope Scope,
	place Place,
	decision NotificationDecision,
	subscription PushSubscription,
) pushDelivery {
	return pushDelivery{
		subscription:      subscription,
		payload:           []byte(`{"workspace_id":"test"}`),
		workspaceID:       scope.WorkspaceID,
		installationID:    scope.InstallationID,
		authorityEpoch:    scope.AuthorityEpoch,
		placeID:           place.PlaceID,
		placeKind:         place.Kind,
		workspaceMemberID: decision.workspaceMemberID,
		placeMemberID:     decision.placeMemberID,
	}
}

func TestNormalizeVAPIDSubjectRejectsInvalidHTTPSContacts(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "email", input: "ops@example.com", want: "ops@example.com"},
		{name: "mailto", input: "mailto:ops@example.com", want: "ops@example.com"},
		{name: "https", input: "https://example.com/contact", want: "https://example.com/contact"},
		{name: "empty host", input: "https://", wantErr: true},
		{name: "invalid host", input: "https://bad host/contact", wantErr: true},
		{name: "userinfo", input: "https://user@example.com/contact", wantErr: true},
		{name: "fragment", input: "https://example.com/contact#private", wantErr: true},
		{name: "http", input: "http://example.com/contact", wantErr: true},
		{name: "not email", input: "operator", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := normalizeVAPIDSubject(test.input)
			if test.wantErr {
				if err == nil {
					t.Fatalf("normalizeVAPIDSubject(%q) = %q, want error", test.input, got)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("normalizeVAPIDSubject(%q) = %q, %v; want %q", test.input, got, err, test.want)
			}
		})
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
	workspace, place := w.workspaceWithChannel(t, ctx)
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
	stale.send(ctx, pushDeliveryFor(first.Scope, place, NotificationDecision{
		Participant:       w.humanA,
		workspaceMemberID: activeMembershipID(t, ctx, w, workspace.WorkspaceID, w.humanA),
	}, saved))
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

func TestPushSubscriptionSavePurgesExpiredRowsAndRecoversEndpoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, _ := w.workspaceWithChannel(t, ctx)
	configureTestPushEgress(w.store.Store)
	owner := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB)
	oldEndpoints := []string{
		"https://push.example.test/expired-one",
		"https://push.example.test/expired-two",
		"https://push.example.test/reused",
	}
	for index, endpoint := range oldEndpoints {
		sessionID := strings.Repeat(string(rune('F'+index)), 43)
		if _, err := owner.SavePushSubscription(
			ctx, testPushSession(sessionID), endpoint, testPushP256dh, testPushAuth,
		); err != nil {
			t.Fatalf("save old subscription %d: %v", index, err)
		}
	}
	if _, err := w.store.pool.Exec(ctx, `
		UPDATE push_subscriptions
		SET session_expires_at = now() - interval '1 minute'
		WHERE human_id = $1`, w.humanB.ID); err != nil {
		t.Fatalf("expire subscriptions: %v", err)
	}

	freshSession := testPushSession(strings.Repeat("N", 43))
	if _, err := owner.SavePushSubscription(
		ctx,
		freshSession,
		oldEndpoints[2],
		"rotated-p256dh",
		"rotated-auth",
	); err != nil {
		t.Fatalf("save after natural session expiry: %v", err)
	}
	var count int
	var sessionID, p256dh, auth string
	if err := w.store.pool.QueryRow(ctx, `
		SELECT count(*), min(browser_session_id), min(p256dh), min(auth)
		FROM push_subscriptions WHERE human_id = $1`, w.humanB.ID).Scan(
		&count, &sessionID, &p256dh, &auth,
	); err != nil {
		t.Fatal(err)
	}
	if count != 1 || sessionID != freshSession.ID ||
		p256dh != "rotated-p256dh" || auth != "rotated-auth" {
		t.Fatalf("post-expiry subscriptions = count %d session %q keys %q/%q", count, sessionID, p256dh, auth)
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
	decision := []NotificationDecision{{
		Participant:       w.humanB,
		Reason:            NotifyReasonAll,
		workspaceMemberID: activeMembershipID(t, ctx, w, workspace.WorkspaceID, w.humanB),
	}}

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

func TestPushIntentCannotCrossWorkspaceRejoinTenure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, place := w.workspaceWithChannel(t, ctx)
	configureTestPushEgress(w.store.Store)
	recipient := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB)
	if _, err := recipient.SavePushSubscription(
		ctx,
		testPushSession(strings.Repeat("R", 43)),
		"https://push.example.test/rejoin",
		testPushP256dh,
		testPushAuth,
	); err != nil {
		t.Fatal(err)
	}
	message := w.send(t, ctx, place.PlaceID, w.humanA, "old tenure intent")
	sender := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	decision := notificationDecisionFor(t, ctx, sender, message.MessageID, w.humanB)
	oldTenure := decision.workspaceMemberID
	if oldTenure == "" || decision.placeMemberID != "" {
		t.Fatalf("channel intent tenure = workspace %q place %q", oldTenure, decision.placeMemberID)
	}

	if err := w.store.RemoveWorkspaceMember(ctx, workspace.WorkspaceID, w.humanB); err != nil {
		t.Fatal(err)
	}
	if err := w.store.AddWorkspaceMember(ctx, workspace.WorkspaceID, w.humanB, RoleMember); err != nil {
		t.Fatal(err)
	}
	newTenure := activeMembershipID(t, ctx, w, workspace.WorkspaceID, w.humanB)
	if newTenure == oldTenure {
		t.Fatal("workspace rejoin reused the old membership tenure")
	}

	client := &recordingPushClient{}
	dispatcher, err := NewPushDispatcher(
		ctx, w.store.Store, testPushSessionAuthorizer{allow: true}, "mailto:test@example.com",
	)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.client = client
	currentSender := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	dispatcher.deliver(ctx, currentSender.Scope, place, []NotificationDecision{decision})
	if client.count() != 0 {
		t.Fatal("old notification intent crossed into the recipient's new Workspace tenure")
	}
}

func TestPrivatePushIntentPinsBothMembershipTenures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, _ := w.workspaceWithChannel(t, ctx)
	place, _, err := w.store.EnsureDM(ctx, w.humanA, w.humanB)
	if err != nil {
		t.Fatal(err)
	}
	message := w.send(t, ctx, place.PlaceID, w.humanA, "private tenure intent")
	sender := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	decision := notificationDecisionFor(t, ctx, sender, message.MessageID, w.humanB)
	if want := activeMembershipID(t, ctx, w, workspace.WorkspaceID, w.humanB); decision.workspaceMemberID != want {
		t.Fatalf("intent workspace tenure = %q, want %q", decision.workspaceMemberID, want)
	}
	if want := activePlaceMembershipID(t, ctx, w, place.PlaceID, w.humanB); decision.placeMemberID != want {
		t.Fatalf("intent place tenure = %q, want %q", decision.placeMemberID, want)
	}
}

func TestPushSendLeaseFencesConcurrentWorkspaceRemoval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, place := w.workspaceWithChannel(t, ctx)
	configureTestPushEgress(w.store.Store)
	recipient := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB)
	if _, err := recipient.SavePushSubscription(
		ctx,
		testPushSession(strings.Repeat("L", 43)),
		"https://push.example.test/lease",
		testPushP256dh,
		testPushAuth,
	); err != nil {
		t.Fatal(err)
	}
	message := w.send(t, ctx, place.PlaceID, w.humanA, "lease ordering")
	sender := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	decision := notificationDecisionFor(t, ctx, sender, message.MessageID, w.humanB)

	client := &blockingPushClient{started: make(chan struct{}), release: make(chan struct{})}
	dispatcher, err := NewPushDispatcher(
		ctx, w.store.Store, testPushSessionAuthorizer{allow: true}, "mailto:test@example.com",
	)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.client = client
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(client.release) }) }
	defer release()
	deliveryDone := make(chan struct{})
	go func() {
		dispatcher.deliver(ctx, sender.Scope, place, []NotificationDecision{decision})
		close(deliveryDone)
	}()
	select {
	case <-client.started:
	case <-ctx.Done():
		t.Fatal("push transport did not start")
	}

	removeDone := make(chan error, 1)
	go func() {
		removeDone <- w.store.RemoveWorkspaceMember(ctx, workspace.WorkspaceID, w.humanB)
	}()
	waitForWaitingBackend(t, ctx, w.store.pool)
	select {
	case err := <-removeDone:
		t.Fatalf("membership removal completed before the in-flight send released its tenure: %v", err)
	default:
	}
	release()
	select {
	case <-deliveryDone:
	case <-ctx.Done():
		t.Fatal("push delivery did not finish after transport release")
	}
	select {
	case err := <-removeDone:
		if err != nil {
			t.Fatalf("remove recipient after send: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("membership removal did not finish after push lease release")
	}
}

func TestPushSendLeaseRejectsDisabledAndReenabledInstallationEpoch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, place := w.workspaceWithChannel(t, ctx)
	configureTestPushEgress(w.store.Store)
	recipient := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB)
	subscription, err := recipient.SavePushSubscription(
		ctx,
		testPushSession(strings.Repeat("I", 43)),
		"https://push.example.test/installation-epoch",
		testPushP256dh,
		testPushAuth,
	)
	if err != nil {
		t.Fatal(err)
	}
	message := w.send(t, ctx, place.PlaceID, w.humanA, "installation epoch")
	sender := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	decision := notificationDecisionFor(t, ctx, sender, message.MessageID, w.humanB)
	delivery := pushDeliveryFor(sender.Scope, place, decision, subscription)

	dispatcher, err := NewPushDispatcher(
		ctx, w.store.Store, testPushSessionAuthorizer{allow: true}, "mailto:test@example.com",
	)
	if err != nil {
		t.Fatal(err)
	}
	client := &recordingPushClient{}
	dispatcher.client = client
	if _, err := w.apps.SetEnabledByID(
		ctx, sender.Scope.InstallationID, w.humanA, false,
	); err != nil {
		t.Fatalf("disable Messaging: %v", err)
	}
	dispatcher.send(ctx, delivery)
	if client.count() != 0 {
		t.Fatal("disabled Messaging installation reached push transport")
	}
	reenabled, err := w.apps.SetEnabledByID(
		ctx, sender.Scope.InstallationID, w.humanA, true,
	)
	if err != nil {
		t.Fatalf("re-enable Messaging: %v", err)
	}
	if reenabled.AuthorityEpoch == sender.Scope.AuthorityEpoch {
		t.Fatal("re-enable reused the prior authority epoch")
	}
	dispatcher.send(ctx, delivery)
	if client.count() != 0 {
		t.Fatal("old authority epoch reached push transport after re-enable")
	}
}

func TestPushSendLeaseFencesConcurrentInstallationDisable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, place := w.workspaceWithChannel(t, ctx)
	configureTestPushEgress(w.store.Store)
	recipient := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB)
	subscription, err := recipient.SavePushSubscription(
		ctx,
		testPushSession(strings.Repeat("J", 43)),
		"https://push.example.test/installation-lease",
		testPushP256dh,
		testPushAuth,
	)
	if err != nil {
		t.Fatal(err)
	}
	message := w.send(t, ctx, place.PlaceID, w.humanA, "installation lease")
	sender := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	decision := notificationDecisionFor(t, ctx, sender, message.MessageID, w.humanB)
	delivery := pushDeliveryFor(sender.Scope, place, decision, subscription)

	client := &blockingPushClient{started: make(chan struct{}), release: make(chan struct{})}
	dispatcher, err := NewPushDispatcher(
		ctx, w.store.Store, testPushSessionAuthorizer{allow: true}, "mailto:test@example.com",
	)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.client = client
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(client.release) }) }
	defer release()
	sendDone := make(chan struct{})
	go func() {
		dispatcher.send(ctx, delivery)
		close(sendDone)
	}()
	select {
	case <-client.started:
	case <-ctx.Done():
		t.Fatal("push transport did not start")
	}
	disableDone := make(chan error, 1)
	go func() {
		_, err := w.apps.SetEnabledByID(
			ctx, sender.Scope.InstallationID, w.humanA, false,
		)
		disableDone <- err
	}()
	waitForWaitingBackend(t, ctx, w.store.pool)
	select {
	case err := <-disableDone:
		t.Fatalf("installation disable completed before the in-flight send released its authority: %v", err)
	default:
	}
	release()
	select {
	case <-sendDone:
	case <-ctx.Done():
		t.Fatal("push send did not finish after transport release")
	}
	select {
	case err := <-disableDone:
		if err != nil {
			t.Fatalf("disable Messaging after send: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("installation disable did not finish after push authority release")
	}
}

func TestPushSendErrorsNeverLogEndpointOrTransportDetails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, place := w.workspaceWithChannel(t, ctx)
	configureTestPushEgress(w.store.Store)
	recipient := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB)
	endpoint := "https://push.example.test/leaky-endpoint-token"
	subscription, err := recipient.SavePushSubscription(
		ctx,
		testPushSession(strings.Repeat("S", 43)),
		endpoint,
		testPushP256dh,
		testPushAuth,
	)
	if err != nil {
		t.Fatal(err)
	}
	message := w.send(t, ctx, place.PlaceID, w.humanA, "log privacy")
	sender := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	decision := notificationDecisionFor(t, ctx, sender, message.MessageID, w.humanB)
	delivery := pushDeliveryFor(sender.Scope, place, decision, subscription)

	failures := []error{
		&url.Error{Op: "Post", URL: endpoint, Err: context.DeadlineExceeded},
		&url.Error{Op: "Post", URL: endpoint, Err: &net.DNSError{Err: "secret dns detail", Name: "secret-dns.example"}},
		&url.Error{Op: "Post", URL: endpoint, Err: x509.UnknownAuthorityError{Cert: &x509.Certificate{RawSubject: []byte("secret tls detail")}}},
		&url.Error{Op: "Post", URL: endpoint, Err: errors.New("secret transport detail")},
	}
	index := 0
	dispatcher, err := NewPushDispatcher(
		ctx, w.store.Store, testPushSessionAuthorizer{allow: true}, "mailto:test@example.com",
	)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.client = pushClientFunc(func(*http.Request) (*http.Response, error) {
		failure := failures[index]
		index++
		return nil, failure
	})

	var output bytes.Buffer
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	log.SetOutput(&output)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
	})
	for range failures {
		dispatcher.send(ctx, delivery)
	}
	got := output.String()
	for _, reason := range []string{"timeout", "dns", "tls", "transport"} {
		if !strings.Contains(got, "send failed ("+reason+")") {
			t.Fatalf("safe reason %q missing from logs: %q", reason, got)
		}
	}
	for _, secret := range []string{
		endpoint,
		"leaky-endpoint-token",
		"secret dns detail",
		"secret-dns.example",
		"secret tls detail",
		"secret transport detail",
	} {
		if strings.Contains(got, secret) {
			t.Fatalf("push log exposed %q: %q", secret, got)
		}
	}
}

func TestPushMutationRequiresApplicationJSON(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, _ := w.workspaceWithChannel(t, ctx)
	scope := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA).Scope
	server := NewServer(w.store.core, stubSessions{})
	server.AllowedOrigins = []string{testOrigin}
	server.Push = &PushDispatcher{}
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	testServer := httptest.NewServer(mux)
	defer testServer.Close()
	query := url.Values{
		"workspace_id":    {scope.WorkspaceID},
		"installation_id": {scope.InstallationID},
		"authority_epoch": {strconv.FormatInt(scope.AuthorityEpoch, 10)},
	}.Encode()

	for _, method := range []string{http.MethodPost, http.MethodDelete} {
		for _, contentType := range []string{"", "text/plain"} {
			request, err := http.NewRequest(
				method,
				testServer.URL+"/messaging/push-subscriptions?"+query,
				strings.NewReader(`{}`),
			)
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Origin", testOrigin)
			if contentType != "" {
				request.Header.Set("Content-Type", contentType)
			}
			request.AddCookie(&http.Cookie{
				Name:  agentevents.BrowserSessionCookie,
				Value: w.humanA.ID,
			})
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			_ = response.Body.Close()
			if response.StatusCode != http.StatusUnsupportedMediaType {
				t.Fatalf("%s Content-Type %q = %d, want 415", method, contentType, response.StatusCode)
			}
		}
	}
}

func TestPushFanoutRoundRobinDoesNotLetEightSlowDevicesStarveAnotherHuman(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	slowHuman := Human("slow-human")
	fastHuman := Human("fast-human")
	humans := []NotificationDecision{
		{Participant: slowHuman, workspaceMemberID: "slow-tenure"},
		{Participant: fastHuman, workspaceMemberID: "fast-tenure"},
	}
	subscriptions := map[string][]PushSubscription{
		slowHuman.Key(): make([]PushSubscription, maxPushSubscriptionsPerHuman),
		fastHuman.Key(): {{Endpoint: "fast"}},
	}
	for index := range subscriptions[slowHuman.Key()] {
		subscriptions[slowHuman.Key()][index].Endpoint = "slow-" + strconv.Itoa(index)
	}
	deliveries := pushDeliveriesRoundRobin(
		Scope{WorkspaceID: "workspace"},
		Place{PlaceID: "place", Kind: PlaceChannel},
		humans,
		subscriptions,
		[]byte(`{}`),
	)
	if len(deliveries) != maxPushSubscriptionsPerHuman+1 ||
		deliveries[1].subscription.Endpoint != "fast" {
		t.Fatalf("round-robin delivery order = %+v", deliveries)
	}

	fastSent := make(chan struct{})
	var fastOnce sync.Once
	var active atomic.Int32
	var maximum atomic.Int32
	var slowStarted atomic.Int32
	var slowTimedOut atomic.Int32
	done := make(chan struct{})
	go func() {
		runBoundedPushFanout(ctx, deliveries, 100*time.Millisecond, func(sendCtx context.Context, delivery pushDelivery) {
			current := active.Add(1)
			defer active.Add(-1)
			for {
				previous := maximum.Load()
				if current <= previous || maximum.CompareAndSwap(previous, current) {
					break
				}
			}
			if strings.HasPrefix(delivery.subscription.Endpoint, "slow-") {
				slowStarted.Add(1)
				<-sendCtx.Done()
				if errors.Is(sendCtx.Err(), context.DeadlineExceeded) {
					slowTimedOut.Add(1)
				}
				return
			}
			fastOnce.Do(func() { close(fastSent) })
		})
		close(done)
	}()
	select {
	case <-fastSent:
	case <-ctx.Done():
		t.Fatal("another Human's fast endpoint was starved by eight slow devices")
	}
	if got := slowTimedOut.Load(); got != 0 {
		t.Fatalf("fast endpoint started only after %d slow timeouts", got)
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("bounded fanout did not converge")
	}
	if got := slowStarted.Load(); got != maxPushSubscriptionsPerHuman {
		t.Fatalf("slow endpoints started = %d, want %d", got, maxPushSubscriptionsPerHuman)
	}
	if got := slowTimedOut.Load(); got != maxPushSubscriptionsPerHuman {
		t.Fatalf("slow endpoints timed out = %d, want %d", got, maxPushSubscriptionsPerHuman)
	}
	if got := maximum.Load(); got > pushFanoutConcurrency {
		t.Fatalf("maximum concurrency = %d, cap %d", got, pushFanoutConcurrency)
	}
}

func TestBoundedPushFanoutCapsConcurrencyAndHonorsGlobalCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	deliveries := make([]pushDelivery, pushFanoutConcurrency+3)
	started := make(chan struct{}, len(deliveries))
	var active atomic.Int32
	var maximum atomic.Int32
	done := make(chan struct{})
	go func() {
		runBoundedPushFanout(ctx, deliveries, time.Hour, func(sendCtx context.Context, _ pushDelivery) {
			current := active.Add(1)
			for {
				previous := maximum.Load()
				if current <= previous || maximum.CompareAndSwap(previous, current) {
					break
				}
			}
			started <- struct{}{}
			<-sendCtx.Done()
			active.Add(-1)
		})
		close(done)
	}()
	for range pushFanoutConcurrency {
		select {
		case <-started:
		case <-time.After(time.Second):
			cancel()
			t.Fatal("fanout did not fill its fixed worker pool")
		}
	}
	select {
	case <-started:
		cancel()
		t.Fatal("fanout started more than its fixed concurrency cap")
	case <-time.After(50 * time.Millisecond):
	}
	if got := maximum.Load(); got != pushFanoutConcurrency {
		cancel()
		t.Fatalf("maximum concurrency = %d, want %d", got, pushFanoutConcurrency)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("global cancellation did not stop bounded fanout")
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
