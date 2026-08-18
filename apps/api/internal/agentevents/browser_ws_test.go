package agentevents

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sumi-studio/sumi/apps/api/internal/directchat"
)

type dispositionBeforeAppendReturn struct {
	gateway *DurableGateway
	claims  TokenClaims
}

type countingDirectChatSpawner struct {
	mu             sync.Mutex
	ensureTargets  []string
	ensureContexts []context.Context
	touchTargets   []string
}

type blockingDirectChatSpawner struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type dialBrowserResult struct {
	conn     *websocket.Conn
	response *http.Response
	err      error
}

func (s *blockingDirectChatSpawner) EnsureRunning(ctx context.Context, _ string) error {
	s.once.Do(func() { close(s.started) })
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (*blockingDirectChatSpawner) Touch(string) {}

func (s *countingDirectChatSpawner) EnsureRunning(ctx context.Context, agentID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureTargets = append(s.ensureTargets, agentID)
	s.ensureContexts = append(s.ensureContexts, ctx)
	return nil
}

func (s *countingDirectChatSpawner) Touch(agentID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.touchTargets = append(s.touchTargets, agentID)
}

func (s *countingDirectChatSpawner) snapshot() (
	ensureTargets []string,
	ensureContexts []context.Context,
	touchTargets []string,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.ensureTargets...),
		append([]context.Context(nil), s.ensureContexts...),
		append([]string(nil), s.touchTargets...)
}

type blockingAdmissionSessionAuthorizer struct {
	claims           UserSessionClaims
	authorizeStarted chan struct{}
	release          chan struct{}
	mu               sync.Mutex
	revoked          bool
}

func (s *blockingAdmissionSessionAuthorizer) VerifySession(
	context.Context,
	string,
) (UserSessionClaims, error) {
	return s.claims, nil
}

func (s *blockingAdmissionSessionAuthorizer) AuthorizeSession(
	ctx context.Context,
	_ UserSessionClaims,
	operation func() error,
) error {
	close(s.authorizeStarted)
	select {
	case <-s.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	s.mu.Lock()
	revoked := s.revoked
	s.mu.Unlock()
	if revoked {
		return errors.New("browser session revoked during admission")
	}
	return operation()
}

func (s *blockingAdmissionSessionAuthorizer) revoke() {
	s.mu.Lock()
	s.revoked = true
	s.mu.Unlock()
}

type mutableDirectChatAuthorizer struct {
	mu             sync.RWMutex
	allowed        bool
	installationID string
	authorityEpoch int64
}

type coordinatedDirectChatAuthorizer struct {
	mu      sync.Mutex
	current bool
}

func newCoordinatedDirectChatAuthorizer() *coordinatedDirectChatAuthorizer {
	return &coordinatedDirectChatAuthorizer{current: true}
}

func (a *coordinatedDirectChatAuthorizer) AuthorizeDirectChat(
	_ context.Context,
	_ string,
	_ string,
	_ string,
	_ int64,
) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.current {
		return errors.New("human is not the current Employer")
	}
	return nil
}

func (a *coordinatedDirectChatAuthorizer) transfer(
	fence *directchat.LifecycleFence,
	started chan<- struct{},
) {
	close(started)
	release, err := fence.AcquireMutation(context.Background())
	if err != nil {
		return
	}
	defer release()
	a.mu.Lock()
	a.current = false
	a.mu.Unlock()
}

type blockingCommandAppender struct {
	gateway   *DurableGateway
	started   chan struct{}
	release   chan struct{}
	completed chan struct{}
	once      sync.Once
}

func (a *blockingCommandAppender) Append(
	ctx context.Context,
	provenance DirectChatProvenance,
	idempotencyKey string,
	command json.RawMessage,
) (CommandEnvelope, error) {
	a.once.Do(func() { close(a.started) })
	select {
	case <-a.release:
	case <-ctx.Done():
		return CommandEnvelope{}, ctx.Err()
	}
	if a.completed != nil {
		defer close(a.completed)
	}
	if a.gateway == nil {
		return CommandEnvelope{
			Seq:                1,
			CommandID:          "00000000-0000-4000-8000-000000000001",
			PersonalityAgentID: provenance.PersonalityAgentID,
			Provenance:         provenance,
			Command:            command,
		}, nil
	}
	return a.gateway.Append(ctx, provenance, idempotencyKey, command)
}

func (a *mutableDirectChatAuthorizer) AuthorizeDirectChat(
	_ context.Context,
	_ string,
	_ string,
	installationID string,
	authorityEpoch int64,
) error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if !a.allowed ||
		(a.installationID != "" && a.installationID != installationID) ||
		(a.authorityEpoch != 0 && a.authorityEpoch != authorityEpoch) {
		return ErrDirectChatAuthorizationDenied
	}
	return nil
}

func (a *mutableDirectChatAuthorizer) setInstallationID(installationID string) {
	a.mu.Lock()
	a.installationID = installationID
	a.mu.Unlock()
}

func (a *mutableDirectChatAuthorizer) setAuthorityEpoch(authorityEpoch int64) {
	a.mu.Lock()
	a.authorityEpoch = authorityEpoch
	a.mu.Unlock()
}

func (a *mutableDirectChatAuthorizer) setAllowed(allowed bool) {
	a.mu.Lock()
	a.allowed = allowed
	a.mu.Unlock()
}

func (a dispositionBeforeAppendReturn) Append(
	ctx context.Context,
	provenance DirectChatProvenance,
	idempotencyKey string,
	command json.RawMessage,
) (CommandEnvelope, error) {
	envelope, err := a.gateway.Append(ctx, provenance, idempotencyKey, command)
	if err != nil {
		return CommandEnvelope{}, err
	}
	seq := uint64(1)
	disposition, err := json.Marshal(map[string]any{
		"type":        "command_disposition",
		"command_id":  envelope.CommandID,
		"command_seq": envelope.Seq,
		"status":      "applied",
	})
	if err != nil {
		return CommandEnvelope{}, err
	}
	if err := a.gateway.Receive(ctx, a.claims, Envelope{
		Seq:                &seq,
		PersonalityAgentID: envelope.PersonalityAgentID,
		Event:              disposition,
	}); err != nil {
		return CommandEnvelope{}, err
	}
	// Give the event pump time to reach its serialized socket write. The
	// receipt transaction must still keep this newly-created disposition from
	// overtaking command_accepted on the same connection.
	time.Sleep(25 * time.Millisecond)
	return envelope, nil
}

func TestBrowserWebSocketAdmitsCommandsAndStreamsDurableAndVolatileEvents(t *testing.T) {
	gateway := openRuntimeGateway(t)
	sessions, err := NewHMACUserSessionVerifier(testSecret, "", newTestBrowserSessionRevocationStore())
	if err != nil {
		t.Fatal(err)
	}
	server := newAuthorizedBrowserServer(sessions, gateway, gateway)
	server.AllowedOrigins = []string{"https://web.example"}
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	claims := userSessionWireClaims{TenantID: "tenant-1", UserID: "user-1", PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Exp: time.Now().Add(time.Hour).Unix(), Aud: defaultBrowserAudience}
	conn := dialBrowserWS(t, httpServer, signBrowserSession(t, testSecret, claims), "018f47a2-9b3c-7def-8abc-0123456789ab")
	defer conn.Close()
	if err := conn.WriteJSON(browserHello{Type: "hello", LastEventSeq: 0}); err != nil {
		t.Fatal(err)
	}
	assertDirectChatStatus(t, conn, "unavailable")
	receipt := "hydrated-1"
	if err := gateway.PublishRuntimeState(claims.PersonalityAgentID, 7, &receipt); err != nil {
		t.Fatalf("publish authoritative ready state: %v", err)
	}
	assertDirectChatStatus(t, conn, "ready")

	seq := uint64(1)
	agentClaims := TokenClaims{TenantID: "tenant-1", PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Generation: 7}
	// Drive the abort guard from the durable run lifecycle, not internal map
	// mutation.
	if err := gateway.Receive(context.Background(), agentClaims, Envelope{Seq: &seq, PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Event: json.RawMessage(`{"type":"agent_start"}`)}); err != nil {
		t.Fatalf("persist durable agent_start: %v", err)
	}
	if replay, err := gateway.EventCatchUp(context.Background(), "018f47a2-9b3c-7def-8abc-0123456789ab", 0); err != nil || len(replay) != 1 {
		t.Fatalf("read durable event for browser replay: events=%d err=%v", len(replay), err)
	}
	assertBrowserEvent(t, conn, "agent_start", true)

	seq = 2
	if err := gateway.Receive(context.Background(), agentClaims, Envelope{Seq: &seq, PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Event: json.RawMessage(`{"type":"tool_execution_start","tool_call_id":"call-1","tool_name":"read_file","args":{}}`)}); err != nil {
		t.Fatalf("persist durable tool event: %v", err)
	}
	assertBrowserEvent(t, conn, "tool_execution_start", true)
	volatile := Envelope{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Event: json.RawMessage(`{"type":"message_update","message_id":"00000000-0000-4000-8000-000000000001","event":{"type":"text_delta","content_index":0,"delta":"stream"}}`)}
	if err := gateway.Receive(context.Background(), agentClaims, volatile); err != nil {
		t.Fatalf("publish volatile stream event: %v", err)
	}
	assertBrowserEvent(t, conn, "message_update", false)

	seq = 3
	if err := gateway.Receive(context.Background(), agentClaims, Envelope{Seq: &seq, PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Event: json.RawMessage(`{"type":"approval_requested","request":{"id":"request-1","tool_call_id":"call-1","tool_name":"read_file","action":{"reviewable":"read"},"args_summary":"read"}}`)}); err != nil {
		t.Fatalf("publish durable approval_requested: %v", err)
	}
	assertBrowserEvent(t, conn, "approval_requested", true)

	// The abort and approval_decision variants are only accepted when a run is
	// in flight and an approval is pending, respectively.
	for index, command := range []json.RawMessage{
		json.RawMessage(`{"type":"user_message","text":"steer me","attachments":[]}`),
		json.RawMessage(`{"type":"abort"}`),
		json.RawMessage(`{"type":"approval_decision","request_id":"request-1","decision":{"type":"approve_once"}}`),
	} {
		if err := conn.WriteJSON(browserCommandFrame{Type: "command", IdempotencyKey: fmt.Sprintf("idempotency-%d", index), Command: command}); err != nil {
			t.Fatal(err)
		}
		var accepted browserCommandAcceptedFrame
		conn.SetReadDeadline(time.Now().Add(time.Second))
		if err := conn.ReadJSON(&accepted); err != nil {
			t.Fatalf("read command admission: %v", err)
		}
		if accepted.Type != "command_accepted" ||
			accepted.IdempotencyKey != fmt.Sprintf("idempotency-%d", index) ||
			accepted.Seq == 0 ||
			accepted.CommandID == "" {
			t.Fatalf("unexpected command admission: %+v", accepted)
		}
	}

	// A changed authenticated record under an existing key is a correlated
	// terminal rejection; it must not close the socket into a retry loop.
	if err := conn.WriteJSON(browserCommandFrame{
		Type:           "command",
		IdempotencyKey: "idempotency-0",
		Command:        json.RawMessage(`{"type":"user_message","text":"changed","attachments":[]}`),
	}); err != nil {
		t.Fatal(err)
	}
	var conflict browserCommandRejectedFrame
	if err := conn.ReadJSON(&conflict); err != nil {
		t.Fatal(err)
	}
	if conflict.Type != "command_rejected" ||
		conflict.IdempotencyKey != "idempotency-0" ||
		conflict.RejectReason != RejectIdempotencyConflict {
		t.Fatalf("unexpected idempotency conflict frame: %+v", conflict)
	}
	if err := conn.WriteJSON(browserCommandFrame{
		Type:           "command",
		IdempotencyKey: "after-conflict",
		Command:        json.RawMessage(`{"type":"user_message","text":"still open","attachments":[]}`),
	}); err != nil {
		t.Fatal(err)
	}
	var accepted browserCommandAcceptedFrame
	if err := conn.ReadJSON(&accepted); err != nil {
		t.Fatal(err)
	}
	if accepted.Type != "command_accepted" || accepted.IdempotencyKey != "after-conflict" {
		t.Fatalf("socket did not continue after conflict: %+v", accepted)
	}
}

func TestBrowserWebSocketRequiresExactInstallationScopeAndBoundAuthorizer(t *testing.T) {
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	gateway := openRuntimeGateway(t)
	sessions, err := NewHMACUserSessionVerifier(
		testSecret,
		"",
		newTestBrowserSessionRevocationStore(),
	)
	if err != nil {
		t.Fatal(err)
	}
	session := signBrowserSession(t, testSecret, userSessionWireClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: personalityAgentID,
		Exp:                time.Now().Add(time.Hour).Unix(),
		Aud:                defaultBrowserAudience,
	})

	for _, testCase := range []struct {
		name       string
		query      string
		authorizer DirectChatAuthorizer
		wantStatus int
	}{
		{name: "missing", authorizer: allowDirectChatAuthorizer{}, wantStatus: http.StatusBadRequest},
		{name: "empty installation", query: "?installation_id=&authority_epoch=1", authorizer: allowDirectChatAuthorizer{}, wantStatus: http.StatusBadRequest},
		{name: "malformed installation", query: "?installation_id=not-a-uuid&authority_epoch=1", authorizer: allowDirectChatAuthorizer{}, wantStatus: http.StatusBadRequest},
		{name: "wrong-version installation", query: "?installation_id=018f47a2-9b3c-4def-8abc-0123456789ab&authority_epoch=1", authorizer: allowDirectChatAuthorizer{}, wantStatus: http.StatusBadRequest},
		{name: "wrong-variant installation", query: "?installation_id=018f47a2-9b3c-7def-0abc-0123456789ab&authority_epoch=1", authorizer: allowDirectChatAuthorizer{}, wantStatus: http.StatusBadRequest},
		{name: "duplicate installation", query: "?installation_id=" + testDirectChatInstallationID + "&installation_id=" + testDirectChatInstallationID + "&authority_epoch=1", authorizer: allowDirectChatAuthorizer{}, wantStatus: http.StatusBadRequest},
		{name: "missing epoch", query: "?installation_id=" + testDirectChatInstallationID, authorizer: allowDirectChatAuthorizer{}, wantStatus: http.StatusBadRequest},
		{name: "duplicate epoch", query: "?installation_id=" + testDirectChatInstallationID + "&authority_epoch=1&authority_epoch=1", authorizer: allowDirectChatAuthorizer{}, wantStatus: http.StatusBadRequest},
		{name: "stale epoch", query: "?installation_id=" + testDirectChatInstallationID + "&authority_epoch=2", authorizer: allowDirectChatAuthorizer{}, wantStatus: http.StatusForbidden},
		{name: "wrong", query: "?installation_id=0198f0f4-9b72-7000-8000-000000000099&authority_epoch=1", authorizer: allowDirectChatAuthorizer{}, wantStatus: http.StatusForbidden},
		{name: "unbound", query: "?installation_id=" + testDirectChatInstallationID + "&authority_epoch=1", wantStatus: http.StatusServiceUnavailable},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			browser := NewBrowserServer(sessions, gateway, gateway)
			browser.SetLifecycleFence(directchat.NewLifecycleFence())
			browser.AllowedOrigins = []string{browserAuthTestOrigin}
			browser.Authorizer = testCase.authorizer
			server := httptest.NewServer(browser)
			defer server.Close()
			request, err := http.NewRequest(
				http.MethodGet,
				server.URL+"/direct-chat/ws"+testCase.query,
				nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Origin", browserAuthTestOrigin)
			request.AddCookie(&http.Cookie{Name: BrowserSessionCookie, Value: session})
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != testCase.wantStatus {
				t.Fatalf("status=%d, want %d", response.StatusCode, testCase.wantStatus)
			}
			if testCase.wantStatus == http.StatusBadRequest {
				body, err := io.ReadAll(response.Body)
				if err != nil {
					t.Fatal(err)
				}
				if string(body) != "invalid_scope" {
					t.Fatalf("invalid scope body = %q", body)
				}
			}
			if stats := browser.ConnectionStats(); stats != (BrowserConnectionStats{}) {
				t.Fatalf("rejected scope registered connection: %+v", stats)
			}
		})
	}
}

func TestBrowserWebSocketFirstAdmissionPrecedesItsRacingDisposition(t *testing.T) {
	gateway := openRuntimeGateway(t)
	gateway.PollInterval = time.Millisecond
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	const generation = uint64(7)
	receipt := "hydrated-racing-disposition"
	if err := gateway.PublishRuntimeState(personalityAgentID, generation, &receipt); err != nil {
		t.Fatal(err)
	}
	agentClaims := TokenClaims{
		TenantID:           "tenant-1",
		PersonalityAgentID: personalityAgentID,
		Generation:         generation,
	}
	sessions, err := NewHMACUserSessionVerifier(testSecret, "", newTestBrowserSessionRevocationStore())
	if err != nil {
		t.Fatal(err)
	}
	server := newAuthorizedBrowserServer(
		sessions,
		dispositionBeforeAppendReturn{gateway: gateway, claims: agentClaims},
		gateway,
	)
	server.AllowedOrigins = []string{"https://web.example"}
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	sessionClaims := userSessionWireClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: personalityAgentID,
		Exp:                time.Now().Add(time.Hour).Unix(),
		Aud:                defaultBrowserAudience,
	}
	conn := dialBrowserWS(
		t,
		httpServer,
		signBrowserSession(t, testSecret, sessionClaims),
		personalityAgentID,
	)
	defer conn.Close()
	if err := conn.WriteJSON(browserHello{Type: "hello", LastEventSeq: 0}); err != nil {
		t.Fatal(err)
	}
	assertDirectChatStatus(t, conn, "ready")
	if err := conn.WriteJSON(browserCommandFrame{
		Type:           "command",
		IdempotencyKey: "racing-disposition",
		Command:        json.RawMessage(`{"type":"user_message","text":"race","attachments":[]}`),
	}); err != nil {
		t.Fatal(err)
	}

	var accepted browserCommandAcceptedFrame
	if err := conn.ReadJSON(&accepted); err != nil {
		t.Fatal(err)
	}
	if accepted.Type != "command_accepted" {
		t.Fatalf("racing terminal disposition overtook admission: %+v", accepted)
	}
	assertBrowserEvent(t, conn, "command_disposition", true)
}

func TestBrowserWebSocketIdempotentAcceptanceCarriesAuthoritativeDispositionAfterRestart(t *testing.T) {
	for _, status := range []string{"applied", "superseded", "rejected"} {
		t.Run(status, func(t *testing.T) {
			tmp := t.TempDir()
			storeDir := filepath.Join(tmp, "commands")
			runtimeDir := filepath.Join(tmp, "runtime")
			store, gateway, err := openGatewayAt(t, storeDir, runtimeDir)
			if err != nil {
				t.Fatal(err)
			}

			const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
			const generation = uint64(7)
			receipt := "hydrated-authoritative-disposition"
			if err := gateway.PublishRuntimeState(personalityAgentID, generation, &receipt); err != nil {
				t.Fatal(err)
			}
			claims := TokenClaims{
				TenantID:           "tenant-1",
				PersonalityAgentID: personalityAgentID,
				Generation:         generation,
			}
			provenance := testDirectChatProvenance(personalityAgentID)
			originalBody := json.RawMessage(`{"type":"user_message","text":"original","attachments":[]}`)
			original, err := gateway.Append(context.Background(), provenance, "lost-receipt-key", originalBody)
			if err != nil {
				t.Fatal(err)
			}
			unrelated := make([]CommandEnvelope, 0, 33)
			for index := 0; index < 33; index++ {
				body := json.RawMessage(fmt.Sprintf(
					`{"type":"user_message","text":"unrelated-%d","attachments":[]}`,
					index,
				))
				command, err := gateway.Append(
					context.Background(),
					provenance,
					fmt.Sprintf("unrelated-key-%d", index),
					body,
				)
				if err != nil {
					t.Fatal(err)
				}
				unrelated = append(unrelated, command)
			}

			eventSeq := uint64(1)
			originalDisposition := map[string]any{
				"type":        "command_disposition",
				"command_id":  original.CommandID,
				"command_seq": original.Seq,
				"status":      status,
			}
			if status == "rejected" {
				originalDisposition["reject_reason"] = "not_allowed"
			}
			rawDisposition, err := json.Marshal(originalDisposition)
			if err != nil {
				t.Fatal(err)
			}
			if err := gateway.Receive(context.Background(), claims, Envelope{
				Seq:                &eventSeq,
				PersonalityAgentID: personalityAgentID,
				Event:              rawDisposition,
			}); err != nil {
				t.Fatal(err)
			}
			for _, command := range unrelated {
				eventSeq++
				raw, err := json.Marshal(map[string]any{
					"type":        "command_disposition",
					"command_id":  command.CommandID,
					"command_seq": command.Seq,
					"status":      "applied",
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := gateway.Receive(context.Background(), claims, Envelope{
					Seq:                &eventSeq,
					PersonalityAgentID: personalityAgentID,
					Event:              raw,
				}); err != nil {
					t.Fatal(err)
				}
			}
			if eventSeq <= 32 {
				t.Fatalf("counterexample requires more than 32 later dispositions, got event tail %d", eventSeq)
			}

			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if err := gateway.runtimeDir.Close(); err != nil {
				t.Fatal(err)
			}
			store, gateway, err = openGatewayAt(t, storeDir, runtimeDir)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			defer gateway.runtimeDir.Close()

			sessions, err := NewHMACUserSessionVerifier(testSecret, "", newTestBrowserSessionRevocationStore())
			if err != nil {
				t.Fatal(err)
			}
			server := newAuthorizedBrowserServer(sessions, gateway, gateway)
			server.AllowedOrigins = []string{"https://web.example"}
			mux := http.NewServeMux()
			mux.Handle("GET /direct-chat/ws", server)
			httpServer := httptest.NewServer(mux)
			defer httpServer.Close()

			sessionClaims := userSessionWireClaims{
				TenantID:           "tenant-1",
				UserID:             "user-1",
				PersonalityAgentID: personalityAgentID,
				Exp:                time.Now().Add(time.Hour).Unix(),
				Aud:                defaultBrowserAudience,
			}
			conn := dialBrowserWS(
				t,
				httpServer,
				signBrowserSession(t, testSecret, sessionClaims),
				personalityAgentID,
			)
			defer conn.Close()
			if err := conn.WriteJSON(browserHello{Type: "hello", LastEventSeq: eventSeq}); err != nil {
				t.Fatal(err)
			}
			assertDirectChatStatus(t, conn, "ready")

			for attempt := 0; attempt < 2; attempt++ {
				if err := conn.WriteJSON(browserCommandFrame{
					Type:           "command",
					IdempotencyKey: "lost-receipt-key",
					Command:        originalBody,
				}); err != nil {
					t.Fatal(err)
				}
				var accepted browserCommandAcceptedFrame
				if err := conn.ReadJSON(&accepted); err != nil {
					t.Fatal(err)
				}
				if accepted.CommandID != original.CommandID ||
					accepted.Seq != original.Seq ||
					string(accepted.Disposition) != string(rawDisposition) {
					t.Fatalf("idempotent acceptance lost authoritative disposition: %+v", accepted)
				}
			}

			if err := conn.WriteJSON(browserCommandFrame{
				Type:           "command",
				IdempotencyKey: "no-terminal-key",
				Command:        json.RawMessage(`{"type":"user_message","text":"no terminal","attachments":[]}`),
			}); err != nil {
				t.Fatal(err)
			}
			var admitted browserCommandAcceptedFrame
			if err := conn.ReadJSON(&admitted); err != nil {
				t.Fatal(err)
			}
			if len(admitted.Disposition) != 0 {
				t.Fatalf("new admission unexpectedly carried disposition: %s", admitted.Disposition)
			}
		})
	}
}

func TestBrowserEventPumpCatchesUpDurableCommitBeforeQueuedVolatileEvent(t *testing.T) {
	injectedReceiveFailure := errors.New("injected receive failure")
	for _, testCase := range []struct {
		name         string
		receiveError error
	}{
		{name: "success"},
		{name: "receive failure", receiveError: injectedReceiveFailure},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			gateway := openRuntimeGateway(t)
			gateway.PollInterval = time.Hour
			const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
			claims := TokenClaims{
				TenantID:           "tenant-1",
				PersonalityAgentID: personalityAgentID,
				Generation:         7,
			}
			receipt := "ready"
			if err := gateway.PublishRuntimeState(personalityAgentID, claims.Generation, &receipt); err != nil {
				t.Fatal(err)
			}
			seq := uint64(1)
			if err := gateway.Receive(context.Background(), claims, Envelope{
				Seq:                &seq,
				PersonalityAgentID: personalityAgentID,
				Event:              json.RawMessage(`{"type":"agent_start"}`),
			}); err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			volatile := make(chan Envelope)
			appendResult := make(chan error, 1)
			var frames []browserEventFrame
			write := func(frame any) error {
				eventFrame, ok := frame.(browserEventFrame)
				if !ok {
					t.Fatalf("unexpected browser frame: %#v", frame)
				}
				frames = append(frames, eventFrame)
				if eventFrame.Envelope.Seq != nil && *eventFrame.Envelope.Seq == 1 {
					go func() {
						next := uint64(2)
						err := testCase.receiveError
						if err == nil {
							err = gateway.Receive(ctx, claims, Envelope{
								Seq:                &next,
								PersonalityAgentID: personalityAgentID,
								Event:              json.RawMessage(`{"type":"message_start","message_id":"00000000-0000-4000-8000-000000000001","message":{"role":"assistant","content":[],"model":"fixture","provider":"fixture","origin":{"provider_instance_id":"fixture","protocol":"open_ai_responses","model":"fixture"},"usage":{"input":0,"output":0,"cache_read":0,"cache_write":0,"reasoning":0,"total_tokens":0},"stop_reason":"stop","error_message":null,"provider_code":null,"interrupted":false,"timestamp":"2026-07-28T00:00:00Z"}}`),
							})
						}
						appendResult <- err
						if err != nil {
							cancel()
							return
						}
						select {
						case volatile <- Envelope{
							PersonalityAgentID: personalityAgentID,
							Event:              json.RawMessage(`{"type":"message_update","message_id":"00000000-0000-4000-8000-000000000001","event":{"type":"text_delta","content_index":0,"delta":"stream"}}`),
						}:
						case <-ctx.Done():
						}
					}()
				}
				if eventFrame.Envelope.Seq == nil {
					cancel()
				}
				return nil
			}

			server := &BrowserServer{Events: gateway}
			err := server.browserEventPump(ctx, personalityAgentID, 0, directChatReadiness{ready: true}, volatile, nil, write)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("browser event pump returned %v, want context cancellation", err)
			}
			appendErr := <-appendResult
			if testCase.receiveError != nil {
				if !errors.Is(appendErr, injectedReceiveFailure) {
					t.Fatalf("concurrent durable append returned %v, want injected receive failure", appendErr)
				}
				if len(frames) != 1 || frames[0].Envelope.Seq == nil || *frames[0].Envelope.Seq != 1 {
					t.Fatalf("browser event frames after receive failure = %+v, want only durable seq 1", frames)
				}
				return
			}
			if appendErr != nil {
				t.Fatalf("append durable event between catch-up and volatile delivery: %v", appendErr)
			}
			if len(frames) != 3 {
				t.Fatalf("browser event frames = %d, want durable seq 1, durable seq 2, volatile", len(frames))
			}
			if frames[0].Envelope.Seq == nil || *frames[0].Envelope.Seq != 1 ||
				frames[1].Envelope.Seq == nil || *frames[1].Envelope.Seq != 2 ||
				frames[2].Envelope.Seq != nil {
				t.Fatalf("browser event order = %+v, want durable seq 1, durable seq 2, volatile", frames)
			}
		})
	}
}

func TestBrowserWebSocketRejectsUnavailableWithoutDurableCommand(t *testing.T) {
	gateway := openRuntimeGateway(t)
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	if err := gateway.PublishRuntimeState(personalityAgentID, 7, nil); err != nil {
		t.Fatal(err)
	}
	sessions, err := NewHMACUserSessionVerifier(testSecret, "", newTestBrowserSessionRevocationStore())
	if err != nil {
		t.Fatal(err)
	}
	server := newAuthorizedBrowserServer(sessions, gateway, gateway)
	server.AllowedOrigins = []string{"https://web.example"}
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	claims := userSessionWireClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: personalityAgentID,
		Exp:                time.Now().Add(time.Hour).Unix(),
		Aud:                defaultBrowserAudience,
	}
	conn := dialBrowserWS(t, httpServer, signBrowserSession(t, testSecret, claims), personalityAgentID)
	defer conn.Close()
	if err := conn.WriteJSON(browserHello{Type: "hello", LastEventSeq: 0}); err != nil {
		t.Fatal(err)
	}
	assertDirectChatStatus(t, conn, "unavailable")

	command := browserCommandFrame{
		Type:           "command",
		IdempotencyKey: "unavailable-command",
		Command:        json.RawMessage(`{"type":"user_message","text":"not yet","attachments":[]}`),
	}
	if err := conn.WriteJSON(command); err != nil {
		t.Fatal(err)
	}
	var rejected browserCommandRejectedFrame
	if err := conn.ReadJSON(&rejected); err != nil {
		t.Fatal(err)
	}
	if rejected.Type != "command_rejected" ||
		rejected.IdempotencyKey != command.IdempotencyKey ||
		rejected.RejectReason != RejectUnavailable {
		t.Fatalf("unexpected unavailable rejection: %+v", rejected)
	}
	if hasCommands, err := gateway.commands.HasCommands(context.Background(), personalityAgentID); err != nil || hasCommands {
		t.Fatalf("NotReady browser command reached durable log: hasCommands=%v err=%v", hasCommands, err)
	}

	receipt := "browser-ready"
	if err := gateway.PublishRuntimeState(personalityAgentID, 7, &receipt); err != nil {
		t.Fatal(err)
	}
	assertDirectChatStatus(t, conn, "ready")
	if err := conn.WriteJSON(command); err != nil {
		t.Fatal(err)
	}
	var accepted browserCommandAcceptedFrame
	if err := conn.ReadJSON(&accepted); err != nil {
		t.Fatal(err)
	}
	if accepted.Type != "command_accepted" || accepted.IdempotencyKey != command.IdempotencyKey {
		t.Fatalf("Ready did not admit previously rejected command: %+v", accepted)
	}
}

func TestBrowserWebSocketRejectsMissingExpiredAndMalformedPersonalityAgentSessions(t *testing.T) {
	gateway := openRuntimeGateway(t)
	sessions, err := NewHMACUserSessionVerifier(testSecret, "", newTestBrowserSessionRevocationStore())
	if err != nil {
		t.Fatal(err)
	}
	server := newAuthorizedBrowserServer(sessions, gateway, gateway)
	server.AllowedOrigins = []string{"https://web.example"}
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	wsURL := strings.Replace(httpServer.URL, "http", "ws", 1) + "/direct-chat/ws?installation_id=" + testDirectChatInstallationID + "&authority_epoch=1"
	for _, test := range []struct {
		name   string
		cookie string
	}{
		{"missing", ""},
		{"expired", signBrowserSession(t, testSecret, userSessionWireClaims{TenantID: "tenant", UserID: "user", PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Exp: time.Now().Add(-time.Hour).Unix(), Aud: defaultBrowserAudience})},
		{"malformed-personality-agent", signBrowserSession(t, testSecret, userSessionWireClaims{TenantID: "tenant", UserID: "user", PersonalityAgentID: "other", Exp: time.Now().Add(time.Hour).Unix(), Aud: defaultBrowserAudience})},
	} {
		t.Run(test.name, func(t *testing.T) {
			header := http.Header{"Origin": {"https://web.example"}}
			if test.cookie != "" {
				header.Set("Cookie", BrowserSessionCookie+"="+test.cookie)
			}
			conn, response, err := websocket.DefaultDialer.Dial(wsURL, header)
			if conn != nil {
				conn.Close()
			}
			if err == nil || response == nil || (response.StatusCode != http.StatusUnauthorized && response.StatusCode != http.StatusForbidden) {
				t.Fatalf("expected session rejection, response=%v err=%v", response, err)
			}
		})
	}

	header := http.Header{
		"Origin": {"https://web.example"},
		"Cookie": {
			BrowserSessionCookie + "=one; " + BrowserSessionCookie + "=two",
		},
	}
	conn, response, err := websocket.DefaultDialer.Dial(wsURL, header)
	if conn != nil {
		conn.Close()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected duplicate cookie rejection, response=%v err=%v", response, err)
	}
}

func TestBrowserWebSocketRejectsOriginAndAuthorityBeforeRuntimeActivity(t *testing.T) {
	gateway := openRuntimeGateway(t)
	sessions, err := NewHMACUserSessionVerifier(
		testSecret,
		"",
		newTestBrowserSessionRevocationStore(),
	)
	if err != nil {
		t.Fatal(err)
	}
	spawner := &countingDirectChatSpawner{}
	server := newAuthorizedBrowserServer(sessions, gateway, gateway)
	server.AllowedOrigins = []string{browserAuthTestOrigin}
	server.Spawner = spawner
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	claims := UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: personalityAgentID,
	}
	validSession, err := sessions.IssueSession(context.Background(), claims, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	revokedSession, err := sessions.IssueSession(context.Background(), claims, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessions.RevokeSession(context.Background(), revokedSession); err != nil {
		t.Fatal(err)
	}

	wsURL := strings.Replace(httpServer.URL, "http", "ws", 1) + "/direct-chat/ws?installation_id=" + testDirectChatInstallationID + "&authority_epoch=1"
	for _, test := range []struct {
		name       string
		origin     string
		session    string
		wantStatus int
	}{
		{
			name:       "disallowed origin with valid session",
			origin:     "https://evil.example",
			session:    validSession,
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "invalid session",
			origin:     browserAuthTestOrigin,
			session:    "not-a-session",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "revoked session",
			origin:     browserAuthTestOrigin,
			session:    revokedSession,
			wantStatus: http.StatusUnauthorized,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			header := http.Header{
				"Origin": {test.origin},
				"Cookie": {BrowserSessionCookie + "=" + test.session},
			}
			conn, response, dialErr := websocket.DefaultDialer.Dial(wsURL, header)
			if conn != nil {
				conn.Close()
			}
			if response != nil && response.Body != nil {
				defer response.Body.Close()
			}
			if dialErr == nil || response == nil || response.StatusCode != test.wantStatus {
				t.Fatalf(
					"runtime-activity fence response=%v err=%v, want status %d",
					response,
					dialErr,
					test.wantStatus,
				)
			}
			ensures, _, touches := spawner.snapshot()
			if len(ensures) != 0 || len(touches) != 0 {
				t.Fatalf("rejected browser caused runtime activity: ensures=%v touches=%v", ensures, touches)
			}
		})
	}

	authorizer := &mutableDirectChatAuthorizer{}
	server.Authorizer = authorizer
	header := http.Header{
		"Origin": {browserAuthTestOrigin},
		"Cookie": {BrowserSessionCookie + "=" + validSession},
	}
	conn, response, dialErr := websocket.DefaultDialer.Dial(wsURL, header)
	if conn != nil {
		conn.Close()
	}
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	if dialErr == nil || response == nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("non-Employer response=%v err=%v, want 403", response, dialErr)
	}
	ensures, _, touches := spawner.snapshot()
	if len(ensures) != 0 || len(touches) != 0 {
		t.Fatalf("unauthorized browser caused runtime activity: ensures=%v touches=%v", ensures, touches)
	}
}

func TestBrowserWebSocketRevocationWinsAdmissionBeforeSpawn(t *testing.T) {
	gateway := openRuntimeGateway(t)
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	sessions := &blockingAdmissionSessionAuthorizer{
		claims: UserSessionClaims{
			TenantID:           "tenant-1",
			UserID:             "user-1",
			PersonalityAgentID: personalityAgentID,
			sessionID:          base64.RawURLEncoding.EncodeToString(make([]byte, browserSessionIDBytes)),
			expiresAt:          time.Now().Add(time.Hour),
		},
		authorizeStarted: make(chan struct{}),
		release:          make(chan struct{}),
	}
	spawner := &countingDirectChatSpawner{}
	server := newAuthorizedBrowserServer(sessions, gateway, gateway)
	server.AllowedOrigins = []string{browserAuthTestOrigin}
	server.Spawner = spawner
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	type dialResult struct {
		conn     *websocket.Conn
		response *http.Response
		err      error
	}
	result := make(chan dialResult, 1)
	go func() {
		wsURL := strings.Replace(httpServer.URL, "http", "ws", 1) + "/direct-chat/ws?installation_id=" + testDirectChatInstallationID + "&authority_epoch=1"
		header := http.Header{
			"Origin": {browserAuthTestOrigin},
			"Cookie": {BrowserSessionCookie + "=verified-before-race"},
		}
		conn, response, err := websocket.DefaultDialer.Dial(wsURL, header)
		result <- dialResult{conn: conn, response: response, err: err}
	}()

	select {
	case <-sessions.authorizeStarted:
	case <-time.After(time.Second):
		close(sessions.release)
		t.Fatal("browser admission did not enter the final session lease")
	}
	sessions.revoke()
	close(sessions.release)

	var got dialResult
	select {
	case got = <-result:
	case <-time.After(time.Second):
		t.Fatal("revoked browser admission did not return")
	}
	if got.conn != nil {
		got.conn.Close()
	}
	if got.response != nil && got.response.Body != nil {
		defer got.response.Body.Close()
	}
	if got.err == nil || got.response == nil || got.response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("revocation race response=%v err=%v, want 401", got.response, got.err)
	}
	ensures, _, touches := spawner.snapshot()
	if len(ensures) != 0 || len(touches) != 0 {
		t.Fatalf("revoked admission caused runtime activity: ensures=%v touches=%v", ensures, touches)
	}
	if stats := server.ConnectionStats(); stats != (BrowserConnectionStats{}) {
		t.Fatalf("revoked admission registered a connection: %+v", stats)
	}
}

func TestBrowserWebSocketLogoutCompletesWhileSpawnerIsBlocked(t *testing.T) {
	gateway := openRuntimeGateway(t)
	sessions, err := NewHMACUserSessionVerifier(
		testSecret,
		"",
		gateway,
	)
	if err != nil {
		t.Fatal(err)
	}
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	session, err := sessions.IssueSession(context.Background(), UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: personalityAgentID,
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	spawner := &blockingDirectChatSpawner{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	server := newAuthorizedBrowserServer(sessions, gateway, gateway)
	server.AllowedOrigins = []string{browserAuthTestOrigin}
	server.Spawner = spawner
	server.SpawnTimeout = time.Second
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	type dialResult struct {
		conn     *websocket.Conn
		response *http.Response
		err      error
	}
	dialed := make(chan dialResult, 1)
	go func() {
		wsURL := strings.Replace(httpServer.URL, "http", "ws", 1) + "/direct-chat/ws?installation_id=" + testDirectChatInstallationID + "&authority_epoch=1"
		header := http.Header{
			"Origin": {browserAuthTestOrigin},
			"Cookie": {BrowserSessionCookie + "=" + session},
		}
		conn, response, err := websocket.DefaultDialer.Dial(wsURL, header)
		dialed <- dialResult{conn: conn, response: response, err: err}
	}()
	select {
	case <-spawner.started:
	case <-time.After(time.Second):
		close(spawner.release)
		t.Fatal("browser admission did not reach blocked runtime provisioning")
	}

	revoked := make(chan error, 1)
	go func() {
		_, revokeErr := sessions.RevokeSession(context.Background(), session)
		revoked <- revokeErr
	}()
	select {
	case revokeErr := <-revoked:
		if revokeErr != nil {
			close(spawner.release)
			t.Fatalf("revoke session while spawner blocked: %v", revokeErr)
		}
	case <-time.After(time.Second):
		close(spawner.release)
		t.Fatal("global browser-session revocation waited on blocked spawner")
	}
	close(spawner.release)

	var got dialResult
	select {
	case got = <-dialed:
	case <-time.After(time.Second):
		t.Fatal("revoked admission did not finish after provisioning returned")
	}
	if got.conn != nil {
		got.conn.Close()
	}
	if got.response != nil && got.response.Body != nil {
		defer got.response.Body.Close()
	}
	if got.err == nil || got.response == nil || got.response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("post-spawn revocation response=%v err=%v, want 401", got.response, got.err)
	}
	if stats := server.ConnectionStats(); stats != (BrowserConnectionStats{}) {
		t.Fatalf("revoked post-spawn admission registered a connection: %+v", stats)
	}
}

func TestBrowserWebSocketLifecycleMutationWaitsForBoundedProvisioning(t *testing.T) {
	newAdmission := func(
		t *testing.T,
		spawnTimeout time.Duration,
	) (*blockingDirectChatSpawner, *directchat.LifecycleFence, <-chan dialBrowserResult) {
		t.Helper()
		gateway := openRuntimeGateway(t)
		sessions, err := NewHMACUserSessionVerifier(testSecret, "", gateway)
		if err != nil {
			t.Fatal(err)
		}
		session, err := sessions.IssueSession(context.Background(), UserSessionClaims{
			TenantID:           "tenant-1",
			UserID:             "user-1",
			PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
		}, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		spawner := &blockingDirectChatSpawner{
			started: make(chan struct{}),
			release: make(chan struct{}),
		}
		fence := directchat.NewLifecycleFence()
		server := newAuthorizedBrowserServer(sessions, gateway, gateway)
		server.SetLifecycleFence(fence)
		server.AllowedOrigins = []string{browserAuthTestOrigin}
		server.Spawner = spawner
		server.SpawnTimeout = spawnTimeout
		mux := http.NewServeMux()
		mux.Handle("GET /direct-chat/ws", server)
		httpServer := httptest.NewServer(mux)
		t.Cleanup(httpServer.Close)
		dialed := make(chan dialBrowserResult, 1)
		go func() {
			wsURL := strings.Replace(httpServer.URL, "http", "ws", 1) +
				"/direct-chat/ws?installation_id=" + testDirectChatInstallationID + "&authority_epoch=1"
			header := http.Header{
				"Origin": {browserAuthTestOrigin},
				"Cookie": {BrowserSessionCookie + "=" + session},
			}
			conn, response, dialErr := websocket.DefaultDialer.Dial(wsURL, header)
			dialed <- dialBrowserResult{conn: conn, response: response, err: dialErr}
		}()
		select {
		case <-spawner.started:
		case <-time.After(time.Second):
			t.Fatal("browser admission did not reach provisioning")
		}
		return spawner, fence, dialed
	}

	t.Run("mutation waits and the admitted spawn finishes in its prior epoch", func(t *testing.T) {
		spawner, fence, dialed := newAdmission(t, time.Second)
		mutationDone := make(chan error, 1)
		go func() {
			release, err := fence.AcquireMutation(context.Background())
			if err == nil {
				release()
			}
			mutationDone <- err
		}()
		select {
		case err := <-mutationDone:
			close(spawner.release)
			t.Fatalf("lifecycle mutation crossed blocked provisioning: %v", err)
		case <-time.After(40 * time.Millisecond):
		}
		close(spawner.release)
		got := <-dialed
		if got.response != nil && got.response.Body != nil {
			defer got.response.Body.Close()
		}
		if got.err != nil || got.conn == nil {
			t.Fatalf("operation-first admission = response %v, error %v", got.response, got.err)
		}
		defer got.conn.Close()
		if err := <-mutationDone; err != nil {
			t.Fatalf("mutation after provisioning: %v", err)
		}
	})

	t.Run("spawn timeout releases the lifecycle fence", func(t *testing.T) {
		_, fence, dialed := newAdmission(t, 30*time.Millisecond)
		mutationDone := make(chan error, 1)
		go func() {
			release, err := fence.AcquireMutation(context.Background())
			if err == nil {
				release()
			}
			mutationDone <- err
		}()
		select {
		case err := <-mutationDone:
			if err != nil {
				t.Fatalf("mutation after spawn timeout: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("timed-out provisioning wedged direct-chat lifecycle")
		}
		got := <-dialed
		if got.response != nil && got.response.Body != nil {
			defer got.response.Body.Close()
		}
		// The upgrade is accepted so the timed-out provisioning can be named on
		// the only channel the browser can read.
		if got.err != nil || got.conn == nil {
			t.Fatalf("spawn timeout response=%v err=%v, want an accepted upgrade", got.response, got.err)
		}
		defer got.conn.Close()
		got.conn.SetReadDeadline(time.Now().Add(time.Second))
		_, _, readErr := got.conn.ReadMessage()
		closeErr, ok := readErr.(*websocket.CloseError)
		if !ok {
			t.Fatalf("read after spawn timeout = %v, want a close frame", readErr)
		}
		if closeErr.Code != DirectChatRuntimeUnavailableCloseCode ||
			closeErr.Text != DirectChatRuntimeUnavailableCloseReason {
			t.Fatalf("spawn timeout close = %d %q, want %d %q",
				closeErr.Code, closeErr.Text,
				DirectChatRuntimeUnavailableCloseCode, DirectChatRuntimeUnavailableCloseReason)
		}
	})
}

func TestBrowserWebSocketSpawnUsesServerTimeoutAndSessionTarget(t *testing.T) {
	spawner := &countingDirectChatSpawner{}
	_, server, conn := openLiveAuthorizedBrowser(
		t,
		nil,
		spawner,
		time.Hour,
		false,
	)
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	waitForBrowserConnectionStats(
		t,
		server,
		BrowserConnectionStats{Active: 0, Accepted: 1},
	)
	ensures, ensureContexts, touches := spawner.snapshot()
	if len(ensures) != 1 || ensures[0] != "018f47a2-9b3c-7def-8abc-0123456789ab" {
		t.Fatalf("spawn targets = %v", ensures)
	}
	if len(ensureContexts) != 1 {
		t.Fatalf("spawn contexts = %v", ensureContexts)
	}
	deadline, ok := ensureContexts[0].Deadline()
	if !ok || time.Until(deadline) > server.spawnTimeout() {
		t.Fatalf("spawn context did not carry server-owned timeout: deadline=%v ok=%v", deadline, ok)
	}
	if len(touches) != 0 {
		t.Fatalf("closing an idle socket touched the runtime: %v", touches)
	}
}

func TestBrowserWebSocketRevalidatesCurrentEmployerOnLiveBoundaries(t *testing.T) {
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"

	t.Run("command admission", func(t *testing.T) {
		authorizer := &mutableDirectChatAuthorizer{allowed: true}
		spawner := &countingDirectChatSpawner{}
		gateway, server, conn := openLiveAuthorizedBrowser(
			t,
			authorizer,
			spawner,
			time.Hour,
			true,
		)
		authorizer.setAllowed(false)
		if err := conn.WriteJSON(browserCommandFrame{
			Type:           "command",
			IdempotencyKey: "former-employer",
			Command:        json.RawMessage(`{"type":"user_message","text":"denied","attachments":[]}`),
		}); err != nil {
			t.Fatal(err)
		}
		assertBrowserConnectionClosedBeforeFrame(t, conn)
		if hasCommands, err := gateway.commands.HasCommands(context.Background(), personalityAgentID); err != nil || hasCommands {
			t.Fatalf("former Employer command reached durable log: hasCommands=%v err=%v", hasCommands, err)
		}
		_, _, touches := spawner.snapshot()
		if len(touches) != 0 {
			t.Fatalf("former Employer command touched runtime: %v", touches)
		}
		waitForBrowserConnectionStats(t, server, BrowserConnectionStats{Active: 0, Accepted: 1})
	})

	t.Run("private event", func(t *testing.T) {
		authorizer := &mutableDirectChatAuthorizer{allowed: true}
		spawner := &countingDirectChatSpawner{}
		gateway, server, conn := openLiveAuthorizedBrowser(
			t,
			authorizer,
			spawner,
			time.Hour,
			false,
		)
		authorizer.setAllowed(false)
		seq := uint64(1)
		if err := gateway.Receive(context.Background(), TokenClaims{
			TenantID:           "tenant-1",
			PersonalityAgentID: personalityAgentID,
			Generation:         1,
		}, Envelope{
			Seq:                &seq,
			PersonalityAgentID: personalityAgentID,
			Event:              json.RawMessage(`{"type":"agent_start"}`),
		}); err != nil {
			t.Fatal(err)
		}
		assertBrowserConnectionClosedBeforeFrame(t, conn)
		_, _, touches := spawner.snapshot()
		if len(touches) != 0 {
			t.Fatalf("denied private event touched runtime: %v", touches)
		}
		waitForBrowserConnectionStats(t, server, BrowserConnectionStats{Active: 0, Accepted: 1})
	})

	t.Run("readiness status", func(t *testing.T) {
		authorizer := &mutableDirectChatAuthorizer{allowed: true}
		gateway, server, conn := openLiveAuthorizedBrowser(
			t,
			authorizer,
			nil,
			time.Hour,
			false,
		)
		authorizer.setAllowed(false)
		receipt := "ready-after-employment-change"
		if err := gateway.PublishRuntimeState(personalityAgentID, 1, &receipt); err != nil {
			t.Fatal(err)
		}
		assertBrowserConnectionClosedBeforeFrame(t, conn)
		waitForBrowserConnectionStats(t, server, BrowserConnectionStats{Active: 0, Accepted: 1})
	})

	t.Run("idle socket", func(t *testing.T) {
		authorizer := &mutableDirectChatAuthorizer{allowed: true}
		_, server, conn := openLiveAuthorizedBrowser(
			t,
			authorizer,
			nil,
			10*time.Millisecond,
			false,
		)
		authorizer.setAllowed(false)
		assertBrowserConnectionClosedBeforeFrame(t, conn)
		waitForBrowserConnectionStats(t, server, BrowserConnectionStats{Active: 0, Accepted: 1})
	})

	t.Run("uninstall and reinstall cannot revive the old socket", func(t *testing.T) {
		authorizer := &mutableDirectChatAuthorizer{
			allowed:        true,
			installationID: testDirectChatInstallationID,
		}
		_, server, conn := openLiveAuthorizedBrowser(
			t,
			authorizer,
			nil,
			10*time.Millisecond,
			false,
		)
		authorizer.setInstallationID("0198f0f4-9b72-7000-8000-000000000052")
		assertBrowserConnectionClosedBeforeFrame(t, conn)
		waitForBrowserConnectionStats(t, server, BrowserConnectionStats{Active: 0, Accepted: 1})
	})

	t.Run("fast same-ID disable and re-enable cannot revive the old socket epoch", func(t *testing.T) {
		authorizer := &mutableDirectChatAuthorizer{
			allowed:        true,
			installationID: testDirectChatInstallationID,
			authorityEpoch: testDirectChatAuthorityEpoch,
		}
		_, server, conn := openLiveAuthorizedBrowser(
			t,
			authorizer,
			nil,
			10*time.Millisecond,
			false,
		)
		authorizer.setAuthorityEpoch(testDirectChatAuthorityEpoch + 1)
		assertBrowserConnectionClosedBeforeFrame(t, conn)
		waitForBrowserConnectionStats(t, server, BrowserConnectionStats{Active: 0, Accepted: 1})
	})
}

func TestBrowserWebSocketTwoClientsCannotShareStaleSameIDAuthorityEpoch(t *testing.T) {
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	gateway := openRuntimeGateway(t)
	sessions, err := NewHMACUserSessionVerifier(
		testSecret,
		"",
		newTestBrowserSessionRevocationStore(),
	)
	if err != nil {
		t.Fatal(err)
	}
	authorizer := &mutableDirectChatAuthorizer{
		allowed:        true,
		installationID: testDirectChatInstallationID,
		authorityEpoch: 1,
	}
	server := newAuthorizedBrowserServer(sessions, gateway, gateway)
	server.Authorizer = authorizer
	server.AuthorizationPollInterval = 10 * time.Millisecond
	server.AllowedOrigins = []string{browserAuthTestOrigin}
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()
	cookie := signBrowserSession(t, testSecret, userSessionWireClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: personalityAgentID,
		Exp:                time.Now().Add(time.Hour).Unix(),
		Aud:                defaultBrowserAudience,
	})

	first, response, err := dialBrowserWSWithEpoch(httpServer, cookie, 1)
	if err != nil {
		t.Fatalf("dial first browser epoch: status=%v err=%v", responseStatus(response), err)
	}
	defer first.Close()
	if err := first.WriteJSON(browserHello{Type: "hello"}); err != nil {
		t.Fatal(err)
	}
	assertDirectChatStatus(t, first, "unavailable")

	// Another tab commits disable -> enable and observes epoch 2. The first
	// socket retains epoch 1 and must close; a reconnect carrying that stale
	// value is denied even though installation_id is unchanged.
	authorizer.setAuthorityEpoch(2)
	assertBrowserConnectionClosedBeforeFrame(t, first)
	stale, response, err := dialBrowserWSWithEpoch(httpServer, cookie, 1)
	if stale != nil {
		_ = stale.Close()
		t.Fatal("stale browser epoch unexpectedly reconnected")
	}
	if response != nil {
		defer response.Body.Close()
	}
	if err == nil || responseStatus(response) != http.StatusForbidden {
		t.Fatalf("stale epoch admission: status=%v err=%v", responseStatus(response), err)
	}

	current, response, err := dialBrowserWSWithEpoch(httpServer, cookie, 2)
	if err != nil {
		t.Fatalf("dial current browser epoch: status=%v err=%v", responseStatus(response), err)
	}
	defer current.Close()
	if err := current.WriteJSON(browserHello{Type: "hello"}); err != nil {
		t.Fatal(err)
	}
	assertDirectChatStatus(t, current, "unavailable")
}

func responseStatus(response *http.Response) int {
	if response == nil {
		return 0
	}
	return response.StatusCode
}

func TestBrowserWebSocketEmployerTransferSerializesCommandAppend(t *testing.T) {
	authorizer := newCoordinatedDirectChatAuthorizer()
	appender := &blockingCommandAppender{
		started:   make(chan struct{}),
		release:   make(chan struct{}),
		completed: make(chan struct{}),
	}
	_, server, conn := openLiveAuthorizedBrowserWithOptions(
		t,
		authorizer,
		nil,
		10*time.Millisecond,
		true,
		appender,
		nil,
	)
	fence := directchat.NewLifecycleFence()
	server.SetLifecycleFence(fence)
	if err := conn.WriteJSON(browserCommandFrame{
		Type:           "command",
		IdempotencyKey: "transfer-vs-append",
		Command:        json.RawMessage(`{"type":"user_message","text":"serialize me","attachments":[]}`),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-appender.started:
	case <-time.After(time.Second):
		t.Fatal("command append did not start under Employer authority lease")
	}

	transferStarted := make(chan struct{})
	transferDone := make(chan struct{})
	go func() {
		authorizer.transfer(fence, transferStarted)
		close(transferDone)
	}()
	<-transferStarted
	select {
	case <-transferDone:
		t.Fatal("Employer transfer crossed an in-flight command append")
	default:
	}
	close(appender.release)
	select {
	case <-appender.completed:
	case <-time.After(time.Second):
		t.Fatal("command append did not complete")
	}
	conn.SetReadDeadline(time.Now().Add(time.Second))
	var accepted browserCommandAcceptedFrame
	if err := conn.ReadJSON(&accepted); err != nil {
		t.Fatalf("read command acceptance before Employer transfer: %v", err)
	}
	if accepted.Type != "command_accepted" ||
		accepted.IdempotencyKey != "transfer-vs-append" {
		t.Fatalf("unexpected command acceptance: %+v", accepted)
	}
	select {
	case <-transferDone:
	case <-time.After(time.Second):
		t.Fatal("Employer transfer did not complete after command append")
	}
	assertBrowserConnectionClosedBeforeFrame(t, conn)
	waitForBrowserConnectionStats(t, server, BrowserConnectionStats{Active: 0, Accepted: 1})
}

func TestBrowserWebSocketQueuedLifecycleMutationDoesNotDeadlockCommandAcceptance(t *testing.T) {
	appender := &blockingCommandAppender{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	_, server, conn := openLiveAuthorizedBrowserWithOptions(
		t,
		allowDirectChatAuthorizer{},
		nil,
		time.Hour,
		true,
		appender,
		nil,
	)
	fence := directchat.NewLifecycleFence()
	server.SetLifecycleFence(fence)

	acceptanceWriteStarted := make(chan struct{})
	releaseAcceptanceWrite := make(chan struct{})
	var acceptanceWrite sync.Once
	server.beforeWrite = func() {
		acceptanceWrite.Do(func() {
			close(acceptanceWriteStarted)
			<-releaseAcceptanceWrite
		})
	}
	if err := conn.WriteJSON(browserCommandFrame{
		Type:           "command",
		IdempotencyKey: "queued-lifecycle-writer",
		Command:        json.RawMessage(`{"type":"user_message","text":"one permit only","attachments":[]}`),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-appender.started:
	case <-time.After(time.Second):
		t.Fatal("command append did not start under its lifecycle operation permit")
	}

	mutationAcquired := make(chan struct{})
	mutationDone := make(chan error, 1)
	go func() {
		release, err := fence.AcquireMutation(context.Background())
		if err == nil {
			close(mutationAcquired)
			release()
		}
		mutationDone <- err
	}()
	waitForLifecycleMutationQueue(t, fence)
	select {
	case <-mutationAcquired:
		t.Fatal("lifecycle mutation crossed the blocked command effect")
	default:
	}

	close(appender.release)
	select {
	case <-acceptanceWriteStarted:
	case <-time.After(time.Second):
		t.Fatal("queued lifecycle writer deadlocked command acceptance")
	}
	select {
	case <-mutationAcquired:
		t.Fatal("lifecycle mutation crossed the command acceptance write")
	default:
	}
	close(releaseAcceptanceWrite)

	conn.SetReadDeadline(time.Now().Add(time.Second))
	var accepted browserCommandAcceptedFrame
	if err := conn.ReadJSON(&accepted); err != nil {
		t.Fatalf("read command acceptance: %v", err)
	}
	if accepted.Type != "command_accepted" ||
		accepted.IdempotencyKey != "queued-lifecycle-writer" {
		t.Fatalf("unexpected command acceptance: %+v", accepted)
	}
	select {
	case err := <-mutationDone:
		if err != nil {
			t.Fatalf("lifecycle mutation after command acceptance: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("lifecycle mutation did not proceed after command acceptance")
	}
}

// A full-capacity writer queued behind an active operation makes the weighted
// semaphore reject following read probes until their contexts expire. This
// proves the writer is actually between the command's outer permit and its
// acceptance write instead of relying on scheduler timing.
func waitForLifecycleMutationQueue(t *testing.T, fence *directchat.LifecycleFence) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		probeContext, cancelProbe := context.WithTimeout(
			context.Background(),
			5*time.Millisecond,
		)
		releaseProbe, err := fence.AcquireOperation(probeContext)
		cancelProbe()
		if errors.Is(err, context.DeadlineExceeded) {
			return
		}
		if err != nil {
			t.Fatalf("probe lifecycle writer queue: %v", err)
		}
		releaseProbe()
		if time.Now().After(deadline) {
			t.Fatal("lifecycle mutation never queued behind command operation")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestBrowserWebSocketEmployerTransferSerializesPrivateWrite(t *testing.T) {
	authorizer := newCoordinatedDirectChatAuthorizer()
	writeStarted := make(chan struct{})
	releaseWrite := make(chan struct{})
	var writeMu sync.Mutex
	writeCalls := 0
	beforeWrite := func() {
		writeMu.Lock()
		writeCalls++
		call := writeCalls
		writeMu.Unlock()
		if call == 2 {
			close(writeStarted)
			<-releaseWrite
		}
	}
	gateway, server, conn := openLiveAuthorizedBrowserWithOptions(
		t,
		authorizer,
		nil,
		time.Hour,
		false,
		nil,
		beforeWrite,
	)
	fence := directchat.NewLifecycleFence()
	server.SetLifecycleFence(fence)
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	claims := TokenClaims{
		TenantID:           "tenant-1",
		PersonalityAgentID: personalityAgentID,
		Generation:         1,
	}
	seq := uint64(1)
	if err := gateway.Receive(context.Background(), claims, Envelope{
		Seq:                &seq,
		PersonalityAgentID: personalityAgentID,
		Event:              json.RawMessage(`{"type":"agent_start"}`),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-writeStarted:
	case <-time.After(time.Second):
		t.Fatal("private event write did not start under Employer authority lease")
	}

	transferStarted := make(chan struct{})
	transferDone := make(chan struct{})
	go func() {
		authorizer.transfer(fence, transferStarted)
		close(transferDone)
	}()
	<-transferStarted
	select {
	case <-transferDone:
		t.Fatal("Employer transfer crossed an in-flight private event write")
	default:
	}
	close(releaseWrite)
	assertBrowserEvent(t, conn, "agent_start", true)
	select {
	case <-transferDone:
	case <-time.After(time.Second):
		t.Fatal("Employer transfer did not complete after private event write")
	}

	seq = 2
	if err := gateway.Receive(context.Background(), claims, Envelope{
		Seq:                &seq,
		PersonalityAgentID: personalityAgentID,
		Event:              json.RawMessage(`{"type":"agent_end"}`),
	}); err != nil {
		t.Fatal(err)
	}
	assertBrowserConnectionClosedBeforeFrame(t, conn)
	waitForBrowserConnectionStats(t, server, BrowserConnectionStats{Active: 0, Accepted: 1})
}

func TestBrowserWebSocketReconnectsFromDurableCursor(t *testing.T) {
	gateway := openRuntimeGateway(t)
	sessions, err := NewHMACUserSessionVerifier(testSecret, "", newTestBrowserSessionRevocationStore())
	if err != nil {
		t.Fatal(err)
	}
	server := newAuthorizedBrowserServer(sessions, gateway, gateway)
	server.AllowedOrigins = []string{"https://web.example"}
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()
	cookie := signBrowserSession(t, testSecret, userSessionWireClaims{TenantID: "tenant", UserID: "user", PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Exp: time.Now().Add(time.Hour).Unix(), Aud: defaultBrowserAudience})
	claims := TokenClaims{TenantID: "tenant", PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Generation: 1}
	if err := gateway.PublishRuntimeState(claims.PersonalityAgentID, claims.Generation, nil); err != nil {
		t.Fatal(err)
	}
	seq := uint64(1)
	if err := gateway.Receive(context.Background(), claims, Envelope{Seq: &seq, PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Event: json.RawMessage(`{"type":"agent_start"}`)}); err != nil {
		t.Fatal(err)
	}
	first := dialBrowserWS(t, httpServer, cookie, "018f47a2-9b3c-7def-8abc-0123456789ab")
	waitForBrowserConnectionStats(t, server, BrowserConnectionStats{Active: 1, Accepted: 1})
	if err := first.WriteJSON(browserHello{Type: "hello", LastEventSeq: 0}); err != nil {
		t.Fatal(err)
	}
	assertBrowserEvent(t, first, "agent_start", true)
	assertDirectChatStatus(t, first, "unavailable")
	_ = first.Close()
	waitForBrowserConnectionStats(t, server, BrowserConnectionStats{Active: 0, Accepted: 1})
	seq = 2
	if err := gateway.Receive(context.Background(), claims, Envelope{Seq: &seq, PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Event: json.RawMessage(`{"type":"agent_end"}`)}); err != nil {
		t.Fatal(err)
	}
	second := dialBrowserWS(t, httpServer, cookie, "018f47a2-9b3c-7def-8abc-0123456789ab")
	defer second.Close()
	waitForBrowserConnectionStats(t, server, BrowserConnectionStats{Active: 1, Accepted: 2})
	if err := second.WriteJSON(browserHello{Type: "hello", LastEventSeq: 1}); err != nil {
		t.Fatal(err)
	}
	assertBrowserEvent(t, second, "agent_end", true)
	assertDirectChatStatus(t, second, "unavailable")
}

func TestBrowserLogoutClosesOnlyMatchingLiveSessionAndStopsReconnect(t *testing.T) {
	gateway := openRuntimeGateway(t)
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	receipt := "ready"
	if err := gateway.PublishRuntimeState(personalityAgentID, 1, &receipt); err != nil {
		t.Fatal(err)
	}
	sessions, err := NewHMACUserSessionVerifier(testSecret, "", newTestBrowserSessionRevocationStore())
	if err != nil {
		t.Fatal(err)
	}
	browser := newAuthorizedBrowserServer(sessions, gateway, gateway)
	browser.AllowedOrigins = []string{browserAuthTestOrigin}
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", browser)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	sessionClaims := UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: personalityAgentID,
	}
	firstSession, err := sessions.IssueSession(context.Background(), sessionClaims, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	secondSession, err := sessions.IssueSession(context.Background(), sessionClaims, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	first := dialBrowserWS(t, httpServer, firstSession, personalityAgentID)
	defer first.Close()
	second := dialBrowserWS(t, httpServer, secondSession, personalityAgentID)
	defer second.Close()
	for _, conn := range []*websocket.Conn{first, second} {
		if err := conn.WriteJSON(browserHello{Type: "hello"}); err != nil {
			t.Fatal(err)
		}
		assertDirectChatStatus(t, conn, "ready")
	}
	waitForBrowserConnectionStats(t, browser, BrowserConnectionStats{Active: 2, Accepted: 2})

	auth, err := NewBrowserAuthServer(
		&fakeFirebaseVerifier{},
		&fakeBindingResolver{},
		sessions,
		[]string{browserAuthTestOrigin},
		true,
	)
	if err != nil {
		t.Fatal(err)
	}
	auth.Connections = browser
	csrf, csrfCookie := obtainCSRF(t, auth)
	logout := httptest.NewRequest(http.MethodPost, "/auth/logout", nil)
	logout.Header.Set("Origin", browserAuthTestOrigin)
	logout.Header.Set("X-CSRF-Token", csrf)
	logout.AddCookie(csrfCookie)
	logout.AddCookie(&http.Cookie{Name: BrowserSessionCookie, Value: firstSession})
	logoutRecorder := httptest.NewRecorder()
	auth.serveLogout(logoutRecorder, logout)
	if logoutRecorder.Code != http.StatusNoContent {
		t.Fatalf("logout: %d %s", logoutRecorder.Code, logoutRecorder.Body.String())
	}
	assertBrowserConnectionClosedBeforeFrame(t, first)
	waitForBrowserConnectionStats(t, browser, BrowserConnectionStats{Active: 1, Accepted: 2})

	command := browserCommandFrame{
		Type:           "command",
		IdempotencyKey: "still-authorized",
		Command:        json.RawMessage(`{"type":"user_message","text":"hi","attachments":[]}`),
	}
	if err := second.WriteJSON(command); err != nil {
		t.Fatal(err)
	}
	var accepted browserCommandAcceptedFrame
	if err := second.ReadJSON(&accepted); err != nil {
		t.Fatalf("unrelated session was closed: %v", err)
	}
	if accepted.Type != "command_accepted" {
		t.Fatalf("unexpected command result: %+v", accepted)
	}

	wsURL := strings.Replace(httpServer.URL, "http", "ws", 1) + "/direct-chat/ws?installation_id=" + testDirectChatInstallationID + "&authority_epoch=1"
	header := http.Header{
		"Origin": {"https://web.example"},
		"Cookie": {BrowserSessionCookie + "=" + firstSession},
	}
	reconnected, response, err := websocket.DefaultDialer.Dial(wsURL, header)
	if reconnected != nil {
		reconnected.Close()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("revoked session reconnected: response=%v err=%v", response, err)
	}
}

func TestBrowserSessionLineageLogoutStopsSuccessorOutboundFramesAcrossGateways(
	t *testing.T,
) {
	tmp := t.TempDir()
	store, firstGateway, err := openGatewayAt(
		t,
		filepath.Join(tmp, "commands"),
		filepath.Join(tmp, "runtime"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	secondGateway, err := OpenDurableGateway(firstGateway.dir, store)
	if err != nil {
		t.Fatal(err)
	}
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	receipt := "ready"
	if err := firstGateway.PublishRuntimeState(
		personalityAgentID,
		1,
		&receipt,
	); err != nil {
		t.Fatal(err)
	}
	firstSessions, err := NewHMACUserSessionVerifier(
		testSecret,
		"",
		firstGateway,
	)
	if err != nil {
		t.Fatal(err)
	}
	secondSessions, err := NewHMACUserSessionVerifier(
		testSecret,
		"",
		secondGateway,
	)
	if err != nil {
		t.Fatal(err)
	}
	sessionClaims := UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: personalityAgentID,
	}
	currentSession, err := firstSessions.IssueSession(
		context.Background(),
		sessionClaims,
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, successorSession, valid, err := firstSessions.RotateSession(
		context.Background(),
		currentSession,
		sessionClaims,
		2*time.Minute,
	)
	if err != nil || !valid {
		t.Fatalf("rotate browser session: valid=%v err=%v", valid, err)
	}
	browser := newAuthorizedBrowserServer(secondSessions, secondGateway, secondGateway)
	browser.AllowedOrigins = []string{browserAuthTestOrigin}
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", browser)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	conn := dialBrowserWS(
		t,
		httpServer,
		successorSession,
		personalityAgentID,
	)
	defer conn.Close()
	if err := conn.WriteJSON(browserHello{Type: "hello"}); err != nil {
		t.Fatal(err)
	}
	assertDirectChatStatus(t, conn, "ready")
	if _, valid, err := firstSessions.RevokeSessionForLogout(
		context.Background(),
		currentSession,
	); err != nil || !valid {
		t.Fatalf("logout ancestor session: valid=%v err=%v", valid, err)
	}

	seq := uint64(1)
	if err := firstGateway.Receive(context.Background(), TokenClaims{
		TenantID:           "tenant-1",
		PersonalityAgentID: personalityAgentID,
		Generation:         1,
	}, Envelope{
		Seq:                &seq,
		PersonalityAgentID: personalityAgentID,
		Event:              json.RawMessage(`{"type":"agent_start"}`),
	}); err != nil {
		t.Fatal(err)
	}
	assertBrowserConnectionClosedBeforeFrame(t, conn)
	waitForBrowserConnectionStats(
		t,
		browser,
		BrowserConnectionStats{Active: 0, Accepted: 1},
	)
}

func TestBrowserWebSocketClosesAtSessionExpiry(t *testing.T) {
	gateway := openRuntimeGateway(t)
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	receipt := "ready"
	if err := gateway.PublishRuntimeState(personalityAgentID, 1, &receipt); err != nil {
		t.Fatal(err)
	}
	sessions, err := NewHMACUserSessionVerifier(testSecret, "", newTestBrowserSessionRevocationStore())
	if err != nil {
		t.Fatal(err)
	}
	browser := newAuthorizedBrowserServer(sessions, gateway, gateway)
	browser.AllowedOrigins = []string{browserAuthTestOrigin}
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", browser)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	expires := time.Now().Add(2 * time.Second).Unix()
	session := signBrowserSession(t, testSecret, userSessionWireClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: personalityAgentID,
		Iat:                expires - int64(time.Minute/time.Second),
		Exp:                expires,
		Aud:                defaultBrowserAudience,
	})
	conn := dialBrowserWS(t, httpServer, session, personalityAgentID)
	defer conn.Close()
	if err := conn.WriteJSON(browserHello{Type: "hello"}); err != nil {
		t.Fatal(err)
	}
	assertDirectChatStatus(t, conn, "ready")
	conn.SetReadDeadline(time.Now().Add(4 * time.Second))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("session remained live past its signed expiry")
	} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		t.Fatalf("client deadline elapsed before the server closed expired session: %v", err)
	}
	waitForBrowserConnectionStats(t, browser, BrowserConnectionStats{Active: 0, Accepted: 1})
}

func TestBrowserWebSocketExpiryStopsReplayWritesAndCommandAdmission(t *testing.T) {
	gateway := openRuntimeGateway(t)
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	agentClaims := TokenClaims{TenantID: "tenant-1", PersonalityAgentID: personalityAgentID, Generation: 1}
	if err := gateway.PublishRuntimeState(personalityAgentID, agentClaims.Generation, nil); err != nil {
		t.Fatal(err)
	}
	seq := uint64(1)
	if err := gateway.Receive(context.Background(), agentClaims, Envelope{
		Seq:                &seq,
		PersonalityAgentID: personalityAgentID,
		Event:              json.RawMessage(`{"type":"agent_start"}`),
	}); err != nil {
		t.Fatal(err)
	}
	sessions, err := NewHMACUserSessionVerifier(testSecret, "", newTestBrowserSessionRevocationStore())
	if err != nil {
		t.Fatal(err)
	}
	browser := newAuthorizedBrowserServer(sessions, gateway, gateway)
	browser.AllowedOrigins = []string{browserAuthTestOrigin}
	writeReached := make(chan struct{})
	releaseWrite := make(chan struct{})
	var writeHookCalls int
	browser.beforeWrite = func() {
		writeHookCalls++
		if writeHookCalls == 1 {
			close(writeReached)
			<-releaseWrite
		}
	}
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", browser)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	expires := time.Now().Add(2 * time.Second).Unix()
	session := signBrowserSession(t, testSecret, userSessionWireClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: personalityAgentID,
		Iat:                expires - int64(time.Minute/time.Second),
		Exp:                expires,
		Aud:                defaultBrowserAudience,
	})
	conn := dialBrowserWS(t, httpServer, session, personalityAgentID)
	defer conn.Close()
	if err := conn.WriteJSON(browserHello{Type: "hello"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-writeReached:
	case <-time.After(time.Second):
		t.Fatal("durable replay did not reach its first write")
	}
	if err := conn.WriteJSON(browserCommandFrame{
		Type:           "command",
		IdempotencyKey: "must-not-append-after-expiry",
		Command:        json.RawMessage(`{"type":"user_message","text":"expired","attachments":[]}`),
	}); err != nil {
		t.Fatal(err)
	}
	if wait := time.Until(time.Unix(expires, 0).Add(100 * time.Millisecond)); wait > 0 {
		time.Sleep(wait)
	}
	close(releaseWrite)
	assertBrowserConnectionClosedBeforeFrame(t, conn)
	if hasCommands, err := gateway.commands.HasCommands(context.Background(), personalityAgentID); err != nil || hasCommands {
		t.Fatalf("expiry crossing replay appended command: hasCommands=%v err=%v", hasCommands, err)
	}
}

func openLiveAuthorizedBrowser(
	t *testing.T,
	authorizer DirectChatAuthorizer,
	spawner DirectChatSpawner,
	authorizationPollInterval time.Duration,
	ready bool,
) (*DurableGateway, *BrowserServer, *websocket.Conn) {
	return openLiveAuthorizedBrowserWithOptions(
		t,
		authorizer,
		spawner,
		authorizationPollInterval,
		ready,
		nil,
		nil,
	)
}

func openLiveAuthorizedBrowserWithOptions(
	t *testing.T,
	authorizer DirectChatAuthorizer,
	spawner DirectChatSpawner,
	authorizationPollInterval time.Duration,
	ready bool,
	appender CommandAppender,
	beforeWrite func(),
) (*DurableGateway, *BrowserServer, *websocket.Conn) {
	t.Helper()
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	gateway := openRuntimeGateway(t)
	var receipt *string
	if ready {
		value := "ready"
		receipt = &value
	}
	if err := gateway.PublishRuntimeState(personalityAgentID, 1, receipt); err != nil {
		t.Fatal(err)
	}
	sessions, err := NewHMACUserSessionVerifier(
		testSecret,
		"",
		newTestBrowserSessionRevocationStore(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if appender == nil {
		appender = gateway
	}
	server := newAuthorizedBrowserServer(sessions, appender, gateway)
	server.AllowedOrigins = []string{browserAuthTestOrigin}
	if authorizer != nil {
		server.Authorizer = authorizer
	}
	server.Spawner = spawner
	server.AuthorizationPollInterval = authorizationPollInterval
	server.beforeWrite = beforeWrite
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", server)
	httpServer := httptest.NewServer(mux)
	t.Cleanup(httpServer.Close)
	session := signBrowserSession(t, testSecret, userSessionWireClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: personalityAgentID,
		Exp:                time.Now().Add(time.Hour).Unix(),
		Aud:                defaultBrowserAudience,
	})
	conn := dialBrowserWS(t, httpServer, session, personalityAgentID)
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.WriteJSON(browserHello{Type: "hello"}); err != nil {
		t.Fatal(err)
	}
	status := "unavailable"
	if ready {
		status = "ready"
	}
	assertDirectChatStatus(t, conn, status)
	return gateway, server, conn
}

func waitForBrowserConnectionStats(t *testing.T, server *BrowserServer, want BrowserConnectionStats) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		got := server.ConnectionStats()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("browser connection stats did not settle: got %+v, want %+v", got, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func dialBrowserWS(t *testing.T, server *httptest.Server, cookie, personalityAgentID string) *websocket.Conn {
	t.Helper()
	conn, response, err := dialBrowserWSWithEpoch(
		server,
		cookie,
		testDirectChatAuthorityEpoch,
	)
	if err != nil {
		t.Fatalf("dial browser websocket: status=%v err=%v", responseStatus(response), err)
	}
	return conn
}

func dialBrowserWSWithEpoch(
	server *httptest.Server,
	cookie string,
	authorityEpoch int64,
) (*websocket.Conn, *http.Response, error) {
	wsURL := strings.Replace(server.URL, "http", "ws", 1) +
		fmt.Sprintf(
			"/direct-chat/ws?installation_id=%s&authority_epoch=%d",
			testDirectChatInstallationID,
			authorityEpoch,
		)
	header := http.Header{
		"Origin": {browserAuthTestOrigin},
		"Cookie": {BrowserSessionCookie + "=" + cookie},
	}
	return websocket.DefaultDialer.Dial(wsURL, header)
}

func assertBrowserEvent(t *testing.T, conn *websocket.Conn, eventType string, durable bool) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(time.Second))
	var frame browserEventFrame
	if err := conn.ReadJSON(&frame); err != nil {
		t.Fatalf("read browser event: %v", err)
	}
	if frame.Type != "event" || (frame.Envelope.Seq != nil) != durable {
		t.Fatalf("unexpected browser event frame: %+v", frame)
	}
	var event struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(frame.Envelope.Event, &event); err != nil || event.Type != eventType {
		t.Fatalf("unexpected event: %s (%v)", frame.Envelope.Event, err)
	}
}

func assertDirectChatStatus(t *testing.T, conn *websocket.Conn, want string) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(time.Second))
	var frame directChatStatusFrame
	if err := conn.ReadJSON(&frame); err != nil {
		t.Fatalf("read direct-chat status: %v", err)
	}
	if frame.Type != "direct_chat_status" || frame.Status != want {
		t.Fatalf("unexpected direct-chat status: %+v", frame)
	}
}

func assertDirectChatStatusReason(t *testing.T, conn *websocket.Conn, want, reason string) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(time.Second))
	var frame directChatStatusFrame
	if err := conn.ReadJSON(&frame); err != nil {
		t.Fatalf("read direct-chat status: %v", err)
	}
	if frame.Type != "direct_chat_status" || frame.Status != want || frame.Reason != reason {
		t.Fatalf("unexpected direct-chat status: %+v, want status=%q reason=%q", frame, want, reason)
	}
}

func assertBrowserConnectionClosedBeforeFrame(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(time.Second))
	_, raw, err := conn.ReadMessage()
	if err == nil {
		t.Fatalf("expected connection to close before any frame, got %s", raw)
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		t.Fatalf("server hung instead of closing browser connection: %v", err)
	}
	var closeErr *websocket.CloseError
	if !errors.As(err, &closeErr) &&
		!errors.Is(err, io.EOF) &&
		!errors.Is(err, net.ErrClosed) &&
		!errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("expected websocket close/EOF, got %T: %v", err, err)
	}
}

func signBrowserSession(t *testing.T, secret []byte, claims userSessionWireClaims) string {
	t.Helper()
	if claims.Iat == 0 && claims.Exp != 0 {
		claims.Iat = claims.Exp - int64(maxBrowserSessionTTL/time.Second)
	}
	if claims.SID == "" {
		claims.SID = base64.RawURLEncoding.EncodeToString(make([]byte, browserSessionIDBytes))
	}
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, deriveBrowserSessionSigningKey(secret))
	_, _ = mac.Write([]byte(header + "." + encoded))
	return header + "." + encoded + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func TestBrowserWebSocketReplayFailureClosesBeforeStatusOrCommandAdmission(t *testing.T) {
	gateway := openRuntimeGateway(t)
	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	receipt := "ready"
	if err := gateway.PublishRuntimeState(personalityAgentID, 1, &receipt); err != nil {
		t.Fatal(err)
	}
	seq := uint64(1)
	if err := gateway.Receive(context.Background(), TokenClaims{
		TenantID:           "tenant-1",
		PersonalityAgentID: personalityAgentID,
		Generation:         1,
	}, Envelope{
		Seq:                &seq,
		PersonalityAgentID: personalityAgentID,
		Event:              json.RawMessage(`{"type":"agent_start"}`),
	}); err != nil {
		t.Fatal(err)
	}

	sessions, err := NewHMACUserSessionVerifier(testSecret, "", newTestBrowserSessionRevocationStore())
	if err != nil {
		t.Fatal(err)
	}
	server := newAuthorizedBrowserServer(sessions, gateway, gateway)
	server.AllowedOrigins = []string{"https://web.example"}
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	cookie := signBrowserSession(t, testSecret, userSessionWireClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: personalityAgentID,
		Exp:                time.Now().Add(time.Hour).Unix(),
		Aud:                defaultBrowserAudience,
	})
	conn := dialBrowserWS(t, httpServer, cookie, personalityAgentID)
	defer conn.Close()
	if err := conn.WriteJSON(browserHello{Type: "hello", LastEventSeq: 2}); err != nil {
		t.Fatal(err)
	}
	_ = conn.WriteJSON(browserCommandFrame{
		Type:           "command",
		IdempotencyKey: "must-not-be-admitted",
		Command:        json.RawMessage(`{"type":"user_message","text":"blocked by replay","attachments":[]}`),
	})

	assertBrowserConnectionClosedBeforeFrame(t, conn)
	if hasCommands, err := gateway.commands.HasCommands(context.Background(), personalityAgentID); err != nil || hasCommands {
		t.Fatalf("replay failure admitted a durable command: hasCommands=%v err=%v", hasCommands, err)
	}
}

func TestBrowserServerCommandStateGuards(t *testing.T) {
	gateway := openRuntimeGateway(t)
	sessions, err := NewHMACUserSessionVerifier(testSecret, "", newTestBrowserSessionRevocationStore())
	if err != nil {
		t.Fatal(err)
	}
	server := newAuthorizedBrowserServer(sessions, gateway, gateway)

	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
	claims := TokenClaims{TenantID: "tenant", PersonalityAgentID: personalityAgentID, Generation: 1}
	if err := gateway.PublishRuntimeState(personalityAgentID, claims.Generation, nil); err != nil {
		t.Fatal(err)
	}

	if reason, reject := server.checkCommandState(personalityAgentID, browserCommandHead{Type: "abort"}); !reject {
		t.Fatal("expected abort to be rejected when no run is in flight")
	} else if reason != RejectNotAllowed {
		t.Fatalf("expected not_allowed, got %q", reason)
	}

	if reason, reject := server.checkCommandState(personalityAgentID, browserCommandHead{Type: "approval_decision", RequestID: "request-1"}); !reject {
		t.Fatal("expected approval_decision to be rejected when no approval is pending")
	} else if reason != RejectNotAllowed {
		t.Fatalf("expected not_allowed, got %q", reason)
	}

	seq := uint64(1)
	if err := gateway.Receive(context.Background(), claims, Envelope{Seq: &seq, PersonalityAgentID: personalityAgentID, Event: json.RawMessage(`{"type":"agent_start"}`)}); err != nil {
		t.Fatalf("receive agent_start: %v", err)
	}
	if reason, reject := server.checkCommandState(personalityAgentID, browserCommandHead{Type: "abort"}); reject {
		t.Fatalf("expected abort to be accepted during in-flight run, got %q", reason)
	}

	seq = 2
	if err := gateway.Receive(context.Background(), claims, Envelope{Seq: &seq, PersonalityAgentID: personalityAgentID, Event: json.RawMessage(`{"type":"approval_requested","request":{"id":"request-1","tool_call_id":"call-1","tool_name":"read_file","action":{"reviewable":"read"},"args_summary":"read"}}`)}); err != nil {
		t.Fatalf("receive approval_requested: %v", err)
	}
	if reason, reject := server.checkCommandState(personalityAgentID, browserCommandHead{Type: "approval_decision", RequestID: "request-1"}); reject {
		t.Fatalf("expected approval_decision to be accepted for pending request, got %q", reason)
	}
	if reason, reject := server.checkCommandState(personalityAgentID, browserCommandHead{Type: "approval_decision", RequestID: "request-2"}); !reject || reason != RejectNotAllowed {
		t.Fatalf("expected approval_decision to be rejected for unknown request, got reject=%v reason=%q", reject, reason)
	}
}

func TestBrowserWebSocketAdmitsCommandsAfterGatewayRestart(t *testing.T) {
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
		Event:              json.RawMessage(`{"type":"approval_requested","request":{"id":"request-1","tool_call_id":"call-1","tool_name":"read_file","action":{"reviewable":"read"},"args_summary":"read"}}`),
	}); err != nil {
		t.Fatalf("receive approval_requested: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close command store: %v", err)
	}

	store, gateway, err = openGatewayAt(t, storeDir, runtimeDir)
	if err != nil {
		t.Fatalf("reopen gateway: %v", err)
	}
	defer store.Close()

	sessions, err := NewHMACUserSessionVerifier(testSecret, "", newTestBrowserSessionRevocationStore())
	if err != nil {
		t.Fatal(err)
	}
	server := newAuthorizedBrowserServer(sessions, gateway, gateway)
	server.AllowedOrigins = []string{"https://web.example"}
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	cookie := signBrowserSession(t, testSecret, userSessionWireClaims{TenantID: "tenant-1", UserID: "user-1", PersonalityAgentID: personalityAgentID, Exp: time.Now().Add(time.Hour).Unix(), Aud: defaultBrowserAudience})
	conn := dialBrowserWS(t, httpServer, cookie, personalityAgentID)
	defer conn.Close()
	if err := conn.WriteJSON(browserHello{Type: "hello", LastEventSeq: 2}); err != nil {
		t.Fatal(err)
	}
	assertDirectChatStatus(t, conn, "unavailable")
	receipt := "restart-ready"
	if err := gateway.PublishRuntimeState(personalityAgentID, claims.Generation, &receipt); err != nil {
		t.Fatal(err)
	}
	assertDirectChatStatus(t, conn, "ready")

	commands := []json.RawMessage{
		json.RawMessage(`{"type":"abort"}`),
		json.RawMessage(`{"type":"approval_decision","request_id":"request-1","decision":{"type":"approve_once"}}`),
		json.RawMessage(`{"type":"approval_decision","request_id":"request-unknown","decision":{"type":"approve_once"}}`),
	}
	for index, command := range commands {
		if err := conn.WriteJSON(browserCommandFrame{Type: "command", IdempotencyKey: fmt.Sprintf("idempotency-%d", index), Command: command}); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 2; i++ {
		var accepted browserCommandAcceptedFrame
		conn.SetReadDeadline(time.Now().Add(time.Second))
		if err := conn.ReadJSON(&accepted); err != nil {
			t.Fatalf("read command admission for accepted command %d: %v", i, err)
		}
		if accepted.Type != "command_accepted" || accepted.Seq == 0 || accepted.CommandID == "" {
			t.Fatalf("expected command_accepted with allocated seq and command_id, got %+v", accepted)
		}
	}

	var rejected browserCommandRejectedFrame
	conn.SetReadDeadline(time.Now().Add(time.Second))
	if err := conn.ReadJSON(&rejected); err != nil {
		t.Fatalf("read rejected command: %v", err)
	}
	if rejected.Type != "command_rejected" || rejected.RejectReason != RejectNotAllowed {
		t.Fatalf("expected command_rejected with not_allowed, got %+v", rejected)
	}
}

func TestBrowserWebSocketFailsClosedOnCorruptDurableState(t *testing.T) {
	tmp := t.TempDir()
	storeDir := filepath.Join(tmp, "commands")
	runtimeDir := filepath.Join(tmp, "runtime")

	store, gateway, err := openGatewayAt(t, storeDir, runtimeDir)
	if err != nil {
		t.Fatalf("open gateway: %v", err)
	}

	const personalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ab"
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

	sessions, err := NewHMACUserSessionVerifier(testSecret, "", newTestBrowserSessionRevocationStore())
	if err != nil {
		t.Fatal(err)
	}
	server := newAuthorizedBrowserServer(sessions, gateway, gateway)
	server.AllowedOrigins = []string{"https://web.example"}
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	wsURL := strings.Replace(httpServer.URL, "http", "ws", 1) + "/direct-chat/ws?installation_id=" + testDirectChatInstallationID + "&authority_epoch=1"
	header := http.Header{"Origin": {"https://web.example"}, "Cookie": {BrowserSessionCookie + "=" + signBrowserSession(t, testSecret, userSessionWireClaims{TenantID: "tenant-1", UserID: "user-1", PersonalityAgentID: personalityAgentID, Exp: time.Now().Add(time.Hour).Unix(), Aud: defaultBrowserAudience})}}
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		t.Fatalf("dial browser websocket: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(browserHello{Type: "hello", LastEventSeq: 0}); err != nil {
		t.Fatal(err)
	}

	// After the server attempts to rebuild from the corrupt durable log it must
	// fail closed and close the connection instead of defaulting to an empty
	// "no turn / no approval" state that would admit the next command.
	// The close may win the race with this write. Either outcome is acceptable,
	// but the following read must observe a prompt close rather than a timeout.
	_ = conn.WriteJSON(browserCommandFrame{Type: "command", IdempotencyKey: "ignored", Command: json.RawMessage(`{"type":"abort"}`)})
	assertBrowserConnectionClosedBeforeFrame(t, conn)
}

func TestDecodeBrowserCommandRequiresContractValidIdempotencyKey(t *testing.T) {
	command := `{"type":"user_message","text":"hi","attachments":[]}`
	for name, key := range map[string]string{
		"empty":     "",
		"oversized": strings.Repeat("k", MaxIdempotencyKeyBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			raw, err := json.Marshal(browserCommandFrame{
				Type:           "command",
				IdempotencyKey: key,
				Command:        json.RawMessage(command),
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeBrowserCommand(raw); err == nil {
				t.Fatalf("accepted invalid idempotency key length %d", len(key))
			}
		})
	}
}

func TestBrowserOutboundFramesRejectMalformedContractShapes(t *testing.T) {
	tests := []struct {
		name   string
		raw    string
		target func() any
	}{
		{
			name:   "event missing envelope",
			raw:    `{"type":"event"}`,
			target: func() any { return &browserEventFrame{} },
		},
		{
			name:   "browser event leaks internal target",
			raw:    `{"type":"event","envelope":{"personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","event":{"type":"error","message":"x"}}}`,
			target: func() any { return &browserEventFrame{} },
		},
		{
			name:   "browser event has null seq",
			raw:    `{"type":"event","envelope":{"seq":null,"event":{"type":"error","message":"x"}}}`,
			target: func() any { return &browserEventFrame{} },
		},
		{
			name:   "accepted missing correlation key",
			raw:    `{"type":"command_accepted","command_id":"00000000-0000-4000-8000-000000000001","seq":1}`,
			target: func() any { return &browserCommandAcceptedFrame{} },
		},
		{
			name:   "accepted unknown field",
			raw:    `{"type":"command_accepted","idempotency_key":"key","command_id":"00000000-0000-4000-8000-000000000001","seq":1,"extra":true}`,
			target: func() any { return &browserCommandAcceptedFrame{} },
		},
		{
			name:   "accepted disposition command mismatch",
			raw:    `{"type":"command_accepted","idempotency_key":"key","command_id":"00000000-0000-4000-8000-000000000001","seq":1,"disposition":{"type":"command_disposition","command_id":"00000000-0000-4000-8000-000000000002","command_seq":1,"status":"applied"}}`,
			target: func() any { return &browserCommandAcceptedFrame{} },
		},
		{
			name:   "accepted disposition sequence mismatch",
			raw:    `{"type":"command_accepted","idempotency_key":"key","command_id":"00000000-0000-4000-8000-000000000001","seq":1,"disposition":{"type":"command_disposition","command_id":"00000000-0000-4000-8000-000000000001","command_seq":2,"status":"applied"}}`,
			target: func() any { return &browserCommandAcceptedFrame{} },
		},
		{
			name:   "accepted disposition is nonterminal",
			raw:    `{"type":"command_accepted","idempotency_key":"key","command_id":"00000000-0000-4000-8000-000000000001","seq":1,"disposition":{"type":"command_disposition","command_id":"00000000-0000-4000-8000-000000000001","command_seq":1,"status":"received"}}`,
			target: func() any { return &browserCommandAcceptedFrame{} },
		},
		{
			name:   "accepted disposition is null",
			raw:    `{"type":"command_accepted","idempotency_key":"key","command_id":"00000000-0000-4000-8000-000000000001","seq":1,"disposition":null}`,
			target: func() any { return &browserCommandAcceptedFrame{} },
		},
		{
			name:   "accepted disposition has unknown field",
			raw:    `{"type":"command_accepted","idempotency_key":"key","command_id":"00000000-0000-4000-8000-000000000001","seq":1,"disposition":{"type":"command_disposition","command_id":"00000000-0000-4000-8000-000000000001","command_seq":1,"status":"applied","extra":true}}`,
			target: func() any { return &browserCommandAcceptedFrame{} },
		},
		{
			name:   "rejected missing correlation key",
			raw:    `{"type":"command_rejected","reject_reason":"schema_violation"}`,
			target: func() any { return &browserCommandRejectedFrame{} },
		},
		{
			name:   "rejected unknown reason",
			raw:    `{"type":"command_rejected","idempotency_key":"key","reject_reason":"other"}`,
			target: func() any { return &browserCommandRejectedFrame{} },
		},
		{
			name:   "status unknown value",
			raw:    `{"type":"direct_chat_status","status":"connecting"}`,
			target: func() any { return &directChatStatusFrame{} },
		},
		{
			name:   "status unknown field",
			raw:    `{"type":"direct_chat_status","status":"ready","extra":true}`,
			target: func() any { return &directChatStatusFrame{} },
		},
		{
			name:   "ready status carries unavailable reason",
			raw:    `{"type":"direct_chat_status","status":"ready","reason":"stopped"}`,
			target: func() any { return &directChatStatusFrame{} },
		},
		{
			name:   "status unknown reason",
			raw:    `{"type":"direct_chat_status","status":"unavailable","reason":"restarting"}`,
			target: func() any { return &directChatStatusFrame{} },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := json.Unmarshal([]byte(test.raw), test.target()); err == nil {
				t.Fatalf("accepted malformed browser frame: %s", test.raw)
			}
		})
	}

	var volatile browserEventFrame
	if err := json.Unmarshal(
		[]byte(`{"type":"event","envelope":{"event":{"type":"error","message":"x"}}}`),
		&volatile,
	); err != nil {
		t.Fatalf("valid target-free volatile browser event rejected: %v", err)
	}
	if volatile.Envelope.Seq != nil {
		t.Fatalf("volatile browser event gained seq: %+v", volatile)
	}
	var unavailable directChatStatusFrame
	if err := json.Unmarshal(
		[]byte(`{"type":"direct_chat_status","status":"unavailable"}`),
		&unavailable,
	); err != nil {
		t.Fatalf("valid unavailable status rejected: %v", err)
	}
	for _, status := range []string{"applied", "superseded", "rejected"} {
		rejectReason := ""
		if status == "rejected" {
			rejectReason = `,"reject_reason":"not_allowed"`
		}
		raw := fmt.Sprintf(
			`{"type":"command_accepted","idempotency_key":"key","command_id":"00000000-0000-4000-8000-000000000001","seq":1,"disposition":{"type":"command_disposition","command_id":"00000000-0000-4000-8000-000000000001","command_seq":1,"status":%q%s}}`,
			status,
			rejectReason,
		)
		var accepted browserCommandAcceptedFrame
		if err := json.Unmarshal([]byte(raw), &accepted); err != nil {
			t.Fatalf("valid %s accepted disposition rejected: %v", status, err)
		}
	}
}

type failingDirectChatSpawner struct {
	err error
}

func (s *failingDirectChatSpawner) EnsureRunning(context.Context, string) error {
	return s.err
}

func (*failingDirectChatSpawner) Touch(string) {}

// A pre-upgrade 503 is unreadable to a page: the browser reports only a close
// without a status, exactly as it does for an expired session, a disallowed
// origin, or an offline network. The API therefore accepts the upgrade this
// authorized session already earned and names the cause in the close frame.
func TestBrowserWebSocketNamesAnUnstartableRuntimeInTheCloseFrame(t *testing.T) {
	gateway := openRuntimeGateway(t)
	sessions, err := NewHMACUserSessionVerifier(
		testSecret,
		"",
		newTestBrowserSessionRevocationStore(),
	)
	if err != nil {
		t.Fatal(err)
	}
	server := newAuthorizedBrowserServer(sessions, gateway, gateway)
	server.AllowedOrigins = []string{browserAuthTestOrigin}
	server.Spawner = &failingDirectChatSpawner{
		err: errors.New("supervisor prepare failed"),
	}
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	session, err := sessions.IssueSession(context.Background(), UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	wsURL := strings.Replace(httpServer.URL, "http", "ws", 1) +
		"/direct-chat/ws?installation_id=" + testDirectChatInstallationID + "&authority_epoch=1"
	conn, response, err := websocket.DefaultDialer.Dial(wsURL, http.Header{
		"Origin": {browserAuthTestOrigin},
		"Cookie": {BrowserSessionCookie + "=" + session},
	})
	if err != nil {
		t.Fatalf("upgrade must be accepted so the cause can be read: err=%v response=%v", err, response)
	}
	defer conn.Close()
	if response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade status=%d, want 101", response.StatusCode)
	}

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, _, readErr := conn.ReadMessage()
	closeErr, ok := readErr.(*websocket.CloseError)
	if !ok {
		t.Fatalf("read after unstartable runtime = %v, want a close frame", readErr)
	}
	if closeErr.Code != DirectChatRuntimeUnavailableCloseCode {
		t.Fatalf("close code=%d, want %d", closeErr.Code, DirectChatRuntimeUnavailableCloseCode)
	}
	if closeErr.Text != DirectChatRuntimeUnavailableCloseReason {
		t.Fatalf("close reason=%q, want %q", closeErr.Text, DirectChatRuntimeUnavailableCloseReason)
	}
	waitForBrowserConnectionStats(
		t,
		server,
		BrowserConnectionStats{Active: 0, Accepted: 1},
	)
}

// A rejection the browser cannot attribute must stay unattributed: these fail
// before the upgrade, so the page sees only a closed socket and must not blame
// the agent runtime.
func TestBrowserWebSocketRejectsUnauthorizedSessionsBeforeAnyCloseCode(t *testing.T) {
	gateway := openRuntimeGateway(t)
	sessions, err := NewHMACUserSessionVerifier(
		testSecret,
		"",
		newTestBrowserSessionRevocationStore(),
	)
	if err != nil {
		t.Fatal(err)
	}
	server := newAuthorizedBrowserServer(sessions, gateway, gateway)
	server.AllowedOrigins = []string{browserAuthTestOrigin}
	server.Spawner = &countingDirectChatSpawner{}
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	wsURL := strings.Replace(httpServer.URL, "http", "ws", 1) +
		"/direct-chat/ws?installation_id=" + testDirectChatInstallationID + "&authority_epoch=1"
	for _, test := range []struct {
		name       string
		origin     string
		session    string
		wantStatus int
	}{
		{
			name:       "invalid session",
			origin:     browserAuthTestOrigin,
			session:    "not-a-session",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "disallowed origin",
			origin:     "https://evil.example",
			session:    "not-a-session",
			wantStatus: http.StatusForbidden,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn, response, dialErr := websocket.DefaultDialer.Dial(wsURL, http.Header{
				"Origin": {test.origin},
				"Cookie": {BrowserSessionCookie + "=" + test.session},
			})
			if conn != nil {
				conn.Close()
			}
			if dialErr == nil {
				t.Fatal("unauthorized dial was upgraded")
			}
			if response == nil || response.StatusCode != test.wantStatus {
				t.Fatalf("response=%v, want status %d", response, test.wantStatus)
			}
			if response.Body != nil {
				defer response.Body.Close()
			}
			// No close frame exists on this path, so nothing can carry the
			// runtime diagnosis and the page must not infer one.
			if _, ok := response.Header["Sec-Websocket-Accept"]; ok {
				t.Fatal("unauthorized dial completed a handshake")
			}
		})
	}
}
