package agentevents

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/directchat"
)

// The /terminal/* surface authorizes through the participant-owned
// 'terminal' AppInstallation via TerminalAuthorizer — never through the
// direct-chat installation. These tests pin the boundary at the HTTP/WS
// edge: only TerminalAuthorizer is consulted, nil fails closed, denials
// map to the shapes the Web client expects, and every mutation frame
// re-authorizes.

const testTerminalInstallationID = "0198f0f4-9b72-7000-8000-0000000000a1"
const testTerminalScopeQuery = "installation_id=" + testTerminalInstallationID + "&authority_epoch=1"
const testTerminalPersonaID = "018f47a2-9b3c-7def-8abc-0123456789ab"

var testTerminalSecret = []byte("terminal-route-session-secret-32by")

// mutableTerminalAuthorizer records every call and denies unless the
// presented installation/epoch match what the terminal app resolved.
type mutableTerminalAuthorizer struct {
	mu             sync.RWMutex
	allowed        bool
	installationID string
	authorityEpoch int64
	calls          int
	lastHuman      string
	lastAgent      string
}

func (a *mutableTerminalAuthorizer) AuthorizeTerminal(
	_ context.Context,
	humanID,
	agentID,
	installationID string,
	authorityEpoch int64,
) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	a.lastHuman, a.lastAgent = humanID, agentID
	if !a.allowed ||
		(a.installationID != "" && a.installationID != installationID) ||
		(a.authorityEpoch != 0 && a.authorityEpoch != authorityEpoch) {
		return ErrDirectChatAuthorizationDenied
	}
	return nil
}

func (a *mutableTerminalAuthorizer) setAllowed(allowed bool) {
	a.mu.Lock()
	a.allowed = allowed
	a.mu.Unlock()
}

func (a *mutableTerminalAuthorizer) callCount() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.calls
}

// denyDirectChatAuthorizer proves the terminal path never consults the
// direct-chat authority: wiring it changes nothing.
type denyDirectChatAuthorizer struct{ calls int }

func (a *denyDirectChatAuthorizer) AuthorizeDirectChat(
	context.Context, string, string, string, int64,
) error {
	a.calls++
	return ErrDirectChatAuthorizationDenied
}

// fakeTerminalBackend is a controllable in-memory TerminalBackend.
type fakeTerminalBackend struct {
	mu         sync.Mutex
	sessions   map[string]agentstate.TerminalSession
	inputs     []agentstate.TerminalInput
	nextInput  int64
	listErr    error
	createErr  error
	getErr     error
	readErr    error
	inputErr   error
	controlErr error
	closeErr   error
	readChunks []agentstate.TerminalOutputChunk
}

func newFakeTerminalBackend() *fakeTerminalBackend {
	return &fakeTerminalBackend{sessions: map[string]agentstate.TerminalSession{}}
}

func (b *fakeTerminalBackend) add(session agentstate.TerminalSession) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sessions[session.SessionID] = session
}

func (b *fakeTerminalBackend) ListTerminalSessions(_ context.Context, personaID string) ([]agentstate.TerminalSession, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.listErr != nil {
		return nil, b.listErr
	}
	out := []agentstate.TerminalSession{}
	for _, s := range b.sessions {
		if s.PersonaID == personaID {
			out = append(out, s)
		}
	}
	return out, nil
}

func (b *fakeTerminalBackend) CreateTerminalSession(_ context.Context, personaID, name, requestedBy, createdBy string) (agentstate.TerminalSession, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.createErr != nil {
		return agentstate.TerminalSession{}, b.createErr
	}
	s := agentstate.TerminalSession{
		SessionID: "0198f0f4-9b72-7000-8000-00000000cc01", PersonaID: personaID,
		Name: name, Mode: "pty", Backend: "cloud", Status: "requested",
		RequestedBy: requestedBy, CreatedBy: createdBy,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	b.sessions[s.SessionID] = s
	return s, nil
}

func (b *fakeTerminalBackend) GetTerminalSession(_ context.Context, personaID, sessionID string) (agentstate.TerminalSession, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.getErr != nil {
		return agentstate.TerminalSession{}, b.getErr
	}
	s, ok := b.sessions[sessionID]
	if !ok || s.PersonaID != personaID {
		return agentstate.TerminalSession{}, agentstate.ErrTerminalNotFound
	}
	return s, nil
}

func (b *fakeTerminalBackend) ReadTerminalOutput(_ context.Context, personaID, sessionID string, cursor int64, _ int) (agentstate.TerminalOutputRead, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.readErr != nil {
		return agentstate.TerminalOutputRead{}, b.readErr
	}
	s, ok := b.sessions[sessionID]
	if !ok || s.PersonaID != personaID {
		return agentstate.TerminalOutputRead{}, agentstate.ErrTerminalNotFound
	}
	// Same filter as the store: only chunks extending past the cursor.
	var chunks []agentstate.TerminalOutputChunk
	next := cursor
	for _, c := range b.readChunks {
		end := c.Base + int64(len(c.Data))
		if c.Kind == "gap" && c.GapTo != nil {
			end = *c.GapTo
		}
		if end > cursor {
			chunks = append(chunks, c)
		}
		if end > next {
			next = end
		}
	}
	return agentstate.TerminalOutputRead{
		Session:    s,
		Cursor:     cursor,
		NextCursor: next,
		Chunks:     chunks,
	}, nil
}

func (b *fakeTerminalBackend) SubmitTerminalInput(_ context.Context, personaID, sessionID, source, kind string, payload map[string]any) (agentstate.TerminalInput, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.inputErr != nil {
		return agentstate.TerminalInput{}, b.inputErr
	}
	s, ok := b.sessions[sessionID]
	if !ok || s.PersonaID != personaID {
		return agentstate.TerminalInput{}, agentstate.ErrTerminalNotFound
	}
	b.nextInput++
	in := agentstate.TerminalInput{
		InputID: fmt.Sprintf("0198f0f4-9b72-7000-8000-00000000dd%02d", b.nextInput), SessionID: sessionID,
		SessionEpoch: s.Epoch, Seq: b.nextInput, Kind: kind, Payload: payload, Source: source,
		Status: "intended", CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	b.inputs = append(b.inputs, in)
	return in, nil
}

func (b *fakeTerminalBackend) ListTerminalInputs(_ context.Context, personaID, sessionID string, afterSeq int64, _ int) ([]agentstate.TerminalInput, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.sessions[sessionID]; !ok || b.sessions[sessionID].PersonaID != personaID {
		return nil, agentstate.ErrTerminalNotFound
	}
	var out []agentstate.TerminalInput
	for _, in := range b.inputs {
		if in.SessionID == sessionID && in.Seq > afterSeq {
			out = append(out, in)
		}
	}
	return out, nil
}

func (b *fakeTerminalBackend) SetTerminalControl(_ context.Context, personaID, sessionID string, hold bool, _ time.Duration) (agentstate.TerminalSession, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.controlErr != nil {
		return agentstate.TerminalSession{}, b.controlErr
	}
	s, ok := b.sessions[sessionID]
	if !ok || s.PersonaID != personaID {
		return agentstate.TerminalSession{}, agentstate.ErrTerminalNotFound
	}
	if hold {
		s.ControlHolder = "human"
	} else {
		s.ControlHolder = ""
	}
	b.sessions[sessionID] = s
	return s, nil
}

func (b *fakeTerminalBackend) CloseTerminalSession(_ context.Context, personaID, sessionID, reason string) (agentstate.TerminalSession, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closeErr != nil {
		return agentstate.TerminalSession{}, b.closeErr
	}
	s, ok := b.sessions[sessionID]
	if !ok || s.PersonaID != personaID {
		return agentstate.TerminalSession{}, agentstate.ErrTerminalNotFound
	}
	s.Status = "ending"
	s.EndReason = reason
	b.sessions[sessionID] = s
	return s, nil
}

func newTerminalBrowserFixture(t *testing.T) (
	*HMACUserSessionVerifier,
	*BrowserServer,
	*fakeTerminalBackend,
	*mutableTerminalAuthorizer,
	*http.ServeMux,
) {
	t.Helper()
	verifier, err := NewHMACUserSessionVerifier(testTerminalSecret, "", newTestBrowserSessionRevocationStore())
	if err != nil {
		t.Fatal(err)
	}
	backend := newFakeTerminalBackend()
	authorizer := &mutableTerminalAuthorizer{
		allowed:        true,
		installationID: testTerminalInstallationID,
		authorityEpoch: 1,
	}
	s := NewBrowserServer(verifier, nil, nil)
	s.AllowedOrigins = []string{"https://sumi.example"}
	s.Terminals = backend
	s.TerminalAuthorizer = authorizer
	s.SetLifecycleFence(directchat.NewLifecycleFence())
	mux := http.NewServeMux()
	s.RegisterTerminalRoutes(mux)
	return verifier, s, backend, authorizer, mux
}

func terminalCookie(t *testing.T, v *HMACUserSessionVerifier) *http.Cookie {
	t.Helper()
	signed, err := v.IssueSession(context.Background(), UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-terminal",
		PersonalityAgentID: testTerminalPersonaID,
	}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return &http.Cookie{Name: BrowserSessionCookie, Value: signed}
}

func terminalDo(t *testing.T, mux *http.ServeMux, cookie *http.Cookie, method, target, origin string, body *strings.Reader) *httptest.ResponseRecorder {
	t.Helper()
	var reader *strings.Reader
	if body == nil {
		reader = strings.NewReader("")
	} else {
		reader = body
	}
	r := httptest.NewRequest(method, target, reader)
	if cookie != nil {
		r.AddCookie(cookie)
	}
	if origin != "" {
		r.Header.Set("Origin", origin)
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	return w
}

func TestTerminalRESTUsesTerminalAppBoundary(t *testing.T) {
	verifier, server, backend, authorizer, mux := newTerminalBrowserFixture(t)
	// Wire a direct-chat authorizer that denies everything: if the
	// terminal surface ever borrowed it, every request would 403.
	server.Authorizer = &denyDirectChatAuthorizer{}
	cookie := terminalCookie(t, verifier)
	backend.add(agentstate.TerminalSession{
		SessionID: "0198f0f4-9b72-7000-8000-00000000bb01", PersonaID: testTerminalPersonaID,
		Name: "sh", Mode: "pty", Backend: "cloud", Status: "active",
		RequestedBy: "human", CreatedBy: "human", CreatedAt: time.Now(), UpdatedAt: time.Now(),
	})
	q := "?" + testTerminalScopeQuery
	sid := "session_id=0198f0f4-9b72-7000-8000-00000000bb01"

	cases := []struct {
		name   string
		method string
		target string
		body   *strings.Reader
		want   int
	}{
		{"list", http.MethodGet, "/terminal/list" + q, nil, http.StatusOK},
		{"open", http.MethodPost, "/terminal/open" + q, strings.NewReader(`{"name":"sh"}`), http.StatusOK},
		{"session", http.MethodGet, "/terminal/session" + q + "&" + sid, nil, http.StatusOK},
		{"read", http.MethodGet, "/terminal/read" + q + "&" + sid, nil, http.StatusOK},
		{"input", http.MethodPost, "/terminal/input" + q, strings.NewReader(`{"session_id":"0198f0f4-9b72-7000-8000-00000000bb01","kind":"stdin","data":"bHM="}`), http.StatusOK},
		{"control", http.MethodPost, "/terminal/control" + q, strings.NewReader(`{"session_id":"0198f0f4-9b72-7000-8000-00000000bb01","hold":true}`), http.StatusOK},
		{"close", http.MethodPost, "/terminal/close" + q, strings.NewReader(`{"session_id":"0198f0f4-9b72-7000-8000-00000000bb01"}`), http.StatusOK},
	}
	for _, tc := range cases {
		w := terminalDo(t, mux, cookie, tc.method, tc.target, "", tc.body)
		if w.Code != tc.want {
			t.Fatalf("%s: %d %s, want %d", tc.name, w.Code, w.Body.String(), tc.want)
		}
	}
	if authorizer.callCount() != len(cases) {
		t.Fatalf("terminal authorizer calls = %d, want %d", authorizer.callCount(), len(cases))
	}
	if got := server.Authorizer.(*denyDirectChatAuthorizer).calls; got != 0 {
		t.Fatalf("direct-chat authorizer consulted %d times for terminal routes", got)
	}
}

func TestTerminalRoutesFailClosedWithoutTerminalAuthorizer(t *testing.T) {
	verifier, server, _, _, mux := newTerminalBrowserFixture(t)
	server.TerminalAuthorizer = nil
	server.Authorizer = allowDirectChatAuthorizer{}
	cookie := terminalCookie(t, verifier)
	for _, target := range []string{
		"/terminal/list?" + testTerminalScopeQuery,
		"/terminal/ws?" + testTerminalScopeQuery + "&session_id=0198f0f4-9b72-7000-8000-00000000bb01",
	} {
		w := terminalDo(t, mux, cookie, http.MethodGet, target, "", nil)
		if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "authorization unavailable") {
			t.Fatalf("%s without TerminalAuthorizer: %d %s", target, w.Code, w.Body.String())
		}
	}
}

func TestTerminalRESTRefusals(t *testing.T) {
	verifier, _, _, _, mux := newTerminalBrowserFixture(t)
	cookie := terminalCookie(t, verifier)

	w := terminalDo(t, mux, cookie, http.MethodGet,
		"/terminal/list?installation_id=0198f0f4-9b72-7000-8000-00000000ff99&authority_epoch=1", "", nil)
	if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "not authorized") {
		t.Fatalf("wrong installation: %d %s", w.Code, w.Body.String())
	}
	w = terminalDo(t, mux, cookie, http.MethodGet,
		"/terminal/list?installation_id="+testTerminalInstallationID+"&authority_epoch=2", "", nil)
	if w.Code != http.StatusForbidden {
		t.Fatalf("stale epoch: %d %s", w.Code, w.Body.String())
	}
	w = terminalDo(t, mux, cookie, http.MethodGet,
		"/terminal/list?"+testTerminalScopeQuery, "https://evil.example", nil)
	if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "origin not allowed") {
		t.Fatalf("cross-origin: %d %s", w.Code, w.Body.String())
	}
	w = terminalDo(t, mux, nil, http.MethodGet, "/terminal/list?"+testTerminalScopeQuery, "", nil)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("missing session: %d", w.Code)
	}
	// A valid session for a different persona authorizes (the authorizer
	// binds claims to the installation), but the derived persona sees no
	// sessions — the route never accepts a caller-chosen persona.
	other, err := verifier.IssueSession(context.Background(), UserSessionClaims{
		TenantID: "tenant-1", UserID: "user-other",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789cc",
	}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	w = terminalDo(t, mux, &http.Cookie{Name: BrowserSessionCookie, Value: other},
		http.MethodGet, "/terminal/list?"+testTerminalScopeQuery, "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("other persona list: %d %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"sessions":[]`) && strings.Contains(w.Body.String(), "0198f0f4-9b72-7000-8000-00000000bb01") {
		t.Fatalf("other persona must not see foreign sessions: %s", w.Body.String())
	}
}

func TestTerminalErrorShapes(t *testing.T) {
	verifier, _, backend, _, mux := newTerminalBrowserFixture(t)
	cookie := terminalCookie(t, verifier)
	sid := "0198f0f4-9b72-7000-8000-00000000bb02"
	q := "?" + testTerminalScopeQuery + "&session_id=" + sid

	cases := []struct {
		name    string
		inject  func()
		method  string
		target  string
		body    *strings.Reader
		want    int
		wantMsg string
	}{
		{"not found", func() { backend.getErr = agentstate.ErrTerminalNotFound },
			http.MethodGet, "/terminal/session" + q, nil, http.StatusNotFound, "terminal session not found"},
		{"ended", func() { backend.getErr = agentstate.ErrTerminalEnded },
			http.MethodGet, "/terminal/session" + q, nil, http.StatusConflict, "terminal session has ended"},
		{"not live", func() { backend.getErr = agentstate.ErrTerminalNotLive },
			http.MethodGet, "/terminal/session" + q, nil, http.StatusConflict, "terminal session is not live"},
		{"control held", func() { backend.getErr = agentstate.ErrTerminalControl },
			http.MethodGet, "/terminal/session" + q, nil, http.StatusConflict, "terminal control is held"},
		{"capacity", func() { backend.createErr = agentstate.ErrTerminalCapacity },
			http.MethodPost, "/terminal/open?" + testTerminalScopeQuery, strings.NewReader(`{}`), http.StatusTooManyRequests, "too many live terminal sessions"},
		{"backend unavailable", func() { backend.createErr = agentstate.ErrTerminalBackend },
			http.MethodPost, "/terminal/open?" + testTerminalScopeQuery, strings.NewReader(`{}`), http.StatusServiceUnavailable, "terminal backend unavailable"},
	}
	for _, tc := range cases {
		backend.getErr, backend.createErr = nil, nil
		tc.inject()
		w := terminalDo(t, mux, cookie, tc.method, tc.target, "", tc.body)
		if w.Code != tc.want || !strings.Contains(w.Body.String(), tc.wantMsg) {
			t.Fatalf("%s: %d %s, want %d %q", tc.name, w.Code, w.Body.String(), tc.want, tc.wantMsg)
		}
	}
}

func TestTerminalWSUsesTerminalBoundary(t *testing.T) {
	verifier, server, backend, authorizer, mux := newTerminalBrowserFixture(t)
	server.Authorizer = &denyDirectChatAuthorizer{}
	server.AuthorizationPollInterval = 20 * time.Millisecond
	cookie := terminalCookie(t, verifier)
	session := agentstate.TerminalSession{
		SessionID: "0198f0f4-9b72-7000-8000-00000000bb01", PersonaID: testTerminalPersonaID,
		Name: "sh", Mode: "pty", Backend: "cloud", Status: "active",
		RequestedBy: "human", CreatedBy: "human", CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	backend.add(session)

	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()
	wsURL := strings.Replace(httpServer.URL, "http", "ws", 1) +
		"/terminal/ws?" + testTerminalScopeQuery + "&session_id=" + session.SessionID
	header := http.Header{}
	header.Add("Cookie", cookie.String())
	header.Add("Origin", "https://sumi.example")
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		t.Fatalf("terminal ws dial: %v (resp=%v)", err, resp)
	}
	defer conn.Close()

	readFrame := func() map[string]any {
		t.Helper()
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		var frame map[string]any
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read frame: %v", err)
		}
		return frame
	}

	if f := readFrame(); f["type"] != "session" {
		t.Fatalf("first frame = %v, want session", f)
	}
	// stdin frame re-authorizes and lands in the ledger as 'intended'.
	if err := conn.WriteJSON(map[string]any{
		"type": "stdin", "data": base64.StdEncoding.EncodeToString([]byte("ls\n")),
	}); err != nil {
		t.Fatal(err)
	}
	if f := readFrame(); f["type"] != "input_ack" || f["status"] != "intended" {
		t.Fatalf("stdin ack = %v", f)
	}
	callsAfterInput := authorizer.callCount()
	if callsAfterInput < 2 {
		t.Fatalf("terminal authorizer calls = %d, want attach + input", callsAfterInput)
	}
	// Revoke the installation: the next mutation frame is refused and the
	// periodic recheck drops the socket with an error frame.
	authorizer.setAllowed(false)
	if err := conn.WriteJSON(map[string]any{
		"type": "stdin", "data": base64.StdEncoding.EncodeToString([]byte("id\n")),
	}); err != nil {
		t.Fatal(err)
	}
	sawDenied := false
	for i := 0; i < 20 && !sawDenied; i++ {
		f := readFrame()
		if f["type"] == "error" {
			sawDenied = true
		}
	}
	if !sawDenied {
		t.Fatal("revoked installation must surface an error frame")
	}
	// The socket then closes on the auth recheck.
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			break
		}
	}
	if got := server.Authorizer.(*denyDirectChatAuthorizer).calls; got != 0 {
		t.Fatalf("direct-chat authorizer consulted %d times for terminal ws", got)
	}
}

func TestTerminalWSEmitsEachChunkOnce(t *testing.T) {
	verifier, server, backend, _, mux := newTerminalBrowserFixture(t)
	server.AuthorizationPollInterval = time.Hour
	cookie := terminalCookie(t, verifier)
	backend.add(agentstate.TerminalSession{
		SessionID: "0198f0f4-9b72-7000-8000-00000000bb01", PersonaID: testTerminalPersonaID,
		Status: "active", Mode: "pty", Backend: "cloud",
		OutputBytes: 8, CreatedAt: time.Now(), UpdatedAt: time.Now(),
	})
	backend.readChunks = []agentstate.TerminalOutputChunk{
		{Kind: "data", Base: 0, Data: []byte("hello")},
		{Kind: "data", Base: 5, Data: []byte("!!!")},
	}
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()
	header := http.Header{}
	header.Add("Cookie", cookie.String())
	header.Add("Origin", "https://sumi.example")
	conn, _, err := websocket.DefaultDialer.Dial(
		strings.Replace(httpServer.URL, "http", "ws", 1)+
			"/terminal/ws?"+testTerminalScopeQuery+"&session_id=0198f0f4-9b72-7000-8000-00000000bb01",
		header)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	frames := make(chan map[string]any, 32)
	go func() {
		defer close(frames)
		for {
			var frame map[string]any
			if err := conn.ReadJSON(&frame); err != nil {
				return
			}
			frames <- frame
		}
	}()
	var outputs []map[string]any
	deadline := time.After(900 * time.Millisecond)
loop:
	for {
		select {
		case frame, ok := <-frames:
			if !ok {
				break loop
			}
			if frame["type"] == "output" {
				outputs = append(outputs, frame)
			}
		case <-deadline:
			break loop
		}
	}
	// Two retained chunks, emitted exactly once each — the socket's
	// cursor must advance past emitted bytes, not replay them.
	if len(outputs) != 2 {
		t.Fatalf("output frames = %d, want exactly 2: %v", len(outputs), outputs)
	}
	if outputs[0]["base"].(float64) != 0 || outputs[1]["base"].(float64) != 5 {
		t.Fatalf("output bases = %v", outputs)
	}
}

func TestTerminalWSDoesNotHoldLifecyclePermitForSocketLifetime(t *testing.T) {
	verifier, server, backend, _, mux := newTerminalBrowserFixture(t)
	server.AuthorizationPollInterval = time.Hour // isolate the permit question
	fence := directchat.NewLifecycleFence()
	server.SetLifecycleFence(fence)
	cookie := terminalCookie(t, verifier)
	backend.add(agentstate.TerminalSession{
		SessionID: "0198f0f4-9b72-7000-8000-00000000bb01", PersonaID: testTerminalPersonaID,
		Name: "sh", Mode: "pty", Backend: "cloud", Status: "active",
		RequestedBy: "human", CreatedBy: "human", CreatedAt: time.Now(), UpdatedAt: time.Now(),
	})
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()
	header := http.Header{}
	header.Add("Cookie", cookie.String())
	header.Add("Origin", "https://sumi.example")
	conn, _, err := websocket.DefaultDialer.Dial(
		strings.Replace(httpServer.URL, "http", "ws", 1)+
			"/terminal/ws?"+testTerminalScopeQuery+"&session_id=0198f0f4-9b72-7000-8000-00000000bb01",
		header)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	// An install/enable/uninstall mutation must not wait behind an idle
	// attached socket. Bounded acquisition proves the admission permit
	// was released at upgrade.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	release, err := fence.AcquireMutation(ctx)
	if err != nil {
		t.Fatalf("lifecycle mutation blocked by live terminal socket: %v", err)
	}
	release()
}

func TestTerminalWSAttachRefusals(t *testing.T) {
	verifier, server, backend, _, mux := newTerminalBrowserFixture(t)
	cookie := terminalCookie(t, verifier)
	backend.add(agentstate.TerminalSession{
		SessionID: "0198f0f4-9b72-7000-8000-00000000bb01", PersonaID: testTerminalPersonaID,
		Status: "active", Mode: "pty", Backend: "cloud",
	})
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()
	dial := func(query string, origin string) (*websocket.Conn, *http.Response, error) {
		header := http.Header{}
		header.Add("Cookie", cookie.String())
		if origin != "" {
			header.Add("Origin", origin)
		}
		return websocket.DefaultDialer.Dial(
			strings.Replace(httpServer.URL, "http", "ws", 1)+"/terminal/ws?"+query, header)
	}
	// Wrong installation: refused before upgrade.
	_, resp, err := dial("installation_id=0198f0f4-9b72-7000-8000-00000000ff99&authority_epoch=1&session_id=0198f0f4-9b72-7000-8000-00000000bb01", "https://sumi.example")
	if err == nil || resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("wrong-installation attach: err=%v resp=%v", err, resp)
	}
	// Cross-origin: refused before upgrade.
	_, resp, err = dial(testTerminalScopeQuery+"&session_id=0198f0f4-9b72-7000-8000-00000000bb01", "https://evil.example")
	if err == nil || resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-origin attach: err=%v resp=%v", err, resp)
	}
	// Missing session row for this persona: 404 before upgrade.
	_, resp, err = dial(testTerminalScopeQuery+"&session_id=0198f0f4-9b72-7000-8000-00000000ffff", "https://sumi.example")
	if err == nil || resp == nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("foreign session attach: err=%v resp=%v", err, resp)
	}
	// Revoked browser login: 401 before upgrade.
	server.Sessions = nil
	_, resp, err = dial(testTerminalScopeQuery+"&session_id=0198f0f4-9b72-7000-8000-00000000bb01", "https://sumi.example")
	if err == nil || resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unbound sessions attach: err=%v resp=%v", err, resp)
	}
}
