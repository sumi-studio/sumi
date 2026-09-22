package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
	"github.com/sumi-studio/sumi/apps/api/internal/directchat"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
	workspacecontrol "github.com/sumi-studio/sumi/apps/api/internal/workspace"
)

// TAPI-03: the whole terminal surface composed the way cmd/server wires
// it — production HMAC session adapter on the durable revocation store,
// the production composite authorizer, the actual apps store, and the real
// PostgreSQL agentstate backend behind real HTTP routes and a real
// WebSocket. The only simulated part is the physical runner: its claim/
// disposition/output reports go through the same agentstate methods the
// real termexec writer calls — no PTY is claimed to exist here.

const compositionOrigin = "https://sumi-composed.test"

var compositionSecret = []byte("terminal-composition-secret-32byte")

type composedTerminalWorld struct {
	t         *testing.T
	pool      *pgxpool.Pool
	fence     *directchat.LifecycleFence
	koseki    *koseki.Store
	apps      *applicationapps.Store
	state     *agentstate.Store
	sessions  *agentevents.HMACUserSessionVerifier
	browser   *agentevents.BrowserServer
	mux       *http.ServeMux
	server    *httptest.Server
	humanID   string
	agentID   string
	cookie    *http.Cookie
	gateway   *agentevents.DurableGateway
	cmdStore  *agentevents.CommandStore
	terminals agentevents.TerminalBackend
}

func newComposedTerminalWorld(t *testing.T) *composedTerminalWorld {
	t.Helper()
	pool := kosekiResolverTestPool(t)
	ctx := context.Background()
	fence := directchat.NewLifecycleFence()
	kosekiStore := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1", fence)
	wsStore := workspacecontrol.New(pool)
	appStore := applicationapps.New(pool, wsStore, fence)
	stateStore := agentstate.NewStore(pool)
	stateStore.SetDefaultTerminalBackend("cloud")
	stateStore.SetTerminalBackendAvailable("cloud")

	dir := t.TempDir()
	cmdStore, err := agentevents.OpenCommandStore(dir + "/commands")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmdStore.Close() })
	gateway, err := agentevents.OpenDurableGateway(dir+"/runtime", cmdStore)
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := agentevents.NewHMACUserSessionVerifier(compositionSecret, "", gateway)
	if err != nil {
		t.Fatal(err)
	}

	reg, err := kosekiStore.AutoRegister(ctx, "firebase", "uid-composed-"+uuid.NewString()[:8])
	if err != nil {
		t.Fatalf("auto-register: %v", err)
	}
	if _, _, err := stateStore.EnsurePersona(ctx, reg.AgentID, &reg.HumanID, "Composed"); err != nil {
		t.Fatalf("ensure persona: %v", err)
	}
	signed, err := sessions.IssueSession(ctx, agentevents.UserSessionClaims{
		TenantID: "local", UserID: reg.HumanID, PersonalityAgentID: reg.AgentID,
	}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	authorizer := newDirectChatAuthorizer(pool, kosekiStore, appStore)
	mux, browser, _, err := agentevents.NewProductionMux(
		cmdStore, gateway, nil, sessions,
		nil, []string{compositionOrigin}, authorizer, fence,
	)
	if err != nil {
		t.Fatal(err)
	}
	browser.Terminals = stateStore
	browser.TerminalAuthorizer = authorizer
	browser.AuthorizationPollInterval = 40 * time.Millisecond
	browser.RegisterTerminalRoutes(mux)
	wsServer := workspacecontrol.NewServer(wsStore, appStore, sessions)
	wsServer.AllowedOrigins = []string{compositionOrigin}
	wsServer.RegisterRoutes(mux)

	return &composedTerminalWorld{
		t: t, pool: pool, fence: fence, koseki: kosekiStore, apps: appStore,
		state: stateStore, sessions: sessions, browser: browser, mux: mux,
		humanID: reg.HumanID, agentID: reg.AgentID, gateway: gateway,
		cmdStore: cmdStore, terminals: stateStore,
		cookie: &http.Cookie{Name: agentevents.BrowserSessionCookie, Value: signed},
	}
}

func (w *composedTerminalWorld) serve() {
	w.t.Helper()
	w.server = httptest.NewServer(w.mux)
	w.t.Cleanup(w.server.Close)
}

// secondUser registers a second human+secretary and issues its session.
func (w *composedTerminalWorld) secondUser(t *testing.T) (humanID, agentID string, cookie *http.Cookie) {
	t.Helper()
	reg, err := w.koseki.AutoRegister(context.Background(), "firebase", "uid-composed-other-"+uuid.NewString()[:8])
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.state.EnsurePersona(context.Background(), reg.AgentID, &reg.HumanID, "Other"); err != nil {
		t.Fatal(err)
	}
	signed, err := w.sessions.IssueSession(context.Background(), agentevents.UserSessionClaims{
		TenantID: "local", UserID: reg.HumanID, PersonalityAgentID: reg.AgentID,
	}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return reg.HumanID, reg.AgentID,
		&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: signed}
}

func (w *composedTerminalWorld) request(t *testing.T, method, path string, body any, cookie *http.Cookie, headers map[string]string) (int, map[string]any) {
	t.Helper()
	var reader *strings.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = strings.NewReader(string(raw))
	} else {
		reader = strings.NewReader("")
	}
	req, err := http.NewRequest(method, w.server.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if cookie != nil {
		req.AddCookie(cookie)
	}
	if _, ok := headers["Origin"]; !ok {
		req.Header.Set("Origin", compositionOrigin)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var parsed map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		parsed = map[string]any{}
	}
	return resp.StatusCode, parsed
}

func (w *composedTerminalWorld) participantOwner() map[string]any {
	return map[string]any{
		"kind": "participant",
		"participant": map[string]any{
			"kind": "human", "human_id": w.humanID,
		},
	}
}

// installTerminal drives the normal explicit install path the browser's
// TerminalGate button calls: POST /app-installations → InstallAtOperation.
func (w *composedTerminalWorld) installTerminal(t *testing.T, cookie *http.Cookie) (installationID, epoch string) {
	t.Helper()
	if cookie == nil {
		cookie = w.cookie
	}
	status, body := w.request(t, "POST", "/app-installations", map[string]any{
		"owner": w.participantOwner(), "app_id": "terminal",
		"operation_id": uuid.NewString(),
	}, cookie, nil)
	if status != http.StatusCreated {
		t.Fatalf("install terminal = %d: %v", status, body)
	}
	installationID, _ = body["installation_id"].(string)
	epoch, _ = body["authority_epoch"].(string)
	if installationID == "" || epoch == "" {
		t.Fatalf("install response missing scope: %v", body)
	}
	return installationID, epoch
}

func terminalScope(installationID, epoch string) string {
	return "installation_id=" + installationID + "&authority_epoch=" + epoch
}

func (w *composedTerminalWorld) dialTerminalWS(t *testing.T, scope, sessionID string) *websocket.Conn {
	t.Helper()
	header := http.Header{}
	header.Add("Cookie", w.cookie.String())
	header.Add("Origin", compositionOrigin)
	conn, resp, err := websocket.DefaultDialer.Dial(
		strings.Replace(w.server.URL, "http", "ws", 1)+
			"/terminal/ws?"+scope+"&session_id="+sessionID, header)
	if err != nil {
		body := ""
		if resp != nil {
			body = fmt.Sprintf(" (status %d)", resp.StatusCode)
		}
		t.Fatalf("dial terminal ws%s: %v", body, err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func readWSFrame(t *testing.T, conn *websocket.Conn, timeout time.Duration) map[string]any {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	var frame map[string]any
	if err := conn.ReadJSON(&frame); err != nil {
		t.Fatalf("read ws frame: %v", err)
	}
	return frame
}

func wsFrames(conn *websocket.Conn) chan map[string]any {
	out := make(chan map[string]any, 64)
	go func() {
		defer close(out)
		for {
			var frame map[string]any
			if err := conn.ReadJSON(&frame); err != nil {
				return
			}
			out <- frame
		}
	}()
	return out
}

// TestTerminalComposedInstallAndOperationJourney is TAPI-03's positive
// path: catalog → explicit install → every terminal route under the
// returned binding, with the runner's physical reports driven through the
// real state interface (simulated physical part, labeled inline).
func TestTerminalComposedInstallAndOperationJourney(t *testing.T) {
	w := newComposedTerminalWorld(t)
	w.serve()
	ctx := context.Background()

	// 1. Catalog: the 'terminal' descriptor is participant-installable.
	status, catalog := w.request(t, "GET", "/apps/catalog", nil, w.cookie, nil)
	if status != http.StatusOK {
		t.Fatalf("catalog = %d: %v", status, catalog)
	}
	var terminalDescriptor map[string]any
	for _, a := range catalog["apps"].([]any) {
		app := a.(map[string]any)
		if app["app_id"] == "terminal" {
			terminalDescriptor = app
		}
	}
	if terminalDescriptor == nil {
		t.Fatal("terminal missing from catalog")
	}
	if terminalDescriptor["participant_owner_allowed"] != true {
		t.Fatalf("terminal participant_owner_allowed = %v", terminalDescriptor)
	}

	// 2. The browser's normal explicit install path.
	installationID, epoch := w.installTerminal(t, nil)
	scope := terminalScope(installationID, epoch)

	// 3. Installations list shows it enabled for this participant.
	status, list := w.request(t, "GET",
		"/app-installations?owner_kind=participant&participant_kind=human&owner_id="+w.humanID,
		nil, w.cookie, nil)
	if status != http.StatusOK {
		t.Fatalf("installations = %d: %v", status, list)
	}
	var found bool
	for _, i := range list["installations"].([]any) {
		inst := i.(map[string]any)
		if inst["installation_id"] == installationID && inst["app_id"] == "terminal" && inst["state"] == "enabled" {
			found = true
		}
	}
	if !found {
		t.Fatalf("terminal installation not listed enabled: %v", list)
	}

	// 4. Terminal routes under the exact returned binding.
	status, tl := w.request(t, "GET", "/terminal/list?"+scope, nil, w.cookie, nil)
	if status != http.StatusOK || len(tl["sessions"].([]any)) != 0 {
		t.Fatalf("terminal list = %d: %v", status, tl)
	}
	status, opened := w.request(t, "POST", "/terminal/open?"+scope,
		map[string]any{"name": "build"}, w.cookie, nil)
	if status != http.StatusOK {
		t.Fatalf("terminal open = %d: %v", status, opened)
	}
	session := opened["session"].(map[string]any)
	sessionID, _ := session["session_id"].(string)
	if sessionID == "" || session["status"] != "requested" {
		t.Fatalf("opened session = %v", session)
	}

	// Simulated physical part: the runner claims and reports through the
	// same agentstate methods the real termexec writer calls.
	claimed, _, err := w.state.ClaimTerminalSessions(ctx, w.agentID, "runner-1", "cloud", 5*time.Second, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	runnerEpoch := claimed[0].Epoch
	if _, err := w.state.ReportTerminalStatus(ctx, w.agentID, sessionID, "runner-1", runnerEpoch, "active", "", nil, "", ""); err != nil {
		t.Fatalf("report active: %v", err)
	}
	if _, err := w.state.AppendTerminalOutput(ctx, w.agentID, sessionID, "runner-1", runnerEpoch,
		[]agentstate.TerminalOutputChunk{{Kind: "data", Base: 0, Data: []byte("hello $ ")}}); err != nil {
		t.Fatalf("append output: %v", err)
	}

	// Read returns the retained chunk.
	status, read := w.request(t, "GET",
		"/terminal/read?"+scope+"&session_id="+sessionID, nil, w.cookie, nil)
	if status != http.StatusOK {
		t.Fatalf("terminal read = %d: %v", status, read)
	}
	chunks := read["chunks"].([]any)
	if len(chunks) != 1 || chunks[0].(map[string]any)["data"] != "aGVsbG8gJCA=" {
		t.Fatalf("read chunks = %v", chunks)
	}

	// Input acceptance is durable but NOT delivery.
	status, input := w.request(t, "POST", "/terminal/input?"+scope,
		map[string]any{"session_id": sessionID, "kind": "stdin", "data": "ls -la\n"}, w.cookie, nil)
	if status != http.StatusOK {
		t.Fatalf("terminal input = %d: %v", status, input)
	}
	inputWire := input["input"].(map[string]any)
	inputID, _ := inputWire["input_id"].(string)
	if inputWire["status"] != "intended" {
		t.Fatalf("input status = %v, want intended", inputWire)
	}

	// TAPI-04: the durable ledger is the outcome surface.
	status, inputs := w.request(t, "GET",
		"/terminal/inputs?"+scope+"&session_id="+sessionID, nil, w.cookie, nil)
	if status != http.StatusOK {
		t.Fatalf("terminal inputs = %d: %v", status, inputs)
	}
	rows := inputs["inputs"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["status"] != "intended" {
		t.Fatalf("inputs = %v", rows)
	}

	// Runner dequeues then reports a written outcome — visible to the person.
	pending, err := w.state.PendingTerminalInputs(ctx, w.agentID, sessionID, "runner-1", runnerEpoch)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending inputs: %v %v", pending, err)
	}
	if _, err := w.state.ReportTerminalInputDisposition(ctx, w.agentID, sessionID, inputID, "runner-1", runnerEpoch, "dequeued", nil); err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if _, err := w.state.ReportTerminalInputDisposition(ctx, w.agentID, sessionID, inputID, "runner-1", runnerEpoch, "written", nil); err != nil {
		t.Fatalf("write report: %v", err)
	}
	_, inputs = w.request(t, "GET", "/terminal/inputs?"+scope+"&session_id="+sessionID, nil, w.cookie, nil)
	rows = inputs["inputs"].([]any)
	if rows[0].(map[string]any)["status"] != "written" {
		t.Fatalf("written disposition = %v", rows)
	}

	// Exclusive human control: the secretary's source is refused while held.
	status, ctrl := w.request(t, "POST", "/terminal/control?"+scope,
		map[string]any{"session_id": sessionID, "hold": true}, w.cookie, nil)
	if status != http.StatusOK || ctrl["session"].(map[string]any)["control_holder"] != "human" {
		t.Fatalf("control = %d: %v", status, ctrl)
	}
	if _, err := w.state.SubmitTerminalInput(ctx, w.agentID, sessionID, "agent", "stdin",
		map[string]any{"data": "sudo rm -rf /\n"}); !errors.Is(err, agentstate.ErrTerminalControl) {
		t.Fatalf("agent input while control held = %v", err)
	}
	// The person's own input still lands.
	status, _ = w.request(t, "POST", "/terminal/input?"+scope,
		map[string]any{"session_id": sessionID, "kind": "stdin", "data": "pwd\n"}, w.cookie, nil)
	if status != http.StatusOK {
		t.Fatal("person input refused while holding control")
	}
	status, _ = w.request(t, "POST", "/terminal/control?"+scope,
		map[string]any{"session_id": sessionID, "hold": false}, w.cookie, nil)
	if status != http.StatusOK {
		t.Fatal("release control failed")
	}

	// Close: the session ends and stays observable with its outcome.
	status, closed := w.request(t, "POST", "/terminal/close?"+scope,
		map[string]any{"session_id": sessionID}, w.cookie, nil)
	if status != http.StatusOK {
		t.Fatalf("close = %d: %v", status, closed)
	}
}

// TestTerminalComposedRefusals exercises every denial shape under the
// actual production composition — stale/wrong/disabled/uninstalled app
// binding, revoked login, wrong persona, cross-origin mutation.
func TestTerminalComposedRefusals(t *testing.T) {
	w := newComposedTerminalWorld(t)
	w.serve()
	ctx := context.Background()
	installationID, epoch := w.installTerminal(t, nil)
	scope := terminalScope(installationID, epoch)

	// Missing cookie → 401.
	status, _ := w.request(t, "GET", "/terminal/list?"+scope, nil, nil, nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("no cookie = %d", status)
	}
	// Bad signature → 401.
	badCookie := &http.Cookie{Name: agentevents.BrowserSessionCookie, Value: "forged"}
	status, _ = w.request(t, "GET", "/terminal/list?"+scope, nil, badCookie, nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("forged cookie = %d", status)
	}
	// Cross-origin mutation → 403.
	status, _ = w.request(t, "POST", "/terminal/open?"+scope,
		map[string]any{"name": "x"}, w.cookie, map[string]string{"Origin": "https://evil.example"})
	if status != http.StatusForbidden {
		t.Fatalf("cross-origin = %d", status)
	}
	// Wrong app: the auto-installed direct-chat installation cannot
	// authorize terminal routes.
	actor := participant.Human(w.humanID)
	directChat, err := w.apps.ResolveEnabledInstallation(ctx,
		applicationapps.ParticipantOwner(actor), actor, "direct-chat")
	if err != nil {
		t.Fatalf("resolve direct-chat: %v", err)
	}
	status, _ = w.request(t, "GET",
		"/terminal/list?"+terminalScope(directChat.InstallationID, "1"), nil, w.cookie, nil)
	if status != http.StatusForbidden {
		t.Fatalf("direct-chat installation on terminal route = %d", status)
	}
	// Stale epoch → 403.
	status, _ = w.request(t, "GET", "/terminal/list?"+terminalScope(installationID, "9"), nil, w.cookie, nil)
	if status != http.StatusForbidden {
		t.Fatalf("stale epoch = %d", status)
	}
	// Missing installation → 403 (fail closed, not a 404 leak). The id
	// must be uuidv7-shaped or the scope parser itself rejects earlier.
	status, _ = w.request(t, "GET",
		"/terminal/list?"+terminalScope(uuid.Must(uuid.NewV7()).String(), "1"), nil, w.cookie, nil)
	if status != http.StatusForbidden {
		t.Fatalf("unknown installation = %d", status)
	}

	// Wrong persona: a second user's session cannot borrow the binding —
	// the installation owner is the first human.
	otherHuman, _, otherCookie := w.secondUser(t)
	status, _ = w.request(t, "GET", "/terminal/list?"+scope, nil, otherCookie, nil)
	if status != http.StatusForbidden {
		t.Fatalf("foreign session on borrowed installation = %d", status)
	}
	// And even installing their own terminal app cannot expose the first
	// persona's sessions: the persona comes from their own claims.
	owner := map[string]any{
		"kind":        "participant",
		"participant": map[string]any{"kind": "human", "human_id": otherHuman},
	}
	status, inst := w.request(t, "POST", "/app-installations", map[string]any{
		"owner": owner, "app_id": "terminal", "operation_id": uuid.NewString(),
	}, otherCookie, nil)
	if status != http.StatusCreated {
		t.Fatalf("second user install = %d: %v", status, inst)
	}
	otherScope := terminalScope(inst["installation_id"].(string), inst["authority_epoch"].(string))
	status, otherList := w.request(t, "GET", "/terminal/list?"+otherScope, nil, otherCookie, nil)
	if status != http.StatusOK || len(otherList["sessions"].([]any)) != 0 {
		t.Fatalf("second persona leaked first persona's sessions: %v", otherList)
	}

	// Disabled app → 403 on the same scope; the disable bumps authority
	// so a re-enable mints a new epoch rather than reviving this one.
	status, _ = w.request(t, "PUT", "/app-installations/"+installationID+"/state",
		map[string]any{"state": "disabled"}, w.cookie, nil)
	if status != http.StatusOK {
		t.Fatalf("disable = %d", status)
	}
	status, _ = w.request(t, "GET", "/terminal/list?"+scope, nil, w.cookie, nil)
	if status != http.StatusForbidden {
		t.Fatalf("disabled installation = %d", status)
	}
	status, enabled := w.request(t, "PUT", "/app-installations/"+installationID+"/state",
		map[string]any{"state": "enabled"}, w.cookie, nil)
	if status != http.StatusOK {
		t.Fatalf("enable = %d: %v", status, enabled)
	}
	if enabled["authority_epoch"] == epoch {
		t.Fatal("re-enable did not bump authority epoch")
	}
	status, _ = w.request(t, "GET", "/terminal/list?"+scope, nil, w.cookie, nil)
	if status != http.StatusForbidden {
		t.Fatalf("pre-disable epoch revived = %d", status)
	}
	status, _ = w.request(t, "GET",
		"/terminal/list?"+terminalScope(installationID, enabled["authority_epoch"].(string)),
		nil, w.cookie, nil)
	if status != http.StatusOK {
		t.Fatalf("current epoch refused = %d", status)
	}

	// Revoked login: the durable revocation store turns the session's
	// next verification into a 401 — no window where the cookie still works.
	if _, err := w.sessions.RevokeSession(ctx, w.cookie.Value); err != nil {
		t.Fatalf("revoke session: %v", err)
	}
	status, _ = w.request(t, "GET",
		"/terminal/list?"+terminalScope(installationID, enabled["authority_epoch"].(string)),
		nil, w.cookie, nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("revoked session = %d", status)
	}
	signed, err := w.sessions.IssueSession(ctx, agentevents.UserSessionClaims{
		TenantID: "local", UserID: w.humanID, PersonalityAgentID: w.agentID,
	}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	w.cookie.Value = signed

	// Uninstalled app → the exact binding dies with the row.
	status, _ = w.request(t, "DELETE", "/app-installations/"+installationID, nil, w.cookie, nil)
	if status != http.StatusOK && status != http.StatusNoContent {
		t.Fatalf("uninstall = %d", status)
	}
	status, _ = w.request(t, "GET",
		"/terminal/list?"+terminalScope(installationID, enabled["authority_epoch"].(string)),
		nil, w.cookie, nil)
	if status != http.StatusForbidden {
		t.Fatalf("uninstalled installation = %d", status)
	}
}

// gatedTerminalBackend pauses one backend call while the caller still
// holds the lifecycle operation permit — the TAPI-02 overlap probe.
type gatedTerminalBackend struct {
	inner    agentevents.TerminalBackend
	gate     chan struct{}
	entered  chan string
	gateOnce sync.Once
}

func (g *gatedTerminalBackend) hold(method string) {
	g.gateOnce.Do(func() { close(g.entered) })
	<-g.gate
	_ = method
}

func (g *gatedTerminalBackend) ListTerminalSessions(ctx context.Context, p string) ([]agentstate.TerminalSession, error) {
	return g.inner.ListTerminalSessions(ctx, p)
}
func (g *gatedTerminalBackend) CreateTerminalSession(ctx context.Context, p, n, r, c string) (agentstate.TerminalSession, error) {
	return g.inner.CreateTerminalSession(ctx, p, n, r, c)
}
func (g *gatedTerminalBackend) GetTerminalSession(ctx context.Context, p, s string) (agentstate.TerminalSession, error) {
	return g.inner.GetTerminalSession(ctx, p, s)
}
func (g *gatedTerminalBackend) ReadTerminalOutput(ctx context.Context, p, s string, c, ec int64, l int) (agentstate.TerminalOutputRead, error) {
	return g.inner.ReadTerminalOutput(ctx, p, s, c, ec, l)
}
func (g *gatedTerminalBackend) SubmitTerminalInput(ctx context.Context, p, s, src, k string, pl map[string]any) (agentstate.TerminalInput, error) {
	g.hold("SubmitTerminalInput")
	return g.inner.SubmitTerminalInput(ctx, p, s, src, k, pl)
}
func (g *gatedTerminalBackend) ListTerminalInputs(ctx context.Context, p, s string, a int64, l int) ([]agentstate.TerminalInput, error) {
	return g.inner.ListTerminalInputs(ctx, p, s, a, l)
}
func (g *gatedTerminalBackend) SetTerminalControl(ctx context.Context, p, s string, h bool, l time.Duration) (agentstate.TerminalSession, error) {
	return g.inner.SetTerminalControl(ctx, p, s, h, l)
}
func (g *gatedTerminalBackend) CloseTerminalSession(ctx context.Context, p, s, r string) (agentstate.TerminalSession, error) {
	return g.inner.CloseTerminalSession(ctx, p, s, r)
}

// TestTerminalComposedLifecycleOverlap is TAPI-02 at the route layer: a
// terminal operation paused *after* authorization (holding the fence
// permit) must exclude a concurrent disable of the 'terminal' app; the
// mutation then commits, the epoch dies, and an attached socket's next
// mutating frame is refused before its effect.
func TestTerminalComposedLifecycleOverlap(t *testing.T) {
	w := newComposedTerminalWorld(t)
	w.serve()
	installationID, epoch := w.installTerminal(t, nil)
	scope := terminalScope(installationID, epoch)

	gate := &gatedTerminalBackend{
		inner: w.terminals, gate: make(chan struct{}),
		entered: make(chan string),
	}
	w.browser.Terminals = gate

	// Open a session through the ungated paths first.
	status, opened := w.request(t, "POST", "/terminal/open?"+scope,
		map[string]any{"name": "overlap"}, w.cookie, nil)
	if status != http.StatusOK {
		t.Fatalf("open = %d: %v", status, opened)
	}
	sessionID := opened["session"].(map[string]any)["session_id"].(string)

	// REST overlap: an authorized input paused inside the backend blocks
	// the concurrent disable until the effect lands.
	inputDone := make(chan int, 1)
	go func() {
		code, _ := w.request(t, "POST", "/terminal/input?"+scope,
			map[string]any{"session_id": sessionID, "kind": "stdin", "data": "make\n"},
			w.cookie, nil)
		inputDone <- code
	}()
	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("input never reached the backend")
	}
	disableDone := make(chan int, 1)
	go func() {
		code, _ := w.request(t, "PUT", "/app-installations/"+installationID+"/state",
			map[string]any{"state": "disabled"}, w.cookie, nil)
		disableDone <- code
	}()
	select {
	case code := <-disableDone:
		t.Fatalf("disable crossed in-flight terminal operation: %d", code)
	case <-time.After(60 * time.Millisecond):
	}
	close(gate.gate)
	if code := <-inputDone; code != http.StatusOK {
		t.Fatalf("in-flight input = %d", code)
	}
	if code := <-disableDone; code != http.StatusOK {
		t.Fatalf("disable after release = %d", code)
	}

	// Before/after: the pre-disable scope is dead on REST.
	status, _ = w.request(t, "GET", "/terminal/list?"+scope, nil, w.cookie, nil)
	if status != http.StatusForbidden {
		t.Fatalf("stale scope after disable = %d", status)
	}

	// Re-enable for the socket half of the proof.
	status, enabled := w.request(t, "PUT", "/app-installations/"+installationID+"/state",
		map[string]any{"state": "enabled"}, w.cookie, nil)
	if status != http.StatusOK {
		t.Fatalf("re-enable = %d", status)
	}
	scope2 := terminalScope(installationID, enabled["authority_epoch"].(string))

	// Attach a socket, then pause its mutating frame the same way: the
	// already-attached socket's frame holds a fresh permit, so the
	// disable still waits for the frame's effect.
	gate2 := &gatedTerminalBackend{
		inner: w.terminals, gate: make(chan struct{}),
		entered: make(chan string),
	}
	w.browser.Terminals = gate2
	conn := w.dialTerminalWS(t, scope2, sessionID)
	frames := wsFrames(conn)
	first := <-frames
	if first["type"] != "session" {
		t.Fatalf("first ws frame = %v", first)
	}
	if err := conn.WriteJSON(map[string]any{"type": "stdin", "data": "aGVsbG8="}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-gate2.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("ws stdin frame never reached the backend")
	}
	disable2Done := make(chan int, 1)
	go func() {
		code, _ := w.request(t, "PUT", "/app-installations/"+installationID+"/state",
			map[string]any{"state": "disabled"}, w.cookie, nil)
		disable2Done <- code
	}()
	select {
	case code := <-disable2Done:
		t.Fatalf("disable crossed in-flight ws frame: %d", code)
	case <-time.After(60 * time.Millisecond):
	}
	close(gate2.gate)
	if code := <-disable2Done; code != http.StatusOK {
		t.Fatalf("second disable after release = %d", code)
	}

	// After the disable commits, the attached socket's mutating frames
	// are refused before effect and the periodic recheck closes it.
	_ = conn.WriteJSON(map[string]any{"type": "stdin", "data": "d29ybGQ="})
	sawRefusal := false
	deadline := time.After(3 * time.Second)
	for !sawRefusal {
		select {
		case frame, ok := <-frames:
			if !ok {
				sawRefusal = true
				continue
			}
			if frame["type"] == "error" {
				sawRefusal = true
			}
		case <-deadline:
			t.Fatal("attached socket stayed live after its installation was disabled")
		}
	}
}

// TestTerminalComposedInputOutcomes is TAPI-04's evidence path: a failed
// physical operation and an uncertain delivery are observable through the
// same authorized surface, and 'unknown' inputs are never re-delivered.
func TestTerminalComposedInputOutcomes(t *testing.T) {
	w := newComposedTerminalWorld(t)
	w.serve()
	ctx := context.Background()
	installationID, epoch := w.installTerminal(t, nil)
	scope := terminalScope(installationID, epoch)

	status, opened := w.request(t, "POST", "/terminal/open?"+scope,
		map[string]any{"name": "outcomes"}, w.cookie, nil)
	if status != http.StatusOK {
		t.Fatalf("open = %d: %v", status, opened)
	}
	sessionID := opened["session"].(map[string]any)["session_id"].(string)

	// Simulated physical part: claim under a short lease, then let it
	// lapse so reclaim has to fence the old claim.
	claimed, _, err := w.state.ClaimTerminalSessions(ctx, w.agentID, "runner-1", "cloud", 90*time.Millisecond, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	epoch1 := claimed[0].Epoch

	// Input 1: reported failed by the runner — a definite physical failure.
	status, in1 := w.request(t, "POST", "/terminal/input?"+scope,
		map[string]any{"session_id": sessionID, "kind": "signal", "signal": "INT"}, w.cookie, nil)
	if status != http.StatusOK {
		t.Fatalf("signal input = %d", status)
	}
	id1 := in1["input"].(map[string]any)["input_id"].(string)
	if _, err := w.state.ReportTerminalInputDisposition(ctx, w.agentID, sessionID, id1, "runner-1", epoch1,
		"failed", map[string]any{"reason": "no foreground process group"}); err != nil {
		t.Fatalf("failed report: %v", err)
	}

	// Input 2: dequeued but never dispositioned — the claim then lapses,
	// and the reclaim marks it 'unknown' rather than re-delivering.
	status, in2 := w.request(t, "POST", "/terminal/input?"+scope,
		map[string]any{"session_id": sessionID, "kind": "stdin", "data": "rm -rf build\n"}, w.cookie, nil)
	if status != http.StatusOK {
		t.Fatalf("stdin input = %d", status)
	}
	id2 := in2["input"].(map[string]any)["input_id"].(string)
	if _, err := w.state.ReportTerminalInputDisposition(ctx, w.agentID, sessionID, id2, "runner-1", epoch1, "dequeued", nil); err != nil {
		t.Fatalf("dequeue report: %v", err)
	}
	time.Sleep(150 * time.Millisecond) // let the claim lease lapse
	reclaimed, interrupted, err := w.state.ClaimTerminalSessions(ctx, w.agentID, "runner-2", "cloud", 5*time.Second, 1)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if len(reclaimed) != 1 || len(interrupted) != 1 {
		t.Fatalf("reclaim = claimed %v interrupted %v", reclaimed, interrupted)
	}

	// The person sees the honest outcomes through the authorized route.
	_, inputs := w.request(t, "GET", "/terminal/inputs?"+scope+"&session_id="+sessionID, nil, w.cookie, nil)
	rows := inputs["inputs"].([]any)
	if len(rows) != 2 {
		t.Fatalf("ledger = %v", rows)
	}
	if rows[0].(map[string]any)["status"] != "failed" {
		t.Fatalf("failed input = %v", rows[0])
	}
	if rows[1].(map[string]any)["status"] != "unknown" {
		t.Fatalf("uncertain input = %v, want unknown", rows[1])
	}
	if rows[1].(map[string]any)["input_id"] != id2 {
		t.Fatalf("ledger seq mismatch: %v", rows)
	}

	// The new claim never re-delivers the 'unknown' input.
	pending, err := w.state.PendingTerminalInputs(ctx, w.agentID, sessionID, "runner-2", reclaimed[0].Epoch)
	if err != nil {
		t.Fatalf("pending after reclaim: %v", err)
	}
	for _, p := range pending {
		if p.InputID == id2 {
			t.Fatalf("unknown input was re-queued for delivery: %v", p)
		}
	}

	// after_seq filters like an incremental sync cursor.
	_, inputs = w.request(t, "GET",
		"/terminal/inputs?"+scope+"&session_id="+sessionID+"&after_seq=1", nil, w.cookie, nil)
	rows = inputs["inputs"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["seq"].(float64) != 2 {
		t.Fatalf("after_seq filter = %v", rows)
	}
}
