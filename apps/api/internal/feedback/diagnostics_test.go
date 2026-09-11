package feedback

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/testfs"
)

func TestDiagnosticBoundary(t *testing.T) {
	var payload struct {
		Diagnostics *Diagnostics `json:"diagnostics"`
	}
	request := httptest.NewRequest("POST", "/feedback/threads", strings.NewReader(`{"diagnostics":{"cookie":"secret"}}`))
	if decode(request, &payload) == nil {
		t.Fatal("unexpected diagnostic fields accepted")
	}
	diagnostic := &Diagnostics{Version: 1, CapturedAt: time.Now(), Viewport: DiagnosticViewport{Width: 390, Height: 800, Scale: 1, PixelRatio: 3}}
	if !diagnostic.valid() {
		t.Fatal("valid snapshot rejected")
	}
	diagnostic.Source = &DiagnosticSource{Path: "/direct?token=secret", CapturedAt: time.Now()}
	if diagnostic.valid() {
		t.Fatal("URL query accepted")
	}
	diagnostic.Source = nil
	diagnostic.Browser = strings.Repeat("x", 1025)
	if diagnostic.valid() {
		t.Fatal("unbounded diagnostics accepted")
	}
}

type diagnosticSessions struct {
	sessions
	paid string
}

func (s diagnosticSessions) VerifySession(_ context.Context, cookie string) (agentevents.UserSessionClaims, error) {
	return agentevents.UserSessionClaims{UserID: cookie, PersonalityAgentID: s.paid}, nil
}

func TestDiagnosticCaptureUsesSessionPAAndSurvivesUnavailableRuntime(t *testing.T) {
	w := fixture(t)
	commands, err := agentevents.OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = commands.Close() })
	gateway, err := agentevents.OpenDurableGateway(testfs.PrivateDir(t), commands)
	if err != nil {
		t.Fatal(err)
	}
	receipt := "private-hydration-receipt"
	if err = gateway.PublishRuntimeState(w.pa.ID, 9, &receipt); err != nil {
		t.Fatal(err)
	}
	s := &Server{Store: w.s, Gateway: gateway, Sessions: diagnosticSessions{paid: w.pa.ID}, AllowedOrigins: []string{"https://sumi.test"}}
	mux := http.NewServeMux()
	s.RegisterRoutes(mux)
	request := func(body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/feedback/diagnostics", strings.NewReader(body))
		r.Header.Set("Origin", "https://sumi.test")
		r.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: w.human.ID})
		res := httptest.NewRecorder()
		mux.ServeHTTP(res, r)
		return res
	}
	if res := request(`{"personality_agent_id":"` + w.stranger.ID + `"}`); res.Code != 400 {
		t.Fatal("caller selected diagnostic PA", res.Code)
	}
	res := request(`{}`)
	var observation ServerObservation
	if err = json.Unmarshal(res.Body.Bytes(), &observation); err != nil || res.Code != 200 || observation.PersonalityAgentID != w.pa.ID || observation.Generation != "9" || observation.Ready == nil || !*observation.Ready || observation.CapturedAt.IsZero() {
		t.Fatal("invalid diagnostic capture", res.Code, res.Body.String(), err)
	}
	if strings.Contains(res.Body.String(), receipt) {
		t.Fatal("private runtime receipt leaked")
	}
	s.Gateway = nil
	res = request(`{}`)
	if err = json.Unmarshal(res.Body.Bytes(), &observation); err != nil || res.Code != 200 || observation.Status != "unavailable" {
		t.Fatal("runtime failure blocked observation", res.Code, res.Body.String())
	}
	s.Sessions = diagnosticSessions{sessions: sessions{revoked: true}, paid: w.pa.ID}
	if res = request(`{}`); res.Code != 401 {
		t.Fatal("revoked diagnostic capture", res.Code)
	}
	s.Sessions = diagnosticSessions{paid: w.pa.ID}
	if _, err = w.pool.Exec(context.Background(), `UPDATE app_installations SET enabled=false WHERE owner_kind='human' AND owner_id=$1 AND app_id='feedback'`, w.human.ID); err != nil {
		t.Fatal(err)
	}
	if res = request(`{}`); res.Code != 403 {
		t.Fatal("disabled Feedback captured diagnostics", res.Code)
	}
}

func TestDiagnosticObservationAndBrowserEvidenceBoundary(t *testing.T) {
	d := &Diagnostics{Version: 1, CapturedAt: time.Now(), Viewport: DiagnosticViewport{Width: 390, Height: 800, Scale: 1, PixelRatio: 3}, ServerObservation: &ServerObservation{CapturedAt: time.Now(), Status: "available", Generation: "7", ReadinessReason: "ready"}, ClientEvents: []ClientEvent{{At: time.Now(), Kind: "click", Path: "/direct", Summary: "Feedback"}}, Selection: &DiagnosticSelection{Tag: "button", Selector: "button", Label: "送信", CapturedAt: time.Now(), Rect: DiagnosticRect{X: 10, Y: 20, Width: 100, Height: 40}}}
	if !d.valid() {
		t.Fatal("valid bounded evidence rejected")
	}
	d.ServerObservation.Generation = "7junk"
	if d.valid() {
		t.Fatal("invalid server observation accepted")
	}
	d.ServerObservation.Generation = "7"
	d.ClientEvents[0].Path = "/direct?token=secret"
	if d.valid() {
		t.Fatal("event query accepted")
	}
	d.ClientEvents[0].Path = "/direct"
	d.ClientEvents[0].Kind = "input"
	if d.valid() {
		t.Fatal("input observation accepted")
	}
	d.ClientEvents[0].Kind = "click"
	d.Selection.Label = strings.Repeat("写", 201)
	if d.valid() {
		t.Fatal("unbounded selection label accepted")
	}
	var payload struct {
		Diagnostics *Diagnostics `json:"diagnostics"`
	}
	raw := `{"diagnostics":{"server_observation":{"status":"available","bearer":"hidden"}}}`
	if decode(httptest.NewRequest("POST", "/", strings.NewReader(raw)), &payload) == nil {
		t.Fatal("server observation allowed authority field")
	}
	d.Selection.Label = "選択"
	encoded, err := json.Marshal(d)
	if err != nil || len(encoded) > 32<<10 {
		t.Fatal(err)
	}
}
