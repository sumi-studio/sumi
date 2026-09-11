package feedback

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
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
	d := &Diagnostics{Version: 1, CapturedAt: time.Now(), Viewport: DiagnosticViewport{Width: 390, Height: 800, Scale: 1, PixelRatio: 3}, ServerObservation: &ServerObservation{CapturedAt: time.Now(), Status: "available", Generation: "7", ReadinessReason: "ready"}, ClientEvents: []ClientEvent{{At: time.Now(), Kind: "click", Path: "/direct", Summary: "Feedback"}}, Annotations: []DiagnosticAnnotation{testDiagnosticAnnotation()}}
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
	d.Annotations[0].Label = strings.Repeat("写", 201)
	if d.valid() {
		t.Fatal("unbounded annotation label accepted")
	}
	var payload struct {
		Diagnostics *Diagnostics `json:"diagnostics"`
	}
	raw := `{"diagnostics":{"server_observation":{"status":"available","bearer":"hidden"}}}`
	if decode(httptest.NewRequest("POST", "/", strings.NewReader(raw)), &payload) == nil {
		t.Fatal("server observation allowed authority field")
	}
	d.Annotations[0].Label = "選択"
	encoded, err := json.Marshal(d)
	if err != nil || len(encoded) > 32<<10 {
		t.Fatal(err)
	}
}

func testDiagnosticAnnotation() DiagnosticAnnotation {
	return DiagnosticAnnotation{DiagnosticSelection: DiagnosticSelection{Tag: "button", Selector: "#send", Label: "送信", CapturedAt: time.Date(2026, 9, 12, 1, 2, 3, 0, time.UTC), Rect: DiagnosticRect{X: 10, Y: 20, Width: 100, Height: 40}}, Number: 1, Kind: "element", Path: "/direct", ScrollY: 200, ViewportWidth: 390, ViewportHeight: 800}
}

func TestDiagnosticAnnotationsValidateReferencesAndRegionGeometry(t *testing.T) {
	base := func() *Diagnostics {
		return &Diagnostics{Version: 1, CapturedAt: time.Now(), Viewport: DiagnosticViewport{Width: 390, Height: 800, Scale: 1, PixelRatio: 3}, Annotations: []DiagnosticAnnotation{testDiagnosticAnnotation()}}
	}
	for _, test := range []struct {
		name   string
		change func(*Diagnostics)
	}{
		{"duplicate number", func(d *Diagnostics) { d.Annotations = append(d.Annotations, d.Annotations[0]) }},
		{"zero number", func(d *Diagnostics) { d.Annotations[0].Number = 0 }},
		{"large number", func(d *Diagnostics) { d.Annotations[0].Number = 10000 }},
		{"unknown kind", func(d *Diagnostics) { d.Annotations[0].Kind = "attachment" }},
		{"query", func(d *Diagnostics) { d.Annotations[0].Path = "/direct?token=secret" }},
		{"fragment", func(d *Diagnostics) { d.Annotations[0].Path = "/direct#secret" }},
		{"absolute URL", func(d *Diagnostics) { d.Annotations[0].Path = "https://example.com" }},
		{"large path", func(d *Diagnostics) { d.Annotations[0].Path = "/" + strings.Repeat("x", 512) }},
		{"zero region width", func(d *Diagnostics) { d.Annotations[0].Kind = "region"; d.Annotations[0].Rect.Width = 0 }},
		{"zero region height", func(d *Diagnostics) { d.Annotations[0].Kind = "region"; d.Annotations[0].Rect.Height = 0 }},
		{"negative size", func(d *Diagnostics) { d.Annotations[0].Rect.Width = -1 }},
		{"large size", func(d *Diagnostics) { d.Annotations[0].Rect.Height = 1e8 + 1 }},
		{"nonfinite coordinate", func(d *Diagnostics) { d.Annotations[0].Rect.X = math.NaN() }},
		{"large scroll", func(d *Diagnostics) { d.Annotations[0].ScrollY = 1e8 + 1 }},
		{"large viewport", func(d *Diagnostics) { d.Annotations[0].ViewportWidth = 100001 }},
		{"negative viewport", func(d *Diagnostics) { d.Annotations[0].ViewportHeight = -1 }},
		{"missing timestamp", func(d *Diagnostics) { d.Annotations[0].CapturedAt = time.Time{} }},
		{"large selector", func(d *Diagnostics) { d.Annotations[0].Selector = strings.Repeat("x", 513) }},
		{"large label", func(d *Diagnostics) { d.Annotations[0].Label = strings.Repeat("写", 201) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			d := base()
			test.change(d)
			if d.valid() {
				t.Fatal("invalid annotation accepted")
			}
		})
	}
	d := base()
	d.Annotations[0].Number = 9999
	d.Annotations[0].Label = strings.Repeat("写", 200)
	for i := 1; i < 10; i++ {
		annotation := testDiagnosticAnnotation()
		annotation.Number = i
		annotation.Kind = "region"
		annotation.Tag = ""
		annotation.Selector = ""
		d.Annotations = append(d.Annotations, annotation)
	}
	if !d.valid() {
		t.Fatal("ten uniquely numbered element/region annotations rejected")
	}
	d.Annotations = append(d.Annotations, testDiagnosticAnnotation())
	d.Annotations[10].Number = 100
	if d.valid() {
		t.Fatal("eleven annotations accepted")
	}
}

func TestDiagnosticAnnotationsWireAndImmutableRetry(t *testing.T) {
	w := fixture(t)
	ctx := context.Background()
	region := testDiagnosticAnnotation()
	region.Number = 2
	region.Kind = "region"
	region.Tag = ""
	region.Selector = ""
	region.Rect = DiagnosticRect{X: 50, Y: 60, Width: 240, Height: 160}
	d := &Diagnostics{Version: 1, CapturedAt: time.Now().UTC(), Viewport: DiagnosticViewport{Width: 390, Height: 800, Scale: 1, PixelRatio: 3}, Annotations: []DiagnosticAnnotation{testDiagnosticAnnotation(), region}}
	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Diagnostics *Diagnostics `json:"diagnostics"`
	}
	if err = decode(httptest.NewRequest("POST", "/feedback/threads", bytes.NewReader(append(append([]byte(`{"diagnostics":`), raw...), '}'))), &payload); err != nil || !payload.Diagnostics.valid() {
		t.Fatal("flat annotations wire rejected", err)
	}
	for _, invalid := range []string{`{"diagnostics":{"selection":{}}}`, `{"diagnostics":{"annotations":[{"attachment_id":"untrusted"}]}}`} {
		if err = decode(httptest.NewRequest("POST", "/feedback/threads", strings.NewReader(invalid)), &payload); err == nil {
			t.Fatal("unknown annotation/retired selection field accepted")
		}
	}
	nonce := uuid.NewString()
	thread, err := w.s.Create(ctx, w.human, "Numbered targets", "Please inspect #1 and #2", nonce, d)
	if err != nil {
		t.Fatal(err)
	}
	detail, err := w.s.Open(ctx, w.dev, thread.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := json.Marshal(detail.Thread.Diagnostics)
	if err != nil || !bytes.Equal(raw, persisted) {
		t.Fatal("annotation references/geometry changed in storage", err)
	}
	retry, err := w.s.Create(ctx, w.human, "Numbered targets", "Please inspect #1 and #2", nonce, d)
	if err != nil {
		t.Fatal(err)
	}
	retried, err := json.Marshal(retry.Diagnostics)
	if err != nil || !bytes.Equal(raw, retried) {
		t.Fatal("retry changed annotations", err)
	}
	d.Annotations[1].Number = 3
	if _, err = w.s.Create(ctx, w.human, "Numbered targets", "Please inspect #1 and #2", nonce, d); !errors.Is(err, ErrRequest) {
		t.Fatal("retry replaced numbered references", err)
	}
}
