package feedback

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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
