package agentevents

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewProductionMux_NilStoreReturnsError(t *testing.T) {
	store, err := OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatalf("open command store: %v", err)
	}
	defer store.Close()
	gateway, err := OpenDurableGateway(privateRuntimeDir(t), store)
	if err != nil {
		t.Fatalf("open durable gateway: %v", err)
	}

	_, _, _, err = NewProductionMux(nil, gateway, nil, nil, nil, nil, nil)
	if err == nil {
		t.Fatal("expected NewProductionMux to return an error for nil store")
	}
	if !errors.Is(err, errCommandAppenderRequired) {
		t.Fatalf("expected errCommandAppenderRequired, got %v", err)
	}
}

func TestNewProductionMux_WiresBrowserOriginPolicyToCommandIngress(t *testing.T) {
	store, err := OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatalf("open command store: %v", err)
	}
	defer store.Close()
	gateway, err := OpenDurableGateway(privateRuntimeDir(t), store)
	if err != nil {
		t.Fatalf("open durable gateway: %v", err)
	}

	mux, _, _, err := NewProductionMux(
		store,
		gateway,
		nil,
		&fakeSessionVerifier{},
		nil,
		[]string{testBrowserOrigin},
		nil,
	)
	if err != nil {
		t.Fatalf("new production mux: %v", err)
	}

	for _, tc := range []struct {
		name       string
		origin     string
		wantStatus int
	}{
		{name: "allowed reaches session boundary", origin: testBrowserOrigin, wantStatus: http.StatusUnauthorized},
		{name: "disallowed", origin: "https://evil.example", wantStatus: http.StatusForbidden},
		{name: "missing", wantStatus: http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(
				http.MethodPost,
				"/direct-chat/commands",
				strings.NewReader(`{"type":"user_message","text":"hi","attachments":[]}`),
			)
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
			}
			recorder := httptest.NewRecorder()

			mux.ServeHTTP(recorder, req)

			if recorder.Code != tc.wantStatus {
				t.Fatalf("got status %d, want %d", recorder.Code, tc.wantStatus)
			}
		})
	}
}
