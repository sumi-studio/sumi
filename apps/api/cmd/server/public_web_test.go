package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

func TestPublicWebRegistrationRequiresLocalControlAuthentication(t *testing.T) {
	store, err := agentevents.OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	gateway, err := agentevents.OpenDurableGateway(privateRuntimeDir(t), store)
	if err != nil {
		t.Fatal(err)
	}
	control, err := agentevents.NewLocalControlServer(gateway, []byte("public-web-test-signing-secret-32-bytes"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = registerPublicWeb(control); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	if err = control.RegisterRoutes(mux); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	request := httptest.NewRequest("POST", "/public-web/read", strings.NewReader(`{"url":"https://127.0.0.1/"}`))
	request.RemoteAddr = "127.0.0.1:10000"
	mux.ServeHTTP(w, request)
	if w.Code != http.StatusUnauthorized {
		t.Fatal(w.Code, w.Body.String())
	}
	if err = registerPublicWeb(nil); err != nil {
		t.Fatal(err)
	}
}
