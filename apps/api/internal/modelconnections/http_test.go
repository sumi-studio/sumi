package modelconnections

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/sumi-studio/sumi/apps/api/internal/chatgpt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestUnavailableAndUnauthenticatedStatus(t *testing.T) {
	service := &Service{Authenticate: func(*http.Request) (chatgpt.LoginIdentity, error) {
		return chatgpt.LoginIdentity{HumanID: "human"}, nil
	}}
	mux := http.NewServeMux()
	service.RegisterRoutes(mux)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/api/model-connections", nil))
	if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal(w.Code)
	}
	service.Authenticate = func(*http.Request) (chatgpt.LoginIdentity, error) {
		return chatgpt.LoginIdentity{}, errors.New("no session")
	}
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/api/model-connections", nil))
	if w.Code != 401 {
		t.Fatal("unauthenticated status", w.Code)
	}
}

func TestHTTPWriteOnlyAndSessionRevocation(t *testing.T) {
	store := fixture(t)
	human := owner
	revoked := false
	service := &Service{Store: store, Authenticate: func(*http.Request) (chatgpt.LoginIdentity, error) {
		return chatgpt.LoginIdentity{HumanID: human, Authorize: func(ctx context.Context, effect func(context.Context) error) error {
			if revoked {
				return errors.New("revoked")
			}
			return effect(ctx)
		}}, nil
	}}
	mux := http.NewServeMux()
	service.RegisterRoutes(mux)
	request := func(method, path, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))
		return w
	}
	w := request("POST", "/api/model-connections/api", `{"name":"mine","preset":"openai-chat","baseUrl":"https://api.example/v1","model":"custom","apiKey":"private-test-key"}`)
	if w.Code != 200 || strings.Contains(w.Body.String(), "private-test-key") {
		t.Fatalf("save response %d", w.Code)
	}
	var c Connection
	if err := json.Unmarshal(w.Body.Bytes(), &c); err != nil {
		t.Fatal(err)
	}
	human = other
	w = request("DELETE", "/api/model-connections/api/"+c.ID, "")
	if w.Code != 404 {
		t.Fatal("cross owner", w.Code)
	}
	human = owner
	revoked = true
	w = request("DELETE", "/api/model-connections/api/"+c.ID, "")
	if w.Code != 503 {
		t.Fatal("revoked session", w.Code)
	}
	if _, err := store.Resolve(context.Background(), owner, c.ID); err != nil {
		t.Fatal("revoked mutation ran", err)
	}
}

func TestHTTPExtraHeadersWriteOnly(t *testing.T) {
	store := fixture(t)
	service := &Service{Store: store, Authenticate: func(*http.Request) (chatgpt.LoginIdentity, error) {
		return chatgpt.LoginIdentity{HumanID: owner, Authorize: func(ctx context.Context, effect func(context.Context) error) error {
			return effect(ctx)
		}}, nil
	}}
	mux := http.NewServeMux()
	service.RegisterRoutes(mux)
	request := func(method, path, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))
		return w
	}
	// Headers save with the key and are never echoed back.
	w := request("POST", "/api/model-connections/api", `{"name":"gw","preset":"anthropic","baseUrl":"https://api.example/v1","model":"m","apiKey":"private-test-key","extraHeaders":{"X-Gateway-Session":"gw-7"}}`)
	if w.Code != 200 || strings.Contains(w.Body.String(), "gw-7") || strings.Contains(w.Body.String(), "private-test-key") {
		t.Fatalf("save echoed secrets: %d %s", w.Code, w.Body.String())
	}
	var c Connection
	if err := json.Unmarshal(w.Body.Bytes(), &c); err != nil {
		t.Fatal(err)
	}
	access, err := store.Resolve(context.Background(), owner, c.ID)
	if err != nil || access.ExtraHeaders["X-Gateway-Session"] != "gw-7" {
		t.Fatalf("headers not sealed with credential: %v %+v", err, access)
	}
	// The list response carries no header material either.
	w = request("GET", "/api/model-connections", "")
	if w.Code != 200 || strings.Contains(w.Body.String(), "gw-7") || strings.Contains(w.Body.String(), "X-Gateway-Session") {
		t.Fatalf("list leaked headers: %s", w.Body.String())
	}
	// Reserved names are refused before anything is stored.
	w = request("POST", "/api/model-connections/api", `{"name":"gw2","preset":"anthropic","baseUrl":"https://api.example/v1","model":"m","apiKey":"k","extraHeaders":{"Authorization":"Bearer evil"}}`)
	if w.Code != 400 {
		t.Fatalf("reserved header accepted: %d", w.Code)
	}
	// Headers without a resubmitted key are refused on update.
	w = request("PUT", "/api/model-connections/api/"+c.ID, `{"name":"gw","preset":"anthropic","baseUrl":"https://api.example/v1","model":"m","extraHeaders":{"X-Tenant":"t"}}`)
	if w.Code != 400 {
		t.Fatalf("keyless header change accepted: %d", w.Code)
	}
}
