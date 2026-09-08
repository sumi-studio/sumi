package chatgpt

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeLoginStore struct {
	mu      sync.Mutex
	status  Status
	commits int
}

func (s *fakeLoginStore) Connect(_ context.Context, _ string, _ Credentials, selection Selection) (Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commits++
	s.status = Status{Connected: true, ConnectionID: "connection", AccountID: "account", Selection: selection}
	return s.status, nil
}
func (s *fakeLoginStore) Status(context.Context, string) (Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status, nil
}
func (s *fakeLoginStore) Disconnect(context.Context, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = Status{}
	return nil
}
func (s *fakeLoginStore) SetSelection(context.Context, string, string, Selection) error { return nil }

type fakeDeviceClient struct {
	entered chan struct{}
	release chan struct{}
}

func (c *fakeDeviceClient) BeginDevice(context.Context) (DeviceLogin, error) {
	return DeviceLogin{VerificationURL: "https://auth.openai.com/codex/device", UserCode: "ABCD", ExpiresAt: time.Now().Add(time.Minute), deviceAuthID: "never-expose-device-auth", interval: time.Millisecond}, nil
}
func (c *fakeDeviceClient) PollDevice(ctx context.Context, _ DeviceLogin) (Credentials, bool, error) {
	close(c.entered)
	select {
	case <-c.release:
	case <-ctx.Done():
	}
	// Deliberately return tokens even after cancellation to test the commit fence.
	return Credentials{AccessToken: "never-expose-access", RefreshToken: "never-expose-refresh"}, false, nil
}
func loginFixture(t *testing.T) (*LoginService, *http.ServeMux, *fakeLoginStore, *fakeDeviceClient, *atomic.Bool) {
	t.Helper()
	store := &fakeLoginStore{}
	client := &fakeDeviceClient{entered: make(chan struct{}), release: make(chan struct{})}
	valid := &atomic.Bool{}
	valid.Store(true)
	service := &LoginService{store: store, oauth: client, flows: make(map[string]*loginFlow), authenticate: func(r *http.Request) (LoginIdentity, error) {
		human, session := r.Header.Get("Test-Human"), r.Header.Get("Test-Session")
		if human == "" {
			human = owner
		}
		if session == "" {
			session = "session"
		}
		return LoginIdentity{HumanID: human, SessionID: session, Authorize: func(ctx context.Context, effect func(context.Context) error) error {
			if !valid.Load() {
				return errors.New("logged out")
			}
			return effect(ctx)
		}}, nil
	}}
	mux := http.NewServeMux()
	service.RegisterRoutes(mux)
	t.Cleanup(service.Close)
	return service, mux, store, client, valid
}
func loginRequest(mux *http.ServeMux, method, path, body, human, session string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Test-Human", human)
	req.Header.Set("Test-Session", session)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}
func beginLogin(t *testing.T, mux *http.ServeMux) loginView {
	t.Helper()
	response := loginRequest(mux, "POST", "/api/model-connections/chatgpt/login", "", "", "")
	if response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	var v loginView
	if err := json.Unmarshal(response.Body.Bytes(), &v); err != nil {
		t.Fatal(err)
	}
	return v
}
func waitLogin(t *testing.T, mux *http.ServeMux, id string) loginView {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var v loginView
		response := loginRequest(mux, "GET", "/api/model-connections/chatgpt/login/"+id, "", "", "")
		if err := json.Unmarshal(response.Body.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		if v.Status != "pending" {
			return v
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("login did not finish")
	return loginView{}
}
func TestLoginSessionOwnershipAndSafeCompletion(t *testing.T) {
	_, mux, store, client, _ := loginFixture(t)
	flow := beginLogin(t, mux)
	<-client.entered
	for _, identity := range [][2]string{{other, "session"}, {owner, "different-session"}} {
		response := loginRequest(mux, "GET", "/api/model-connections/chatgpt/login/"+flow.LoginID, "", identity[0], identity[1])
		if response.Code != 404 {
			t.Fatal("foreign session read login")
		}
		response = loginRequest(mux, "DELETE", "/api/model-connections/chatgpt/login/"+flow.LoginID, "", identity[0], identity[1])
		if response.Code != 404 {
			t.Fatal("foreign session cancelled login")
		}
	}
	close(client.release)
	done := waitLogin(t, mux, flow.LoginID)
	if done.Status != "completed" || done.Connection == nil || done.Connection.Model != "gpt-6-astra" || done.Connection.Effort != "medium" {
		t.Fatalf("completion=%+v", done)
	}
	status := loginRequest(mux, "GET", "/api/model-connections/chatgpt", "", "", "")
	if strings.Contains(status.Body.String(), "never-expose") || !strings.Contains(status.Body.String(), "next_start") {
		t.Fatal("unsafe/incomplete status", status.Body.String())
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.commits != 1 {
		t.Fatal("unexpected connect count")
	}
}
func TestCancelledDisconnectedOrLoggedOutLoginCannotCommit(t *testing.T) {
	for _, action := range []string{"cancel", "disconnect", "logout"} {
		t.Run(action, func(t *testing.T) {
			service, mux, store, client, valid := loginFixture(t)
			flow := beginLogin(t, mux)
			<-client.entered
			switch action {
			case "cancel":
				loginRequest(mux, "DELETE", "/api/model-connections/chatgpt/login/"+flow.LoginID, "", "", "")
			case "disconnect":
				loginRequest(mux, "DELETE", "/api/model-connections/chatgpt", "", "", "")
			case "logout":
				valid.Store(false)
			}
			close(client.release)
			done := waitLogin(t, mux, flow.LoginID)
			if done.Status == "completed" {
				t.Fatal("cancelled/revoked login completed")
			}
			service.Close()
			store.mu.Lock()
			defer store.mu.Unlock()
			if store.commits != 0 {
				t.Fatal("connection resurrected after cancellation/logout")
			}
		})
	}
}

func TestSelectionNotificationRunsAfterCommitAndOutsideLoginLock(t *testing.T) {
	service, mux, store, client, valid := loginFixture(t)
	notified := make(chan string, 4)
	service.applySelection = func(human string) {
		if !service.mu.TryLock() {
			t.Error("activation callback ran under login mutex")
		} else {
			service.mu.Unlock()
		}
		notified <- human
	}
	flow := beginLogin(t, mux)
	<-client.entered
	close(client.release)
	waitLogin(t, mux, flow.LoginID)
	select {
	case human := <-notified:
		if human != owner {
			t.Fatal("wrong activation owner")
		}
	case <-time.After(time.Second):
		t.Fatal("connect not enqueued")
	}
	store.mu.Lock()
	connected := store.status.Connected
	store.mu.Unlock()
	if !connected {
		t.Fatal("activation before connect")
	}
	response := loginRequest(mux, "PUT", "/api/model-connections/chatgpt/model", `{"connectionId":"connection","model":"gpt-6-astra","effort":"low"}`, "", "")
	if response.Code != 200 {
		t.Fatal(response.Body.String())
	}
	select {
	case <-notified:
	default:
		t.Fatal("selection not enqueued")
	}
	valid.Store(false)
	loginRequest(mux, "DELETE", "/api/model-connections/chatgpt", "", "", "")
	select {
	case <-notified:
		t.Fatal("unauthorized effect enqueued")
	default:
	}
	valid.Store(true)
	response = loginRequest(mux, "DELETE", "/api/model-connections/chatgpt", "", "", "")
	if response.Code != 200 {
		t.Fatal(response.Body.String())
	}
	select {
	case <-notified:
	default:
		t.Fatal("disconnect not enqueued")
	}
	store.mu.Lock()
	connected = store.status.Connected
	store.mu.Unlock()
	if connected {
		t.Fatal("activation before disconnect")
	}
}

type unavailableDeviceClient struct{}

func (unavailableDeviceClient) BeginDevice(context.Context) (DeviceLogin, error) {
	return DeviceLogin{}, ErrDeviceLoginUnavailable
}
func (unavailableDeviceClient) PollDevice(context.Context, DeviceLogin) (Credentials, bool, error) {
	panic("unavailable login polled")
}
func TestUnavailableDeviceLoginExplainsSetting(t *testing.T) {
	service, mux, _, _, _ := loginFixture(t)
	service.oauth = unavailableDeviceClient{}
	response := loginRequest(mux, "POST", "/api/model-connections/chatgpt/login", "", "", "")
	if response.Code != 502 || !strings.Contains(response.Body.String(), "ChatGPTの設定") {
		t.Fatal("missing actionable login error", response.Body.String())
	}
}
