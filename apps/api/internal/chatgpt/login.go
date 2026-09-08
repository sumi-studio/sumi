package chatgpt

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
)

type LoginIdentity struct {
	HumanID   string
	SessionID string
	// Authorize revalidates the initiating session while executing a side effect.
	Authorize func(context.Context, func(context.Context) error) error
}
type loginStore interface {
	Connect(context.Context, string, Credentials, Selection) (Status, error)
	Status(context.Context, string) (Status, error)
	Disconnect(context.Context, string) error
	SetSelection(context.Context, string, string, Selection) error
}
type deviceClient interface {
	BeginDevice(context.Context) (DeviceLogin, error)
	PollDevice(context.Context, DeviceLogin) (Credentials, bool, error)
}
type connectionView struct {
	Status
	Activation string `json:"activation"`
}
type loginView struct {
	LoginID         string          `json:"loginId"`
	VerificationURL string          `json:"verificationUrl,omitempty"`
	UserCode        string          `json:"userCode,omitempty"`
	ExpiresAt       time.Time       `json:"expiresAt"`
	IntervalMs      int64           `json:"intervalMs"`
	Status          string          `json:"status"`
	Error           string          `json:"error,omitempty"`
	Connection      *connectionView `json:"connection,omitempty"`
}
type loginFlow struct {
	view     loginView
	identity LoginIdentity
	device   DeviceLogin
	cancel   context.CancelFunc
}
type LoginService struct {
	store          loginStore
	oauth          deviceClient
	authenticate   func(*http.Request) (LoginIdentity, error)
	applySelection func(string) // nonblocking runtime activation enqueue
	mu             sync.Mutex   // serializes commits with disconnect/cancel; never held during OAuth requests
	flows          map[string]*loginFlow
	closed         bool
	workers        sync.WaitGroup
}

func NewLoginService(store *Store, oauth *OAuthClient, authenticate func(*http.Request) (LoginIdentity, error), applySelection func(string)) *LoginService {
	return &LoginService{store: store, oauth: oauth, authenticate: authenticate, applySelection: applySelection, flows: make(map[string]*loginFlow)}
}
func (s *LoginService) notifySelection(humanID string) {
	if s.applySelection != nil {
		s.applySelection(humanID)
	}
}
func (s *LoginService) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/model-connections/chatgpt", s.status)
	mux.HandleFunc("DELETE /api/model-connections/chatgpt", s.disconnect)
	mux.HandleFunc("POST /api/model-connections/chatgpt/login", s.start)
	mux.HandleFunc("GET /api/model-connections/chatgpt/login/{id}", s.flowStatus)
	mux.HandleFunc("DELETE /api/model-connections/chatgpt/login/{id}", s.cancelFlow)
	mux.HandleFunc("PUT /api/model-connections/chatgpt/model", s.model)
}
func (s *LoginService) Close() {
	s.mu.Lock()
	s.closed = true
	for _, f := range s.flows {
		if f.view.Status == "pending" {
			f.view.Status = "cancelled"
			f.cancel()
		}
	}
	s.mu.Unlock()
	s.workers.Wait()
}
func loginJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func loginError(w http.ResponseWriter, status int, message string) {
	loginJSON(w, status, map[string]any{"error": map[string]string{"message": message}})
}
func (s *LoginService) identity(w http.ResponseWriter, r *http.Request) (LoginIdentity, bool) {
	if s.authenticate == nil {
		loginError(w, 401, "Sign in to Sumi to continue")
		return LoginIdentity{}, false
	}
	id, err := s.authenticate(r)
	if err != nil || id.HumanID == "" || id.SessionID == "" || id.Authorize == nil {
		loginError(w, 401, "Sign in to Sumi to continue")
		return LoginIdentity{}, false
	}
	return id, true
}
func (s *LoginService) status(w http.ResponseWriter, r *http.Request) {
	id, ok := s.identity(w, r)
	if !ok {
		return
	}
	status, err := s.store.Status(r.Context(), id.HumanID)
	if err != nil {
		loginError(w, 503, "Could not read ChatGPT connection")
		return
	}
	loginJSON(w, 200, struct {
		Status
		Activation string `json:"activation"`
	}{status, "next_start"})
}
func (s *LoginService) prune() {
	for id, f := range s.flows {
		if time.Now().After(f.view.ExpiresAt) {
			f.cancel()
			delete(s.flows, id)
		}
	}
}
func (s *LoginService) start(w http.ResponseWriter, r *http.Request) {
	identity, ok := s.identity(w, r)
	if !ok {
		return
	}
	deadline := time.Now().Add(15 * time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	flow := &loginFlow{view: loginView{LoginID: uuid.NewString(), ExpiresAt: deadline, Status: "pending"}, identity: identity, cancel: cancel}
	s.mu.Lock()
	s.prune()
	err := identity.Authorize(r.Context(), func(context.Context) error {
		if s.closed || len(s.flows) >= 512 {
			return ErrLoginFailed
		}
		for _, old := range s.flows {
			if old.identity.HumanID == identity.HumanID && old.view.Status == "pending" {
				old.view.Status = "cancelled"
				old.cancel()
			}
		}
		s.flows[flow.view.LoginID] = flow
		return nil
	})
	s.mu.Unlock()
	if err != nil {
		cancel()
		loginError(w, 503, "Could not start ChatGPT login")
		return
	}
	device, err := s.oauth.BeginDevice(r.Context())
	s.mu.Lock()
	if flow.view.Status != "pending" || s.closed || ctx.Err() != nil {
		cancel()
		s.mu.Unlock()
		loginError(w, 409, "ChatGPT login was cancelled")
		return
	}
	if err != nil {
		flow.view.Status = "failed"
		flow.view.Error = "Could not start ChatGPT login"
		if errors.Is(err, ErrDeviceLoginUnavailable) {
			flow.view.Error = "ChatGPTのデバイスコードログインを利用できません。ChatGPTの設定で有効になっているか確認して、もう一度お試しください。"
		}
		message := flow.view.Error
		cancel()
		s.mu.Unlock()
		loginError(w, 502, message)
		return
	}
	if device.interval <= 0 {
		device.interval = 5 * time.Second
	}
	if device.ExpiresAt.After(deadline) {
		device.ExpiresAt = deadline
	}
	flow.device = device
	flow.view.VerificationURL = device.VerificationURL
	flow.view.UserCode = device.UserCode
	flow.view.IntervalMs = device.interval.Milliseconds()
	flow.view.ExpiresAt = device.ExpiresAt
	view := flow.view
	s.workers.Add(1)
	go s.poll(ctx, flow)
	s.mu.Unlock()
	loginJSON(w, 200, view)
}
func (s *LoginService) poll(ctx context.Context, f *loginFlow) {
	defer s.workers.Done()
	defer f.cancel()
	for {
		timer := time.NewTimer(f.device.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			s.finish(f, "expired", "")
			return
		case <-timer.C:
		}
		if !time.Now().Before(f.device.ExpiresAt) {
			s.finish(f, "expired", "")
			return
		}
		credentials, pending, err := s.oauth.PollDevice(ctx, f.device)
		if err != nil {
			if ctx.Err() != nil {
				s.finish(f, "expired", "")
			} else {
				s.finish(f, "failed", "ChatGPT login failed; please try again")
			}
			return
		}
		if pending {
			continue
		}
		s.mu.Lock()
		if f.view.Status != "pending" || s.closed || ctx.Err() != nil || s.flows[f.view.LoginID] != f {
			s.mu.Unlock()
			return
		}
		var status Status
		err = f.identity.Authorize(ctx, func(authCtx context.Context) error {
			var e error
			status, e = s.store.Connect(authCtx, f.identity.HumanID, credentials, Selection{Model: "gpt-6-astra", Effort: "medium"})
			return e
		})
		if err != nil {
			f.view.Status = "failed"
			f.view.Error = "ChatGPT login could not be saved; sign in to Sumi and try again"
		} else {
			f.view.Status = "completed"
			f.view.Connection = &connectionView{Status: status, Activation: "next_start"}
		}
		s.mu.Unlock()
		if err == nil {
			s.notifySelection(f.identity.HumanID)
		}
		return
	}
}
func (s *LoginService) finish(f *loginFlow, status, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f.view.Status == "pending" {
		f.view.Status = status
		f.view.Error = message
	}
}
func (s *LoginService) find(id LoginIdentity, key string) *loginFlow {
	f := s.flows[key]
	if f == nil || f.identity.HumanID != id.HumanID || f.identity.SessionID != id.SessionID {
		return nil
	}
	return f
}
func (s *LoginService) flowStatus(w http.ResponseWriter, r *http.Request) {
	id, ok := s.identity(w, r)
	if !ok {
		return
	}
	s.mu.Lock()
	f := s.find(id, r.PathValue("id"))
	if f == nil {
		s.mu.Unlock()
		loginError(w, 404, "ChatGPT login not found")
		return
	}
	if f.view.Status == "pending" && !time.Now().Before(f.view.ExpiresAt) {
		f.view.Status = "expired"
		f.cancel()
	}
	view := f.view
	s.mu.Unlock()
	loginJSON(w, 200, view)
}
func (s *LoginService) cancelFlow(w http.ResponseWriter, r *http.Request) {
	id, ok := s.identity(w, r)
	if !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.find(id, r.PathValue("id"))
	if f == nil {
		loginError(w, 404, "ChatGPT login not found")
		return
	}
	err := id.Authorize(r.Context(), func(context.Context) error {
		if f.view.Status == "pending" {
			f.view.Status = "cancelled"
			f.cancel()
		}
		return nil
	})
	if err != nil {
		loginError(w, 401, "Sign in to Sumi to continue")
		return
	}
	loginJSON(w, 200, f.view)
}
func (s *LoginService) disconnect(w http.ResponseWriter, r *http.Request) {
	id, ok := s.identity(w, r)
	if !ok {
		return
	}
	s.mu.Lock()
	err := id.Authorize(r.Context(), func(ctx context.Context) error {
		for _, f := range s.flows {
			if f.identity.HumanID == id.HumanID && f.view.Status == "pending" {
				f.view.Status = "cancelled"
				f.cancel()
			}
		}
		return s.store.Disconnect(ctx, id.HumanID)
	})
	s.mu.Unlock()
	if err != nil {
		loginError(w, 503, "Could not disconnect ChatGPT")
		return
	}
	s.notifySelection(id.HumanID)
	loginJSON(w, 200, struct {
		Status
		Activation string `json:"activation"`
	}{Status{}, "next_start"})
}
func (s *LoginService) model(w http.ResponseWriter, r *http.Request) {
	id, ok := s.identity(w, r)
	if !ok {
		return
	}
	var request struct {
		ConnectionID string `json:"connectionId"`
		Model        string `json:"model"`
		Effort       string `json:"effort"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil || request.ConnectionID == "" || request.Model != "gpt-6-astra" || !validSelection(Selection{request.Model, request.Effort}) {
		loginError(w, 400, "Choose a supported ChatGPT model and effort")
		return
	}
	err := id.Authorize(r.Context(), func(ctx context.Context) error {
		return s.store.SetSelection(ctx, id.HumanID, request.ConnectionID, Selection{request.Model, request.Effort})
	})
	if errors.Is(err, ErrNotConnected) {
		loginError(w, 409, "ChatGPT connection changed; reload and try again")
		return
	}
	if err != nil {
		loginError(w, 503, "Could not update ChatGPT model")
		return
	}
	s.notifySelection(id.HumanID)
	s.status(w, r)
}
