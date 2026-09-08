package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/chatgpt"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
)

const chatGPTAccessPath = "/internal/providers/chatgpt/access"

type chatGPTConnectionStore interface {
	Status(context.Context, string) (chatgpt.Status, error)
	ResolveAccess(context.Context, string, string, string, chatgpt.RefreshFunc) (chatgpt.Access, error)
}
type chatGPTEmployerAuthority interface {
	CurrentEmployer(context.Context, string) (string, string, error)
	AuthorizeCurrentHumanEmployer(context.Context, string, string, func() error) error
}
type chatGPTRuntime struct {
	connections chatGPTConnectionStore
	employers   chatGPTEmployerAuthority
	refresh     chatgpt.RefreshFunc
}
type runtimeActivationResolver func(context.Context, string, runtimeprovision.ActivationConfig) (runtimeprovision.ActivationConfig, error)

func chatGPTStoreFromEnv(pool *pgxpool.Pool) (*chatgpt.Store, error) {
	raw := strings.TrimSpace(os.Getenv("SUMI_CHATGPT_CREDENTIAL_KEY"))
	if raw == "" {
		return nil, nil
	}
	key, err := base64.StdEncoding.DecodeString(raw)
	if err != nil || len(key) != 32 {
		return nil, errors.New("SUMI_CHATGPT_CREDENTIAL_KEY must encode 32 bytes")
	}
	defer clear(key)
	if pool == nil {
		return nil, errors.New("ChatGPT connections require the control-plane database")
	}
	return chatgpt.New(pool, key)
}

func (c *chatGPTRuntime) withHuman(ctx context.Context, pa string, operation func(string) error) error {
	kind, human, err := c.employers.CurrentEmployer(ctx, pa)
	if err != nil {
		return err
	}
	if kind != "human" {
		return errors.New("ChatGPT connection requires a current Human employer")
	}
	return c.employers.AuthorizeCurrentHumanEmployer(ctx, human, pa, func() error { return operation(human) })
}

func (c *chatGPTRuntime) activation(ctx context.Context, pa string, base runtimeprovision.ActivationConfig) (runtimeprovision.ActivationConfig, error) {
	kind, human, err := c.employers.CurrentEmployer(ctx, pa)
	if err != nil {
		return base, err
	}
	if kind != "human" {
		return base, nil
	}
	err = c.employers.AuthorizeCurrentHumanEmployer(ctx, human, pa, func() error {
		status, err := c.connections.Status(ctx, human)
		if err != nil {
			return err
		}
		if !status.Connected {
			return nil
		}
		if status.ReconnectRequired {
			return chatgpt.ErrReconnectRequired
		}
		base.ModelPreset = "chatgpt-responses"
		base.ModelID = status.Model
		base.ModelReasoningEffort = status.Effort
		base.ModelAccountScope = status.AccountID
		base.ChatGPTConnectionID = status.ConnectionID
		base.ProviderAPIKey = ""
		return nil
	})
	return base, err
}

func (c *chatGPTRuntime) register(control *agentevents.LocalControlServer) error {
	return control.RegisterAuthorizedRoute("POST "+chatGPTAccessPath, c.access)
}
func (c *chatGPTRuntime) access(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	w.Header().Set("Cache-Control", "no-store")
	var request struct {
		ConnectionID  string `json:"connection_id"`
		RejectedToken string `json:"rejected_access_token,omitempty"`
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 70<<10))
	if err != nil || agentevents.DecodeStrictJSON(body, &request) != nil || request.ConnectionID == "" || len(request.ConnectionID) > 256 || len(request.RejectedToken) > 65536 {
		http.Error(w, "invalid ChatGPT access request", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	committed := false
	err = c.withHuman(ctx, authorization.PersonalityAgentID, func(human string) error {
		access, err := c.connections.ResolveAccess(ctx, human, request.ConnectionID, request.RejectedToken, c.refresh)
		if err != nil {
			return err
		}
		// Secret disclosure remains inside the current-employer lease.
		_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(30 * time.Second))
		w.Header().Set("Content-Type", "application/json")
		committed = true
		return json.NewEncoder(w).Encode(struct {
			ConnectionID string    `json:"connection_id"`
			AccountID    string    `json:"account_id"`
			AccessToken  string    `json:"access_token"`
			ExpiresAt    time.Time `json:"expires_at"`
			Model        string    `json:"model"`
			Effort       string    `json:"effort"`
		}{access.ConnectionID, access.AccountID, access.Token, access.ExpiresAt, access.Model, access.Effort})
	})
	if err != nil && !committed {
		http.Error(w, "ChatGPT connection unavailable; reconnect or retry", http.StatusServiceUnavailable)
	}
}

func chatGPTBrowserIdentity(sessions agentevents.UserSessionAuthorizer, origins []string) func(*http.Request) (chatgpt.LoginIdentity, error) {
	return func(r *http.Request) (chatgpt.LoginIdentity, error) {
		denied := errors.New("ChatGPT connection authentication required")
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			if len(r.Header.Values("Origin")) > 0 && !agentevents.BrowserOriginAllowed(r, origins) {
				return chatgpt.LoginIdentity{}, denied
			}
		} else if !agentevents.BrowserOriginAllowed(r, origins) || !agentevents.BrowserCSRFValid(r) {
			return chatgpt.LoginIdentity{}, denied
		}
		cookies := r.CookiesNamed(agentevents.BrowserSessionCookie)
		if sessions == nil || len(cookies) != 1 {
			return chatgpt.LoginIdentity{}, denied
		}
		claims, err := sessions.VerifySession(r.Context(), cookies[0].Value)
		if err != nil {
			return chatgpt.LoginIdentity{}, denied
		}
		if sessions.AuthorizeSession(r.Context(), claims, func() error { return nil }) != nil {
			return chatgpt.LoginIdentity{}, denied
		}
		return chatgpt.LoginIdentity{HumanID: claims.UserID, SessionID: claims.BrowserSessionID(), Authorize: func(ctx context.Context, effect func(context.Context) error) error {
			return sessions.AuthorizeSession(ctx, claims, func() error { return effect(ctx) })
		}}, nil
	}
}
