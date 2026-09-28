package main

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/chatgpt"
)

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
