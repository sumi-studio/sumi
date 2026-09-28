package main

import (
	"context"
	"errors"
	"net/http"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/browseridentity"
)

func browserIdentity(sessions agentevents.UserSessionAuthorizer, origins []string) func(*http.Request) (browseridentity.Identity, error) {
	return func(r *http.Request) (browseridentity.Identity, error) {
		denied := errors.New("browser session authentication required")
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			if len(r.Header.Values("Origin")) > 0 && !agentevents.BrowserOriginAllowed(r, origins) {
				return browseridentity.Identity{}, denied
			}
		} else if !agentevents.BrowserOriginAllowed(r, origins) || !agentevents.BrowserCSRFValid(r) {
			return browseridentity.Identity{}, denied
		}
		cookies := r.CookiesNamed(agentevents.BrowserSessionCookie)
		if sessions == nil || len(cookies) != 1 {
			return browseridentity.Identity{}, denied
		}
		claims, err := sessions.VerifySession(r.Context(), cookies[0].Value)
		if err != nil {
			return browseridentity.Identity{}, denied
		}
		if sessions.AuthorizeSession(r.Context(), claims, func() error { return nil }) != nil {
			return browseridentity.Identity{}, denied
		}
		return browseridentity.Identity{HumanID: claims.UserID, SessionID: claims.BrowserSessionID(), Authorize: func(ctx context.Context, effect func(context.Context) error) error {
			return sessions.AuthorizeSession(ctx, claims, func() error { return effect(ctx) })
		}}, nil
	}
}
