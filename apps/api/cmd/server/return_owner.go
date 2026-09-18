package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/returnsession"
)

// secretaryReturnMount is the return server's service (kept for the
// background sweep) and its mounted routes.
type secretaryReturnMount struct {
	service *returnsession.Service
	server  *returnsession.Server
}

var errOwnerProofRejected = errors.New("a signed-in Sumi Cloud session is required")

// ownerSessionProof is the production returnsession.OwnerProof: the signed
// browser session cookie resolves to the human and secretary that live
// session owns — never ids taken from the request body. Mutating owner
// calls additionally require the browser origin and the CSRF double-submit
// token, the same checks the other cookie-authenticated surfaces use.
type ownerSessionProof struct {
	sessions *agentevents.HMACUserSessionVerifier
	origins  []string
}

func (p ownerSessionProof) OwnerClaims(ctx context.Context, r *http.Request) (returnsession.Owner, error) {
	if p.sessions == nil {
		return returnsession.Owner{}, errOwnerProofRejected
	}
	if r.Method != http.MethodGet {
		if !agentevents.BrowserOriginAllowed(r, p.origins) || !agentevents.BrowserCSRFValid(r) {
			return returnsession.Owner{}, errOwnerProofRejected
		}
	}
	cookies := r.CookiesNamed(agentevents.BrowserSessionCookie)
	if len(cookies) != 1 {
		return returnsession.Owner{}, errOwnerProofRejected
	}
	claims, err := p.sessions.VerifySession(ctx, cookies[0].Value)
	if err != nil {
		return returnsession.Owner{}, err
	}
	return returnsession.Owner{HumanID: claims.UserID, PersonaID: claims.PersonalityAgentID}, nil
}

// secretaryReturnFromEnv wires the secretary-return surface. With the
// public base URL unset the feature is simply off: no routes, no sweep, and
// no owner path ever consults return_sessions. With it set, browser-session
// authentication is required — the only thing that can produce signed-in
// owner claims — and the same service instance serves the routes and the
// sweep so deadlines agree.
func secretaryReturnFromEnv(
	pool *pgxpool.Pool,
	sessions *agentevents.HMACUserSessionVerifier,
	allowedOrigins []string,
) (*secretaryReturnMount, error) {
	base := strings.TrimSpace(os.Getenv(transferPublicBaseURLEnv))
	if base == "" {
		return nil, nil
	}
	if sessions == nil || pool == nil {
		return nil, fmt.Errorf("%s requires browser-session authentication and a control-plane database", transferPublicBaseURLEnv)
	}
	// The file policy is deliberately undecided: the shared-file question
	// is still the user's open choice, so Config{} leaves new-move
	// admission refused. The public base URL makes the routes reachable —
	// it is not the policy answer, and no env var stands in for it. When
	// the decision lands this Config gains its real FilePolicy value.
	service := returnsession.New(pool, returnsession.Config{})
	server, err := returnsession.NewServer(service,
		ownerSessionProof{sessions: sessions, origins: allowedOrigins}, base)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", transferPublicBaseURLEnv, err)
	}
	return &secretaryReturnMount{service: service, server: server}, nil
}
