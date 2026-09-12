package agentevents

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
)

type EnrollmentInvitation struct {
	ID         string     `json:"id"`
	IssuedBy   string     `json:"issued_by"`
	Email      string     `json:"email,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
	ExpiresAt  time.Time  `json:"expires_at"`
	RevokedAt  *time.Time `json:"revoked_at,omitempty"`
	ConsumedAt *time.Time `json:"consumed_at,omitempty"`
	ConsumedBy string     `json:"consumed_by,omitempty"`
}
type EnrollmentInvitationStore interface {
	IssueEnrollmentInvite(context.Context, string, string, time.Duration) (EnrollmentInvitation, string, error)
	ListEnrollmentInvites(context.Context) ([]EnrollmentInvitation, error)
	RevokeEnrollmentInvite(context.Context, string) error
	InspectEnrollmentInvite(context.Context, string) (EnrollmentInvitation, error)
}

// IsEnrollmentAdmin checks only explicit deployment authority. Callers must
// authenticate the Human separately and hold their session/operation boundary.
func (s *BrowserAuthServer) IsEnrollmentAdmin(humanID string) bool {
	return s != nil && s.EnrollmentInvitations != nil && s.EnrollmentAdmins[humanID]
}

func (s *BrowserAuthServer) registerEnrollmentRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /auth/invitations", s.serveEnrollmentInvitations)
	mux.HandleFunc("POST /auth/invitations", s.serveEnrollmentInvitations)
	mux.HandleFunc("POST /auth/invitations/inspect", s.serveInspectEnrollmentInvite)
	mux.HandleFunc("POST /auth/invitations/{id}/revoke", s.serveRevokeEnrollmentInvite)
}
func (s *BrowserAuthServer) withEnrollmentAdmin(w http.ResponseWriter, r *http.Request, fn func(UserSessionClaims)) {
	if r.Method == http.MethodGet {
		if !s.allowSafeReadOrigin(w, r) {
			return
		}
	} else if !s.allowOrigin(w, r) || !s.requireCSRF(w, r) {
		return
	}
	cookie, err := uniqueBrowserSessionCookie(r)
	if err != nil {
		writeBrowserAuthError(w, 401, "authentication required")
		return
	}
	claims, err := s.Sessions.VerifySession(r.Context(), cookie.Value)
	if err != nil {
		writeBrowserAuthError(w, 401, "authentication required")
		return
	}
	if !s.IsEnrollmentAdmin(claims.UserID) {
		writeBrowserAuthError(w, 403, "invitation_admin_required")
		return
	}
	if s.EnrollmentInvitations == nil {
		writeBrowserAuthError(w, 503, "invitations unavailable")
		return
	}
	// Keep revocation and operator authority valid through the bounded operation.
	ran := false
	err = s.Sessions.AuthorizeSession(r.Context(), claims, func() error { ran = true; fn(claims); return nil })
	if err != nil && !ran {
		writeBrowserAuthError(w, 401, "authentication required")
	}
}
func (s *BrowserAuthServer) serveEnrollmentInvitations(w http.ResponseWriter, r *http.Request) {
	s.withEnrollmentAdmin(w, r, func(claims UserSessionClaims) {
		if r.Method == http.MethodGet {
			items, err := s.EnrollmentInvitations.ListEnrollmentInvites(r.Context())
			if err != nil {
				writeBrowserAuthError(w, 503, "invitations unavailable")
				return
			}
			writeBrowserAuthJSON(w, 200, map[string]any{"can_invite": true, "invitations": items})
			return
		}
		if !s.allowAuthAllocation(w, r) {
			return
		}
		var body struct {
			Email            string `json:"email"`
			ExpiresInSeconds int64  `json:"expires_in_seconds"`
		}
		if !decodeAuthJSON(w, r, &body) {
			return
		}
		if body.ExpiresInSeconds == 0 {
			body.ExpiresInSeconds = 7 * 24 * 60 * 60
		}
		if body.ExpiresInSeconds < 60 || body.ExpiresInSeconds > 30*24*60*60 {
			writeBrowserAuthError(w, 400, "invalid_invitation")
			return
		}
		v, token, err := s.EnrollmentInvitations.IssueEnrollmentInvite(r.Context(), claims.UserID, body.Email, time.Duration(body.ExpiresInSeconds)*time.Second)
		if err != nil {
			if errors.Is(err, ErrBrowserEnrollmentInvite) {
				writeBrowserAuthError(w, 400, "invalid_invitation")
			} else {
				writeBrowserAuthError(w, 503, "invitations unavailable")
			}
			return
		}
		writeBrowserAuthJSON(w, 201, map[string]any{"invitation": v, "token": token})
	})
}
func (s *BrowserAuthServer) serveRevokeEnrollmentInvite(w http.ResponseWriter, r *http.Request) {
	s.withEnrollmentAdmin(w, r, func(_ UserSessionClaims) {
		id := r.PathValue("id")
		if _, err := uuid.Parse(id); err != nil {
			writeBrowserAuthError(w, 400, "invalid_invitation")
			return
		}
		var body struct{}
		if !decodeAuthJSON(w, r, &body) {
			return
		}
		if err := s.EnrollmentInvitations.RevokeEnrollmentInvite(r.Context(), id); err != nil {
			if errors.Is(err, ErrBrowserEnrollmentInvite) {
				writeBrowserAuthError(w, 404, "invitation_unavailable")
			} else {
				writeBrowserAuthError(w, 503, "invitations unavailable")
			}
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(204)
	})
}
func (s *BrowserAuthServer) serveInspectEnrollmentInvite(w http.ResponseWriter, r *http.Request) {
	if !s.allowOrigin(w, r) || !s.requireCSRF(w, r) || !s.allowAuthAllocation(w, r) {
		return
	}
	var body struct {
		Token string `json:"token"`
	}
	if !decodeAuthJSON(w, r, &body) {
		return
	}
	if s.EnrollmentInvitations == nil {
		writeBrowserAuthError(w, 503, "invitations unavailable")
		return
	}
	v, err := s.EnrollmentInvitations.InspectEnrollmentInvite(r.Context(), body.Token)
	if err != nil {
		if errors.Is(err, ErrBrowserEnrollmentInvite) {
			writeBrowserAuthError(w, 403, "invitation_required")
		} else {
			writeBrowserAuthError(w, 503, "invitations unavailable")
		}
		return
	}
	writeBrowserAuthJSON(w, 200, map[string]any{"valid": true, "expires_at": v.ExpiresAt})
}
