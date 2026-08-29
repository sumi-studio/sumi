package main

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

const maxHumanProfileRequestBytes = 4 * 1024

// humanProfileServer is the self-service Human settings boundary. The HumanId
// comes only from the signed browser session; clients cannot nominate another
// Human whose profile should be changed.
type humanProfileServer struct {
	store          *koseki.Store
	sessions       agentevents.UserSessionAuthorizer
	allowedOrigins []string
}

func newHumanProfileServer(store *koseki.Store, sessions agentevents.UserSessionAuthorizer, allowedOrigins []string) *humanProfileServer {
	return &humanProfileServer{store: store, sessions: sessions, allowedOrigins: append([]string(nil), allowedOrigins...)}
}

func (s *humanProfileServer) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /auth/profile", s.serveRead)
	mux.HandleFunc("POST /auth/profile", s.serveUpdate)
}

func (s *humanProfileServer) serveRead(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	called := false
	var profile koseki.HumanProfile
	err := s.sessions.AuthorizeSession(r.Context(), claims, func() error {
		called = true
		var readErr error
		profile, readErr = s.store.HumanProfile(r.Context(), claims.UserID)
		return readErr
	})
	if !called {
		writeHumanProfileError(w, http.StatusUnauthorized, "authentication_required")
		return
	}
	if err != nil {
		writeHumanProfileStoreError(w, err)
		return
	}
	writeHumanProfile(w, profile)
}

func (s *humanProfileServer) serveUpdate(w http.ResponseWriter, r *http.Request) {
	if !agentevents.BrowserOriginAllowed(r, s.allowedOrigins) {
		writeHumanProfileError(w, http.StatusForbidden, "origin_not_allowed")
		return
	}
	if !agentevents.BrowserCSRFValid(r) {
		writeHumanProfileError(w, http.StatusForbidden, "invalid_csrf_token")
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeHumanProfileError(w, http.StatusUnsupportedMediaType, "application_json_required")
		return
	}
	claims, ok := s.viewer(w, r)
	if !ok {
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxHumanProfileRequestBytes))
	if err != nil {
		writeHumanProfileError(w, http.StatusRequestEntityTooLarge, "request_too_large")
		return
	}
	var request struct {
		DisplayName *string `json:"display_name"`
		Tagline     *string `json:"tagline"`
	}
	if agentevents.DecodeStrictJSON(body, &request) != nil ||
		(request.DisplayName == nil && request.Tagline == nil) {
		writeHumanProfileError(w, http.StatusBadRequest, "invalid_request")
		return
	}

	called := false
	var profile koseki.HumanProfile
	err = s.sessions.AuthorizeSession(r.Context(), claims, func() error {
		called = true
		var updateErr error
		profile, updateErr = s.store.UpdateHumanProfile(
			r.Context(), claims.UserID, request.DisplayName, request.Tagline,
		)
		return updateErr
	})
	if !called {
		writeHumanProfileError(w, http.StatusUnauthorized, "authentication_required")
		return
	}
	if err != nil {
		writeHumanProfileStoreError(w, err)
		return
	}
	writeHumanProfile(w, profile)
}

func (s *humanProfileServer) viewer(w http.ResponseWriter, r *http.Request) (agentevents.UserSessionClaims, bool) {
	cookies := r.CookiesNamed(agentevents.BrowserSessionCookie)
	if len(cookies) != 1 || s.sessions == nil || s.store == nil {
		writeHumanProfileError(w, http.StatusUnauthorized, "authentication_required")
		return agentevents.UserSessionClaims{}, false
	}
	claims, err := s.sessions.VerifySession(r.Context(), cookies[0].Value)
	if err != nil {
		writeHumanProfileError(w, http.StatusUnauthorized, "authentication_required")
		return agentevents.UserSessionClaims{}, false
	}
	return claims, true
}

func writeHumanProfileStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, koseki.ErrInvalidDisplayName):
		writeHumanProfileError(w, http.StatusBadRequest, "invalid_display_name")
	case errors.Is(err, koseki.ErrInvalidTagline):
		writeHumanProfileError(w, http.StatusBadRequest, "invalid_tagline")
	case errors.Is(err, koseki.ErrEmptyHumanProfilePatch):
		writeHumanProfileError(w, http.StatusBadRequest, "invalid_request")
	default:
		// A valid signed session must always name a live Human. Missing rows and
		// database failures are control-plane availability failures, not caller
		// errors and not identity existence oracles.
		writeHumanProfileError(w, http.StatusServiceUnavailable, "profile_unavailable")
	}
}

func writeHumanProfile(w http.ResponseWriter, profile koseki.HumanProfile) {
	writeHumanProfileJSON(w, http.StatusOK, struct {
		User struct {
			ID          string `json:"id"`
			DisplayName string `json:"display_name"`
		} `json:"user"`
		Profile struct {
			Participant struct {
				Kind    string `json:"kind"`
				HumanID string `json:"human_id"`
			} `json:"participant"`
			DisplayName string `json:"display_name"`
			Tagline     string `json:"tagline"`
		} `json:"profile"`
	}{User: struct {
		ID          string `json:"id"`
		DisplayName string `json:"display_name"`
	}{ID: profile.HumanID, DisplayName: profile.DisplayName}, Profile: struct {
		Participant struct {
			Kind    string `json:"kind"`
			HumanID string `json:"human_id"`
		} `json:"participant"`
		DisplayName string `json:"display_name"`
		Tagline     string `json:"tagline"`
	}{Participant: struct {
		Kind    string `json:"kind"`
		HumanID string `json:"human_id"`
	}{Kind: "human", HumanID: profile.HumanID}, DisplayName: profile.DisplayName, Tagline: profile.Tagline}})
}

func writeHumanProfileError(w http.ResponseWriter, status int, code string) {
	writeHumanProfileJSON(w, status, map[string]string{"error": code})
}

func writeHumanProfileJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
