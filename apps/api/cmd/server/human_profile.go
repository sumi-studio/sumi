package main

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/messaging"
)

const maxHumanProfileRequestBytes = 4 * 1024

// humanProfileServer is the self-service Human settings boundary. The HumanId
// comes only from the signed browser session; clients cannot nominate another
// Human whose profile should be changed.
type humanProfileServer struct {
	messaging      *messaging.Server
	sessions       agentevents.UserSessionAuthorizer
	allowedOrigins []string
}

func newHumanProfileServer(messagingServer *messaging.Server, sessions agentevents.UserSessionAuthorizer, allowedOrigins []string) *humanProfileServer {
	return &humanProfileServer{messaging: messagingServer, sessions: sessions, allowedOrigins: append([]string(nil), allowedOrigins...)}
}

func (s *humanProfileServer) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /auth/profile", s.serveUpdate)
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
	cookies := r.CookiesNamed(agentevents.BrowserSessionCookie)
	if len(cookies) != 1 || s.sessions == nil || s.messaging == nil {
		writeHumanProfileError(w, http.StatusUnauthorized, "authentication_required")
		return
	}
	claims, err := s.sessions.VerifySession(r.Context(), cookies[0].Value)
	if err != nil {
		writeHumanProfileError(w, http.StatusUnauthorized, "authentication_required")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxHumanProfileRequestBytes))
	if err != nil {
		writeHumanProfileError(w, http.StatusRequestEntityTooLarge, "request_too_large")
		return
	}
	var request struct {
		DisplayName string `json:"display_name"`
	}
	if agentevents.DecodeStrictJSON(body, &request) != nil {
		writeHumanProfileError(w, http.StatusBadRequest, "invalid_request")
		return
	}

	called := false
	var profile messaging.ParticipantProfile
	err = s.sessions.AuthorizeSession(r.Context(), claims, func() error {
		called = true
		var updateErr error
		profile, updateErr = s.messaging.SetHumanProfile(r.Context(), claims.UserID, request.DisplayName)
		return updateErr
	})
	if !called {
		writeHumanProfileError(w, http.StatusUnauthorized, "authentication_required")
		return
	}
	if err != nil {
		switch {
		case errors.Is(err, messaging.ErrInvalidDisplayName):
			writeHumanProfileError(w, http.StatusBadRequest, "invalid_display_name")
		case errors.Is(err, messaging.ErrParticipantNotFound):
			// A valid signed session must always name a live Human. Treat a
			// missing row as control-plane inconsistency, not caller error.
			writeHumanProfileError(w, http.StatusServiceUnavailable, "profile_unavailable")
		default:
			writeHumanProfileError(w, http.StatusServiceUnavailable, "profile_unavailable")
		}
		return
	}
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
			Revision    int64  `json:"revision"`
		} `json:"profile"`
	}{User: struct {
		ID          string `json:"id"`
		DisplayName string `json:"display_name"`
	}{ID: claims.UserID, DisplayName: profile.DisplayName}, Profile: struct {
		Participant struct {
			Kind    string `json:"kind"`
			HumanID string `json:"human_id"`
		} `json:"participant"`
		DisplayName string `json:"display_name"`
		Tagline     string `json:"tagline"`
		Revision    int64  `json:"revision"`
	}{Participant: struct {
		Kind    string `json:"kind"`
		HumanID string `json:"human_id"`
	}{Kind: string(profile.Participant.Kind), HumanID: profile.Participant.ID}, DisplayName: profile.ProjectedDisplayName(), Tagline: profile.Tagline, Revision: profile.Revision}})
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
