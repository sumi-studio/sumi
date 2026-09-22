package browsertabs

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/chatgpt"
)

type Service struct {
	Store        *Store
	Authenticate func(*http.Request) (chatgpt.LoginIdentity, error)
}

func (s *Service) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/browser-tabs", s.human)
	mux.HandleFunc("POST /api/browser-tabs", s.human)
	mux.HandleFunc("DELETE /api/browser-tabs/{id}", s.human)
	mux.HandleFunc("POST /api/browser-host/tabs/{id}/poll", s.host)
	mux.HandleFunc("POST /api/browser-host/tabs/{id}/complete", s.host)
}
func reply(w http.ResponseWriter, status int, out any) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(out)
}
func problem(w http.ResponseWriter, e error) {
	status := 503
	code := "browser_unavailable"
	if errors.Is(e, ErrInvalid) || errors.Is(e, agentstate.ErrBadRequest) {
		status = 400
		code = "invalid_request"
	}
	if errors.Is(e, ErrUnavailable) {
		status = 403
		code = "not_authorized"
	}
	if errors.Is(e, agentstate.ErrJobConflict) || errors.Is(e, agentstate.ErrJobNotClaimed) {
		status = 409
		code = "result_conflict"
	}
	reply(w, status, map[string]string{"error": code})
}
func decode(w http.ResponseWriter, r *http.Request, out any) bool {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 300<<10))
	d.DisallowUnknownFields()
	var tail any
	return d.Decode(out) == nil && d.Decode(&tail) == io.EOF
}
func (s *Service) human(w http.ResponseWriter, r *http.Request) {
	if s.Authenticate == nil {
		problem(w, ErrUnavailable)
		return
	}
	identity, e := s.Authenticate(r)
	if e != nil || identity.HumanID == "" || identity.Authorize == nil {
		problem(w, ErrUnavailable)
		return
	}
	if s.Store == nil {
		problem(w, errors.New("unavailable"))
		return
	}
	var input AttachInput
	if r.Method == "POST" && !decode(w, r, &input) {
		problem(w, ErrInvalid)
		return
	}
	var out any
	e = identity.Authorize(r.Context(), func(ctx context.Context) error {
		var err error
		switch r.Method {
		case "GET":
			out, err = s.Store.List(ctx, identity.HumanID)
		case "POST":
			var a Attachment
			var token string
			a, token, err = s.Store.Attach(ctx, identity.HumanID, input)
			out = map[string]any{"attachment": a, "host_token": token}
		case "DELETE":
			err = s.Store.Revoke(ctx, identity.HumanID, r.PathValue("id"))
			out = map[string]bool{"revoked": err == nil}
		}
		return err
	})
	if e != nil {
		problem(w, e)
		return
	}
	reply(w, 200, out)
}
func (s *Service) host(w http.ResponseWriter, r *http.Request) {
	if s.Store == nil {
		problem(w, errors.New("unavailable"))
		return
	}
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || r.Header.Get("Origin") != "" {
		problem(w, ErrUnavailable)
		return
	}
	id := r.PathValue("id")
	if strings.HasSuffix(r.URL.Path, "/poll") {
		j, e := s.Store.Claim(r.Context(), id, token)
		if e != nil {
			problem(w, e)
			return
		}
		reply(w, 200, map[string]any{"job": j})
		return
	}
	var in struct {
		JobID  string         `json:"job_id"`
		Status string         `json:"status"`
		Result map[string]any `json:"result"`
		Error  string         `json:"error"`
	}
	if !decode(w, r, &in) {
		problem(w, ErrInvalid)
		return
	}
	j, e := s.Store.Complete(r.Context(), id, token, in.JobID, in.Status, in.Result, in.Error)
	if e != nil {
		problem(w, e)
		return
	}
	reply(w, 200, map[string]any{"job_id": j.JobID, "status": j.Status})
}
