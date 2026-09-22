package mcpconnections

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/sumi-studio/sumi/apps/api/internal/chatgpt"
)

type Service struct {
	Store        *Store
	Authenticate func(*http.Request) (chatgpt.LoginIdentity, error)
}

func (s *Service) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/mcp-connections", s.serve)
	mux.HandleFunc("POST /api/mcp-connections", s.serve)
	mux.HandleFunc("PUT /api/mcp-connections/{id}", s.serve)
	mux.HandleFunc("DELETE /api/mcp-connections/{id}", s.serve)
}
func (s *Service) serve(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	reply := func(status int, v any) { w.WriteHeader(status); _ = json.NewEncoder(w).Encode(v) }
	if s.Authenticate == nil {
		reply(401, map[string]string{"error": "Sign in to Sumi"})
		return
	}
	identity, e := s.Authenticate(r)
	if e != nil || identity.HumanID == "" || identity.Authorize == nil {
		reply(401, map[string]string{"error": "Sign in to Sumi"})
		return
	}
	if s.Store == nil {
		reply(503, map[string]string{"error": "MCP connections unavailable: server credential key is not configured"})
		return
	}
	var out any
	if r.Method == "GET" {
		out, e = s.Store.List(r.Context(), identity.HumanID)
	} else {
		var in Input
		if r.Method != "DELETE" {
			d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
			d.DisallowUnknownFields()
			var tail any
			if d.Decode(&in) != nil || d.Decode(&tail) != io.EOF {
				reply(400, map[string]string{"error": "Invalid MCP connection"})
				return
			}
		}
		e = identity.Authorize(r.Context(), func(ctx context.Context) error {
			if r.Method == "DELETE" {
				out = map[string]bool{"deleted": true}
				return s.Store.Delete(ctx, identity.HumanID, r.PathValue("id"))
			}
			var err error
			out, err = s.Store.Save(ctx, identity.HumanID, r.PathValue("id"), in)
			return err
		})
	}
	if e != nil {
		status := 503
		if errors.Is(e, ErrInvalid) {
			status = 400
		}
		if errors.Is(e, ErrUnavailable) {
			status = 404
		}
		reply(status, map[string]string{"error": "MCP connection request could not be completed"})
		return
	}
	reply(200, out)
}
