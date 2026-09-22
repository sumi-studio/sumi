package mcpconnections

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// RegisterLocalRoutes takes the real host's persona-scoped human capability,
// not a synthetic Cloud identity. It is separate from model/Core routes.
func (s *Store) RegisterLocalRoutes(mux *http.ServeMux, authorize func(http.ResponseWriter, *http.Request) bool) {
	serve := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json")
		if !authorize(w, r) {
			return
		}
		var out any
		var e error
		switch r.Method {
		case "GET":
			out, e = s.ListLocal(r.Context())
		case "DELETE":
			e = s.DeleteLocal(r.Context(), r.PathValue("id"))
			out = map[string]bool{"deleted": e == nil}
		default:
			d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 40<<10))
			d.DisallowUnknownFields()
			var in LocalInput
			var tail any
			if d.Decode(&in) != nil || d.Decode(&tail) != io.EOF {
				e = ErrInvalid
			} else {
				out, e = s.SaveLocal(r.Context(), r.PathValue("id"), in)
			}
		}
		if e != nil {
			status := 503
			if errors.Is(e, ErrInvalid) {
				status = 400
			}
			if errors.Is(e, ErrUnavailable) {
				status = 404
			}
			w.WriteHeader(status)
			out = map[string]string{"error": "Local MCP connection request could not be completed"}
		}
		_ = json.NewEncoder(w).Encode(out)
	}
	for _, route := range []string{"GET /fm/{persona}/mcp-connections", "POST /fm/{persona}/mcp-connections", "PUT /fm/{persona}/mcp-connections/{id}", "DELETE /fm/{persona}/mcp-connections/{id}"} {
		mux.HandleFunc(route, serve)
	}
}
