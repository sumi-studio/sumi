package feedback

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

type Server struct {
	Store          *Store
	Sessions       agentevents.UserSessionAuthorizer
	AllowedOrigins []string
}

func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /feedback/bootstrap", s.public("bootstrap"))
	mux.HandleFunc("GET /feedback/threads", s.public("list"))
	mux.HandleFunc("POST /feedback/threads", s.public("create"))
	mux.HandleFunc("GET /feedback/threads/{thread_id}", s.public("open"))
	mux.HandleFunc("POST /feedback/threads/{thread_id}/messages", s.public("reply"))
	mux.HandleFunc("PATCH /feedback/threads/{thread_id}", s.public("status"))
	mux.HandleFunc("PUT /feedback/threads/{thread_id}/read", s.public("read"))
}
func (s *Server) public(op string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) { s.browser(w, r, op) }
}
func (s *Server) RegisterLocalControlRoutes(control *agentevents.LocalControlServer) error {
	for _, op := range []string{"bootstrap", "list", "open", "create", "reply", "status", "read"} {
		if err := control.RegisterAuthorizedRoute("POST /local-control/v1/feedback:"+op, func(w http.ResponseWriter, r *http.Request, auth agentevents.LocalRuntimeAuthorization) {
			value, err := s.dispatch(r, participant.PersonalityAgent(auth.PersonalityAgentID), op, true)
			respond(w, value, err, op)
		}); err != nil {
			return err
		}
	}
	return nil
}
func (s *Server) browser(w http.ResponseWriter, r *http.Request, op string) {
	if r.Method != http.MethodGet && !agentevents.BrowserOriginAllowed(r, s.AllowedOrigins) {
		writeError(w, 403, "forbidden")
		return
	}
	cookies := r.CookiesNamed(agentevents.BrowserSessionCookie)
	if len(cookies) != 1 || s.Sessions == nil {
		writeError(w, 401, "unauthorized")
		return
	}
	claims, err := s.Sessions.VerifySession(r.Context(), cookies[0].Value)
	if err != nil {
		writeError(w, 401, "unauthorized")
		return
	}
	actor := participant.Human(claims.UserID)
	if actor.Validate() != nil {
		writeError(w, 401, "unauthorized")
		return
	}
	var value any
	var operationErr error
	called := false
	// Bind read/write admission to the authenticated session, including logout.
	err = s.Sessions.AuthorizeSession(r.Context(), claims, func() error {
		called = true
		value, operationErr = s.dispatch(r, actor, op, false)
		return operationErr
	})
	if !called {
		writeError(w, 401, "unauthorized")
		return
	}
	if operationErr == nil && err != nil {
		writeError(w, 401, "unauthorized")
		return
	}
	respond(w, value, operationErr, op)
}
func respond(w http.ResponseWriter, value any, err error, op string) {
	if err != nil {
		status, code := errorCode(err)
		if status == 500 {
			log.Printf("feedback %s: %v", op, err)
		}
		writeError(w, status, code)
		return
	}
	if op == "read" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(value)
}
func writeError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": code})
}
func decode(r *http.Request, value any) error {
	// 20k Unicode scalars can each occupy 12 bytes as escaped surrogate pairs.
	raw, err := io.ReadAll(io.LimitReader(r.Body, (256<<10)+1))
	if err != nil || len(raw) > 256<<10 {
		return ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return ErrInvalid
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return ErrInvalid
	}
	return nil
}
func query(r *http.Request, key string) (string, error) {
	vs := r.URL.Query()[key]
	if len(vs) > 1 {
		return "", ErrInvalid
	}
	if len(vs) == 0 {
		return "", nil
	}
	return vs[0], nil
}
func (s *Server) dispatch(r *http.Request, actor participant.Ref, op string, local bool) (any, error) {
	ctx := r.Context()
	switch op {
	case "bootstrap":
		if local {
			if err := decode(r, &struct{}{}); err != nil {
				return nil, err
			}
		}
		return s.Store.Bootstrap(ctx, actor)
	case "list":
		var p struct {
			Status string `json:"status"`
			Cursor string `json:"cursor"`
		}
		if local {
			if err := decode(r, &p); err != nil {
				return nil, err
			}
		} else {
			var err error
			p.Status, err = query(r, "status")
			if err != nil {
				return nil, err
			}
			p.Cursor, err = query(r, "cursor")
			if err != nil {
				return nil, err
			}
		}
		return s.Store.List(ctx, actor, p.Status, p.Cursor)
	case "open":
		var p struct {
			ThreadID string `json:"thread_id"`
			Cursor   string `json:"cursor"`
		}
		if local {
			if err := decode(r, &p); err != nil {
				return nil, err
			}
		} else {
			p.ThreadID = r.PathValue("thread_id")
			var err error
			p.Cursor, err = query(r, "cursor")
			if err != nil {
				return nil, err
			}
		}
		return s.Store.Open(ctx, actor, p.ThreadID, p.Cursor)
	case "create":
		var p struct {
			Title     string `json:"title"`
			Body      string `json:"body"`
			RequestID string `json:"request_id"`
		}
		if err := decode(r, &p); err != nil {
			return nil, err
		}
		return s.Store.Create(ctx, actor, p.Title, p.Body, p.RequestID)
	case "reply":
		var p struct {
			ThreadID  string `json:"thread_id,omitempty"`
			Body      string `json:"body"`
			RequestID string `json:"request_id"`
		}
		if err := decode(r, &p); err != nil {
			return nil, err
		}
		if !local {
			if p.ThreadID != "" {
				return nil, ErrInvalid
			}
			p.ThreadID = r.PathValue("thread_id")
		}
		return s.Store.Reply(ctx, actor, p.ThreadID, p.Body, p.RequestID)
	case "status":
		var p struct {
			ThreadID string `json:"thread_id,omitempty"`
			Status   string `json:"status"`
			Revision int64  `json:"revision"`
		}
		if err := decode(r, &p); err != nil {
			return nil, err
		}
		if !local {
			if p.ThreadID != "" {
				return nil, ErrInvalid
			}
			p.ThreadID = r.PathValue("thread_id")
		}
		return s.Store.Status(ctx, actor, p.ThreadID, p.Status, p.Revision)
	case "read":
		var p struct {
			ThreadID string `json:"thread_id,omitempty"`
			Revision int64  `json:"revision"`
		}
		if err := decode(r, &p); err != nil {
			return nil, err
		}
		if !local {
			if p.ThreadID != "" {
				return nil, ErrInvalid
			}
			p.ThreadID = r.PathValue("thread_id")
		}
		return nil, s.Store.Read(ctx, actor, p.ThreadID, p.Revision)
	}
	return nil, ErrInvalid
}
