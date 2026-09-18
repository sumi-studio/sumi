package agentevents

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

// TerminalBackend is the narrow surface the person-facing terminal
// routes use. The implementation is the core state store: every call
// is persona-scoped, so a verified session can only ever reach its own
// secretary's terminal rows — the route never accepts a caller-chosen
// persona or scope.
type TerminalBackend interface {
	ListTerminalSessions(ctx context.Context, personaID string) ([]agentstate.TerminalSession, error)
	CreateTerminalSession(ctx context.Context, personaID, name, requestedBy, createdBy string) (agentstate.TerminalSession, error)
	GetTerminalSession(ctx context.Context, personaID, sessionID string) (agentstate.TerminalSession, error)
	ReadTerminalOutput(ctx context.Context, personaID, sessionID string, cursor int64, limit int) (agentstate.TerminalOutputRead, error)
	SubmitTerminalInput(ctx context.Context, personaID, sessionID, source, kind string, payload map[string]any) (agentstate.TerminalInput, error)
	ListTerminalInputs(ctx context.Context, personaID, sessionID string, afterSeq int64, limit int) ([]agentstate.TerminalInput, error)
	SetTerminalControl(ctx context.Context, personaID, sessionID string, hold bool, lease time.Duration) (agentstate.TerminalSession, error)
	CloseTerminalSession(ctx context.Context, personaID, sessionID, reason string) (agentstate.TerminalSession, error)
}

const (
	terminalHumanControlLease = 5 * time.Minute
	terminalReadDefaultLimit  = 64 * 1024
	terminalReadMaxLimit      = 512 * 1024
	terminalNameMaxRunes      = 80
	terminalWSPollInterval    = 250 * time.Millisecond
	terminalWSAuthInterval    = 5 * time.Second
	terminalWSMaxRead         = 96 * 1024
)

// RegisterTerminalRoutes mounts the person-facing terminal routes.
// Called only when a TerminalBackend is configured — an unconfigured
// deployment exposes no terminal surface at all.
func (s *BrowserServer) RegisterTerminalRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /terminal/list", s.serveTerminalList)
	mux.HandleFunc("POST /terminal/open", s.serveTerminalOpen)
	mux.HandleFunc("GET /terminal/session", s.serveTerminalGet)
	mux.HandleFunc("GET /terminal/read", s.serveTerminalRead)
	mux.HandleFunc("POST /terminal/input", s.serveTerminalInput)
	mux.HandleFunc("GET /terminal/inputs", s.serveTerminalInputs)
	mux.HandleFunc("POST /terminal/control", s.serveTerminalControl)
	mux.HandleFunc("POST /terminal/close", s.serveTerminalClose)
	mux.HandleFunc("GET /terminal/ws", s.serveTerminalWS)
}

// terminalAuth resolves the verified browser session into the persona
// the terminal rows belong to and runs the shared
// Employer/'terminal'-installation/lifecycle fence around the operation —
// the same authority boundary shape as direct chat, bound to the terminal
// app's own participant-owned installation.
func (s *BrowserServer) terminalAuth(w http.ResponseWriter, r *http.Request, operation func(paid string) error) bool {
	w.Header().Set("Cache-Control", "no-store")
	if s.Sessions == nil || s.Terminals == nil || s.LifecycleFence == nil {
		http.Error(w, "terminal unavailable", http.StatusServiceUnavailable)
		return false
	}
	if s.TerminalAuthorizer == nil {
		http.Error(w, "authorization unavailable", http.StatusServiceUnavailable)
		return false
	}
	if len(r.Header.Values("Origin")) > 0 && !s.checkOrigin(r) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return false
	}
	cookie, err := uniqueBrowserSessionCookie(r)
	if err != nil {
		http.Error(w, "invalid session", http.StatusUnauthorized)
		return false
	}
	claims, err := s.Sessions.VerifySession(r.Context(), cookie.Value)
	if err != nil {
		http.Error(w, "invalid session", http.StatusUnauthorized)
		return false
	}
	scope, err := directChatScopeFromRequest(r)
	if err != nil {
		writeDirectChatInvalidScope(w)
		return false
	}
	paid := claims.PersonalityAgentID
	err = s.authorizeBrowserTerminalOperation(r.Context(), claims, scope, func() error {
		return operation(paid)
	})
	if err == nil {
		return true
	}
	switch {
	case errors.Is(err, ErrDirectChatAuthorizationUnavailable):
		http.Error(w, "authorization unavailable", http.StatusServiceUnavailable)
	case errors.Is(err, agentstate.ErrTerminalNotFound):
		http.Error(w, "terminal session not found", http.StatusNotFound)
	case errors.Is(err, agentstate.ErrTerminalEnded):
		http.Error(w, "terminal session has ended", http.StatusConflict)
	case errors.Is(err, agentstate.ErrTerminalNotLive):
		http.Error(w, "terminal session is not live", http.StatusConflict)
	case errors.Is(err, agentstate.ErrTerminalControl):
		http.Error(w, "terminal control is held", http.StatusConflict)
	case errors.Is(err, agentstate.ErrTerminalBackend):
		http.Error(w, "terminal backend unavailable", http.StatusServiceUnavailable)
	case errors.Is(err, agentstate.ErrTerminalCapacity):
		http.Error(w, "too many live terminal sessions", http.StatusTooManyRequests)
	case errors.Is(err, agentstate.ErrBadRequest):
		http.Error(w, err.Error(), http.StatusBadRequest)
	default:
		http.Error(w, "not authorized", http.StatusForbidden)
	}
	return false
}

func writeTerminalJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func terminalSessionJSON(t agentstate.TerminalSession) map[string]any {
	out := map[string]any{
		"session_id":     t.SessionID,
		"name":           t.Name,
		"mode":           t.Mode,
		"backend":        t.Backend,
		"status":         t.Status,
		"requested_by":   t.RequestedBy,
		"output_bytes":   t.OutputBytes,
		"output_base":    t.OutputBase,
		"control_holder": t.ControlHolder,
		"created_at":     t.CreatedAt,
		"updated_at":     t.UpdatedAt,
	}
	if t.ExitCode != nil {
		out["exit_code"] = *t.ExitCode
	}
	if t.ExitSignal != "" {
		out["exit_signal"] = t.ExitSignal
	}
	if t.EndReason != "" {
		out["end_reason"] = t.EndReason
	}
	if t.EndedAt != nil {
		out["ended_at"] = *t.EndedAt
	}
	return out
}

type terminalOpenRequest struct {
	Name string `json:"name"`
}

func (s *BrowserServer) serveTerminalList(w http.ResponseWriter, r *http.Request) {
	var sessions []agentstate.TerminalSession
	if !s.terminalAuth(w, r, func(paid string) error {
		var err error
		sessions, err = s.Terminals.ListTerminalSessions(r.Context(), paid)
		return err
	}) {
		return
	}
	out := make([]map[string]any, 0, len(sessions))
	for _, t := range sessions {
		out = append(out, terminalSessionJSON(t))
	}
	writeTerminalJSON(w, map[string]any{"sessions": out})
}

func (s *BrowserServer) serveTerminalOpen(w http.ResponseWriter, r *http.Request) {
	var req terminalOpenRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if len([]rune(req.Name)) > terminalNameMaxRunes {
		http.Error(w, "name too long", http.StatusBadRequest)
		return
	}
	var session agentstate.TerminalSession
	if !s.terminalAuth(w, r, func(paid string) error {
		var err error
		session, err = s.Terminals.CreateTerminalSession(r.Context(), paid, req.Name, "human", "human")
		return err
	}) {
		return
	}
	writeTerminalJSON(w, map[string]any{"session": terminalSessionJSON(session)})
}

func terminalSessionIDParam(r *http.Request) string {
	return strings.TrimSpace(r.URL.Query().Get("session_id"))
}

func (s *BrowserServer) serveTerminalGet(w http.ResponseWriter, r *http.Request) {
	sessionID := terminalSessionIDParam(r)
	var session agentstate.TerminalSession
	if !s.terminalAuth(w, r, func(paid string) error {
		var err error
		session, err = s.Terminals.GetTerminalSession(r.Context(), paid, sessionID)
		return err
	}) {
		return
	}
	writeTerminalJSON(w, map[string]any{"session": terminalSessionJSON(session)})
}

func (s *BrowserServer) serveTerminalRead(w http.ResponseWriter, r *http.Request) {
	sessionID := terminalSessionIDParam(r)
	var cursor int64
	if v := strings.TrimSpace(r.URL.Query().Get("cursor")); v != "" {
		if _, err := fmt.Sscanf(v, "%d", &cursor); err != nil || cursor < 0 {
			http.Error(w, "bad cursor", http.StatusBadRequest)
			return
		}
	}
	limit := terminalReadDefaultLimit
	if v := strings.TrimSpace(r.URL.Query().Get("limit")); v != "" {
		var n int64
		if _, err := fmt.Sscanf(v, "%d", &n); err != nil || n <= 0 || n > terminalReadMaxLimit {
			http.Error(w, "bad limit", http.StatusBadRequest)
			return
		}
		limit = int(n)
	}
	var read agentstate.TerminalOutputRead
	if !s.terminalAuth(w, r, func(paid string) error {
		var err error
		read, err = s.Terminals.ReadTerminalOutput(r.Context(), paid, sessionID, cursor, limit)
		return err
	}) {
		return
	}
	chunks := make([]map[string]any, 0, len(read.Chunks))
	for _, c := range read.Chunks {
		item := map[string]any{"kind": c.Kind, "base": c.Base}
		if c.Kind == "data" {
			item["data"] = base64.StdEncoding.EncodeToString(c.Data)
		} else if c.GapTo != nil {
			item["gap_to"] = *c.GapTo
		}
		chunks = append(chunks, item)
	}
	writeTerminalJSON(w, map[string]any{
		"session": terminalSessionJSON(read.Session),
		"chunks":  chunks,
		"cursor":  read.Cursor,
	})
}

type terminalInputRequest struct {
	SessionID string `json:"session_id"`
	Kind      string `json:"kind"`
	Data      string `json:"data"`
	Cols      int    `json:"cols"`
	Rows      int    `json:"rows"`
	Signal    string `json:"signal"`
}

func (s *BrowserServer) serveTerminalInput(w http.ResponseWriter, r *http.Request) {
	var req terminalInputRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, terminalWSMaxRead)).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	payload := map[string]any{}
	switch req.Kind {
	case "stdin":
		payload["data"] = req.Data
	case "resize":
		payload["cols"] = float64(req.Cols)
		payload["rows"] = float64(req.Rows)
	case "signal":
		payload["signal"] = req.Signal
	case "eof":
	default:
		http.Error(w, "unknown input kind", http.StatusBadRequest)
		return
	}
	var input agentstate.TerminalInput
	if !s.terminalAuth(w, r, func(paid string) error {
		var err error
		input, err = s.Terminals.SubmitTerminalInput(r.Context(), paid, req.SessionID, "human", req.Kind, payload)
		return err
	}) {
		return
	}
	writeTerminalJSON(w, map[string]any{"input": map[string]any{
		"input_id": input.InputID,
		"seq":      input.Seq,
		"status":   input.Status,
	}})
}

// serveTerminalInputs is the person's read of the durable input ledger:
// the acceptance ack says 'intended' — queued — and only this surface
// reports what delivery actually learned ('written', 'failed', 'unknown',
// 'interrupted', 'expired'). Reading is pure observation; 'unknown' rows
// are never resent.
func (s *BrowserServer) serveTerminalInputs(w http.ResponseWriter, r *http.Request) {
	sessionID := terminalSessionIDParam(r)
	var afterSeq int64
	if v := strings.TrimSpace(r.URL.Query().Get("after_seq")); v != "" {
		if _, err := fmt.Sscanf(v, "%d", &afterSeq); err != nil || afterSeq < 0 {
			http.Error(w, "bad after_seq", http.StatusBadRequest)
			return
		}
	}
	var (
		session agentstate.TerminalSession
		inputs  []agentstate.TerminalInput
	)
	if !s.terminalAuth(w, r, func(paid string) error {
		var err error
		if session, err = s.Terminals.GetTerminalSession(r.Context(), paid, sessionID); err != nil {
			return err
		}
		inputs, err = s.Terminals.ListTerminalInputs(r.Context(), paid, sessionID, afterSeq, 0)
		return err
	}) {
		return
	}
	wires := make([]map[string]any, 0, len(inputs))
	for _, in := range inputs {
		wire := map[string]any{
			"input_id":   in.InputID,
			"seq":        in.Seq,
			"kind":       in.Kind,
			"payload":    in.Payload,
			"source":     in.Source,
			"status":     in.Status,
			"created_at": in.CreatedAt,
			"updated_at": in.UpdatedAt,
		}
		if in.Detail != nil {
			wire["detail"] = in.Detail
		}
		wires = append(wires, wire)
	}
	writeTerminalJSON(w, map[string]any{
		"session": terminalSessionJSON(session),
		"inputs":  wires,
	})
}

type terminalControlRequest struct {
	SessionID string `json:"session_id"`
	Hold      bool   `json:"hold"`
}

func (s *BrowserServer) serveTerminalControl(w http.ResponseWriter, r *http.Request) {
	var req terminalControlRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	var session agentstate.TerminalSession
	if !s.terminalAuth(w, r, func(paid string) error {
		var err error
		session, err = s.Terminals.SetTerminalControl(r.Context(), paid, req.SessionID, req.Hold, terminalHumanControlLease)
		return err
	}) {
		return
	}
	writeTerminalJSON(w, map[string]any{"session": terminalSessionJSON(session)})
}

type terminalCloseRequest struct {
	SessionID string `json:"session_id"`
}

func (s *BrowserServer) serveTerminalClose(w http.ResponseWriter, r *http.Request) {
	var req terminalCloseRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	var session agentstate.TerminalSession
	if !s.terminalAuth(w, r, func(paid string) error {
		var err error
		session, err = s.Terminals.CloseTerminalSession(r.Context(), paid, req.SessionID, "closed by the person")
		return err
	}) {
		return
	}
	writeTerminalJSON(w, map[string]any{"session": terminalSessionJSON(session)})
}

// Terminal attach WebSocket: streams durable output from a cursor and
// accepts input frames on the same socket. The socket is a reader of
// the shared scrollback — closing it never ends the session, and any
// number of attaches see the same bytes the secretary sees.
//
// Client → server frames:
//
//	{"type":"stdin","data":<base64>}
//	{"type":"resize","cols":n,"rows":n}
//	{"type":"signal","signal":"INT"}
//	{"type":"eof"}
//	{"type":"control","hold":bool}
//	{"type":"close"}
//
// Server → client frames:
//
//	{"type":"session", session:{...}}
//	{"type":"output","base":n,"data":<base64>}
//	{"type":"gap","base":n,"to":m}
//	{"type":"input_ack","input_id","seq","status"}
//	{"type":"ended","status","reason","exit_code"?,"exit_signal"?}
//	{"type":"error","code","message"}
type terminalWSInbound struct {
	Type   string `json:"type"`
	Data   string `json:"data"`
	Cols   int    `json:"cols"`
	Rows   int    `json:"rows"`
	Signal string `json:"signal"`
	Hold   bool   `json:"hold"`
}

func (s *BrowserServer) serveTerminalWS(w http.ResponseWriter, r *http.Request) {
	if s.Sessions == nil || s.Terminals == nil || s.LifecycleFence == nil {
		http.Error(w, "terminal unavailable", http.StatusServiceUnavailable)
		return
	}
	if s.TerminalAuthorizer == nil {
		http.Error(w, "authorization unavailable", http.StatusServiceUnavailable)
		return
	}
	if !s.checkOrigin(r) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return
	}
	cookie, err := uniqueBrowserSessionCookie(r)
	if err != nil {
		http.Error(w, "invalid session", http.StatusUnauthorized)
		return
	}
	claims, err := s.Sessions.VerifySession(r.Context(), cookie.Value)
	if err != nil {
		http.Error(w, "invalid session", http.StatusUnauthorized)
		return
	}
	scope, err := directChatScopeFromRequest(r)
	if err != nil {
		writeDirectChatInvalidScope(w)
		return
	}
	sessionID := terminalSessionIDParam(r)
	var cursor int64
	if v := strings.TrimSpace(r.URL.Query().Get("cursor")); v != "" {
		if _, err := fmt.Sscanf(v, "%d", &cursor); err != nil || cursor < 0 {
			http.Error(w, "bad cursor", http.StatusBadRequest)
			return
		}
	}
	paid := claims.PersonalityAgentID
	// Authorize the attach and verify the session belongs to this
	// persona before the upgrade — a rejected session cannot consume a
	// socket or learn whether the terminal exists.
	var session agentstate.TerminalSession
	releaseLifecycle, err := s.LifecycleFence.AcquireOperation(r.Context())
	if err != nil {
		http.Error(w, "authorization unavailable", http.StatusServiceUnavailable)
		return
	}
	err = s.authorizeBrowserTerminalOperationUnderFence(r.Context(), claims, scope, func() error {
		var gerr error
		session, gerr = s.Terminals.GetTerminalSession(r.Context(), paid, sessionID)
		return gerr
	})
	if err != nil {
		releaseLifecycle()
		switch {
		case errors.Is(err, agentstate.ErrTerminalNotFound):
			http.Error(w, "terminal session not found", http.StatusNotFound)
		case errors.Is(err, ErrDirectChatAuthorizationUnavailable):
			http.Error(w, "authorization unavailable", http.StatusServiceUnavailable)
		default:
			http.Error(w, "not authorized", http.StatusForbidden)
		}
		return
	}
	conn, err := s.upgrader.Upgrade(w, r, nil)
	// The admission epoch ends once the socket is upgraded. Holding the
	// permit for the socket's lifetime would stall every installation and
	// Employer mutation behind an idle attach; live effects and the
	// periodic recheck acquire their own bounded permits instead.
	releaseLifecycle()
	if err != nil {
		return
	}
	s.runTerminalSocket(conn, claims, scope, paid, session, cursor)
}

func (s *BrowserServer) runTerminalSocket(conn *websocket.Conn, claims UserSessionClaims, scope directChatScope, paid string, session agentstate.TerminalSession, cursor int64) {
	defer conn.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn.SetReadLimit(terminalWSMaxRead)

	sendMu := &sync.Mutex{}
	send := func(v any) bool {
		sendMu.Lock()
		defer sendMu.Unlock()
		_ = conn.SetWriteDeadline(time.Now().Add(s.writeTimeout()))
		return conn.WriteJSON(v) == nil
	}
	sendError := func(code, msg string) {
		send(map[string]any{"type": "error", "code": code, "message": msg})
	}
	if !send(map[string]any{"type": "session", "session": terminalSessionJSON(session)}) {
		return
	}
	if session.OutputBase > cursor {
		// The requested cursor predates the retained scrollback — say so
		// explicitly before replaying what survives.
		if !send(map[string]any{"type": "gap", "base": cursor, "to": session.OutputBase}) {
			return
		}
		cursor = session.OutputBase
	}

	// Input loop: human frames become durable ledger entries the runner
	// delivers in order — the same path the secretary's tool effects
	// take, so neither writer's accepted bytes are dropped or reordered.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		for {
			var in terminalWSInbound
			if err := conn.ReadJSON(&in); err != nil {
				return
			}
			switch in.Type {
			case "stdin":
				data, err := base64.StdEncoding.DecodeString(in.Data)
				if err != nil {
					sendError("bad_input", "stdin data must be base64")
					continue
				}
				s.terminalWSInput(ctx, claims, scope, paid, session.SessionID, "stdin",
					map[string]any{"data": string(data)}, send, sendError)
			case "resize":
				s.terminalWSInput(ctx, claims, scope, paid, session.SessionID, "resize",
					map[string]any{"cols": float64(in.Cols), "rows": float64(in.Rows)}, send, sendError)
			case "signal":
				s.terminalWSInput(ctx, claims, scope, paid, session.SessionID, "signal",
					map[string]any{"signal": in.Signal}, send, sendError)
			case "eof":
				s.terminalWSInput(ctx, claims, scope, paid, session.SessionID, "eof",
					map[string]any{}, send, sendError)
			case "control":
				err := s.authorizeBrowserTerminalOperation(ctx, claims, scope, func() error {
					_, err := s.Terminals.SetTerminalControl(ctx, paid, session.SessionID, in.Hold, terminalHumanControlLease)
					return err
				})
				if err != nil {
					sendError(terminalErrorCode(err), "control update rejected")
				}
			case "close":
				err := s.authorizeBrowserTerminalOperation(ctx, claims, scope, func() error {
					_, err := s.Terminals.CloseTerminalSession(ctx, paid, session.SessionID, "closed by the person")
					return err
				})
				if err != nil {
					sendError(terminalErrorCode(err), "close rejected")
				}
			default:
				sendError("bad_input", "unknown frame type")
			}
		}
	}()

	// Output loop: poll the durable scrollback — the runner appends,
	// every attach reads the same absolute offsets, and a gap frame
	// carries a retention boundary instead of silently skipping bytes.
	poll := time.NewTicker(terminalWSPollInterval)
	defer poll.Stop()
	authInterval := s.AuthorizationPollInterval
	if authInterval <= 0 {
		authInterval = terminalWSAuthInterval
	}
	authPoll := time.NewTicker(authInterval)
	defer authPoll.Stop()
	lastStatus := session.Status
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case <-authPoll.C:
			// The attach keeps no stale authority: an expired login or a
			// revoked installation drops the socket instead of leaving a
			// live view into the persona's terminal. This acquires a fresh
			// bounded lifecycle permit per recheck — the admission permit
			// was released at upgrade — so a pending lifecycle mutation
			// blocks this socket's recheck, not the other way around.
			authCtx, authCancel := context.WithTimeout(ctx, s.writeTimeout())
			err := s.authorizeBrowserTerminalOperation(authCtx, claims, scope, func() error { return nil })
			authCancel()
			if err != nil {
				_ = conn.WriteControl(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "authorization expired"),
					time.Now().Add(2*time.Second))
				wg.Wait()
				return
			}
		case <-poll.C:
			read, err := s.Terminals.ReadTerminalOutput(ctx, paid, session.SessionID, cursor, terminalReadDefaultLimit)
			if err != nil {
				if errors.Is(err, agentstate.ErrTerminalNotFound) {
					send(map[string]any{"type": "ended", "status": "lost", "reason": "session removed"})
					wg.Wait()
					return
				}
				continue
			}
			for _, c := range read.Chunks {
				if c.Kind == "gap" {
					frame := map[string]any{"type": "gap", "base": c.Base}
					if c.GapTo != nil {
						frame["to"] = *c.GapTo
					}
					if !send(frame) {
						wg.Wait()
						return
					}
					continue
				}
				if !send(map[string]any{
					"type": "output", "base": c.Base,
					"data": base64.StdEncoding.EncodeToString(c.Data),
				}) {
					wg.Wait()
					return
				}
			}
			// Advance past what was emitted — read.Cursor echoes the
			// request, NextCursor is the offset after the last chunk.
			// Using the echo would re-send the same chunks every poll.
			cursor = read.NextCursor
			if read.Session.Status != lastStatus {
				lastStatus = read.Session.Status
				if !send(map[string]any{"type": "session", "session": terminalSessionJSON(read.Session)}) {
					wg.Wait()
					return
				}
			}
			if lastStatus == "ended" || lastStatus == "lost" {
				frame := map[string]any{"type": "ended", "status": lastStatus, "reason": read.Session.EndReason}
				if read.Session.ExitCode != nil {
					frame["exit_code"] = *read.Session.ExitCode
				}
				if read.Session.ExitSignal != "" {
					frame["exit_signal"] = read.Session.ExitSignal
				}
				send(frame)
				wg.Wait()
				return
			}
		}
	}
}

// terminalWSInput re-authorizes each input frame against the terminal
// installation before the ledger write, matching the REST /terminal/input
// boundary — an attach admitted earlier cannot keep writing after the
// installation or login is revoked.
func (s *BrowserServer) terminalWSInput(ctx context.Context, claims UserSessionClaims, scope directChatScope, paid, sessionID, kind string, payload map[string]any, send func(any) bool, sendError func(string, string)) {
	var input agentstate.TerminalInput
	err := s.authorizeBrowserTerminalOperation(ctx, claims, scope, func() error {
		var ierr error
		input, ierr = s.Terminals.SubmitTerminalInput(ctx, paid, sessionID, "human", kind, payload)
		return ierr
	})
	if err != nil {
		sendError(terminalErrorCode(err), "input rejected")
		return
	}
	send(map[string]any{"type": "input_ack", "input_id": input.InputID, "seq": input.Seq, "status": input.Status})
}

func terminalErrorCode(err error) string {
	switch {
	case errors.Is(err, agentstate.ErrTerminalEnded):
		return "ended"
	case errors.Is(err, agentstate.ErrTerminalNotLive):
		return "not_live"
	case errors.Is(err, agentstate.ErrTerminalControl):
		return "control_held"
	case errors.Is(err, agentstate.ErrTerminalNotFound):
		return "not_found"
	case errors.Is(err, agentstate.ErrBadRequest):
		return "bad_input"
	default:
		return "unavailable"
	}
}
