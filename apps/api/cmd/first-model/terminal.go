package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/directchat"
	"github.com/sumi-studio/sumi/apps/api/internal/localterminal"
	"github.com/sumi-studio/sumi/apps/api/internal/termexec"
)

// Local has no Cloud employer/installation account. The install's existing
// fm capability grants a bounded terminal-only cookie, pinned to its persona.
// The browser terminal implementation remains the same HTTP/WS protocol;
// these session/authority adapters supply the Local host's real capability.
type terminalSession struct {
	claims  agentevents.UserSessionClaims
	expires time.Time
}
type localTerminalAuthority struct {
	mu       sync.Mutex
	persona  string
	core     *agentstate.Store
	sessions map[string]terminalSession
}

func (a *localTerminalAuthority) issue() (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	for key, s := range a.sessions {
		if !now.Before(s.expires) {
			delete(a.sessions, key)
		}
	}
	if len(a.sessions) >= 128 {
		return "", errors.New("too many Local terminal attachments")
	}
	token := make([]byte, 32)
	if _, e := rand.Read(token); e != nil {
		return "", e
	}
	cookie := hex.EncodeToString(token)
	sum := sha256.Sum256([]byte(cookie))
	key := hex.EncodeToString(sum[:])
	a.sessions[key] = terminalSession{claims: agentevents.UserSessionClaims{TenantID: "local", UserID: key, PersonalityAgentID: a.persona}, expires: now.Add(time.Hour)}
	return cookie, nil
}
func (a *localTerminalAuthority) VerifySession(ctx context.Context, cookie string) (agentevents.UserSessionClaims, error) {
	sum := sha256.Sum256([]byte(cookie))
	key := hex.EncodeToString(sum[:])
	a.mu.Lock()
	s, ok := a.sessions[key]
	a.mu.Unlock()
	if !ok || !time.Now().Before(s.expires) {
		return agentevents.UserSessionClaims{}, errors.New("Local terminal session expired")
	}
	return s.claims, nil
}
func (a *localTerminalAuthority) AuthorizeSession(ctx context.Context, claims agentevents.UserSessionClaims, operation func() error) error {
	a.mu.Lock()
	s, ok := a.sessions[claims.UserID]
	a.mu.Unlock()
	if !ok || !time.Now().Before(s.expires) || claims.PersonalityAgentID != a.persona || claims.TenantID != "local" {
		return agentevents.ErrDirectChatAuthorizationDenied
	}
	return operation()
}
func (a *localTerminalAuthority) AuthorizeTerminal(ctx context.Context, human, persona, installation string, epoch int64) error {
	if persona != a.persona || installation != a.persona || epoch != 1 {
		return agentevents.ErrDirectChatAuthorizationDenied
	}
	authority, e := a.core.PersonaAuthority(ctx, persona)
	if e != nil {
		return e
	}
	if authority != "active" {
		return agentevents.ErrDirectChatAuthorizationDenied
	}
	return nil
}

// terminalUnavailable answers the human session-mint route when the Local
// terminal is off for this run — Cloud working store or an untrusted
// journal — with the specific bounded reason. No cookie is minted, no
// terminal route is mounted, and no backend is advertised, so neither a
// person nor the secretary can start a shell this run.
func terminalUnavailable(fm *fmServer, detail map[string]any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if _, ok := fm.scope(w, r); !ok {
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		out := map[string]any{"code": "local_terminal_unavailable", "working_store": os.Getenv("SUMI_LOCAL_WORKING_STORE")}
		for k, v := range detail {
			out[k] = v
		}
		writeJSON(w, http.StatusServiceUnavailable, out)
	}
}

func wireLocalTerminal(core *agentstate.Server, fm *fmServer, mux *http.ServeMux, persona, origin string) (func(), error) {
	journal := strings.TrimSpace(os.Getenv("SUMI_LOCAL_TERMINAL_ROOT"))
	workspace := strings.TrimSpace(os.Getenv("SUMI_WORKSPACE_ROOT"))
	if os.Getenv("SUMI_LOCAL_WORKING_STORE") == "cloud" || (journal == "" && workspace == "") {
		mux.HandleFunc("POST /fm/{persona}/terminal-session", terminalUnavailable(fm, map[string]any{"message": "Local terminal requires this install’s Local workspace; Cloud working storage is not mounted as a Local terminal workspace"}))
		return func() {}, nil
	}
	if journal == "" || workspace == "" {
		return nil, errors.New("Local terminal requires both SUMI_LOCAL_TERMINAL_ROOT and SUMI_WORKSPACE_ROOT")
	}
	if _, e := uuid.Parse(persona); e != nil {
		return nil, e
	}
	backend, e := localterminal.New(localterminal.Config{PersonaID: persona, WorkspaceRoot: workspace, JournalRoot: journal})
	if e != nil {
		if !errors.Is(e, localterminal.ErrJournal) {
			// Configuration and ownership problems are not a journal
			// failure — they still refuse the whole service start.
			return nil, e
		}
		// A record that cannot be validated may hide a live shell, so the
		// terminal stays disabled — but the journal bytes are preserved and
		// only this capability is offline: state, the Core, saved MCP
		// connections and file access all still start. Recovery keeps the
		// operation's identity and uncertain-execution history: restore a
		// valid record for it, then restart. Nothing may be deleted or
		// reset into permission to launch a possibly-live shell.
		log.Printf("Local terminal unavailable this run: %v (journal preserved under %s; restore a valid record retaining each operation's identity and uncertain-execution history, then restart — the terminal stays unavailable until every record validates)", e, journal)
		mux.HandleFunc("POST /fm/{persona}/terminal-session", terminalUnavailable(fm, map[string]any{"reason": "journal_invalid", "message": "Local terminal journal could not be validated (" + e.Error() + "); its records are preserved. Restore a valid record retaining each operation's identity and uncertain-execution history, then restart to re-enable the terminal."}))
		return func() {}, nil
	}
	driver := termexec.New(core.Store(), backend, nil, termexec.Config{RunnerID: "local-terminal-" + persona, Backend: "local", Shell: "/bin/bash", ShellArgs: []string{"--noprofile", "--norc", "-i"}, Interval: 100 * time.Millisecond, PollInterval: 100 * time.Millisecond, HeartbeatEvery: 20})
	core.Store().SetDefaultTerminalBackend("local")
	core.Store().SetTerminalBackendAvailable("local")
	authority := &localTerminalAuthority{persona: persona, core: core.Store(), sessions: map[string]terminalSession{}}
	browser := agentevents.NewBrowserServer(authority, nil, nil)
	browser.Terminals = core.Store()
	browser.TerminalHealth = driver
	browser.TerminalAuthorizer = authority
	browser.SetLifecycleFence(directchat.NewLifecycleFence())
	browser.AllowedOrigins = []string{origin}
	browser.RegisterTerminalRoutes(mux)
	mux.HandleFunc("POST /fm/{persona}/terminal-session", func(w http.ResponseWriter, r *http.Request) {
		id, ok := fm.scope(w, r)
		if !ok {
			return
		}
		if id != persona {
			http.Error(w, "Local terminal belongs to another persona", http.StatusForbidden)
			return
		}
		if e := authority.AuthorizeTerminal(r.Context(), "", persona, persona, 1); e != nil {
			http.Error(w, "Local terminal unavailable for this persona", http.StatusForbidden)
			return
		}
		cookie, e := authority.issue()
		if e != nil {
			http.Error(w, e.Error(), http.StatusTooManyRequests)
			return
		}
		http.SetCookie(w, &http.Cookie{Name: agentevents.BrowserSessionCookie, Value: cookie, Path: "/terminal", HttpOnly: true, SameSite: http.SameSiteStrictMode, MaxAge: 3600})
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, http.StatusOK, map[string]any{"installation_id": persona, "authority_epoch": 1, "expires_in": 3600, "terminal_base": "/terminal", "workspace": "local"})
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); driver.Run(ctx) }()
	return func() { cancel(); <-done; _ = backend.Close() }, nil
}
