package cloudbrowser

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/chatgpt"
)

// Service mounts the person's routes (browser session + CSRF, like the other
// connection settings) and the browser Worker's host routes (shared runtime
// bearer, never reachable through the public web edge).
type Service struct {
	Store        *Store
	Authenticate func(*http.Request) (chatgpt.LoginIdentity, error)
	// RuntimeToken authenticates the browser Worker; WakeURL is its origin.
	RuntimeToken string
	WakeURL      string
	Client       *http.Client

	mu    sync.Mutex
	marks map[string]time.Time
	// holds: the Worker refused a start (Browser Run limit, failure) and
	// asked not to be woken for work before this time.
	holds map[string]time.Time
}

func (s *Service) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/cloud-browser", s.human)
	mux.HandleFunc("POST /api/cloud-browser/profiles", s.human)
	mux.HandleFunc("DELETE /api/cloud-browser/profiles/{id}", s.human)
	mux.HandleFunc("POST /api/cloud-browser/profiles/{id}/viewer-ticket", s.human)
	mux.HandleFunc("POST /api/cloud-browser/profiles/{id}/grants", s.human)
	mux.HandleFunc("DELETE /api/cloud-browser/profiles/{id}/grants/{attachment}", s.human)
	mux.HandleFunc("PUT /api/cloud-browser/jev-key", s.human)
	mux.HandleFunc("DELETE /api/cloud-browser/jev-key", s.human)
	mux.HandleFunc("POST /api/cloud-browser-host/profiles/{id}/begin", s.host)
	mux.HandleFunc("POST /api/cloud-browser-host/profiles/{id}/refresh", s.host)
	mux.HandleFunc("POST /api/cloud-browser-host/profiles/{id}/snapshot", s.host)
	mux.HandleFunc("POST /api/cloud-browser-host/profiles/{id}/state", s.host)
	mux.HandleFunc("POST /api/cloud-browser-host/profiles/{id}/jev-rejected", s.host)
}

func reply(w http.ResponseWriter, status int, out any) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(out)
}

func problem(w http.ResponseWriter, e error) {
	status, code := 503, "cloud_browser_unavailable"
	switch {
	case errors.Is(e, ErrInvalid):
		status, code = 400, "invalid_request"
	case errors.Is(e, ErrUnavailable):
		status, code = 403, "not_authorized"
	case errors.Is(e, ErrNotFound):
		status, code = 404, "not_found"
	case errors.Is(e, ErrStale):
		status, code = 409, "stale"
	}
	reply(w, status, map[string]string{"error": code})
}

func decode(w http.ResponseWriter, r *http.Request, limit int64, out any) bool {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, limit))
	d.DisallowUnknownFields()
	var tail any
	return d.Decode(out) == nil && d.Decode(&tail) == io.EOF
}

func (s *Service) configured() bool { return s != nil && s.Store != nil }

func (s *Service) human(w http.ResponseWriter, r *http.Request) {
	if s.Authenticate == nil {
		problem(w, ErrUnavailable)
		return
	}
	identity, err := s.Authenticate(r)
	if err != nil || identity.HumanID == "" || identity.Authorize == nil {
		problem(w, ErrUnavailable)
		return
	}
	if !s.configured() {
		if r.Method == http.MethodGet && r.URL.Path == "/api/cloud-browser" {
			reply(w, 200, map[string]any{"configured": false})
			return
		}
		problem(w, errors.New("unavailable"))
		return
	}
	var in struct {
		PersonaID    string `json:"persona_id"`
		TabID        string `json:"tab_id"`
		Name         string `json:"name"`
		AllowActions bool   `json:"allow_actions"`
		APIKey       string `json:"api_key"`
	}
	if (r.Method == http.MethodPost || r.Method == http.MethodPut) && r.ContentLength != 0 && !decode(w, r, 16<<10, &in) {
		problem(w, ErrInvalid)
		return
	}
	human := identity.HumanID
	id := r.PathValue("id")
	var out any
	err = identity.Authorize(r.Context(), func(ctx context.Context) error {
		var err error
		switch {
		case r.Method == http.MethodGet:
			var personas []Persona
			var profiles []Profile
			var jev JevStatus
			if personas, err = s.Store.Personas(ctx, human); err != nil {
				return err
			}
			if profiles, err = s.Store.Profiles(ctx, human); err != nil {
				return err
			}
			if jev, err = s.Store.JevStatus(ctx, human); err != nil {
				return err
			}
			out = map[string]any{"configured": true, "personas": personas, "profiles": profiles, "jev": jev, "snapshot_limit_bytes": MaxSnapshotBytes}
		case strings.HasSuffix(r.URL.Path, "/jev-key"):
			if r.Method == http.MethodDelete {
				err = s.Store.DeleteJevKey(ctx, human)
				out = map[string]any{"jev": JevStatus{}}
			} else {
				var st JevStatus
				st, err = s.Store.SetJevKey(ctx, human, in.APIKey)
				out = map[string]any{"jev": st}
			}
		case r.URL.Path == "/api/cloud-browser/profiles":
			var p Profile
			p, err = s.Store.CreateProfile(ctx, human, in.PersonaID)
			out = map[string]any{"profile": p}
		case strings.HasSuffix(r.URL.Path, "/viewer-ticket"):
			var ticket string
			var exp time.Time
			ticket, exp, err = s.Store.IssueTicket(ctx, human, id)
			out = map[string]any{"ticket": ticket, "expires_at": exp, "path": "/browser-cloud/viewer"}
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/grants"):
			var g Grant
			g, err = s.Store.Grant(ctx, human, id, in.TabID, in.Name, in.AllowActions)
			out = map[string]any{"grant": g}
		case r.Method == http.MethodDelete && r.PathValue("attachment") != "":
			err = s.Store.Revoke(ctx, human, id, r.PathValue("attachment"))
			out = map[string]bool{"revoked": err == nil}
		case r.Method == http.MethodDelete:
			err = s.Store.ResetProfile(ctx, human, id)
			out = map[string]bool{"reset": err == nil}
		default:
			err = ErrNotFound
		}
		return err
	})
	if err != nil {
		problem(w, err)
		return
	}
	reply(w, 200, out)
}

func (s *Service) host(w http.ResponseWriter, r *http.Request) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !s.configured() || s.RuntimeToken == "" || !ok || r.Header.Get("Origin") != "" || subtle.ConstantTimeCompare([]byte(token), []byte(s.RuntimeToken)) != 1 {
		problem(w, ErrUnavailable)
		return
	}
	id := r.PathValue("id")
	op := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
	var in struct {
		Fresh       bool            `json:"fresh"`
		Incarnation int64           `json:"incarnation"`
		Known       []string        `json:"known"`
		Seq         int64           `json:"seq"`
		Version     int             `json:"version"`
		TabIDs      []string        `json:"tab_ids"`
		Snapshot    json.RawMessage `json:"snapshot"`
		State       string          `json:"state"`
		KeyVersion  int64           `json:"key_version"`
	}
	if r.ContentLength != 0 && !decode(w, r, MaxSnapshotBytes+(64<<10), &in) {
		problem(w, ErrInvalid)
		return
	}
	ctx := r.Context()
	var out any
	var err error
	switch op {
	case "begin":
		out, err = s.Store.Begin(ctx, id, in.Fresh, in.Incarnation)
	case "refresh":
		out, err = s.Store.Refresh(ctx, id, in.Incarnation, in.Known)
	case "snapshot":
		var at time.Time
		at, err = s.Store.SaveSnapshot(ctx, id, in.Incarnation, in.Seq, in.Version, in.TabIDs, in.Snapshot)
		out = map[string]any{"seq": in.Seq, "saved_at": at}
	case "state":
		err = s.Store.SetState(ctx, id, in.Incarnation, in.State)
		out = map[string]any{"state": in.State}
	case "jev-rejected":
		var rejected bool
		rejected, err = s.Store.JevRejected(ctx, id, in.KeyVersion)
		out = map[string]any{"rejected": rejected}
	default:
		err = ErrNotFound
	}
	if err != nil {
		problem(w, err)
		return
	}
	reply(w, 200, out)
}

// Run wakes the browser Worker for queued Cloud tab work and undelivered
// grant/credential changes. It sends nothing for idle profiles: a sleeping
// profile costs no browser time and no Worker requests.
func (s *Service) Run(ctx context.Context) {
	if !s.configured() || s.WakeURL == "" {
		return
	}
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.Sweep(ctx)
		}
	}
}

// wakeGap spaces repeated wakes for the same still-pending profile: the
// Worker needs a few seconds to start or restore a browser.
const wakeGap = 10 * time.Second

// refreshGap spaces wakes that deliver grant or key changes to a running
// browser, which applies them at once; a start in progress picks them up too.
const refreshGap = 2 * time.Second

// maxHold bounds how long a Worker's retry_after_ms can pause work wakes.
const maxHold = 15 * time.Minute

// wakeReply is the Worker's answer to a wake.
type wakeReply struct {
	Accepted     bool   `json:"accepted"`
	Phase        string `json:"phase"`
	RetryAfterMS int64  `json:"retry_after_ms"`
}

// hosting: phases in which the Worker has, or is building, a browser that
// will receive grant and key changes.
func hosting(phase string) bool {
	switch phase {
	case "live", "starting", "restoring", "saving":
		return true
	}
	return false
}

func (s *Service) Sweep(ctx context.Context) int {
	wakes, err := s.Store.WakeCandidates(ctx, 64)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("cloud browser wake sweep failed: %v", err)
		}
		return 0
	}
	now := time.Now()
	s.mu.Lock()
	if s.marks == nil {
		s.marks = map[string]time.Time{}
		s.holds = map[string]time.Time{}
	}
	for id, at := range s.marks {
		if now.Sub(at) > time.Minute {
			delete(s.marks, id)
		}
	}
	for id, until := range s.holds {
		if !now.Before(until) {
			delete(s.holds, id)
		}
	}
	due := []Wake{}
	for _, w := range wakes {
		gap := wakeGap
		if w.Refresh {
			gap = refreshGap
		}
		if at, ok := s.marks[w.ProfileID]; ok && now.Sub(at) < gap {
			continue
		}
		if _, held := s.holds[w.ProfileID]; held && !w.Refresh {
			continue
		}
		s.marks[w.ProfileID] = now
		due = append(due, w)
	}
	s.mu.Unlock()
	sent := 0
	for _, w := range due {
		answer, err := s.wake(ctx, w)
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("cloud browser wake %s failed: %v", w.ProfileID, err)
			}
			continue
		}
		sent++
		// No browser to deliver changes to (never started, refused, or the
		// Worker lost it): stop waking for them; the next start gets them.
		if w.Refresh && w.RefreshAt != nil && !hosting(answer.Phase) {
			if err := s.Store.DropRefresh(ctx, w.ProfileID, *w.RefreshAt); err != nil && ctx.Err() == nil {
				log.Printf("cloud browser refresh drop %s failed: %v", w.ProfileID, err)
			}
		}
		if w.Work && !answer.Accepted && answer.RetryAfterMS > 0 {
			hold := min(time.Duration(answer.RetryAfterMS)*time.Millisecond, maxHold)
			s.mu.Lock()
			s.holds[w.ProfileID] = time.Now().Add(hold)
			s.mu.Unlock()
		}
	}
	return sent
}

func (s *Service) wake(ctx context.Context, w Wake) (wakeReply, error) {
	client := s.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	body, _ := json.Marshal(map[string]bool{"work": w.Work})
	var answer wakeReply
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(s.WakeURL, "/")+"/profiles/"+url.PathEscape(w.ProfileID)+"/wake", bytes.NewReader(body))
	if err != nil {
		return answer, err
	}
	req.Header.Set("Authorization", "Bearer "+s.RuntimeToken)
	req.Header.Set("Content-Type", "application/json")
	res, err := client.Do(req)
	if err != nil {
		return answer, err
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
	if res.StatusCode/100 != 2 {
		return answer, fmt.Errorf("wake returned %d", res.StatusCode)
	}
	// An unreadable answer counts as "hosting" (nothing is dropped).
	if json.Unmarshal(raw, &answer) != nil {
		answer = wakeReply{Accepted: true, Phase: "live"}
	}
	return answer, nil
}
