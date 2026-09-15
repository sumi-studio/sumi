package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/portable"
	"github.com/sumi-studio/sumi/apps/api/internal/transfersession"
)

var (
	sessionPathRe = regexp.MustCompile(`^(.*)` + regexp.QuoteMeta(transfersession.RoutePrefix) +
		`/sessions/([0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})$`)
	grantRe = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)

	errGrantRejected = errors.New("Cloud rejected the move grant for this session")
	errUnreachable   = errors.New("Cloud is unreachable")
	errHeaderDone    = errors.New("header captured")
)

// parseMoveURL splits a move URL into the session resource URL and the grant
// from its fragment. The session resource is the destination: its origin and
// path name the Cloud and the session, and the transfer id is the session id.
func parseMoveURL(raw string) (sessionURL, sessionID, grant string, err error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", "", fmt.Errorf("the move URL does not parse")
	}
	switch {
	case u.Scheme == "https":
	case u.Scheme == "http" && transfersession.IsLoopbackHost(u.Hostname()):
	default:
		return "", "", "", fmt.Errorf("the move URL must use https")
	}
	if u.Host == "" || u.User != nil {
		return "", "", "", fmt.Errorf("the move URL must name a host and carry no user info")
	}
	if u.RawQuery != "" || u.ForceQuery {
		return "", "", "", fmt.Errorf("the move URL must not carry a query; the grant travels only in the fragment")
	}
	m := sessionPathRe.FindStringSubmatch(u.EscapedPath())
	if m == nil {
		return "", "", "", fmt.Errorf("the move URL is not a Sumi secretary-transfer session URL")
	}
	frag, err := url.ParseQuery(u.Fragment)
	if err != nil || len(frag) != 1 || len(frag["grant"]) != 1 || !grantRe.MatchString(frag.Get("grant")) {
		return "", "", "", fmt.Errorf("the move URL must end with #grant=<grant>")
	}
	return u.Scheme + "://" + u.Host + u.EscapedPath(), m[2], frag.Get("grant"), nil
}

// moveState is the durable record of one move in <home>/move/state.json. It
// records intent and what Cloud said it is; authority is always re-read from
// the Local ledger and the Cloud session, never trusted from this file.
type moveState struct {
	Version     int       `json:"version"`
	SessionURL  string    `json:"session_url"`
	SessionID   string    `json:"session_id"`
	Grant       string    `json:"grant"`
	PersonaID   string    `json:"persona_id"`
	Destination string    `json:"destination_placement_id,omitempty"`
	Outcome     string    `json:"outcome,omitempty"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type mover struct {
	dir       string
	personaID string
	src       *portable.Service
	state     *agentstate.Store
	client    *http.Client
	out       io.Writer

	wait        time.Duration
	poll        time.Duration
	unreachable time.Duration
	sealRetries int
	sealDelay   time.Duration
}

func newMover(home, personaID string, src *portable.Service, state *agentstate.Store, out io.Writer) *mover {
	return &mover{
		dir: filepath.Join(home, "move"), personaID: personaID, src: src, state: state, out: out,
		client: &http.Client{
			// Never follow a redirect: the grant is for this session URL only.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		wait: 30 * time.Minute, poll: 2 * time.Second, unreachable: 5 * time.Minute,
		sealRetries: 5, sealDelay: 10 * time.Second,
	}
}

func (m *mover) say(format string, args ...any) { fmt.Fprintf(m.out, format+"\n", args...) }

func (m *mover) statePath() string { return filepath.Join(m.dir, "state.json") }

func (m *mover) load() (*moveState, error) {
	raw, err := os.ReadFile(m.statePath())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var st moveState
	if err := json.Unmarshal(raw, &st); err != nil {
		return nil, fmt.Errorf("%s is unreadable (%v); it is kept as-is", m.statePath(), err)
	}
	return &st, nil
}

// save replaces the state file atomically: a crash leaves the old or the new
// record, never a torn one.
func (m *mover) save(st *moveState) error {
	st.UpdatedAt = time.Now().UTC()
	raw, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(m.dir, ".state-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), m.statePath()); err != nil {
		return err
	}
	d, err := os.Open(m.dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// lock takes an exclusive, non-blocking lock so two invocations never drive
// the same move at once. The kernel releases it if the process dies.
func (m *mover) lock() (func(), error) {
	if err := os.MkdirAll(m.dir, 0o700); err != nil {
		return nil, err
	}
	if err := os.Chmod(m.dir, 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(m.dir, "lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("another sumi-local-move is already running for this state home")
	}
	return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); f.Close() }, nil
}

func (m *mover) fail(err error) int {
	m.say("sumi-local-move: %v", err)
	return exitError
}

func (m *mover) Start(ctx context.Context, rawURL string) int {
	sessionURL, sessionID, grant, err := parseMoveURL(rawURL)
	if err != nil {
		m.say("sumi-local-move: %v", err)
		return exitUsage
	}
	release, err := m.lock()
	if err != nil {
		return m.fail(err)
	}
	defer release()
	st, err := m.load()
	if err != nil {
		return m.fail(err)
	}
	if st != nil && st.Outcome == "" {
		if st.SessionID == sessionID {
			return m.drive(ctx, st)
		}
		return m.fail(fmt.Errorf("a move to session %s is still in progress; run resume or cancel first", st.SessionID))
	}
	if st != nil {
		if err := os.Rename(m.statePath(), filepath.Join(m.dir, "state-"+st.SessionID+".json")); err != nil {
			return m.fail(err)
		}
	}
	st = &moveState{Version: 1, SessionURL: sessionURL, SessionID: sessionID, Grant: grant, PersonaID: m.personaID}
	if err := m.save(st); err != nil {
		return m.fail(err)
	}
	return m.drive(ctx, st)
}

func (m *mover) current() (*moveState, int) {
	st, err := m.load()
	if err != nil {
		return nil, m.fail(err)
	}
	if st == nil {
		m.say("No move is recorded in %s. Start one with: sumi-local-move start", m.dir)
		return nil, exitUsage
	}
	if st.PersonaID != m.personaID {
		return nil, m.fail(fmt.Errorf("the recorded move is for secretary %s, but this configuration runs %s", st.PersonaID, m.personaID))
	}
	return st, -1
}

func (m *mover) Resume(ctx context.Context) int {
	release, err := m.lock()
	if err != nil {
		return m.fail(err)
	}
	defer release()
	st, code := m.current()
	if st == nil {
		return code
	}
	if st.Outcome != "" {
		m.say("This move already finished: %s.", st.Outcome)
		return exitDone
	}
	return m.drive(ctx, st)
}

// drive advances the move until it finishes, needs the person to finish the
// Cloud registration, or cannot reach Cloud. Every decision re-reads the
// Local ledger and the Cloud session, so it is safe to repeat after any
// interruption.
func (m *mover) drive(ctx context.Context, st *moveState) int {
	waitStart := time.Now()
	uploads := 0
	for {
		v, err := m.fetch(ctx, st)
		if err != nil {
			return m.stopped(ctx, err)
		}
		if err := m.checkDestination(st, v); err != nil {
			return m.fail(err)
		}
		exp, err := m.src.Status(ctx, "export", st.SessionID)
		if errors.Is(err, portable.ErrTransferNotFound) {
			switch v.Status {
			case transfersession.StatusAwaitingBundle:
				if code, done := m.seal(ctx, st, v); done {
					return code
				}
				continue
			case transfersession.StatusCancelled, transfersession.StatusExpired:
				return m.finish(st, "cancelled_unsealed",
					"The Cloud session is %s and this secretary was never sealed here; it stays active on Local.", v.Status)
			default:
				return m.fail(fmt.Errorf("Cloud reports the session %s, but this Local placement never sealed transfer %s; nothing was changed", v.Status, st.SessionID))
			}
		}
		if err != nil {
			return m.fail(err)
		}
		switch exp.Status {
		case "completed":
			return m.finish(st, "transferred",
				"Sumi moved to Sumi Cloud. This Local copy no longer answers. Choose a model connection in Sumi Cloud before the secretary can reply; files in the Local workspace were not carried.")
		case "aborted":
			return m.finish(st, "aborted", "The move was cancelled. The secretary is active on Local again.")
		}
		if exp.DestinationID != v.DestinationPlacementID {
			return m.fail(fmt.Errorf("this secretary was sealed for placement %s, but the session reports %s; nothing was aborted and the secretary stays sealed", exp.DestinationID, v.DestinationPlacementID))
		}

		switch v.Status {
		case transfersession.StatusAwaitingBundle:
			uploads++
			if uploads > 5 {
				m.say("The upload did not finish after %d attempts. The secretary stays sealed (it does not answer). Run: sumi-local-move resume", uploads-1)
				return exitPending
			}
			if err := m.upload(ctx, st); err != nil {
				if errors.Is(err, errUnreachable) || errors.Is(err, context.Canceled) {
					m.say("Upload interrupted: %v. Retrying.", err)
					if serr := sleep(ctx, m.poll); serr != nil {
						return m.stopped(ctx, serr)
					}
					continue
				}
				return m.fail(err)
			}
		case transfersession.StatusStaged, transfersession.StatusProvisioned:
			if v.Status == transfersession.StatusStaged && time.Since(waitStart) >= m.wait {
				until := ""
				if v.ClaimUntil != nil {
					until = " before " + v.ClaimUntil.Local().Format(time.RFC1123)
				}
				m.say("Sumi arrived in Sumi Cloud and is waiting for the registration to finish%s. The secretary does not answer on Local meanwhile. Run sumi-local-move resume afterwards (or cancel).", until)
				return exitPending
			}
			if err := sleep(ctx, m.poll); err != nil {
				return m.stopped(ctx, err)
			}
		case transfersession.StatusActivated:
			if _, err := m.src.Complete(ctx, m.personaID, st.SessionID, v.ActivateProof); err != nil {
				return m.fail(fmt.Errorf("complete the move with Cloud's activation proof: %w", err))
			}
		case transfersession.StatusCancelled, transfersession.StatusExpired:
			if v.RetireProof != "" {
				if _, err := m.src.Abort(ctx, m.personaID, st.SessionID, v.RetireProof); err != nil {
					return m.fail(fmt.Errorf("end the seal with Cloud's retirement proof: %w", err))
				}
				continue
			}
			if code, done := m.requestRetire(ctx, st, waitStart); done {
				return code
			}
		default:
			return m.fail(fmt.Errorf("Cloud reports an unknown session status %q", v.Status))
		}
	}
}

// stopped explains an interruption without changing authority.
func (m *mover) stopped(ctx context.Context, err error) int {
	if errors.Is(err, errGrantRejected) {
		return m.fail(err)
	}
	if errors.Is(err, errUnreachable) || ctx.Err() != nil {
		m.say("Stopped before the move finished (%v). %s Run: sumi-local-move resume", err, m.authorityLine(ctx))
		return exitPending
	}
	return m.fail(err)
}

func (m *mover) authorityLine(ctx context.Context) string {
	ps, err := m.state.PersonaState(context.WithoutCancel(ctx), m.personaID)
	if err != nil {
		return ""
	}
	switch ps.Persona.Authority {
	case "sealed":
		return "The secretary stays sealed on Local: it does not answer and new messages are refused until the move finishes or is cancelled."
	case "active":
		return "The secretary is still active on Local."
	}
	return "Local authority: " + ps.Persona.Authority + "."
}

func (m *mover) finish(st *moveState, outcome, format string, args ...any) int {
	st.Outcome = outcome
	if err := m.save(st); err != nil {
		return m.fail(err)
	}
	m.say(format, args...)
	return exitDone
}

func (m *mover) checkDestination(st *moveState, v transfersession.View) error {
	if v.SessionID != st.SessionID || v.TransferID != st.SessionID {
		return fmt.Errorf("Cloud answered for session %s transfer %s, not %s; nothing was changed", v.SessionID, v.TransferID, st.SessionID)
	}
	if st.Destination == "" {
		st.Destination = v.DestinationPlacementID
		return m.save(st)
	}
	if st.Destination != v.DestinationPlacementID {
		return fmt.Errorf("Cloud now reports placement %s, but this move is addressed to %s; nothing was aborted", v.DestinationPlacementID, st.Destination)
	}
	return nil
}

func (m *mover) seal(ctx context.Context, st *moveState, v transfersession.View) (int, bool) {
	m.say("Sealing the secretary for Sumi Cloud. Carried: core state only (journal, inputs, turns, plans, operations, approvals, reminders, outbox, memory).")
	for _, x := range v.NotIncluded {
		m.say("  not carried: %s — %s", x.Name, x.Reason)
	}
	for i := 1; ; i++ {
		_, err := m.src.Seal(ctx, m.personaID, st.SessionID, v.DestinationPlacementID)
		if err == nil {
			return 0, false
		}
		if errors.Is(err, portable.ErrUnresolvedOperations) {
			if i < m.sealRetries {
				m.say("Sumi is still processing (%v); retrying in %s.", err, m.sealDelay)
				if serr := sleep(ctx, m.sealDelay); serr != nil {
					return m.stopped(ctx, serr), true
				}
				continue
			}
			m.say("Sumi is still processing, so it was not sealed and stays active on Local. Run sumi-local-move resume when it has finished. (%v)", err)
			return exitPending, true
		}
		return m.fail(fmt.Errorf("seal: %w", err)), true
	}
}

// requestRetire asks Cloud to record that this transfer will never run
// there, presenting the sealed bundle's persona id and transfer key so a
// tombstone can be written when the bundle never arrived.
func (m *mover) requestRetire(ctx context.Context, st *moveState, waitStart time.Time) (int, bool) {
	key, err := m.transferKey(ctx, st)
	if err != nil {
		return m.fail(err), true
	}
	v, code, err := m.call(ctx, http.MethodPost, st.SessionURL+"/cancel", st.Grant,
		jsonBody(map[string]string{"persona_id": m.personaID, "transfer_key": key}), "application/json")
	if err != nil {
		return m.stopped(ctx, err), true
	}
	if code == http.StatusConflict {
		return m.fail(fmt.Errorf("Cloud refused to retire the transfer: the session is %s", v.Status)), true
	}
	if v.RetireProof != "" {
		return 0, false
	}
	if time.Since(waitStart) >= m.wait {
		m.say("Cloud has not finished retiring the transfer yet. %s Run: sumi-local-move resume", m.authorityLine(ctx))
		return exitPending, true
	}
	if err := sleep(ctx, m.poll); err != nil {
		return m.stopped(ctx, err), true
	}
	return 0, false
}

type headerCapture struct{ buf bytes.Buffer }

func (h *headerCapture) Write(p []byte) (int, error) {
	if i := bytes.IndexByte(p, '\n'); i >= 0 {
		h.buf.Write(p[:i])
		return i, errHeaderDone
	}
	h.buf.Write(p)
	return len(p), nil
}

// transferKey reads the sealed transfer's key from the bundle header — the
// same value the bundle would have carried to Cloud.
func (m *mover) transferKey(ctx context.Context, st *moveState) (string, error) {
	var hc headerCapture
	if _, err := m.src.Export(ctx, m.personaID, st.SessionID, &hc); err != nil && !errors.Is(err, errHeaderDone) {
		return "", fmt.Errorf("read the sealed transfer key: %w", err)
	}
	var hdr portable.Header
	if err := json.Unmarshal(hc.buf.Bytes(), &hdr); err != nil || hdr.TransferKey == "" {
		return "", fmt.Errorf("read the sealed transfer key: the bundle header is unreadable")
	}
	return hdr.TransferKey, nil
}

func (m *mover) upload(ctx context.Context, st *moveState) error {
	m.say("Uploading the sealed secretary to Sumi Cloud.")
	pr, pw := io.Pipe()
	go func() {
		_, err := m.src.Export(ctx, m.personaID, st.SessionID, pw)
		pw.CloseWithError(err)
	}()
	defer pr.Close()
	v, code, err := m.call(ctx, http.MethodPut, st.SessionURL+"/bundle", st.Grant, pr, "application/x-ndjson")
	if err != nil {
		return err
	}
	switch code {
	case http.StatusOK, http.StatusCreated, http.StatusGone:
		m.say("Cloud session is now %s.", v.Status)
		return nil
	}
	return fmt.Errorf("Cloud refused the bundle (HTTP %d)", code)
}

// fetch reads the session, retrying while Cloud is unreachable for up to
// m.unreachable.
func (m *mover) fetch(ctx context.Context, st *moveState) (transfersession.View, error) {
	deadline := time.Now().Add(m.unreachable)
	delay := m.poll
	for {
		v, code, err := m.call(ctx, http.MethodGet, st.SessionURL, st.Grant, nil, "")
		if err == nil && code == http.StatusOK {
			return v, nil
		}
		if err != nil && !errors.Is(err, errUnreachable) {
			return v, err
		}
		if time.Now().After(deadline) {
			return v, errUnreachable
		}
		m.say("Cloud is not answering (%v); retrying in %s.", err, delay)
		if serr := sleep(ctx, delay); serr != nil {
			return v, serr
		}
		delay = min(delay*2, 30*time.Second)
	}
}

// call sends one request with the grant as a bearer header. Transport
// failures and 5xx are errUnreachable; 401 is errGrantRejected; any other
// answer returns its status and, when present, the session view.
func (m *mover) call(ctx context.Context, method, target, grant string, body io.Reader, contentType string) (transfersession.View, int, error) {
	var v transfersession.View
	rctx := ctx
	if method != http.MethodPut {
		var cancel context.CancelFunc
		rctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(rctx, method, target, body)
	if err != nil {
		return v, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+grant)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	res, err := m.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return v, 0, ctx.Err()
		}
		return v, 0, fmt.Errorf("%w: %v", errUnreachable, err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return v, 0, fmt.Errorf("%w: %v", errUnreachable, err)
	}
	switch {
	case res.StatusCode >= 500:
		return v, res.StatusCode, fmt.Errorf("%w: HTTP %d", errUnreachable, res.StatusCode)
	case res.StatusCode == http.StatusUnauthorized:
		return v, res.StatusCode, errGrantRejected
	case res.StatusCode < 300:
		if err := json.Unmarshal(raw, &v); err != nil {
			return v, res.StatusCode, fmt.Errorf("Cloud answered an unreadable session: %v", err)
		}
		return v, res.StatusCode, nil
	}
	var e struct {
		Error   string                `json:"error"`
		Session *transfersession.View `json:"session"`
	}
	_ = json.Unmarshal(raw, &e)
	if e.Session != nil {
		return *e.Session, res.StatusCode, nil
	}
	return v, res.StatusCode, fmt.Errorf("Cloud answered HTTP %d: %s", res.StatusCode, strings.TrimSpace(e.Error))
}

func jsonBody(v any) io.Reader {
	raw, _ := json.Marshal(v)
	return bytes.NewReader(raw)
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Cancel ends the move before the Cloud account exists. A sealed secretary
// becomes active again only with Cloud's retirement proof.
func (m *mover) Cancel(ctx context.Context) int {
	release, err := m.lock()
	if err != nil {
		return m.fail(err)
	}
	defer release()
	st, code := m.current()
	if st == nil {
		return code
	}
	if st.Outcome != "" {
		m.say("This move already finished: %s.", st.Outcome)
		return exitDone
	}
	exp, err := m.src.Status(ctx, "export", st.SessionID)
	if errors.Is(err, portable.ErrTransferNotFound) {
		v, code, err := m.call(ctx, http.MethodPost, st.SessionURL+"/cancel", st.Grant, jsonBody(map[string]string{}), "application/json")
		switch {
		case err != nil && errors.Is(err, errUnreachable):
			return m.finish(st, "cancelled_unsealed",
				"Cloud did not answer (%v). Nothing was sealed here, so the secretary stays active on Local; the Cloud session stops accepting a bundle by itself.", err)
		case err != nil:
			return m.fail(err)
		case code == http.StatusConflict:
			return m.fail(fmt.Errorf("Cloud refused the cancel: the session is %s", v.Status))
		}
		return m.finish(st, "cancelled_unsealed", "Cancelled. The secretary was never sealed and stays active on Local.")
	}
	if err != nil {
		return m.fail(err)
	}
	switch exp.Status {
	case "completed":
		return m.finish(st, "transferred", "This move already completed; Sumi is in Sumi Cloud.")
	case "aborted":
		return m.finish(st, "aborted", "The move was cancelled. The secretary is active on Local again.")
	}
	key, err := m.transferKey(ctx, st)
	if err != nil {
		return m.fail(err)
	}
	deadline := time.Now().Add(m.wait)
	for {
		v, code, err := m.call(ctx, http.MethodPost, st.SessionURL+"/cancel", st.Grant,
			jsonBody(map[string]string{"persona_id": m.personaID, "transfer_key": key}), "application/json")
		if err != nil {
			return m.stopped(ctx, err)
		}
		if code == http.StatusConflict {
			m.say("Cloud already created the account with this secretary (session %s), so the move cannot be cancelled. Run sumi-local-move resume to finish it.", v.Status)
			return exitError
		}
		if code >= 300 {
			return m.fail(fmt.Errorf("Cloud answered HTTP %d to the cancel", code))
		}
		if v.RetireProof != "" {
			if _, err := m.src.Abort(ctx, m.personaID, st.SessionID, v.RetireProof); err != nil {
				return m.fail(fmt.Errorf("end the seal with Cloud's retirement proof: %w", err))
			}
			return m.finish(st, "aborted", "Cancelled. Sumi Cloud recorded that this transfer will never run there, and the secretary is active on Local again.")
		}
		if time.Now().After(deadline) {
			m.say("Cloud accepted the cancel but has not retired the transfer yet. %s Run: sumi-local-move cancel", m.authorityLine(ctx))
			return exitPending
		}
		if err := sleep(ctx, m.poll); err != nil {
			return m.stopped(ctx, err)
		}
	}
}

// Status reports without changing anything and without taking the lock.
func (m *mover) Status(ctx context.Context) int {
	st, code := m.current()
	if st == nil {
		return code
	}
	if ps, err := m.state.PersonaState(ctx, m.personaID); err == nil {
		m.say("Secretary %s: Local authority %s", m.personaID, ps.Persona.Authority)
	}
	m.say("Move session: %s", st.SessionURL)
	if st.Outcome != "" {
		m.say("Outcome: %s", st.Outcome)
	}
	if exp, err := m.src.Status(ctx, "export", st.SessionID); err == nil {
		m.say("Local transfer: %s (sealed %s)", exp.Status, exp.SealedAt.Local().Format(time.RFC1123))
	} else if errors.Is(err, portable.ErrTransferNotFound) {
		m.say("Local transfer: not sealed")
	}
	v, _, err := m.call(ctx, http.MethodGet, st.SessionURL, st.Grant, nil, "")
	if err != nil {
		m.say("Cloud session: not readable now (%v)", err)
	} else {
		line := fmt.Sprintf("Cloud session: %s; bundle accepted until %s", v.Status, v.AdmitUntil.Local().Format(time.RFC1123))
		if v.ClaimUntil != nil {
			line += "; registration must finish before " + v.ClaimUntil.Local().Format(time.RFC1123)
		}
		m.say("%s", line)
		for _, x := range v.NotIncluded {
			m.say("  not carried: %s — %s", x.Name, x.Reason)
		}
	}
	m.say("Only core state moves. Model connections are never carried: choose one in Sumi Cloud before the secretary can answer there.")
	return exitDone
}
