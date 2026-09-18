package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/returnsession"
	"github.com/sumi-studio/sumi/apps/api/internal/returnsession/returnsessiontest"
)

// retHarness is the return direction's fixture: the Cloud placement is the
// SOURCE, serving the real return-session routes with the FIXTURE browser
// adapter (returnsessiontest — never a production verifier). The Local
// placement is the destination the mover drives against real Postgres.
type retHarness struct {
	t         *testing.T
	ctx       context.Context
	cloud     placement
	local     placement
	sessions  *returnsession.Service
	server    *returnsession.Server
	proof     *returnsessiontest.Proof
	mux       *http.ServeMux
	srv       *httptest.Server
	intercept atomic.Pointer[func(http.ResponseWriter, *http.Request) bool]
	home      string
	human     string
	pid       string // the secretary on Cloud
	slot      string // this install's configured slot (SUMI_PERSONA_ID)
}

func setupReturn(t *testing.T) *retHarness { return setupReturnOn(t, true) }

// setupReturnOn builds the fixture; cloudHome=false leaves the secretary
// off Cloud entirely so the test can run the forward move itself (the
// same-install reclaim path).
func setupReturnOn(t *testing.T, cloudHome bool) *retHarness {
	t.Helper()
	h := &retHarness{
		t: t, ctx: context.Background(),
		cloud: newPlacement(t, false), local: newPlacement(t, true),
		home: t.TempDir(),
	}
	h.human, h.pid, h.slot = uuid.Must(uuid.NewV7()).String(), uuid.Must(uuid.NewV7()).String(), uuid.Must(uuid.NewV7()).String()
	if _, err := h.cloud.pool.Exec(h.ctx, `INSERT INTO humans (human_id) VALUES ($1)`, h.human); err != nil {
		t.Fatal(err)
	}
	if cloudHome {
		if _, _, err := h.cloud.state.EnsurePersona(h.ctx, h.pid, nil, "Cloud secretary"); err != nil {
			t.Fatal(err)
		}
		if _, err := h.cloud.pool.Exec(h.ctx, `UPDATE core_personas SET human_id = $1 WHERE persona_id = $2`, h.human, h.pid); err != nil {
			t.Fatal(err)
		}
		if _, _, err := h.cloud.state.SubmitInput(h.ctx, &agentstate.Input{
			PersonaID: h.pid, InputID: "in-carried", Kind: "message",
			Payload: map[string]any{"text": "明日 9 時に会議"}, ActorKind: "human", ActorID: "owner", SourceSurface: "test",
		}); err != nil {
			t.Fatal(err)
		}
	}
	// FIXTURE POLICY: the common-flow tests run under the explicit
	// test-only FilePolicyFixture. The undecided production gate has its
	// own test (TestReturnRefusedWhileFilePolicyUndecided).
	h.sessions = returnsession.New(h.cloud.pool,
		returnsession.Config{FilePolicy: returnsession.FilePolicyFixture})
	h.proof = returnsessiontest.NewProof()
	h.proof.Add("owner-cookie", h.human, h.pid)
	h.mux = http.NewServeMux()
	h.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if f := h.intercept.Load(); f != nil && (*f)(w, r) {
			return
		}
		h.mux.ServeHTTP(w, r)
	}))
	t.Cleanup(h.srv.Close)
	var err error
	if h.server, err = returnsession.NewServer(h.sessions, h.proof, h.srv.URL); err != nil {
		t.Fatal(err)
	}
	h.server.RegisterRoutes(h.mux)
	return h
}

func (h *retHarness) setIntercept(f func(http.ResponseWriter, *http.Request) bool) {
	if f == nil {
		h.intercept.Store(nil)
		return
	}
	h.intercept.Store(&f)
}

func (h *retHarness) mover() (*mover, *bytes.Buffer) {
	var out bytes.Buffer
	m := newMover(h.home, h.slot, h.local.svc, h.local.state, &out)
	m.wait, m.poll, m.unreachable, m.sealRetries, m.sealDelay = 0, 10*time.Millisecond, 300*time.Millisecond, 2, 10*time.Millisecond
	return m, &out
}

// newReturn opens a return session over the real owner routes and returns
// the URL the owner would paste into the Local command.
func (h *retHarness) newReturn() (sessionID, returnURL string) {
	h.t.Helper()
	req, err := http.NewRequestWithContext(h.ctx, http.MethodPost,
		h.srv.URL+"/api/secretary-return/sessions", nil)
	if err != nil {
		h.t.Fatal(err)
	}
	req.Header.Set("Cookie", returnsessiontest.Cookie+"=owner-cookie")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	var body struct {
		ReturnURL string `json:"return_url"`
		Session   struct {
			SessionID string `json:"session_id"`
		} `json:"session"`
	}
	if res.StatusCode != http.StatusCreated || json.Unmarshal(raw, &body) != nil || body.ReturnURL == "" {
		h.t.Fatalf("create: %d %s", res.StatusCode, raw)
	}
	return body.Session.SessionID, body.ReturnURL
}

func (h *retHarness) sessionStatus(sessionID string) string {
	h.t.Helper()
	var s string
	if err := h.cloud.pool.QueryRow(h.ctx, `SELECT status FROM return_sessions WHERE session_id = $1`, sessionID).Scan(&s); err != nil {
		h.t.Fatal(err)
	}
	return s
}

func (h *retHarness) ownerCancel(sessionID string) int {
	h.t.Helper()
	req, _ := http.NewRequestWithContext(h.ctx, http.MethodPost,
		fmt.Sprintf("%s/api/secretary-return/sessions/%s/cancel", h.srv.URL, sessionID), nil)
	req.Header.Set("Cookie", returnsessiontest.Cookie+"=owner-cookie")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer res.Body.Close()
	return res.StatusCode
}

// writeConfig writes config.env in the real installer's format — values
// single-quoted through deploy/local-host/sumi-local's shq. The retarget
// path must read and preserve that quoting, not just the bare fixture
// format.
func writeConfig(t *testing.T, dir, personaID string) string {
	t.Helper()
	path := filepath.Join(dir, "config.env")
	content := "SUMI_LOCAL_ID='test-install'\nSUMI_PERSONA_ID='" + personaID + "'\nSUMI_LOCAL_DB_MODE=external\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestReturnFreshInstallE2E drives the whole journey over real HTTP and
// real Postgres: owner creates the session, the mover binds, downloads,
// imports into an empty slot, activates, reports, and retargets the
// install's config at the secretary that came home.
func TestReturnFreshInstallE2E(t *testing.T) {
	h := setupReturn(t)
	sessionID, returnURL := h.newReturn()
	config := writeConfig(t, h.home, h.slot)

	m, out := h.mover()
	code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false)
	if code != exitDone || !strings.Contains(out.String(), "active on this Sumi Local") {
		t.Fatalf("return: %d\n%s", code, out)
	}
	if got := authority(t, h.cloud, h.pid); got != "transferred" {
		t.Fatalf("cloud authority %s", got)
	}
	if got := authority(t, h.local, h.pid); got != "active" {
		t.Fatalf("local authority %s", got)
	}
	if got := h.sessionStatus(sessionID); got != returnsession.StatusCompleted {
		t.Fatalf("session %s", got)
	}
	// The carried input arrived.
	var st string
	if err := h.local.pool.QueryRow(h.ctx, `SELECT status FROM core_inputs WHERE persona_id = $1 AND input_id = 'in-carried'`, h.pid).Scan(&st); err != nil || st != "queued" {
		t.Fatalf("carried input: %v %s", err, st)
	}
	// The config was retargeted at the imported secretary — the file now
	// names the persona whose activation committed, not the empty slot id.
	if v, err := configValue(config, "SUMI_PERSONA_ID"); err != nil || v != h.pid {
		t.Fatalf("retargeted config: %v %q", err, v)
	}
	// Status explains the finished run.
	out.Reset()
	if code := m.ReturnStatus(h.ctx, h.local.pool, config); code != exitDone || !strings.Contains(out.String(), "completed") {
		t.Fatalf("status: %d\n%s", code, out)
	}
	// The grant never reached stdout.
	grant := returnURL[strings.Index(returnURL, "#grant=")+7:]
	if strings.Contains(out.String(), grant) {
		t.Fatal("the grant reached stdout")
	}
}

// TestReturnSameInstallReclaimE2E moves the secretary out with the real
// portable ops, then brings it home to the same install — a reclaim over
// the surrendered copy, preserving placement-local history.
func TestReturnSameInstallReclaimE2E(t *testing.T) {
	h := setupReturnOn(t, false)
	ctx := h.ctx

	// This install owned the secretary and surrendered it: seal → export →
	// cloud import → activate → complete. The local slot then holds the
	// transferred copy under the forward transfer's hold.
	forwardID := uuid.Must(uuid.NewV7()).String()
	destID, err := h.cloud.svc.PlacementID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.local.state.EnsurePersona(ctx, h.pid, nil, "Local secretary"); err != nil {
		t.Fatal(err)
	}
	// Placement-local history that must survive: a completed job.
	if _, err := h.local.pool.Exec(ctx, `INSERT INTO core_jobs (persona_id, job_id, kind, request, status, created_by)
		VALUES ($1, 'job-local-1', 'subprocess', '{}', 'done', 'api')`, h.pid); err != nil {
		t.Fatal(err)
	}
	if _, err := h.local.svc.Seal(ctx, h.pid, forwardID, destID); err != nil {
		t.Fatal(err)
	}
	var bundle bytes.Buffer
	if _, err := h.local.svc.Export(ctx, h.pid, forwardID, &bundle); err != nil {
		t.Fatal(err)
	}
	rec, _, err := h.cloud.svc.Import(ctx, &bundle, &h.human, true)
	if err != nil {
		t.Fatal(err)
	}
	act, err := h.cloud.svc.Activate(ctx, rec.PersonaID, forwardID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.local.svc.Complete(ctx, h.pid, forwardID, act.ActivateProof); err != nil {
		t.Fatal(err)
	}
	if _, err := h.cloud.pool.Exec(ctx, `UPDATE core_personas SET human_id = $1 WHERE persona_id = $2`, h.human, h.pid); err != nil {
		t.Fatal(err)
	}
	// The install's configured slot IS the surrendered persona.
	h.slot = h.pid
	sessionID, returnURL := h.newReturn()

	m, out := h.mover()
	config := writeConfig(t, h.home, h.pid)
	code := m.ReturnStart(ctx, returnURL, h.local.pool, config, false)
	if code != exitDone || !strings.Contains(out.String(), "active on this Sumi Local") {
		t.Fatalf("return: %d\n%s", code, out)
	}
	if got := authority(t, h.local, h.pid); got != "active" {
		t.Fatalf("home authority %s", got)
	}
	if got := h.sessionStatus(sessionID); got != returnsession.StatusCompleted {
		t.Fatalf("session %s", got)
	}
	// The reclaim recorded its lineage and kept placement-local history.
	imp, err := h.local.svc.Status(ctx, "import", sessionID)
	if err != nil || imp.Supersedes != forwardID {
		t.Fatalf("reclaim lineage: %v %+v", err, imp)
	}
	var jobs int
	if err := h.local.pool.QueryRow(ctx, `SELECT count(*) FROM core_jobs WHERE persona_id = $1 AND job_id = 'job-local-1'`, h.pid).Scan(&jobs); err != nil || jobs != 1 {
		t.Fatalf("placement-local history: %v %d", err, jobs)
	}
}

// TestReturnCancelAfterSeal stops the download mid-flight, then cancels:
// the destination tombstones the never-arrived transfer, the source
// unseals on the proof, and the secretary is active on Cloud again.
func TestReturnCancelAfterSeal(t *testing.T) {
	h := setupReturn(t)
	sessionID, returnURL := h.newReturn()

	h.setIntercept(func(w http.ResponseWriter, r *http.Request) bool {
		if strings.HasSuffix(r.URL.Path, "/bundle") {
			panic(http.ErrAbortHandler)
		}
		return false
	})
	m, out := h.mover()
	code := m.ReturnStart(h.ctx, returnURL, h.local.pool, "", false)
	if code != exitPending {
		t.Fatalf("stopped return: %d\n%s", code, out)
	}
	if got := authority(t, h.cloud, h.pid); got != "sealed" {
		t.Fatalf("source %s", got)
	}
	h.setIntercept(nil)

	out.Reset()
	code = m.ReturnCancel(h.ctx, h.local.pool, "")
	if code != exitDone {
		t.Fatalf("cancel: %d\n%s", code, out)
	}
	if got := h.sessionStatus(sessionID); got != returnsession.StatusAborted {
		t.Fatalf("session %s", got)
	}
	if got := authority(t, h.cloud, h.pid); got != "active" {
		t.Fatalf("cloud authority %s", got)
	}
	// The tombstone is recorded on this install.
	imp, err := h.local.svc.Status(h.ctx, "import", sessionID)
	if err != nil || imp.Status != "retired" {
		t.Fatalf("tombstone: %v %+v", err, imp)
	}
}

// TestReturnOwnerCancelThenResume: the owner cancels from the browser
// while the mover is mid-flight; the next resume retires the tombstone and
// reports the proof — the source unseals only on it.
func TestReturnOwnerCancelThenResume(t *testing.T) {
	h := setupReturn(t)
	sessionID, returnURL := h.newReturn()

	h.setIntercept(func(w http.ResponseWriter, r *http.Request) bool {
		if strings.HasSuffix(r.URL.Path, "/bundle") {
			panic(http.ErrAbortHandler)
		}
		return false
	})
	m, out := h.mover()
	if code := m.ReturnStart(h.ctx, returnURL, h.local.pool, "", false); code != exitPending {
		t.Fatalf("stopped return: %d\n%s", code, out)
	}
	h.setIntercept(nil)
	if code := h.ownerCancel(sessionID); code != http.StatusOK {
		t.Fatalf("owner cancel: %d", code)
	}
	out.Reset()
	code := m.ReturnResume(h.ctx, h.local.pool, "", false)
	if code != exitDone || !strings.Contains(out.String(), "active on Sumi Cloud") {
		t.Fatalf("resume: %d\n%s", code, out)
	}
	if got := h.sessionStatus(sessionID); got != returnsession.StatusAborted {
		t.Fatalf("session %s", got)
	}
	if got := authority(t, h.cloud, h.pid); got != "active" {
		t.Fatalf("cloud authority %s", got)
	}
}

// TestReturnModelIntentNeedsExplicitChoice carries model intent home: the
// mover stops with needs_rebinding guidance until the operator explicitly
// chooses the install's configured model — nothing silently substitutes.
func TestReturnModelIntentNeedsExplicitChoice(t *testing.T) {
	h := setupReturn(t)
	if _, err := h.cloud.pool.Exec(h.ctx, `UPDATE core_personas SET model_intent = '{"kind":"api"}' WHERE persona_id = $1`, h.pid); err != nil {
		t.Fatal(err)
	}
	_, returnURL := h.newReturn()
	config := writeConfig(t, h.home, h.slot)

	m, out := h.mover()
	code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false)
	if code != exitPending || !strings.Contains(out.String(), "needs_rebinding") {
		t.Fatalf("return: %d\n%s", code, out)
	}
	// The secretary is active here but the intent stands.
	var intent []byte
	if err := h.local.pool.QueryRow(h.ctx, `SELECT model_intent FROM core_personas WHERE persona_id = $1`, h.pid).Scan(&intent); err != nil || len(intent) == 0 {
		t.Fatalf("carried intent: %v %s", err, intent)
	}
	// The explicit operator choice clears it through the authorized path.
	out.Reset()
	code = m.ReturnResume(h.ctx, h.local.pool, config, true)
	if code != exitDone {
		t.Fatalf("resume with model choice: %d\n%s", code, out)
	}
	if err := h.local.pool.QueryRow(h.ctx, `SELECT model_intent FROM core_personas WHERE persona_id = $1`, h.pid).Scan(&intent); err != nil || intent != nil {
		t.Fatalf("cleared intent: %v %s", err, intent)
	}
}

// TestReturnRefusesOccupiedInstall: the local slot already runs a
// different secretary — the mover refuses before any request reaches the
// source, and nothing binds.
func TestReturnRefusesOccupiedInstall(t *testing.T) {
	h := setupReturn(t)
	stranger := uuid.Must(uuid.NewV7()).String()
	if _, _, err := h.local.state.EnsurePersona(h.ctx, stranger, nil, "other secretary"); err != nil {
		t.Fatal(err)
	}
	sessionID, returnURL := h.newReturn()

	m, out := h.mover()
	code := m.ReturnStart(h.ctx, returnURL, h.local.pool, "", false)
	if code != exitError || !strings.Contains(out.String(), "different secretary") {
		t.Fatalf("occupied install: %d\n%s", code, out)
	}
	if got := h.sessionStatus(sessionID); got != returnsession.StatusAwaitingDestination {
		t.Fatalf("the refusal still bound: %s", got)
	}
	if got := authority(t, h.cloud, h.pid); got != "active" {
		t.Fatalf("the refusal sealed the source: %s", got)
	}
	// The unrelated secretary is untouched.
	if got := authority(t, h.local, stranger); got != "active" {
		t.Fatalf("stranger: %s", got)
	}
}

// TestReturnURLRejectsMoveAndMalformed covers the parse boundary: a move
// URL belongs to the other direction's command.
func TestReturnURLRejectsMoveAndMalformed(t *testing.T) {
	good := "https://api.example.com/api/secretary-return/sessions/" +
		uuid.Must(uuid.NewV7()).String() + "#grant=" + strings.Repeat("a", 43)
	u, id, g, err := parseReturnURL(good)
	if err != nil || !strings.HasSuffix(u, id) || g != strings.Repeat("a", 43) {
		t.Fatalf("good URL: %v %s %s", err, u, g)
	}
	for _, raw := range []string{
		// A move URL for the other direction.
		"https://api.example.com/api/secretary-transfer/sessions/" + uuid.Must(uuid.NewV7()).String() + "#grant=" + strings.Repeat("a", 43),
		"ftp://x/api/secretary-return/sessions/" + uuid.Must(uuid.NewV7()).String() + "#grant=abc",
		"https://api.example.com/api/secretary-return/sessions/" + uuid.Must(uuid.NewV7()).String(), // no grant
		"https://api.example.com/api/secretary-return/sessions/" + uuid.Must(uuid.NewV7()).String() + "?x=1#grant=abc",
		"https://user:pw@api.example.com/api/secretary-return/sessions/" + uuid.Must(uuid.NewV7()).String() + "#grant=abc",
		"https://api.example.com/elsewhere/" + uuid.Must(uuid.NewV7()).String() + "#grant=abc",
	} {
		if _, _, _, err := parseReturnURL(raw); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
}

// TestReturnThroughCLI exercises the real command entry: env, stdin, the
// lock, and the exit codes — the packaged receiver path, no repository
// tooling in it.
func TestReturnThroughCLI(t *testing.T) {
	h := setupReturn(t)
	_, returnURL := h.newReturn()
	config := writeConfig(t, h.home, h.slot)

	conn := h.local.pool.Config().ConnConfig
	dbURL := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", conn.User, conn.Password, conn.Host, conn.Port, conn.Database)
	env := map[string]string{
		"SUMI_LOCAL_HOME":   h.home,
		"SUMI_DB_URL":       dbURL,
		"SUMI_PERSONA_ID":   h.slot,
		"SUMI_LOCAL_CONFIG": config,
	}
	getenv := func(k string) string { return env[k] }
	var stdout, stderr bytes.Buffer
	code := run(h.ctx, []string{"return"}, strings.NewReader(returnURL+"\n"), &stdout, &stderr, getenv)
	if code != exitDone {
		t.Fatalf("run return: %d\nstdout: %s\nstderr: %s", code, stdout.String(), stderr.String())
	}
	if got := authority(t, h.local, h.pid); got != "active" {
		t.Fatalf("local authority %s", got)
	}
	if got := authority(t, h.cloud, h.pid); got != "transferred" {
		t.Fatalf("cloud authority %s", got)
	}
	// The grant never reached either output stream.
	grant := returnURL[strings.Index(returnURL, "#grant=")+7:]
	if strings.Contains(stdout.String()+stderr.String(), grant) {
		t.Fatal("the grant reached the output")
	}
	// status reads back through the same entry.
	stdout.Reset()
	stderr.Reset()
	code = run(h.ctx, []string{"return-status"}, strings.NewReader(""), &stdout, &stderr, getenv)
	if code != exitDone || !strings.Contains(stdout.String(), "completed") {
		t.Fatalf("run return-status: %d\n%s\n%s", code, stdout.String(), stderr.String())
	}
	// The state file keeps the grant at 0600.
	fi, err := os.Stat(filepath.Join(h.home, "return", "state.json"))
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("state file mode: %v %v", fi, err)
	}
}

// TestReturnRefusedWhileFilePolicyUndecided is the production shape: the
// routes are mounted (the forward-transfer base URL is set) but the file
// policy is undecided. A session minted while the policy was enabled is
// refused at the destination bind — before any seal — and the command
// says why. Nothing moves: no import, no config, no intent change.
func TestReturnRefusedWhileFilePolicyUndecided(t *testing.T) {
	h := setupReturn(t)

	// Mint a session under the fixture policy (the service-level create —
	// an enabled policy is the only way a session can exist), then serve
	// it from a GATED server: the production Config{}.
	created, _, err := h.sessions.Create(h.ctx, returnsession.Owner{
		HumanID: h.human, PersonaID: h.pid,
	})
	if err != nil {
		t.Fatal(err)
	}
	sessionID := created.View.SessionID

	gated := returnsession.New(h.cloud.pool, returnsession.Config{})
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	gs, err := returnsession.NewServer(gated, h.proof, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	gs.RegisterRoutes(mux)
	returnURL := srv.URL + "/api/secretary-return/sessions/" + sessionID + "#grant=" + created.Grant

	config := writeConfig(t, h.home, h.slot)
	m, out := h.mover()
	code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false)
	if code != exitError || !strings.Contains(out.String(), "undecided") {
		t.Fatalf("undecided return: %d\n%s", code, out)
	}
	// No authority moved anywhere.
	if got := authority(t, h.cloud, h.pid); got != "active" {
		t.Fatalf("cloud authority %s", got)
	}
	if got := h.sessionStatus(sessionID); got != returnsession.StatusAwaitingDestination {
		t.Fatalf("session %s", got)
	}
	var personas int
	if err := h.local.pool.QueryRow(h.ctx, `SELECT count(*) FROM core_personas`).Scan(&personas); err != nil || personas != 0 {
		t.Fatalf("local personas: %v %d", err, personas)
	}
	// config.env and model intent are untouched.
	if v, err := configValue(config, "SUMI_PERSONA_ID"); err != nil || v != h.slot {
		t.Fatalf("config changed: %v %q", err, v)
	}
}

// TestReturnResumeIsIdempotent replays the finished drive: a second resume
// after completion reports the recorded outcome without touching authority.
func TestReturnResumeIsIdempotent(t *testing.T) {
	h := setupReturn(t)
	_, returnURL := h.newReturn()
	config := writeConfig(t, h.home, h.slot)
	m, out := h.mover()
	if code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false); code != exitDone {
		t.Fatalf("return: %d\n%s", code, out)
	}
	out.Reset()
	if code := m.ReturnResume(h.ctx, h.local.pool, config, false); code != exitDone {
		t.Fatalf("resume after done: %d\n%s", code, out)
	}
	if got := authority(t, h.local, h.pid); got != "active" {
		t.Fatalf("local authority %s", got)
	}
	// A stray cancel after completion refuses — the secretary is live here.
	out.Reset()
	if code := m.ReturnCancel(h.ctx, h.local.pool, config); code != exitError {
		t.Fatalf("cancel after done: %d\n%s", code, out)
	}
}
