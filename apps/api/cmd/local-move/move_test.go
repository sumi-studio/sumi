package main

import (
	"bytes"
	"context"
	"errors"
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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/modelconnections"
	"github.com/sumi-studio/sumi/apps/api/internal/portable"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
	"github.com/sumi-studio/sumi/apps/api/internal/transfersession"
	"github.com/sumi-studio/sumi/apps/api/internal/transfersession/transfersessiontest"
)

func TestParseMoveURL(t *testing.T) {
	sid := uuid.Must(uuid.NewV7()).String()
	grant := strings.Repeat("A", 43)
	good := map[string]string{
		"https://api.sumi.example/api/secretary-transfer/sessions/" + sid + "#grant=" + grant:      "https://api.sumi.example/api/secretary-transfer/sessions/" + sid,
		"https://sumi.example/v1/api/secretary-transfer/sessions/" + sid + "#grant=" + grant:       "https://sumi.example/v1/api/secretary-transfer/sessions/" + sid,
		"http://127.0.0.1:12850/api/secretary-transfer/sessions/" + sid + "#grant=" + grant:        "http://127.0.0.1:12850/api/secretary-transfer/sessions/" + sid,
		"http://[::1]:12850/api/secretary-transfer/sessions/" + sid + "#grant=" + grant:            "http://[::1]:12850/api/secretary-transfer/sessions/" + sid,
		"https://api.sumi.example:8443/api/secretary-transfer/sessions/" + sid + "#grant=" + grant: "https://api.sumi.example:8443/api/secretary-transfer/sessions/" + sid,
	}
	for raw, want := range good {
		u, id, g, err := parseMoveURL(raw)
		if err != nil || u != want || id != sid || g != grant {
			t.Fatalf("parse %q = %q %q %q %v", raw, u, id, g, err)
		}
	}
	bad := []string{
		"http://api.sumi.example/api/secretary-transfer/sessions/" + sid + "#grant=" + grant,
		"https://api.sumi.example/api/secretary-transfer/sessions/" + sid + "?grant=" + grant,
		"https://api.sumi.example/api/secretary-transfer/sessions/" + sid + "?x=1#grant=" + grant,
		"https://u@api.sumi.example/api/secretary-transfer/sessions/" + sid + "#grant=" + grant,
		"https://api.sumi.example/api/secretary-transfer/sessions/" + sid,
		"https://api.sumi.example/api/secretary-transfer/sessions/" + sid + "#" + grant,
		"https://api.sumi.example/api/secretary-transfer/sessions/" + sid + "#grant=short",
		"https://api.sumi.example/api/secretary-transfer/sessions/" + sid + "#grant=" + grant + "&x=1",
		"https://api.sumi.example/api/secretary-transfer/sessions/" + sid + "/bundle#grant=" + grant,
		"https://api.sumi.example/api/secretary-transfer/sessions/not-a-uuid#grant=" + grant,
		"https://api.sumi.example/api/secretary-transfer/sessions/0190a0b1-2c3d-4e5f-8a9b-0c1d2e3f4a5b#grant=" + grant,
	}
	for _, raw := range bad {
		if _, _, _, err := parseMoveURL(raw); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
}

type placement struct {
	pool  *pgxpool.Pool
	state *agentstate.Store
	svc   *portable.Service
}

func newPlacement(t *testing.T, source bool) placement {
	t.Helper()
	if src := os.Getenv("SUMI_TRANSFER_SOURCE_DB_URL"); source && src != "" && os.Getenv("SUMI_TEST_DB_URL") != "" {
		dst := os.Getenv("SUMI_TEST_DB_URL")
		os.Setenv("SUMI_TEST_DB_URL", src)
		defer os.Setenv("SUMI_TEST_DB_URL", dst)
	}
	pool := testdb.Create(t)
	if err := db.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return placement{pool: pool, state: agentstate.NewStore(pool), svc: portable.NewService(pool)}
}

const cloudAdmin = "fixture-cloud-state-admin-token"

// cloud is a destination placement serving the real transfer-session routes
// with the FIXTURE registration adapter, plus the real core-state routes so a
// core host can run the arrived secretary. intercept lets a test break a
// request the way a network or a crashing handler would.
type cloud struct {
	t         *testing.T
	ctx       context.Context
	local     placement
	dest      placement
	sessions  *transfersession.Service
	server    *transfersession.Server
	proof     *transfersessiontest.Proof
	mux       *http.ServeMux
	srv       *httptest.Server
	intercept atomic.Pointer[func(http.ResponseWriter, *http.Request) bool]
	home      string
	pid       string
}

func setupMove(t *testing.T) *cloud {
	t.Helper()
	c := &cloud{t: t, ctx: context.Background(), local: newPlacement(t, true), dest: newPlacement(t, false), home: t.TempDir()}
	c.pid = uuid.Must(uuid.NewV7()).String()
	if _, _, err := c.local.state.EnsurePersona(c.ctx, c.pid, nil, "Local secretary"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.local.state.SubmitInput(c.ctx, &agentstate.Input{
		PersonaID: c.pid, InputID: "in-carried", Kind: "message",
		Payload: map[string]any{"text": "明日 9 時に会議"}, ActorKind: "human", ActorID: "owner", SourceSurface: "test",
	}); err != nil {
		t.Fatal(err)
	}
	c.sessions = transfersession.New(c.dest.pool, transfersession.Config{})
	c.proof = transfersessiontest.NewProof()
	c.mux = http.NewServeMux()
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if f := c.intercept.Load(); f != nil && (*f)(w, r) {
			return
		}
		c.mux.ServeHTTP(w, r)
	}))
	t.Cleanup(c.srv.Close)
	var err error
	if c.server, err = transfersession.NewServer(c.sessions, c.proof, c.srv.URL); err != nil {
		t.Fatal(err)
	}
	c.server.RegisterRoutes(c.mux)
	core := agentstate.NewServer(c.dest.pool, cloudAdmin)
	core.SetModelConnections(modelconnections.MetadataOnly(c.dest.pool))
	core.RegisterRoutes(c.mux)
	return c
}

func (c *cloud) setIntercept(f func(http.ResponseWriter, *http.Request) bool) {
	if f == nil {
		c.intercept.Store(nil)
		return
	}
	c.intercept.Store(&f)
}

// failUploads cuts every bundle upload after its first bytes.
func failUploads(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodPut {
		return false
	}
	_, _ = io.CopyN(io.Discard, r.Body, 64)
	panic(http.ErrAbortHandler)
}

func (c *cloud) newSession(uid string) (string, string) {
	c.t.Helper()
	created, _, err := c.sessions.Create(c.ctx, transfersession.Subject{Provider: "firebase", Subject: uid})
	if err != nil {
		c.t.Fatal(err)
	}
	return created.View.SessionID, c.server.MoveURL(created.View.SessionID, created.Grant)
}

// provision runs the FIXTURE account transaction under a live fixture flow.
func (c *cloud) provision(uid, sessionID string) {
	c.t.Helper()
	flow, nonce := uuid.NewString(), uuid.NewString()
	c.proof.Add(flow, nonce, uid, time.Minute)
	if _, _, err := c.proof.ProvisionAccount(c.ctx, c.dest.pool, flow, nonce, sessionID, nil); err != nil {
		c.t.Fatalf("fixture provision: %v", err)
	}
}

func (c *cloud) mover() (*mover, *bytes.Buffer) {
	var out bytes.Buffer
	m := newMover(c.home, c.pid, c.local.svc, c.local.state, &out)
	m.wait, m.poll, m.unreachable, m.sealRetries, m.sealDelay = 0, 10*time.Millisecond, 300*time.Millisecond, 2, 10*time.Millisecond
	return m, &out
}

func authority(t *testing.T, p placement, pid string) string {
	t.Helper()
	var a string
	err := p.pool.QueryRow(context.Background(), `SELECT authority FROM core_personas WHERE persona_id = $1`, pid).Scan(&a)
	if errors.Is(err, pgx.ErrNoRows) {
		return "absent"
	}
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func (c *cloud) sessionStatus(sessionID string) string {
	c.t.Helper()
	var s string
	if err := c.dest.pool.QueryRow(c.ctx, `SELECT status FROM transfer_sessions WHERE session_id = $1`, sessionID).Scan(&s); err != nil {
		c.t.Fatal(err)
	}
	return s
}

func expect(t *testing.T, got, want int, out *bytes.Buffer, contains string) {
	t.Helper()
	if got != want || !strings.Contains(out.String(), contains) {
		t.Fatalf("exit %d (want %d); output lacks %q:\n%s", got, want, contains, out)
	}
	out.Reset()
}

func TestMoveSurvivesInterruptedUploadAndStoppedProcess(t *testing.T) {
	c := setupMove(t)
	uid := "fixture-move-" + c.pid[24:]
	sid, moveURL := c.newSession(uid)
	grant := moveURL[strings.Index(moveURL, "#grant=")+7:]

	c.setIntercept(failUploads)
	m, out := c.mover()
	expect(t, m.Start(c.ctx, moveURL), exitPending, out, "stays sealed")
	if a := authority(t, c.local, c.pid); a != "sealed" {
		t.Fatalf("local authority after interrupted uploads: %s", a)
	}
	for path, mode := range map[string]os.FileMode{filepath.Join(c.home, "move"): 0o700, filepath.Join(c.home, "move", "state.json"): 0o600} {
		if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != mode {
			t.Fatalf("%s: %v %v", path, fi.Mode(), err)
		}
	}

	// The process stopped; a new invocation continues the same transfer.
	c.setIntercept(nil)
	m2, out2 := c.mover()
	release, err := m2.lock()
	if err != nil {
		t.Fatal(err)
	}
	m3, out3 := c.mover()
	expect(t, m3.Resume(c.ctx), exitError, out3, "already running")
	release()
	expect(t, m2.Resume(c.ctx), exitPending, out2, "waiting for the registration")
	if s := c.sessionStatus(sid); s != "staged" {
		t.Fatalf("session after resume: %s", s)
	}

	c.provision(uid, sid)
	expect(t, m2.Resume(c.ctx), exitDone, out2, "Choose a model connection")
	if l, d := authority(t, c.local, c.pid), authority(t, c.dest, c.pid); l != "transferred" || d != "active" {
		t.Fatalf("after completion: local %s, cloud %s", l, d)
	}
	expect(t, m2.Resume(c.ctx), exitDone, out2, "already finished: transferred")
	raw, _ := os.ReadFile(filepath.Join(c.home, "move", "state.json"))
	if !strings.Contains(string(raw), `"outcome": "transferred"`) {
		t.Fatalf("state file: %s", raw)
	}
	for _, b := range []*bytes.Buffer{out, out2, out3} {
		if strings.Contains(b.String(), grant) {
			t.Fatal("the grant was printed")
		}
	}
}

func TestLostUploadResponseIsReplayed(t *testing.T) {
	c := setupMove(t)
	uid := "fixture-lost-" + c.pid[24:]
	sid, moveURL := c.newSession(uid)
	var lost atomic.Bool
	c.setIntercept(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodPut || !lost.CompareAndSwap(false, true) {
			return false
		}
		c.mux.ServeHTTP(httptest.NewRecorder(), r)
		w.WriteHeader(http.StatusBadGateway)
		return true
	})
	m, out := c.mover()
	expect(t, m.Start(c.ctx, moveURL), exitPending, out, "waiting for the registration")
	if s := c.sessionStatus(sid); s != "staged" || !lost.Load() {
		t.Fatalf("session %s, lost=%t", s, lost.Load())
	}
	c.provision(uid, sid)
	// The activate response is not needed either: the next read carries it.
	expect(t, m.Resume(c.ctx), exitDone, out, "moved to Sumi Cloud")
}

func TestCancelAfterSealBeforeBundleRecordsTombstone(t *testing.T) {
	c := setupMove(t)
	sid, moveURL := c.newSession("fixture-cancel-" + c.pid[24:])
	c.setIntercept(failUploads)
	m, out := c.mover()
	expect(t, m.Start(c.ctx, moveURL), exitPending, out, "stays sealed")
	expect(t, m.Cancel(c.ctx), exitDone, out, "active on Local again")
	if a := authority(t, c.local, c.pid); a != "active" {
		t.Fatalf("local authority: %s", a)
	}
	rec, err := c.dest.svc.Status(c.ctx, "import", sid)
	if err != nil || rec.Status != "retired" || rec.ContentSHA256 != "" || c.sessionStatus(sid) != "cancelled" {
		t.Fatalf("cloud tombstone: %+v %v", rec, err)
	}
	if _, _, err := c.local.state.SubmitInput(c.ctx, &agentstate.Input{
		PersonaID: c.pid, InputID: "in-after-cancel", Kind: "message", Payload: map[string]any{"text": "still here?"},
		ActorKind: "human", ActorID: "owner", SourceSurface: "test",
	}); err != nil {
		t.Fatalf("local input after cancel: %v", err)
	}
}

func TestSessionCancelledBeforeSealing(t *testing.T) {
	c := setupMove(t)
	uid := "fixture-early-" + c.pid[24:]
	sid, moveURL := c.newSession(uid)
	if _, err := c.sessions.CancelBySubject(c.ctx, sid, transfersession.Subject{Provider: "firebase", Subject: uid}, ""); err != nil {
		t.Fatal(err)
	}
	m, out := c.mover()
	expect(t, m.Start(c.ctx, moveURL), exitDone, out, "never sealed")
	if _, err := c.local.svc.Status(c.ctx, "export", sid); !errors.Is(err, portable.ErrTransferNotFound) || authority(t, c.local, c.pid) != "active" {
		t.Fatalf("local after a cancelled session: %v", err)
	}
}

func TestBusySecretaryIsNotSealedAndCancelNeedsNoProof(t *testing.T) {
	c := setupMove(t)
	ctx := c.ctx
	gen := func() int64 {
		l, err := c.local.state.AcquireWriter(ctx, c.pid, "local-core", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return l.Generation
	}()
	if _, err := c.local.state.LoadTurn(ctx, c.pid, gen, "turn-1", 50); err != nil {
		t.Fatal(err)
	}
	// An external effect whose result is unknown, as in the portable seal test.
	if _, err := c.local.pool.Exec(ctx, `
		INSERT INTO core_operations (persona_id, operation_id, turn_id, tool, idempotency_key, request, status, claimed_generation)
		VALUES ($1, 'op-mail', 'turn-1', 'mail.send', 'in-carried:tool:0', '{}', 'running', $2)`, c.pid, gen); err != nil {
		t.Fatal(err)
	}
	sid, moveURL := c.newSession("fixture-busy-" + c.pid[24:])
	m, out := c.mover()
	expect(t, m.Start(ctx, moveURL), exitPending, out, "still processing")
	if a := authority(t, c.local, c.pid); a != "active" {
		t.Fatalf("busy secretary was sealed: %s", a)
	}
	expect(t, m.Cancel(ctx), exitDone, out, "never sealed")
	if s := c.sessionStatus(sid); s != "cancelled" {
		t.Fatalf("session: %s", s)
	}
}

func TestAdmissionDeadlineAfterSealEndsWithProof(t *testing.T) {
	c := setupMove(t)
	sid, moveURL := c.newSession("fixture-expired-" + c.pid[24:])
	c.setIntercept(failUploads)
	m, out := c.mover()
	expect(t, m.Start(c.ctx, moveURL), exitPending, out, "stays sealed")
	if _, err := c.dest.pool.Exec(c.ctx, `UPDATE transfer_sessions SET admit_until = now() - interval '1 second' WHERE session_id = $1`, sid); err != nil {
		t.Fatal(err)
	}
	c.setIntercept(nil)
	expect(t, m.Resume(c.ctx), exitDone, out, "active on Local again")
	if a, s := authority(t, c.local, c.pid), c.sessionStatus(sid); a != "active" || s != "expired" {
		t.Fatalf("after expiry: local %s, session %s", a, s)
	}
}

func TestChangedPlacementOrUnreachableCloudNeverAborts(t *testing.T) {
	c := setupMove(t)
	_, moveURL := c.newSession("fixture-ident-" + c.pid[24:])
	c.setIntercept(failUploads)
	m, out := c.mover()
	expect(t, m.Start(c.ctx, moveURL), exitPending, out, "stays sealed")

	old, _ := c.dest.svc.PlacementID(c.ctx)
	if _, err := c.dest.pool.Exec(c.ctx, `UPDATE core_placement SET placement_id = $1`, uuid.Must(uuid.NewV7()).String()); err != nil {
		t.Fatal(err)
	}
	expect(t, m.Resume(c.ctx), exitError, out, "nothing was aborted")
	if a := authority(t, c.local, c.pid); a != "sealed" {
		t.Fatalf("a new placement id changed local authority: %s", a)
	}
	if _, err := c.dest.pool.Exec(c.ctx, `UPDATE core_placement SET placement_id = $1`, old); err != nil {
		t.Fatal(err)
	}

	c.srv.Close()
	expect(t, m.Resume(c.ctx), exitPending, out, "Stopped before the move finished")
	if a := authority(t, c.local, c.pid); a != "sealed" {
		t.Fatalf("an unreachable Cloud changed local authority: %s", a)
	}
}
