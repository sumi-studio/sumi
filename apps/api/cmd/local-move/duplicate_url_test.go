package main

// F234: one move URL pasted into two Sumi Local installs, driven through the
// real command against two real Local databases and the real Cloud routes.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/transfersession"
)

// install is a second Sumi Local: its own database, placement id, secretary
// and state home.
type install struct {
	place placement
	pid   string
	home  string
}

func (c *cloud) secondInstall(t *testing.T, name string) *install {
	t.Helper()
	p := newPlacement(t, true)
	pid := uuid.Must(uuid.NewV7()).String()
	if _, _, err := p.state.EnsurePersona(c.ctx, pid, nil, name); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.state.SubmitInput(c.ctx, &agentstate.Input{
		PersonaID: pid, InputID: "in-other", Kind: "message",
		Payload: map[string]any{"text": "二台目の秘書"}, ActorKind: "human", ActorID: "owner", SourceSurface: "test",
	}); err != nil {
		t.Fatal(err)
	}
	return &install{place: p, pid: pid, home: t.TempDir()}
}

func (i *install) mover() (*mover, *bytes.Buffer) {
	var out bytes.Buffer
	m := newMover(i.home, i.pid, i.place.svc, i.place.state, &out)
	m.wait, m.poll, m.unreachable, m.sealRetries, m.sealDelay = 0, 10*time.Millisecond, 300*time.Millisecond, 2, 10*time.Millisecond
	return m, &out
}

func exportRows(t *testing.T, p placement, pid string) int {
	t.Helper()
	var n int
	if err := p.pool.QueryRow(context.Background(), `SELECT count(*) FROM core_transfers WHERE direction = 'export' AND persona_id = $1`, pid).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// The second install is refused while its secretary is still active, and both
// secretaries end up where they should: the first on Cloud, the second still
// answering on its own Local until it is given a move URL of its own.
func TestOneMoveURLDrivesOneLocalInstall(t *testing.T) {
	c := setupMove(t)
	uid := "fixture-dup-" + c.pid[24:]
	sid, moveURL := c.newSession(uid)

	// The first install takes the URL, seals and stages.
	mA, outA := c.mover()
	expect(t, mA.Start(c.ctx, moveURL), exitPending, outA, "waiting for the registration")
	if s := c.sessionStatus(sid); s != "staged" {
		t.Fatalf("session after the first install: %s", s)
	}
	if a := authority(t, c.local, c.pid); a != "sealed" {
		t.Fatalf("first install authority: %s", a)
	}

	// The same URL is pasted into a second install.
	b := c.secondInstall(t, "second install")
	mB, outB := b.mover()
	code := mB.Start(c.ctx, moveURL)
	if code != exitError {
		t.Fatalf("second install exit %d:\n%s", code, outB)
	}
	for _, want := range []string{"already in use by another Sumi Local", "stays active on this Local", "get a new move URL"} {
		if !strings.Contains(outB.String(), want) {
			t.Fatalf("second install output lacks %q:\n%s", want, outB)
		}
	}
	if strings.Contains(outB.String(), "Sealing") || strings.Contains(outB.String(), "Uploading") {
		t.Fatalf("the second install acted on the URL:\n%s", outB)
	}
	if a := authority(t, b.place, b.pid); a != "active" {
		t.Fatalf("second install authority: %s", a)
	}
	if n := exportRows(t, b.place, b.pid); n != 0 {
		t.Fatalf("the second install sealed: %d export rows", n)
	}
	if _, err := os.Stat(filepath.Join(b.home, "move", "state.json")); !os.IsNotExist(err) {
		t.Fatalf("the refused install kept a move record: %v", err)
	}
	outB.Reset()
	expect(t, mB.Resume(c.ctx), exitUsage, outB, "No move is recorded")
	expect(t, mB.Cancel(c.ctx), exitUsage, outB, "No move is recorded")

	// The first install finishes normally, exactly once.
	c.provision(uid, sid)
	expect(t, mA.Resume(c.ctx), exitDone, outA, "Choose a model connection")
	if l, d := authority(t, c.local, c.pid), authority(t, c.dest, c.pid); l != "transferred" || d != "active" {
		t.Fatalf("first install after completion: local %s, cloud %s", l, d)
	}
	if a := authority(t, b.place, b.pid); a != "active" {
		t.Fatalf("second install after the first completed: %s", a)
	}

	// Ordinary recovery for the second install: its own move URL.
	uid2 := "fixture-dup2-" + b.pid[24:]
	sid2, moveURL2 := c.newSession(uid2)
	mB2, outB2 := b.mover()
	expect(t, mB2.Start(c.ctx, moveURL2), exitPending, outB2, "waiting for the registration")
	c.provision(uid2, sid2)
	expect(t, mB2.Resume(c.ctx), exitDone, outB2, "Choose a model connection")
	if l, d := authority(t, b.place, b.pid), authority(t, c.dest, b.pid); l != "transferred" || d != "active" {
		t.Fatalf("second install after its own move: local %s, cloud %s", l, d)
	}
	for _, o := range []*bytes.Buffer{outA, outB, outB2} {
		if strings.Contains(o.String(), moveURL[strings.Index(moveURL, "#grant=")+7:]) {
			t.Fatal("the grant was printed")
		}
	}
}

// The session closes between the last status read and the take. The take's
// own answer says so, and the command finishes without sealing — a seal here
// would leave the secretary waiting on a retirement proof it never needed.
func TestSessionClosedAtTakeSealsNothing(t *testing.T) {
	c := setupMove(t)
	uid := "fixture-closedtake-" + c.pid[24:]
	sid, moveURL := c.newSession(uid)
	grant := moveURL[strings.Index(moveURL, "#grant=")+7:]

	// The last status the command saw said awaiting_bundle — replayed here
	// even after the session is cancelled underneath it.
	v, err := c.sessions.Status(c.ctx, sid, grant)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.sessions.CancelBySubject(c.ctx, sid,
		transfersession.Subject{Provider: "firebase", Subject: uid}, ""); err != nil {
		t.Fatal(err)
	}
	c.setIntercept(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/sessions/"+sid) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(raw)
			return true
		}
		return false
	})

	m, out := c.mover()
	code := m.Start(c.ctx, moveURL)
	if code != exitDone || !strings.Contains(out.String(), "never sealed") {
		t.Fatalf("start against a session closed at take: %d\n%s", code, out)
	}
	if strings.Contains(out.String(), "Sealing") || strings.Contains(out.String(), "Uploading") {
		t.Fatalf("the command acted on a session that was already closed:\n%s", out)
	}
	if a := authority(t, c.local, c.pid); a != "active" {
		t.Fatalf("authority: %s", a)
	}
	if n := exportRows(t, c.local, c.pid); n != 0 {
		t.Fatalf("export rows: %d", n)
	}
	if s := c.sessionStatus(sid); s != "cancelled" {
		t.Fatalf("session: %s", s)
	}
}

// The credential's account came into existence after the session was
// created — the take is the last moment before a seal at which that can
// still be answered honestly. The refusal must not read as "another Sumi
// Local holds the URL".
func TestAccountCreatedBetweenSessionAndTake(t *testing.T) {
	c := setupMove(t)
	uid := "fixture-lateacct-" + c.pid[24:]
	sid, moveURL := c.newSession(uid)

	// The account transaction for an earlier registration bound the
	// credential between Create and the Local command's start.
	human := uuid.Must(uuid.NewV7()).String()
	if _, err := c.dest.pool.Exec(c.ctx, `INSERT INTO humans (human_id) VALUES ($1)`, human); err != nil {
		t.Fatal(err)
	}
	if _, err := c.dest.pool.Exec(c.ctx, `INSERT INTO credentials (provider, external_subject, human_id)
		VALUES ('firebase', $1, $2)`, uid, human); err != nil {
		t.Fatal(err)
	}

	m, out := c.mover()
	code := m.Start(c.ctx, moveURL)
	if code != exitError || !strings.Contains(out.String(), "Cloud refused this move URL") {
		t.Fatalf("start after the credential acquired an account: %d\n%s", code, out)
	}
	if strings.Contains(out.String(), "another Sumi Local") {
		t.Fatalf("an account refusal was reported as a duplicate URL:\n%s", out)
	}
	if strings.Contains(out.String(), "Sealing") || strings.Contains(out.String(), "Uploading") {
		t.Fatalf("the command sealed for a session that can never provision:\n%s", out)
	}
	if a := authority(t, c.local, c.pid); a != "active" {
		t.Fatalf("authority: %s", a)
	}
	if n := exportRows(t, c.local, c.pid); n != 0 {
		t.Fatalf("export rows: %d", n)
	}
	if s := c.sessionStatus(sid); s != "awaiting_bundle" {
		t.Fatalf("session: %s", s)
	}
	// A retry says the same thing — it does not hang or seal on the way.
	out.Reset()
	if code := m.Resume(c.ctx); code != exitError || !strings.Contains(out.String(), "Cloud refused this move URL") {
		t.Fatalf("resume: %d\n%s", code, out)
	}
}

// The answer to taking the URL is lost after it committed. The retry finds
// the same binding and continues; it never re-takes or re-seals anything.
func TestLostSourceBindingAnswerIsIdempotent(t *testing.T) {
	c := setupMove(t)
	uid := "fixture-bindlost-" + c.pid[24:]
	sid, moveURL := c.newSession(uid)

	c.setIntercept(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/source") {
			return false
		}
		// Commit the binding, then lose the answer the way a cut
		// connection does.
		c.mux.ServeHTTP(httptest.NewRecorder(), r)
		panic(http.ErrAbortHandler)
	})
	m, out := c.mover()
	if code := m.Start(c.ctx, moveURL); code != exitPending {
		t.Fatalf("start with a lost binding answer: %d\n%s", code, out)
	}
	if strings.Contains(out.String(), "Sealing") {
		t.Fatalf("the command sealed before it knew it held the URL:\n%s", out)
	}
	out.Reset()
	// The binding did commit, and it is this install's.
	var persona string
	if err := c.dest.pool.QueryRow(c.ctx,
		`SELECT source_persona_id FROM transfer_sessions WHERE session_id = $1`, sid).Scan(&persona); err != nil {
		t.Fatal(err)
	}
	if persona != c.pid {
		t.Fatalf("binding after the lost answer: %s", persona)
	}
	if a := authority(t, c.local, c.pid); a != "active" {
		t.Fatalf("authority after the lost answer: %s", a)
	}

	c.setIntercept(nil)
	expect(t, m.Resume(c.ctx), exitPending, out, "waiting for the registration")
	if n := exportRows(t, c.local, c.pid); n != 1 {
		t.Fatalf("export rows after the retry: %d", n)
	}
	// Repeating start with the same URL resumes the same move.
	expect(t, m.Start(c.ctx, moveURL), exitPending, out, "waiting for the registration")
	if n := exportRows(t, c.local, c.pid); n != 1 {
		t.Fatalf("export rows after a repeated start: %d", n)
	}
	c.provision(uid, sid)
	expect(t, m.Resume(c.ctx), exitDone, out, "Choose a model connection")
	if l, d := authority(t, c.local, c.pid), authority(t, c.dest, c.pid); l != "transferred" || d != "active" {
		t.Fatalf("after completion: local %s, cloud %s", l, d)
	}
}
