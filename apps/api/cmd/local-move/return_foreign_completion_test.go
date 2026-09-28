package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/sumi-studio/sumi/apps/api/internal/returnsession"
	"github.com/sumi-studio/sumi/apps/api/internal/returnsession/returnsessiontest"
)

// secondReturnInstall is a fresh Local install, with its own database, home and
// slot, pointed at the same Cloud.
type secondReturnInstall struct {
	p      placement
	home   string
	slot   string
	config string
}

func (h *retHarness) secondInstall(t *testing.T) secondReturnInstall {
	t.Helper()
	i := secondReturnInstall{p: newPlacement(t, true), home: t.TempDir(), slot: uuid.Must(uuid.NewV7()).String()}
	i.config = writeConfig(t, i.home, i.slot)
	return i
}

func (i secondReturnInstall) mover() (*mover, *bytes.Buffer) {
	var out bytes.Buffer
	m := newMover(i.home, i.slot, i.p.svc, i.p.state, &out)
	m.wait, m.poll, m.unreachable, m.sealRetries, m.sealDelay = 0, 10*time.Millisecond, 300*time.Millisecond, 2, 10*time.Millisecond
	return m, &out
}

func (i secondReturnInstall) record(t *testing.T) returnState {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(i.home, "return", "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var st returnState
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatal(err)
	}
	return st
}

// A return URL whose session already completed at install 1 is pasted into
// install 2. Nothing may arrive or change there, and no command may report
// the secretary as brought to install 2: `return` and `return-resume` fail
// and say where it went, `return-status` answers (a successful query) and
// says it is not here, `return-cancel` has nothing to cancel, and a later,
// different return URL is still accepted.
//
// Found by the 2026-09-28 Cloud→Local acceptance journey, where install 2
// printed "The secretary is active on this Sumi Local install …" and exited 0.
func TestReturnCompletedElsewhereIsNotReportedHere(t *testing.T) {
	h := setupReturn(t)
	sessionID, returnURL := h.newReturn()
	config := writeConfig(t, h.home, h.slot)
	m, out := h.mover()
	if code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false); code != exitDone {
		t.Fatalf("install 1 return: %d\n%s", code, out)
	}
	first := mustPlacement(h.ctx, h.local.svc)

	other := h.secondInstall(t)
	m2, out2 := other.mover()
	notHere := func(what string, code, want int) {
		t.Helper()
		if code != want {
			t.Fatalf("%s: exit %d, want %d\n%s", what, code, want, out2)
		}
		if s := out2.String(); strings.Contains(s, "active on this") || !strings.Contains(s, first) {
			t.Fatalf("%s must name install 1's placement and never claim the secretary is here:\n%s", what, s)
		}
		out2.Reset()
	}

	notHere("return", m2.ReturnStart(h.ctx, returnURL, other.p.pool, other.config, false), exitError)
	if st := other.record(t); st.Outcome != outcomeElsewhere || st.CompletedAt != first {
		t.Fatalf("recorded outcome %q at %q", st.Outcome, st.CompletedAt)
	}
	notHere("return-resume", m2.ReturnResume(h.ctx, other.p.pool, other.config, false), exitError)
	notHere("return-status", m2.ReturnStatus(h.ctx, other.p.pool, other.config), exitDone)
	notHere("same URL again", m2.ReturnStart(h.ctx, returnURL, other.p.pool, other.config, false), exitError)
	if code := m2.ReturnCancel(h.ctx, other.p.pool, other.config); code != exitError {
		t.Fatalf("return-cancel: %d\n%s", code, out2)
	}
	out2.Reset()

	// None of that changed install 2, install 1 or the session.
	var n int
	if err := other.p.pool.QueryRow(h.ctx, `SELECT count(*) FROM core_personas WHERE persona_id = $1`, h.pid).Scan(&n); err != nil || n != 0 {
		t.Fatalf("install 2 holds %d copies of the secretary (err %v)", n, err)
	}
	if v, err := configValue(other.config, "SUMI_PERSONA_ID"); err != nil || v != other.slot {
		t.Fatalf("install 2 config retargeted: %v %q", err, v)
	}
	if got := authority(t, h.local, h.pid); got != "active" {
		t.Fatalf("install 1 authority %s", got)
	}
	if got := h.sessionStatus(sessionID); got != returnsession.StatusCompleted {
		t.Fatalf("session %s", got)
	}

	// A different secretary's return is not blocked by the settled record:
	// it is archived and the new one brings that secretary to install 2.
	pid2 := uuid.Must(uuid.NewV7()).String()
	if _, _, err := h.cloud.state.EnsurePersona(h.ctx, pid2, nil, "Second secretary"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.cloud.pool.Exec(h.ctx, `UPDATE core_personas SET human_id = $1 WHERE persona_id = $2`, h.human, pid2); err != nil {
		t.Fatal(err)
	}
	h.proof.Add("owner2-cookie", h.human, pid2)
	_, returnURL2 := openReturn(t, h.srv, "owner2-cookie")
	if code := m2.ReturnStart(h.ctx, returnURL2, other.p.pool, other.config, false); code != exitDone {
		t.Fatalf("next return: %d\n%s", code, out2)
	}
	if got := authority(t, other.p, pid2); got != "active" {
		t.Fatalf("second secretary on install 2: %s", got)
	}
	if _, err := os.Stat(filepath.Join(other.home, "return", "state-"+sessionID+".json")); err != nil {
		t.Fatalf("the completed-elsewhere record was not archived: %v", err)
	}
}

// The report of a real activation is lost after Cloud committed it: the
// install that holds the secretary still settles as active on resume —
// the completed-elsewhere answer is only for installs without the import.
func TestLostActivationReceiptStillSettlesHere(t *testing.T) {
	h := setupReturn(t)
	sessionID, returnURL := h.newReturn()
	config := writeConfig(t, h.home, h.slot)
	h.setIntercept(func(w http.ResponseWriter, r *http.Request) bool {
		if !strings.HasSuffix(r.URL.Path, "/activated") {
			return false
		}
		rec := httptest.NewRecorder()
		h.mux.ServeHTTP(rec, r)
		http.Error(w, "lost", http.StatusBadGateway)
		return true
	})
	m, out := h.mover()
	if code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false); code != exitPending {
		t.Fatalf("lost receipt: %d\n%s", code, out)
	}
	if got := h.sessionStatus(sessionID); got != returnsession.StatusCompleted {
		t.Fatalf("Cloud did not commit the activation: %s", got)
	}
	h.setIntercept(nil)
	out.Reset()
	if code := m.ReturnResume(h.ctx, h.local.pool, config, false); code != exitDone || !strings.Contains(out.String(), "active on this") {
		t.Fatalf("resume: %d\n%s", code, out)
	}
	if got := authority(t, h.local, h.pid); got != "active" {
		t.Fatalf("local authority %s", got)
	}
	if v, err := configValue(config, "SUMI_PERSONA_ID"); err != nil || v != h.pid {
		t.Fatalf("config not retargeted: %v %q", err, v)
	}
}

// Cloud refuses the seal because the secretary still has queued work. The
// command stops as pending with guidance, the secretary stays active on
// Cloud, and once the work finishes a resume completes the return.
func TestBusyCloudSecretaryReturnWaitsThenResumes(t *testing.T) {
	h := setupReturn(t)
	sessionID, returnURL := h.newReturn()
	config := writeConfig(t, h.home, h.slot)
	if _, _, err := h.cloud.state.SubmitJob(h.ctx, h.pid, "j-busy", "subprocess",
		map[string]any{"command": []any{"echo", "hi"}}, "api"); err != nil {
		t.Fatal(err)
	}
	m, out := h.mover()
	if code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false); code != exitPending {
		t.Fatalf("busy: %d\n%s", code, out)
	}
	if s := out.String(); !strings.Contains(s, "j-busy") || !strings.Contains(s, "return-resume") {
		t.Fatalf("the refusal did not name the work and the way on:\n%s", s)
	}
	if got := authority(t, h.cloud, h.pid); got != "active" {
		t.Fatalf("Cloud authority %s", got)
	}
	if got := h.sessionStatus(sessionID); got != returnsession.StatusAwaitingDestination {
		t.Fatalf("session %s", got)
	}

	if _, _, err := h.cloud.state.ClaimJobs(h.ctx, h.pid, "runner-1", []string{"subprocess"}, time.Minute, 4, "*"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.cloud.state.CompleteJob(h.ctx, h.pid, "j-busy", "runner-1", "done",
		map[string]any{"exit_code": 0.0}, ""); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if code := m.ReturnResume(h.ctx, h.local.pool, config, false); code != exitDone {
		t.Fatalf("resume after the work finished: %d\n%s", code, out)
	}
	if got := authority(t, h.local, h.pid); got != "active" {
		t.Fatalf("local authority %s", got)
	}
}

// Pressing Enter at the paste prompt, piping whitespace or closing stdin
// must not end `return` or `start` as a success with nothing done.
//
// Found by the 2026-09-28 acceptance journey: an empty URL from a lost
// create answer reached `sumi-local-move return`, which exited 0 silently.
func TestBlankURLLineIsNotSuccess(t *testing.T) {
	h := setupReturn(t)
	config := writeConfig(t, h.home, h.slot)
	conn := h.local.pool.Config().ConnConfig
	env := map[string]string{
		"SUMI_LOCAL_HOME":   h.home,
		"SUMI_DB_URL":       fmt.Sprintf("postgres://%s:%s@%s:%d/%s", conn.User, conn.Password, conn.Host, conn.Port, conn.Database),
		"SUMI_PERSONA_ID":   h.slot,
		"SUMI_LOCAL_CONFIG": config,
	}
	getenv := func(k string) string { return env[k] }
	for _, cmd := range []string{"return", "start"} {
		for name, in := range map[string]string{"enter": "\n", "blanks": " \t \n", "crlf": "\r\n", "eof": "", "blanks-eof": "  "} {
			t.Run(cmd+"/"+name, func(t *testing.T) {
				var stdout, stderr bytes.Buffer
				code := run(h.ctx, []string{cmd}, strings.NewReader(in), &stdout, &stderr, getenv)
				if code != exitUsage || !strings.Contains(stderr.String(), "URL on stdin") {
					t.Fatalf("exit %d\nstdout: %q\nstderr: %q", code, stdout.String(), stderr.String())
				}
				if _, err := os.Stat(filepath.Join(h.home, "return", "state.json")); !os.IsNotExist(err) {
					t.Fatalf("a blank line left a return record: %v", err)
				}
			})
		}
	}
}

// openReturn creates a return session with the given owner cookie.
func openReturn(t *testing.T, srv *httptest.Server, cookie string) (sessionID, returnURL string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/secretary-return/sessions", strings.NewReader(`{"file_mode":"cloud"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Cookie", returnsessiontest.Cookie+"="+cookie)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
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
		t.Fatalf("create: %d %s", res.StatusCode, raw)
	}
	return body.Session.SessionID, body.ReturnURL
}
