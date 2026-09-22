package browsertabs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/chatgpt"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

const owner = "0198f0f4-9b72-7000-8000-000000000701"
const other = "0198f0f4-9b72-7000-8000-000000000702"
const persona = "0198f0f4-9b72-7000-8000-000000000703"
const unrelated = "0198f0f4-9b72-7000-8000-000000000704"

type fixture struct {
	store *Store
	core  *agentstate.Server
	url   string
	t     *testing.T
}

func setup(t *testing.T) *fixture {
	pool := testdb.Create(t)
	ctx := context.Background()
	if e := db.Migrate(ctx, pool); e != nil {
		t.Fatal(e)
	}
	for _, id := range []string{owner, other} {
		if _, e := pool.Exec(ctx, `INSERT INTO humans(human_id)VALUES($1)`, id); e != nil {
			t.Fatal(e)
		}
	}
	core := agentstate.NewServer(pool, "browser-fixture-admin-credential-long")
	for p, h := range map[string]string{persona: owner, unrelated: other} {
		if _, _, e := core.Store().EnsurePersona(ctx, p, &h, "Browser secretary"); e != nil {
			t.Fatal(e)
		}
	}
	store := New(pool, core.Store())
	for name, effect := range store.Effects() {
		if e := core.RegisterToolEffect(name, effect); e != nil {
			t.Fatal(e)
		}
	}
	mux := http.NewServeMux()
	core.RegisterRoutes(mux)
	(&Service{Store: store, Authenticate: func(r *http.Request) (chatgpt.LoginIdentity, error) {
		h := r.Header.Get("Test-Human")
		if h != owner && h != other {
			return chatgpt.LoginIdentity{}, fmt.Errorf("unauthorized")
		}
		return chatgpt.LoginIdentity{HumanID: h, Authorize: func(ctx context.Context, effect func(context.Context) error) error { return effect(ctx) }}, nil
	}}).RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return &fixture{store, core, server.URL, t}
}
func (f *fixture) api(method, path, auth string, in any) (int, []byte) {
	f.t.Helper()
	raw, _ := json.Marshal(in)
	req, _ := http.NewRequest(method, f.url+path, bytes.NewReader(raw))
	if auth == owner || auth == other {
		req.Header.Set("Test-Human", auth)
	} else {
		req.Header.Set("Authorization", "Bearer "+auth)
	}
	res, e := http.DefaultClient.Do(req)
	if e != nil {
		f.t.Fatal(e)
	}
	defer res.Body.Close()
	var b bytes.Buffer
	b.ReadFrom(res.Body)
	return res.StatusCode, b.Bytes()
}
func (f *fixture) attach(write bool) (Attachment, string) {
	f.t.Helper()
	status, b := f.api("POST", "/api/browser-tabs", owner, AttachInput{PersonaID: persona, Name: "Test tab", Tab: TabRef{uuid.NewString(), "fixture", uuid.NewString()}, AllowActions: write})
	if status != 200 {
		f.t.Fatalf("attach %d %s", status, b)
	}
	var out struct {
		Attachment Attachment `json:"attachment"`
		Token      string     `json:"host_token"`
	}
	json.Unmarshal(b, &out)
	return out.Attachment, out.Token
}
func (f *fixture) enqueue(a Attachment, method string) string {
	f.t.Helper()
	tx, e := f.store.Pool.Begin(context.Background())
	if e != nil {
		f.t.Fatal(e)
	}
	defer tx.Rollback(context.Background())
	id := uuid.NewString()
	req := map[string]any{"attachment_id": a.ID}
	if method == "act" {
		req["binding"] = map[string]any{"revision": float64(1), "observationId": uuid.NewString(), "url": "https://example.com/"}
		req["action"] = map[string]any{"kind": "click", "target": "t0"}
	}
	out, e := f.store.Effects()["browser."+method].Apply(context.Background(), tx, persona, id+":tool:0", req)
	if e != nil {
		f.t.Fatal(e)
	}
	if e = tx.Commit(context.Background()); e != nil {
		f.t.Fatal(e)
	}
	return out["job"].(map[string]any)["job_id"].(string)
}

func TestBrowserAuthorizationDurability(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	a, token := f.attach(true)
	status, _ := f.api("POST", "/api/browser-host/tabs/"+a.ID+"/poll", "wrong", nil)
	if status != 403 {
		t.Fatal(status)
	}
	if _, e := f.store.Claim(ctx, a.ID, token); e != nil {
		t.Fatal(e)
	}
	b, token2 := f.attach(false)
	if _, e := f.store.Claim(ctx, b.ID, token2); e != nil {
		t.Fatal(e)
	}
	tx, _ := f.store.Pool.Begin(ctx)
	_, e := f.store.Effects()["browser.observe"].Apply(ctx, tx, unrelated, "unauthorized", map[string]any{"attachment_id": a.ID})
	tx.Rollback(ctx)
	if e == nil {
		t.Fatal("unrelated persona observed tab")
	}
	status, _ = f.api("DELETE", "/api/browser-tabs/"+a.ID, other, nil)
	if status != 403 {
		t.Fatal("other human revoked", status)
	}

	status, body := f.api("GET", "/api/browser-tabs", owner, nil)
	if status != 200 || bytes.Contains(body, []byte(token)) {
		t.Fatal("list leaked token or failed", status)
	}
	status, _ = f.api("POST", "/api/browser-tabs", other, AttachInput{PersonaID: persona, Name: "stolen", Tab: TabRef{uuid.NewString(), "other", uuid.NewString()}})
	if status != 403 {
		t.Fatal("other owner attached", status)
	}
	tx, _ = f.store.Pool.Begin(ctx)
	_, e = f.store.Effects()["browser.act"].Apply(ctx, tx, persona, "read-only", map[string]any{"attachment_id": b.ID, "binding": map[string]any{"revision": float64(1), "observationId": uuid.NewString(), "url": "https://example.com"}, "action": map[string]any{"kind": "click", "target": "t0"}})
	tx.Rollback(ctx)
	if e == nil {
		t.Fatal("read-only grant acted")
	}
	job := f.enqueue(a, "act")
	claimed, e := f.store.Claim(ctx, a.ID, token)
	if e != nil || claimed == nil || claimed.JobID != job {
		t.Fatal(claimed, e)
	}
	if duplicate, e := f.store.Claim(ctx, a.ID, token); e != nil || duplicate != nil {
		t.Fatal("dispatch replay", duplicate, e)
	}
	if _, e = f.store.Complete(ctx, b.ID, token2, job, "done", map[string]any{}, ""); e == nil {
		t.Fatal("other host completed job")
	}
	if e = f.store.Revoke(ctx, owner, a.ID); e != nil {
		t.Fatal(e)
	}
	if _, e = f.store.Claim(ctx, a.ID, token); e == nil {
		t.Fatal("revoked grant polled")
	}
	result := map[string]any{"dispatched": true, "outcome": "returned"}
	for i := 0; i < 2; i++ {
		if _, e = f.store.Complete(ctx, a.ID, token, job, "done", result, ""); e != nil {
			t.Fatal("completion receipt replay", e)
		}
	}
	var notes int
	f.store.Pool.QueryRow(ctx, `SELECT count(*) FROM core_inputs WHERE persona_id=$1 AND input_id=$2`, persona, "job:"+job).Scan(&notes)
	if notes != 1 {
		t.Fatal("notification count", notes)
	}
	c, ct := f.attach(true)
	f.store.Claim(ctx, c.ID, ct)
	lost := f.enqueue(c, "act")
	f.store.Claim(ctx, c.ID, ct)
	f.store.Pool.Exec(ctx, `UPDATE core_jobs SET claim_expires_at=now()-interval '1 second' WHERE job_id=$1`, lost)
	if e = f.store.Sweep(ctx); e != nil {
		t.Fatal(e)
	}
	j, _ := f.core.Store().GetJob(ctx, persona, lost)
	if j.Status != "lost" {
		t.Fatal(j.Status)
	}
	if replay, e := f.store.Claim(ctx, c.ID, ct); e != nil || replay != nil {
		t.Fatal("lost action replayed", replay, e)
	}
	queued := f.enqueue(c, "observe")
	f.store.Pool.Exec(ctx, `UPDATE core_jobs SET created_at=now()-interval '61 seconds' WHERE job_id=$1`, queued)
	if e = f.store.Sweep(ctx); e != nil {
		t.Fatal(e)
	}
	j, _ = f.core.Store().GetJob(ctx, persona, queued)
	if j.Status != "failed" || j.Result["dispatched"] != false {
		t.Fatal(j)
	}
	// General persona runner credentials cannot consume browser dispatches.
	f.enqueue(c, "observe")
	jobs, _, e := f.core.Store().ClaimJobs(ctx, persona, "unrelated-runner", []string{"browser"}, time.Minute, 1, "*")
	if e != nil || len(jobs) != 0 {
		t.Fatal(jobs, e)
	}
}

func TestSecretaryRealBrowser(t *testing.T) {
	if os.Getenv("SUMI_BROWSER_REAL_TEST") != "1" {
		t.Skip("set SUMI_BROWSER_REAL_TEST=1 for actual Electron acceptance")
	}
	f := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.store.Run(ctx)
	root, e := filepath.Abs("../../../../")
	if e != nil {
		t.Fatal(e)
	}
	artifacts := os.Getenv("SUMI_BROWSER_TEST_ARTIFACTS")
	if artifacts == "" {
		t.Fatal("SUMI_BROWSER_TEST_ARTIFACTS required")
	}
	if e = os.MkdirAll(artifacts, 0700); e != nil {
		t.Fatal(e)
	}
	dir, e := os.MkdirTemp(artifacts, "real-")
	if e != nil {
		t.Fatal(e)
	}
	t.Log("artifacts", dir)
	log, e := os.Create(filepath.Join(dir, "electron.log"))
	if e != nil {
		t.Fatal(e)
	}
	defer log.Close()
	child := exec.Command("xvfb-run", "-a", filepath.Join(root, "apps/desktop/node_modules/.bin/electron"), "test/host-acceptance.mjs")
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	child.Dir = filepath.Join(root, "apps/desktop")
	child.Env = append(os.Environ(), "BROWSER_TEST_URL="+f.url, "BROWSER_TEST_PERSONA="+persona, "BROWSER_TEST_HUMAN="+owner, "BROWSER_TEST_ARTIFACTS="+dir)
	child.Stdout = log
	child.Stderr = log
	if e = child.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { syscall.Kill(-child.Process.Pid, syscall.SIGTERM); child.Wait() }()
	deadline := time.Now().Add(12 * time.Second)
	for {
		if _, e = os.Stat(filepath.Join(dir, "ready.json")); e == nil {
			break
		}
		if time.Now().After(deadline) {
			b, _ := os.ReadFile(filepath.Join(dir, "electron.log"))
			t.Fatalf("host not ready: %s", b)
		}
		time.Sleep(50 * time.Millisecond)
	}
	originID := "acceptance:tool:human:" + uuid.NewString()
	originText := "Find my shared Acceptance tab, read what I typed, fill the form and save it once, then read its visible result."
	_, _, e = f.core.Store().SubmitInput(ctx, &agentstate.Input{PersonaID: persona, InputID: originID, Kind: "message", Payload: map[string]any{"text": originText}, ActorKind: "human", ActorID: owner, SourceSurface: "browser-acceptance", Attention: "reply"})
	if e != nil {
		t.Fatal(e)
	}
	core := exec.Command("node", filepath.Join(root, "apps/core/scripts/browser-acceptance-child.mjs"))
	core.Env = append(os.Environ(), "BROWSER_TEST_URL="+f.url, "BROWSER_TEST_PERSONA="+persona, "BROWSER_TEST_TOKEN="+f.core.PersonaToken(persona))
	out, e := core.CombinedOutput()
	os.WriteFile(filepath.Join(dir, "secretary.log"), out, 0600)
	if e != nil {
		t.Fatalf("secretary %v\n%s", e, out)
	}
	deadline = time.Now().Add(time.Second)
	for {
		if _, e = os.Stat(filepath.Join(dir, "visible.json")); e == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("human-visible result missing")
		}
		time.Sleep(25 * time.Millisecond)
	}
	jobs, e := f.core.Store().ListJobs(ctx, persona, nil, 20)
	if e != nil || len(jobs) != 5 {
		t.Fatal("jobs", len(jobs), e)
	}
	sawHumanOrigin := false
	for _, j := range jobs {
		if j.Status != "done" {
			t.Fatal(j)
		}
		var notification map[string]any
		if e := f.store.Pool.QueryRow(ctx, `SELECT payload FROM core_inputs WHERE persona_id=$1 AND input_id=$2`, persona, "job:"+j.JobID).Scan(&notification); e != nil {
			t.Fatal(e)
		}
		// Later operations can start while handling a prior job notification.
		// Resolve the actual originating turn, not an assumed transitive root.
		var startedFrom, expectedText string
		if e := f.store.Pool.QueryRow(ctx, `SELECT i.input_id, CASE WHEN length(i.payload->>'text') > 200 THEN left(i.payload->>'text',200)||'…' ELSE i.payload->>'text' END
          FROM core_operations o JOIN core_turns t USING(persona_id,turn_id)
          JOIN core_inputs i ON i.persona_id=t.persona_id AND i.input_id=t.input_id
          WHERE o.persona_id=$1 AND o.response->'job'->>'job_id'=$2`, persona, j.JobID).Scan(&startedFrom, &expectedText); e != nil {
			t.Fatal(e)
		}
		_, hasProgress := notification["origin_in_progress"].(bool)
		if notification["origin_input_id"] != startedFrom || notification["origin_request"] != expectedText || !hasProgress {
			t.Fatalf("real browser notification: %+v", notification)
		}
		if startedFrom == originID {
			sawHumanOrigin = true
		}
	}
	if !sawHumanOrigin {
		t.Fatal("human request lost its initiating browser job")
	}
	t.Log("PASS actual Secretary → API/DB durable jobs → authenticated host → same visible Electron tab; discovery, human input, fill/click and result observation")
}
