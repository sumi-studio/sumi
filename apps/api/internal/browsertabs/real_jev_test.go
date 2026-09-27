package browsertabs

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

// Actual Secretary → API/DB durable jobs → authenticated host → Jev test
// double → same visible Electron tab. Covers delegated completion with a
// person's concurrent edit, progress + cancellation of a running goal, the
// person's Stop in the host strip, a Jev auth failure (Jev then withdrawn),
// and the unchanged direct path afterwards. The Jev endpoint is
// a contract-checking loopback double, not the live TypeSafe API.
func TestSecretaryJevGoalRealBrowser(t *testing.T) {
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
	os.MkdirAll(artifacts, 0700)
	dir, e := os.MkdirTemp(artifacts, "real-jev-")
	if e != nil {
		t.Fatal(e)
	}
	t.Log("artifacts", dir)
	logf, _ := os.Create(filepath.Join(dir, "electron.log"))
	defer logf.Close()
	site := "http://127.0.0.1:19274"
	child := exec.Command("xvfb-run", "-a", filepath.Join(root, "apps/desktop/node_modules/.bin/electron"), "test/host-jev-acceptance.mjs")
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	child.Dir = filepath.Join(root, "apps/desktop")
	child.Env = append(os.Environ(), "BROWSER_TEST_URL="+f.url, "BROWSER_TEST_PERSONA="+persona, "BROWSER_TEST_HUMAN="+owner, "BROWSER_TEST_ARTIFACTS="+dir, "SUMI_JEV_SITE_PORT=19274", "SUMI_JEV_FIXTURE_PORT=19275")
	child.Stdout = logf
	child.Stderr = logf
	if e = child.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { syscall.Kill(-child.Process.Pid, syscall.SIGTERM); child.Wait() }()
	deadline := time.Now().Add(20 * time.Second)
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
	_, _, e = f.core.Store().SubmitInput(ctx, &agentstate.Input{PersonaID: persona, InputID: "jev-acceptance:" + uuid.NewString(), Kind: "message", Payload: map[string]any{"text": "Use my shared tab: sign me up, then try the pager, and stop it when I ask."}, ActorKind: "human", ActorID: owner, SourceSurface: "browser-acceptance", Attention: "reply"})
	if e != nil {
		t.Fatal(e)
	}
	core := exec.Command("node", filepath.Join(root, "apps/core/scripts/browser-goal-acceptance-child.mjs"))
	core.Env = append(os.Environ(), "BROWSER_TEST_URL="+f.url, "BROWSER_TEST_PERSONA="+persona, "BROWSER_TEST_TOKEN="+f.core.PersonaToken(persona), "BROWSER_TEST_SITE="+site)
	out, e := core.CombinedOutput()
	os.WriteFile(filepath.Join(dir, "secretary.log"), out, 0600)
	if e != nil {
		b, _ := os.ReadFile(filepath.Join(dir, "electron.log"))
		t.Fatalf("secretary %v\n%s\n--- electron ---\n%s", e, out, b)
	}
	time.Sleep(1200 * time.Millisecond)
	var state struct {
		Page  string `json:"page"`
		Saved struct {
			Result string `json:"result"`
			Note   string `json:"note"`
		} `json:"saved"`
		Errors      []string `json:"errors"`
		JevRequests []string `json:"jevRequests"`
	}
	raw, e := os.ReadFile(filepath.Join(dir, "state.json"))
	if e != nil || json.Unmarshal(raw, &state) != nil {
		t.Fatal("visible state", e)
	}
	if state.Saved.Result != "Saved Ada Lovelace <ada@example.test> x1" || state.Saved.Note != "person note" {
		t.Fatalf("person-visible signup %+v", state.Saved)
	}
	// Cancellation stopped the pager goal well before its 30 steps, the
	// person's Stop after two more clicks; the only later click is the
	// secretary's explicit direct action.
	pages, e := strconv.Atoi(strings.TrimPrefix(state.Page, "Page "))
	if e != nil || pages < 4 || pages > 9 {
		t.Fatalf("pager after cancel + person stop + one direct click: %q", state.Page)
	}
	var jobs []agentstate.Job
	jobs, e = f.core.Store().ListJobs(ctx, persona, nil, 50)
	if e != nil {
		t.Fatal(e)
	}
	goals := map[string]int{}
	for _, j := range jobs {
		if j.Request["method"] == "goal" {
			goals[j.Status]++
		}
	}
	if goals["done"] != 1 || goals["cancelled"] != 2 || goals["failed"] != 1 {
		t.Fatalf("goal jobs %v", goals)
	}
	// No credential reaches Jev, job records, or the tool path.
	var leaks int
	f.store.Pool.QueryRow(ctx, `SELECT count(*) FROM core_jobs WHERE persona_id=$1 AND (result::text LIKE '%fixture-key%' OR request::text LIKE '%fixture-key%')`, persona).Scan(&leaks)
	if leaks != 0 {
		t.Fatal("credential material in job records", leaks)
	}
	for _, r := range state.JevRequests {
		if strings.Contains(r, "browser_") || strings.Contains(r, owner) || strings.Contains(r, "fixture-key") {
			t.Fatal("credential or identity sent to Jev")
		}
	}
	t.Logf("PASS Secretary → browser.goal → host Jev loop (test double) → same tab: done with person edit, progress+cancel, person Stop in host strip, auth failure → Jev withdrawn, direct fallback (%d Jev requests)", len(state.JevRequests))
}
