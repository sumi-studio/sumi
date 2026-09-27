package browsertabs

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

// The configured engineering entry (dist/browser-connected.js, owner-only
// config and key file) → API/DB → host → loopback Jev double → a site that
// echoes the private values it saves. No form of a private value — raw,
// escaped, encoded, a line of it, or a prefix cut at a truncation boundary —
// may reach a Jev request or the goal's durable progress, result or
// notification. The values are synthetic.
func TestConnectedEntryKeepsPrivateInputsFromJev(t *testing.T) {
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
	dir, e := os.MkdirTemp(artifacts, "connected-private-")
	if e != nil {
		t.Fatal(e)
	}
	t.Log("artifacts", dir)
	keyFile := filepath.Join(dir, "jev-fixture-key")
	os.WriteFile(keyFile, []byte("sumi-jev-20260927-fixture-key\n"), 0600)
	cfg, _ := json.Marshal(map[string]any{"apiOrigin": f.url, "humanHeaders": map[string]string{"Test-Human": owner}, "personaId": persona, "profileId": "sumi-jev-20260927-connected-private", "name": "Delivery tab", "url": "http://127.0.0.1:19276/", "allowActions": true, "jev": map[string]any{"apiKeyFile": keyFile, "endpoint": "http://127.0.0.1:19277", "model": "jev-fixture"}})
	cfgFile := filepath.Join(dir, "host.json")
	os.WriteFile(cfgFile, cfg, 0600)
	start := func(name string, cmd *exec.Cmd) {
		logf, _ := os.Create(filepath.Join(dir, name+".log"))
		cmd.Stdout, cmd.Stderr = logf, logf
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if e := cmd.Start(); e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM); cmd.Wait(); logf.Close() })
	}
	fixtures := exec.Command("node", "test/connected-private-fixture.mjs")
	fixtures.Dir = filepath.Join(root, "apps/desktop")
	fixtures.Env = append(os.Environ(), "OUT="+dir, "SITE_PORT=19276", "JEV_PORT=19277")
	start("fixtures", fixtures)
	host := exec.Command("xvfb-run", "-a", filepath.Join(root, "apps/desktop/node_modules/.bin/electron"), "dist/browser-connected.js")
	host.Dir = filepath.Join(root, "apps/desktop")
	host.Env = append(os.Environ(), "SUMI_BROWSER_HOST_CONFIG="+cfgFile)
	start("electron", host)

	var id string
	for deadline := time.Now().Add(30 * time.Second); id == ""; time.Sleep(300 * time.Millisecond) {
		if time.Now().After(deadline) {
			b, _ := os.ReadFile(filepath.Join(dir, "electron.log"))
			t.Fatalf("no Jev-available tab: %s", b)
		}
		out, e := f.tool("browser.tabs", map[string]any{})
		if e != nil {
			t.Fatal(e)
		}
		for _, tab := range out["tabs"].([]map[string]any) {
			if tab["operation_layers"].(map[string]any)["jev"] == "available" {
				id = tab["attachment_id"].(string)
			}
		}
	}
	secrets := map[string]string{
		"address":    "1 Quinzel Lane\nZephyrton 40417",
		"passphrase": `correct "horse" \ zygomatic-battery`,
		"token":      "tok_" + strings.Repeat("Q7x9Kp2Lm4", 20),
		"member":     "MEMBER-4411-2233-9988",
	}
	inputs := map[string]any{"name": "Ada Lovelace"}
	private := []any{}
	for k, v := range secrets {
		inputs[k] = v
		private = append(private, k)
	}
	res, e := f.tool("browser.goal", map[string]any{"attachment_id": id, "goal": "Enter my name, delivery address, passphrase, token and member number, then save the form.", "inputs": inputs, "private_inputs": private, "max_steps": float64(10)})
	if e != nil {
		t.Fatal(e)
	}
	goal := res["job"].(map[string]any)["job_id"].(string)
	// Every durable snapshot of the goal (progress while running, then the
	// receipt) is collected for the leak check.
	var durable []string
	var job agentstate.Job
	for deadline := time.Now().Add(90 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		job, _ = f.core.Store().GetJob(ctx, persona, goal)
		raw, _ := json.Marshal(map[string]any{"result": job.Result, "error": job.Error})
		durable = append(durable, string(raw))
		if job.Status == "done" || job.Status == "failed" || job.Status == "cancelled" || job.Status == "lost" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("goal stuck %s", job.Status)
		}
	}
	var notification string
	f.store.Pool.QueryRow(ctx, `SELECT payload::text FROM core_inputs WHERE persona_id=$1 AND input_id=$2`, persona, "job:"+goal).Scan(&notification)
	durable = append(durable, notification)
	receipt, _ := json.MarshalIndent(map[string]any{"status": job.Status, "result": job.Result, "error": job.Error}, "", " ")
	os.WriteFile(filepath.Join(dir, "goal-job.json"), receipt, 0600)
	value, _ := job.Result["value"].(map[string]any)
	if job.Status != "done" || value["goal_outcome"] != "jev_reported_done" {
		t.Fatalf("goal did not complete: %s", receipt)
	}
	// The host typed the real values; only what leaves for Jev is protected.
	var saves []map[string]string
	raw, _ := os.ReadFile(filepath.Join(dir, "site.json"))
	if json.Unmarshal(raw, &saves) != nil || len(saves) == 0 {
		t.Fatal("site did not save", string(raw))
	}
	for k, v := range secrets {
		if saves[0][k] != v {
			t.Fatalf("site saved %s=%q", k, saves[0][k])
		}
	}
	var requests []string
	raw, _ = os.ReadFile(filepath.Join(dir, "jev-requests.json"))
	if json.Unmarshal(raw, &requests) != nil || len(requests) < 3 {
		t.Fatal("Jev requests not recorded", string(raw))
	}
	echoed := false
	for _, r := range requests {
		echoed = echoed || strings.Contains(r, "Saved for Ada Lovelace")
	}
	if !echoed {
		t.Fatal("no Jev request saw the page after saving")
	}
	forms := map[string]string{}
	for k, v := range secrets {
		quoted, _ := json.Marshal(v)
		forms[k+" raw"] = v
		forms[k+" json"] = strings.Trim(string(quoted), `"`)
		forms[k+" form"] = url.QueryEscape(v)
		forms[k+" path"] = url.PathEscape(v)
	}
	for k, v := range map[string]string{
		"address line 1": "Quinzel", "address line 2": "Zephyrton",
		"passphrase part 1": "horse", "passphrase part 2": "zygomatic",
		"token chunk": "Q7x9Kp2Lm4", "member prefix at a cut": "MEMBER-44",
	} {
		forms[k] = v
	}
	leaks := 0
	check := func(where, text string) {
		for name, form := range forms {
			if strings.Contains(strings.ToLower(text), strings.ToLower(form)) {
				leaks++
				t.Errorf("%s contains %s", where, name)
			}
		}
	}
	for i, r := range requests {
		check("Jev request "+strconv.Itoa(i+1), r)
	}
	for _, d := range durable {
		check("goal record", d)
	}
	t.Logf("%d Jev requests, %d durable snapshots, %d private-value leaks", len(requests), len(durable), leaks)
	obs, e := f.tool("browser.observe", map[string]any{"attachment_id": id})
	if e != nil {
		t.Fatal(e)
	}
	o := obs["job"].(map[string]any)["job_id"].(string)
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		j, _ := f.core.Store().GetJob(ctx, persona, o)
		if j.Status == "done" {
			break
		}
		if j.Status != "queued" && j.Status != "running" || time.Now().After(deadline) {
			t.Fatalf("direct observe after the goal: %s %v", j.Status, j.Error)
		}
	}
}
