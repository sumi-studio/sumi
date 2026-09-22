//go:build linux

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"golang.org/x/sys/unix"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/mcpconnections"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

func TestLocalMCPActualBinaryCLISecretaryAndStdio(t *testing.T) {
	pool := testdb.Create(t)
	root := t.TempDir()
	binary := filepath.Join(root, "sumi-local-service")
	child := filepath.Join(root, "real-mcp-server")
	for _, build := range [][]string{{"build", "-buildvcs=false", "-o", binary, "."}, {"test", "-buildvcs=false", "-c", "-o", child, "../../internal/mcpconnections"}} {
		command := exec.Command("go", build...)
		if out, e := command.CombinedOutput(); e != nil {
			t.Fatalf("build %v: %v %s", build, e, out)
		}
	}
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	address := listener.Addr().String()
	listener.Close()
	base := "http://" + address
	admin := "local-mcp-binary-fixture-admin-secret"
	core := agentstate.NewServer(pool, admin)
	fm := &fmServer{secret: []byte(admin)}
	workspace := filepath.Join(root, "workspace")
	logPath := filepath.Join(root, "host.log")
	start := func() func() {
		log, e := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		command := exec.Command(binary)
		command.Env = []string{"PATH=" + os.Getenv("PATH"), "SUMI_DB_URL=" + pool.Config().ConnString(), "SUMI_CORE_STATE_TOKEN=" + admin, "SUMI_FM_LISTEN=" + address, "SUMI_FM_PERSONA_ID=" + localPersona, "SUMI_LOCAL_ID=sumi-local-mcp-20260922-fixture", "SUMI_MODEL_API_KEY=must-not-inherit-model-key", "SUMI_LOCAL_WORKING_STORE=local", "SUMI_WORKSPACE_ROOT=" + workspace, "SUMI_LOCAL_TERMINAL_ROOT=" + filepath.Join(root, "terminals")}
		command.Stdout = log
		command.Stderr = log
		if e := command.Start(); e != nil {
			t.Fatal(e)
		}
		done := make(chan error, 1)
		go func() { done <- command.Wait(); log.Close() }()
		stop := func() {
			_ = command.Process.Signal(syscall.SIGTERM)
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				command.Process.Kill()
				<-done
				t.Error("Local binary did not shut down cleanly")
			}
		}
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			resp, e := http.Get(base + "/health")
			if e == nil {
				resp.Body.Close()
				if resp.StatusCode == 200 {
					return stop
				}
			}
			time.Sleep(30 * time.Millisecond)
		}
		stop()
		raw, _ := os.ReadFile(logPath)
		t.Fatalf("Local binary did not start: %s", raw)
		return nil
	}
	stop := start()
	defer func() {
		if stop != nil {
			stop()
		}
	}()
	request := func(method, path, token string, body any) (int, []byte) {
		raw, _ := json.Marshal(body)
		req, e := http.NewRequest(method, base+path, bytes.NewReader(raw))
		if e != nil {
			t.Fatal(e)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, e := http.DefaultClient.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		defer resp.Body.Close()
		out, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, out
	}
	configText := "SUMI_LOCAL_ID=sumi-local-mcp-20260922-fixture\nSUMI_LOCAL_LISTEN=" + address + "\nSUMI_LOCAL_DB_MODE=external\nSUMI_DB_URL=" + pool.Config().ConnString() + "\nSUMI_CORE_STATE_TOKEN=" + admin + "\nSUMI_PERSONA_ID=" + localPersona + "\n"
	if e := os.WriteFile(filepath.Join(root, "config.env"), []byte(configText), 0600); e != nil {
		t.Fatal(e)
	}
	cli, e := filepath.Abs("../../../../deploy/local-host/sumi-local")
	if e != nil {
		t.Fatal(e)
	}
	cliCall := func(args ...string) []byte {
		command := exec.Command(cli, append([]string{"mcp"}, args...)...)
		command.Env = append(os.Environ(), "SUMI_LOCAL_HOME="+root)
		out, e := command.CombinedOutput()
		if e != nil {
			t.Fatalf("CLI %v: %v %s", args, e, out)
		}
		return out
	}
	cwd := filepath.Join(workspace, localPersona)
	if e := os.MkdirAll(cwd, 0700); e != nil {
		t.Fatal(e)
	}
	cfg := mcpconnections.LocalInput{Name: "Real Local stdio", Transport: "stdio", Enabled: true, Command: child, Args: []string{"-test.run=^TestLocalStdioServerHelper$"}, Cwd: cwd, Env: map[string]string{"SUMI_MCP_STDIO_HELPER": "yes", "SERVER_SECRET": "local-binary-private-secret"}}
	raw, _ := json.Marshal(cfg)
	configFile := filepath.Join(root, "mcp.json")
	os.WriteFile(configFile, raw, 0600)
	saved := cliCall("save", configFile)
	var metadata mcpconnections.Connection
	if e = json.Unmarshal(saved, &metadata); e != nil {
		t.Fatal(e, string(saved))
	}
	listed := cliCall("list")
	if !bytes.Contains(listed, []byte(metadata.ID)) || bytes.Contains(listed, []byte(cfg.Command)) || bytes.Contains(listed, []byte("local-binary-private-secret")) {
		t.Fatal("CLI metadata", string(listed))
	}
	route := "/fm/" + localPersona + "/mcp-connections/" + metadata.ID
	if status, _ := request("PUT", route, core.PersonaToken(localPersona), cfg); status != 401 {
		t.Fatal("Core token changed human settings", status)
	}
	if status, _ := request("DELETE", "/fm/"+foreignPersona+"/mcp-connections/"+metadata.ID, fm.fmToken(foreignPersona), nil); status != 403 {
		t.Fatal("wrong persona changed grant", status)
	}
	var human *string
	if e = pool.QueryRow(context.Background(), `SELECT human_id FROM core_personas WHERE persona_id=$1`, localPersona).Scan(&human); e != nil || human != nil {
		t.Fatal("Local fabricated a Cloud human", human, e)
	}
	coreCall := func(tool string, args map[string]any) string {
		status, body := request("POST", "/fm/"+localPersona+"/inputs", fm.fmToken(localPersona), map[string]any{"text": "Local MCP acceptance " + tool})
		if status != 200 && status != 201 {
			t.Fatal(status, string(body))
		}
		call, _ := json.Marshal(map[string]any{"tool": tool, "request": args})
		script, _ := filepath.Abs("../../../core/scripts/mcp-acceptance-child.mjs")
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, "node", script)
		command.Env = append(os.Environ(), "MCP_TEST_URL="+base, "MCP_TEST_PERSONA="+localPersona, "MCP_TEST_TOKEN="+core.PersonaToken(localPersona), "MCP_TEST_CALL="+string(call))
		out, e := command.CombinedOutput()
		if e != nil {
			t.Fatalf("Core %s: %v %s", tool, e, out)
		}
		if bytes.Contains(out, []byte("local-binary-private-secret")) || bytes.Contains(out, []byte(admin)) {
			t.Fatal("credentials in model context")
		}
		return string(out)
	}
	// No preloaded connection ID or server-tool name: obtain both from actual
	// Secretary-visible tool results, then invoke the discovered schema/name.
	out := coreCall("mcp.connections", map[string]any{})
	discovered := findMCPString(out, "connection_id")
	if discovered == "" || discovered != metadata.ID {
		t.Fatal("Core did not discover saved connection", out)
	}
	stop()
	stop = nil
	stop = start()
	listed = cliCall("list")
	if !bytes.Contains(listed, []byte(discovered)) {
		t.Fatal("config lost after restart", string(listed))
	}
	coreCall("mcp.list_tools", map[string]any{"connection_id": discovered})
	waitJob := func() agentstate.Job {
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			jobs, e := core.Store().ListJobs(context.Background(), localPersona, nil, 1)
			if e != nil {
				t.Fatal(e)
			}
			if len(jobs) > 0 && (jobs[0].Status == "done" || jobs[0].Status == "failed") {
				return jobs[0]
			}
			time.Sleep(30 * time.Millisecond)
		}
		t.Fatal("job did not finish")
		return agentstate.Job{}
	}
	listedJob := waitJob()
	if listedJob.Status != "done" {
		t.Fatal("discovery failed", listedJob)
	}
	out = coreCall("job.status", map[string]any{"job_id": listedJob.JobID})
	name := findMCPString(out, "name") // Tool catalog also has names; find server tool from tools result below.
	var schema map[string]any
	for _, tool := range listedJob.Result["tools"].([]any) {
		candidate := tool.(map[string]any)
		if strings.Contains(out, candidate["name"].(string)) {
			name = candidate["name"].(string)
			schema = candidate
			break
		}
	}
	if name == "" || schema["inputSchema"] == nil || !strings.Contains(out, "inputSchema") {
		t.Fatal("discovered tool schema missing", out)
	}
	coreCall("mcp.call", map[string]any{"connection_id": discovered, "name": name, "arguments": map[string]any{"label": "from-actual-secretary"}})
	invoked := waitJob()
	if invoked.JobID == listedJob.JobID || invoked.Status != "done" {
		t.Fatal("call failed", invoked)
	}
	out = coreCall("job.status", map[string]any{"job_id": invoked.JobID})
	if !strings.Contains(out, "structuredContent") || !strings.Contains(out, "from-actual-secretary") || !strings.Contains(out, "[redacted]") {
		t.Fatal("structured result not usable by Core", out)
	}
	// One failed MCP call leaves the same Local terminal and previous files usable.
	coreCall("terminal.open", map[string]any{"name": "MCP failure isolation"})
	sessions, e := core.Store().ListTerminalSessions(context.Background(), localPersona)
	if e != nil || len(sessions) != 1 {
		t.Fatal(sessions, e)
	}
	terminal := sessions[0].SessionID
	coreCall("terminal.write", map[string]any{"session_id": terminal, "data": "printf old-file > prior.txt\n"})
	coreCall("mcp.call", map[string]any{"connection_id": discovered, "name": name, "arguments": map[string]any{"label": "tool-failed"}})
	failed := waitJob()
	if failed.Status != "failed" || failed.Result["outcome"] != "returned" {
		t.Fatal("MCP tool failure", failed)
	}
	coreCall("terminal.write", map[string]any{"session_id": terminal, "data": "cat prior.txt > after-mcp-failure.txt\n"})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		b, _ := os.ReadFile(filepath.Join(cwd, "after-mcp-failure.txt"))
		if string(b) == "old-file" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if b, _ := os.ReadFile(filepath.Join(cwd, "after-mcp-failure.txt")); string(b) != "old-file" {
		t.Fatal("MCP error destroyed terminal/files", string(b))
	}
	// Normal host SIGTERM drains an in-flight stdio process group. Its job
	// remains uncertain and recovery expires it to lost without re-dispatch.
	coreCall("mcp.call", map[string]any{"connection_id": discovered, "name": name, "arguments": map[string]any{"label": "wait-for-cancel"}})
	var descendant int
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		raw, _ := os.ReadFile(filepath.Join(cwd, "descendant.pid"))
		descendant, _ = strconv.Atoi(string(raw))
		if descendant > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if descendant == 0 {
		t.Fatal("real host MCP descendant did not start")
	}
	fd, e := unix.PidfdOpen(descendant, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer unix.Close(fd)
	defer unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0)
	jobs, e := core.Store().ListJobs(context.Background(), localPersona, nil, 1)
	if e != nil || len(jobs) != 1 {
		t.Fatal(jobs, e)
	}
	interrupted := jobs[0]
	stop()
	stop = nil
	polled, e := unix.Poll([]unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}, 2000)
	if e != nil || polled == 0 {
		t.Fatal("normal host stop left MCP process group alive", e)
	}
	_, e = pool.Exec(context.Background(), `UPDATE core_jobs SET claim_expires_at=now()-interval '1 second' WHERE persona_id=$1 AND job_id=$2`, localPersona, interrupted.JobID)
	if e != nil {
		t.Fatal(e)
	}
	stop = start()
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		j, _ := core.Store().GetJob(context.Background(), localPersona, interrupted.JobID)
		if j.Status == "lost" {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	recovered, _ := core.Store().GetJob(context.Background(), localPersona, interrupted.JobID)
	if recovered.Status != "lost" {
		t.Fatal("interrupted MCP job did not stay lost", recovered)
	}
	effects, _ := os.ReadFile(filepath.Join(cwd, "effects.txt"))
	if strings.Count(string(effects), "wait-for-cancel") != 1 {
		t.Fatal("host restart repeated uncertain effect", string(effects))
	}
	cliCall("delete", metadata.ID)
	var leak bool
	if e = pool.QueryRow(context.Background(), `SELECT EXISTS(SELECT 1 FROM core_jobs WHERE request::text LIKE $1 OR result::text LIKE $1)`, "%local-binary-private-secret%").Scan(&leak); e != nil || leak {
		t.Fatal("credential in jobs", e)
	}
	stop()
	stop = nil
	logs, _ := os.ReadFile(logPath)
	if bytes.Contains(logs, []byte("local-binary-private-secret")) || bytes.Contains(logs, []byte("noisy stderr")) || bytes.Contains(logs, []byte(core.PersonaToken(localPersona))) || bytes.Contains(logs, []byte(fm.fmToken(localPersona))) || bytes.Contains(logs, []byte(admin)) {
		t.Fatal("child stderr/secret in host logs")
	}
	t.Log("actual Local binary + executable CLI + PG NULL-human persona + TypeScript Secretary discovery + real child SDK stdio + structured result; restart persistence, auth refusal and existing terminal/file failure isolation verified (deterministic model)")
}

func findMCPString(log, key string) string {
	var walk func(any) string
	walk = func(v any) string {
		switch x := v.(type) {
		case map[string]any:
			if found, ok := x[key].(string); ok {
				return found
			}
			for _, value := range x {
				if got := walk(value); got != "" {
					return got
				}
			}
		case []any:
			for _, value := range x {
				if got := walk(value); got != "" {
					return got
				}
			}
		case string:
			var nested any
			if json.Unmarshal([]byte(x), &nested) == nil {
				return walk(nested)
			}
		}
		return ""
	}
	for _, line := range strings.Split(log, "\n") {
		var value any
		if json.Unmarshal([]byte(line), &value) == nil {
			if got := walk(value); got != "" {
				return got
			}
		}
	}
	return ""
}
