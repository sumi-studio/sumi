//go:build linux

package mcpconnections

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/sys/unix"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

// Also used as a real executable child by first-model's binary integration.
func TestLocalStdioServerHelper(t *testing.T) {
	if os.Getenv("SUMI_MCP_STDIO_HELPER") == "" {
		return
	}
	for _, key := range []string{"SUMI_CORE_STATE_TOKEN", "SUMI_MODEL_API_KEY", "SUMI_DB_URL"} {
		if os.Getenv(key) != "" {
			os.Exit(81)
		}
	}
	if os.Getenv("MCP_HANG_INIT") == "yes" {
		time.Sleep(time.Minute)
		os.Exit(84)
	}
	if os.Getenv("MCP_BAD_FRAME") == "yes" {
		fmt.Println("not-json")
		time.Sleep(time.Minute)
		os.Exit(82)
	}
	if os.Getenv("MCP_BIG_FRAME") == "yes" {
		fmt.Print(strings.Repeat("x", 3<<20))
		time.Sleep(time.Minute)
		os.Exit(83)
	}
	fmt.Fprint(os.Stderr, strings.Repeat("noisy stderr ", 350000))
	server := mcp.NewServer(&mcp.Implementation{Name: "real-local-fixture", Version: "1"}, nil)
	server.AddTool(&mcp.Tool{Name: "remember_label", InputSchema: map[string]any{"type": "object", "properties": map[string]any{"label": map[string]any{"type": "string"}}, "required": []string{"label"}}, OutputSchema: map[string]any{"type": "object", "properties": map[string]any{"label": map[string]any{"type": "string"}}, "required": []string{"label"}}}, func(ctx context.Context, r *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var args struct {
			Label string `json:"label"`
		}
		if e := json.Unmarshal(r.Params.Arguments, &args); e != nil {
			return nil, e
		}
		file, e := os.OpenFile("effects.txt", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if e != nil {
			return nil, e
		}
		_, e = fmt.Fprintln(file, args.Label)
		file.Close()
		if e != nil {
			return nil, e
		}
		if args.Label == "response-lost" {
			os.Exit(0)
		}
		if args.Label == "wait-for-cancel" {
			child := exec.Command("/bin/sleep", "60")
			if e := child.Start(); e != nil {
				return nil, e
			}
			_ = os.WriteFile("descendant.pid", []byte(strconv.Itoa(child.Process.Pid)), 0600)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		if args.Label == "tool-failed" {
			return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: "fixture error"}}}, nil
		}
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "saved " + os.Getenv("SERVER_SECRET")}}, StructuredContent: map[string]any{"label": args.Label}}, nil
	})
	_ = server.Run(context.Background(), &mcp.StdioTransport{})
	os.Exit(0)
}
func localTestStore(t *testing.T) (*Store, *agentstate.Server, LocalInput) {
	f := setup(t)
	// Local deliberately has NULL human_id, not setup's Cloud owner.
	if _, e := f.store.pool.Exec(context.Background(), `UPDATE core_personas SET human_id=NULL WHERE persona_id=$1`, persona); e != nil {
		t.Fatal(e)
	}
	s, e := NewLocal(f.store.pool, bytes.Repeat([]byte{7}, 32), uuid.NewString(), persona)
	if e != nil {
		t.Fatal(e)
	}
	cfg := LocalInput{Name: "Local server", Transport: "stdio", Enabled: true, Command: os.Args[0], Args: []string{"-test.run=^TestLocalStdioServerHelper$"}, Cwd: t.TempDir(), Env: map[string]string{"SUMI_MCP_STDIO_HELPER": "yes", "SERVER_SECRET": "local-secret-sentinel"}}
	return s, f.core, cfg
}
func localJob(t *testing.T, s *Store, core *agentstate.Server, id, method string, args map[string]any) agentstate.Job {
	t.Helper()
	tx, e := s.pool.Begin(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(context.Background())
	req := map[string]any{"connection_id": id}
	if method == "call" {
		req["name"] = "remember_label"
		req["arguments"] = args
	}
	out, e := s.Effects()["mcp."+method].Apply(context.Background(), tx, persona, "local-mcp-test:"+uuid.NewString()+":tool:0", req)
	if e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(context.Background()); e != nil {
		t.Fatal(e)
	}
	j, e := core.Store().GetJob(context.Background(), persona, out["job"].(map[string]any)["job_id"].(string))
	if e != nil {
		t.Fatal(e)
	}
	return j
}
func TestLocalStdioGrantRecoveryIsolationAndUncertainty(t *testing.T) {
	s, core, cfg := localTestStore(t)
	ctx := context.Background()
	c, e := s.SaveLocal(ctx, "", cfg)
	if e != nil {
		t.Fatal(e)
	}
	// A new host store with the same identity/key discovers persisted settings.
	reopened, e := NewLocal(s.pool, bytes.Repeat([]byte{7}, 32), s.local.host, persona)
	if e != nil {
		t.Fatal(e)
	}
	list, e := reopened.ListLocal(ctx)
	if e != nil || len(list) != 1 || list[0].ID != c.ID {
		t.Fatal("restart config", list, e)
	}
	another, _ := NewLocal(s.pool, bytes.Repeat([]byte{7}, 32), uuid.NewString(), persona)
	list, e = another.ListLocal(ctx)
	if e != nil || len(list) != 0 {
		t.Fatal("other install sees grant", list, e)
	}
	foreign, _ := NewLocal(s.pool, bytes.Repeat([]byte{7}, 32), s.local.host, other)
	list, e = foreign.ListLocal(ctx)
	if e != nil || len(list) != 0 {
		t.Fatal("other persona sees grant", list, e)
	}
	runner := NewRunner(reopened, core.Store())
	j := localJob(t, s, core, c.ID, "call", map[string]any{"label": "response-lost"})
	cloud, _ := New(s.pool, bytes.Repeat([]byte{7}, 32))
	if e = NewRunner(cloud, core.Store()).Tick(ctx); e != nil {
		t.Fatal(e)
	}
	unchanged, _ := core.Store().GetJob(ctx, persona, j.JobID)
	if unchanged.Status != "queued" {
		t.Fatal("Cloud claimed Local job", unchanged)
	}
	if _, _, e = core.Store().SubmitJob(ctx, persona, uuid.NewString(), s.jobKind(), map[string]any{}, "test"); e == nil {
		t.Fatal("generic job submission admitted Local executable")
	}
	if e = runner.Tick(ctx); e != nil {
		t.Fatal(e)
	}
	j, _ = core.Store().GetJob(ctx, persona, j.JobID)
	if j.Status != "failed" || j.Result["outcome"] != "indeterminate" || j.Result["dispatched"] != true {
		t.Fatal("uncertain mutation", j)
	}
	if e = runner.Tick(ctx); e != nil {
		t.Fatal(e)
	}
	raw, _ := os.ReadFile(filepath.Join(cfg.Cwd, "effects.txt"))
	if string(raw) != "response-lost\n" {
		t.Fatal("mutation replay", string(raw))
	}
	j = localJob(t, s, core, c.ID, "call", map[string]any{"label": "after-error"})
	if e = runner.Tick(ctx); e != nil {
		t.Fatal(e)
	}
	j, _ = core.Store().GetJob(ctx, persona, j.JobID)
	result, _ := json.Marshal(j.Result)
	if j.Status != "done" || !bytes.Contains(result, []byte("after-error")) || bytes.Contains(result, []byte("local-secret-sentinel")) {
		t.Fatal("recovery/result", j)
	}
	queued := localJob(t, s, core, c.ID, "call", map[string]any{"label": "revoked"})
	if e = s.DeleteLocal(ctx, c.ID); e != nil {
		t.Fatal(e)
	}
	if e = runner.Tick(ctx); e != nil {
		t.Fatal(e)
	}
	j, _ = core.Store().GetJob(ctx, persona, queued.JobID)
	if j.Status != "failed" || j.Result["dispatched"] != false {
		t.Fatal("revoked call ran", j)
	}
	raw, _ = os.ReadFile(filepath.Join(cfg.Cwd, "effects.txt"))
	if string(raw) != "response-lost\nafter-error\n" {
		t.Fatal("revoked mutation", string(raw))
	}
	t.Log("Local NULL-human grant persists; host/persona scoped; Cloud and generic-job isolation; real stdio mutation response loss executes once; next call succeeds; queued revocation prevents dispatch")
}
func TestLocalStdioMalformedInitializationIsBounded(t *testing.T) {
	for _, key := range []string{"MCP_BAD_FRAME", "MCP_BIG_FRAME", "MCP_HANG_INIT"} {
		t.Run(key, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			wire, stop, e := startStdio(ctx, LocalInput{Command: os.Args[0], Args: []string{"-test.run=^TestLocalStdioServerHelper$"}, Cwd: t.TempDir(), Env: map[string]string{"SUMI_MCP_STDIO_HELPER": "yes", key: "yes"}})
			if e != nil {
				t.Fatal(e)
			}
			defer stop()
			session, e := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).Connect(ctx, wire, nil)
			if e == nil {
				session.Close()
				t.Fatal("bad server initialized")
			}
		})
	}
}

func TestLocalStdioCancellationStopsOwnedProcessGroup(t *testing.T) {
	s, core, cfg := localTestStore(t)
	ctx := context.Background()
	c, e := s.SaveLocal(ctx, "", cfg)
	if e != nil {
		t.Fatal(e)
	}
	j := localJob(t, s, core, c.ID, "call", map[string]any{"label": "wait-for-cancel"})
	done := make(chan error, 1)
	go func() { done <- NewRunner(s, core.Store()).Tick(ctx) }()
	var pid int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		raw, _ := os.ReadFile(filepath.Join(cfg.Cwd, "descendant.pid"))
		pid, _ = strconv.Atoi(string(raw))
		if pid > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pid == 0 {
		t.Fatal("MCP descendant did not start")
	}
	fd, e := unix.PidfdOpen(pid, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer unix.Close(fd)
	defer unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0)
	if _, e = core.Store().CancelJob(ctx, persona, j.JobID); e != nil {
		t.Fatal(e)
	}
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancel did not bound runner")
	}
	poll := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	n, e := unix.Poll(poll, 2000)
	if e != nil || n == 0 {
		t.Fatal("owned descendant survived cancellation", e)
	}
	result, e := core.Store().GetJob(ctx, persona, j.JobID)
	if e != nil || result.Status != "failed" || result.Result["outcome"] != "indeterminate" || result.CancelRequestedAt == nil {
		t.Fatal("cancel disposition", result, e)
	}
	// An interrupted claimed Local job expires to lost, never back to queued.
	claimed := localJob(t, s, core, c.ID, "call", map[string]any{"label": "must-not-replay"})
	jobs, _, e := core.Store().ClaimJobs(ctx, persona, "crashed-local-runner", []string{s.jobKind()}, time.Minute, 1, "*")
	if e != nil || len(jobs) != 1 {
		t.Fatal(jobs, e)
	}
	if _, e = s.pool.Exec(ctx, `UPDATE core_jobs SET claim_expires_at=now()-interval '1 second' WHERE persona_id=$1 AND job_id=$2`, persona, claimed.JobID); e != nil {
		t.Fatal(e)
	}
	if e = NewRunner(s, core.Store()).Tick(ctx); e != nil {
		t.Fatal(e)
	}
	result, _ = core.Store().GetJob(ctx, persona, claimed.JobID)
	if result.Status != "lost" {
		t.Fatal("claimed job relaunched", result)
	}
	raw, _ := os.ReadFile(filepath.Join(cfg.Cwd, "effects.txt"))
	if string(raw) != "wait-for-cancel\n" {
		t.Fatal("cancel/crash effects replayed", string(raw))
	}
}

func TestLocalRemoteConnectionReusesHTTPProtocol(t *testing.T) {
	s, core, _ := localTestStore(t)
	s.allowLoopback = true
	server := mcp.NewServer(&mcp.Implementation{Name: "local-http", Version: "1"}, nil)
	server.AddTool(&mcp.Tool{Name: "remember_label", InputSchema: map[string]any{"type": "object"}}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "http result"}}, StructuredContent: map[string]any{"transport": "remote"}}, nil
	})
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer local-remote-secret" {
			t.Error("Local remote credential missing")
			w.WriteHeader(401)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer remote.Close()
	c, e := s.SaveLocal(context.Background(), "", LocalInput{Name: "Local remote", Transport: "https", Endpoint: remote.URL, Enabled: true, BearerToken: "local-remote-secret"})
	if e != nil {
		t.Fatal(e)
	}
	j := localJob(t, s, core, c.ID, "call", map[string]any{})
	if e = NewRunner(s, core.Store()).Tick(context.Background()); e != nil {
		t.Fatal(e)
	}
	result, e := core.Store().GetJob(context.Background(), persona, j.JobID)
	if e != nil || result.Status != "done" || result.Result["outcome"] != "returned" {
		t.Fatal("Local remote result", result, e)
	}
	var ciphertext []byte
	if e = s.pool.QueryRow(context.Background(), `SELECT configuration_ciphertext FROM local_mcp_connections WHERE connection_id=$1`, c.ID).Scan(&ciphertext); e != nil || bytes.Contains(ciphertext, []byte("local-remote-secret")) {
		t.Fatal("unencrypted configuration", e)
	}
}
