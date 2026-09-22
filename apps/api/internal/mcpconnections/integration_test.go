package mcpconnections

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/chatgpt"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

const owner = "0198f0f4-9b72-7000-8000-000000000201"
const other = "0198f0f4-9b72-7000-8000-000000000202"
const persona = "0198f0f4-9b72-7000-8000-000000000203"
const secret = "mcp-secret-write-only-903"

type fixture struct {
	store  *Store
	core   *agentstate.Server
	server *httptest.Server
	runner *Runner
	t      *testing.T
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
	store, e := New(pool, bytes.Repeat([]byte{8}, 32))
	if e != nil {
		t.Fatal(e)
	}
	store.allowLoopback = true
	core := agentstate.NewServer(pool, "mcp-fixture-admin-token-long")
	if _, _, e := core.Store().EnsurePersona(ctx, persona, strptr(owner), "MCP secretary"); e != nil {
		t.Fatal(e)
	}
	for name, effect := range store.Effects() {
		if e := core.RegisterToolEffect(name, effect); e != nil {
			t.Fatal(e)
		}
	}
	mux := http.NewServeMux()
	core.RegisterRoutes(mux)
	(&Service{Store: store, Authenticate: func(r *http.Request) (chatgpt.LoginIdentity, error) {
		human := r.Header.Get("Test-Human")
		if human != owner && human != other {
			return chatgpt.LoginIdentity{}, fmt.Errorf("unauthenticated")
		}
		return chatgpt.LoginIdentity{HumanID: human, Authorize: func(ctx context.Context, f func(context.Context) error) error { return f(ctx) }}, nil
	}}).RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return &fixture{store, core, server, NewRunner(store, core.Store()), t}
}
func strptr(s string) *string { return &s }
func (f *fixture) api(method, path, human string, body any) (int, []byte) {
	f.t.Helper()
	raw, _ := json.Marshal(body)
	req, e := http.NewRequest(method, f.server.URL+path, bytes.NewReader(raw))
	if e != nil {
		f.t.Fatal(e)
	}
	req.Header.Set("Test-Human", human)
	resp, e := http.DefaultClient.Do(req)
	if e != nil {
		f.t.Fatal(e)
	}
	defer resp.Body.Close()
	var b bytes.Buffer
	b.ReadFrom(resp.Body)
	return resp.StatusCode, b.Bytes()
}
func (f *fixture) save(endpoint string) Connection {
	f.t.Helper()
	status, b := f.api("POST", "/api/mcp-connections", owner, Input{Name: "Remote fixture", Endpoint: endpoint, Enabled: true, BearerToken: secret})
	if status != 200 {
		f.t.Fatalf("save %d %s", status, b)
	}
	if bytes.Contains(b, []byte(secret)) {
		f.t.Fatal("credential leaked")
	}
	var c Connection
	json.Unmarshal(b, &c)
	return c
}
func (f *fixture) coreCall(tool string, request map[string]any) string {
	f.t.Helper()
	ctx := context.Background()
	if _, _, e := f.core.Store().SubmitInput(ctx, &agentstate.Input{PersonaID: persona, InputID: uuid.NewString(), Kind: "message", Payload: map[string]any{"text": "MCP acceptance " + tool}, ActorKind: "human", ActorID: owner, SourceSurface: "test", Attention: "reply"}); e != nil {
		f.t.Fatal(e)
	}
	b, _ := json.Marshal(map[string]any{"tool": tool, "request": request})
	return f.child(string(b))
}
func (f *fixture) child(call string) string {
	f.t.Helper()
	script, e := filepath.Abs("../../../core/scripts/mcp-acceptance-child.mjs")
	if e != nil {
		f.t.Fatal(e)
	}
	cmd := exec.Command("node", script)
	cmd.Env = append(os.Environ(), "MCP_TEST_URL="+f.server.URL, "MCP_TEST_PERSONA="+persona, "MCP_TEST_TOKEN="+f.core.PersonaToken(persona), "MCP_TEST_CALL="+call)
	out, e := cmd.CombinedOutput()
	if e != nil {
		f.t.Fatalf("Core child: %v\n%s", e, out)
	}
	if bytes.Contains(out, []byte(secret)) {
		f.t.Fatalf("credential leaked to model: %s", out)
	}
	return string(out)
}
func (f *fixture) job() agentstate.Job {
	f.t.Helper()
	jobs, e := f.core.Store().ListJobs(context.Background(), persona, nil, 1)
	if e != nil || len(jobs) != 1 {
		f.t.Fatalf("jobs %v %v", jobs, e)
	}
	return jobs[0]
}
func (f *fixture) tick() {
	f.t.Helper()
	if e := f.runner.Tick(context.Background()); e != nil {
		f.t.Fatal(e)
	}
}
func (f *fixture) status(job agentstate.Job) string {
	f.child("null")
	return f.coreCall("job.status", map[string]any{"job_id": job.JobID})
}
func TestAPIThroughSecretaryToRemoteMCP(t *testing.T) {
	f := setup(t)
	var calls, authRequests, initialized, sessionRequests atomic.Int32
	remote := mcp.NewServer(&mcp.Implementation{Name: "acceptance", Version: "1"}, nil)
	remote.AddTool(&mcp.Tool{Name: "record", Description: "Record a label", InputSchema: map[string]any{"type": "object", "properties": map[string]any{"label": map[string]any{"type": "string"}}, "required": []string{"label"}, "additionalProperties": false}, OutputSchema: map[string]any{"type": "object", "properties": map[string]any{"label": map[string]any{"type": "string"}}, "required": []string{"label"}}}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		calls.Add(1)
		var args map[string]any
		json.Unmarshal(req.Params.Arguments, &args)
		_ = req.Session.NotifyProgress(ctx, &mcp.ProgressNotificationParams{ProgressToken: req.Params.Meta["progressToken"], Progress: 1, Total: 1, Message: "recorded"})
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "saved " + args["label"].(string) + " " + secret}}, StructuredContent: map[string]any{"label": args["label"]}}, nil
	})
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return remote }, nil)
	httpRemote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+secret {
			t.Error("missing server credential")
			w.WriteHeader(401)
			return
		}
		authRequests.Add(1)
		if r.Method == "POST" && r.Header.Get("Mcp-Session-Id") == "" {
			initialized.Add(1)
		}
		if r.Header.Get("Mcp-Session-Id") != "" && r.Header.Get("Mcp-Protocol-Version") != "" {
			sessionRequests.Add(1)
		}
		handler.ServeHTTP(w, r)
	}))
	defer httpRemote.Close()
	c := f.save(httpRemote.URL)
	status, b := f.api("GET", "/api/mcp-connections", other, nil)
	if status != 200 || string(bytes.TrimSpace(b)) != "[]" {
		t.Fatalf("ownership %d %s", status, b)
	}
	status, _ = f.api("DELETE", "/api/mcp-connections/"+c.ID, other, nil)
	if status != 404 {
		t.Fatalf("foreign delete %d", status)
	}
	status, _ = f.api("GET", "/api/mcp-connections", "", nil)
	if status != 401 {
		t.Fatalf("auth %d", status)
	}
	out := f.coreCall("mcp.connections", map[string]any{})
	if !strings.Contains(out, c.ID) {
		t.Fatal("connection not visible to secretary")
	}
	f.coreCall("mcp.list_tools", map[string]any{"connection_id": c.ID})
	f.tick()
	j := f.job()
	if j.Status != "done" {
		t.Fatalf("list %+v", j)
	}
	out = f.status(j)
	if !strings.Contains(out, "inputSchema") || !strings.Contains(out, "record") {
		t.Fatal("discovered schema did not reach Core", out)
	}
	f.coreCall("mcp.call", map[string]any{"connection_id": c.ID, "name": "record", "arguments": map[string]any{"label": "actual-remote-value"}})
	f.tick()
	j = f.job()
	if j.Status != "done" {
		t.Fatalf("call %+v", j)
	}
	out = f.status(j)
	if !strings.Contains(out, "actual-remote-value") || !strings.Contains(out, "structuredContent") || !strings.Contains(out, "[redacted]") || !strings.Contains(out, "progress") {
		t.Fatal("remote result/notification missing from Core", out)
	}
	if calls.Load() != 1 || authRequests.Load() < 4 || initialized.Load() < 2 || sessionRequests.Load() < 2 {
		t.Fatalf("protocol lifecycle %d %d %d %d", calls.Load(), authRequests.Load(), initialized.Load(), sessionRequests.Load())
	}
	// Re-reading a receipt or running the worker again cannot repeat the call.
	f.tick()
	if calls.Load() != 1 {
		t.Fatal("call replayed")
	}
	f.coreCall("mcp.call", map[string]any{"connection_id": c.ID, "name": "record", "arguments": map[string]any{"label": 123}})
	f.tick()
	j = f.job()
	if j.Status != "failed" || j.Result["dispatched"] != false || calls.Load() != 1 {
		t.Fatalf("schema validation %+v", j)
	}
	f.child("null")
	// Revocation after admission but before execution is authoritative.
	f.coreCall("mcp.call", map[string]any{"connection_id": c.ID, "name": "record", "arguments": map[string]any{"label": "must-not-run"}})
	status, _ = f.api("DELETE", "/api/mcp-connections/"+c.ID, owner, nil)
	if status != 200 {
		t.Fatal(status)
	}
	f.tick()
	j = f.job()
	if j.Status != "failed" || calls.Load() != 1 || j.Result["dispatched"] != false {
		t.Fatalf("revocation %+v", j)
	}
	var leak bool
	if e := f.store.pool.QueryRow(context.Background(), `SELECT EXISTS(SELECT 1 FROM core_jobs WHERE request::text LIKE $1 OR result::text LIKE $1)`, "%"+secret+"%").Scan(&leak); e != nil || leak {
		t.Fatal("credential in ledger", e)
	}
	t.Log("authenticated connection API -> TypeScript Secretary -> real state HTTP API -> durable PostgreSQL job -> Streamable HTTP MCP -> progress/structured result -> job.status -> Secretary verified")
}

func TestResponseLossIsIndeterminateAndNeverRetried(t *testing.T) {
	f := setup(t)
	var calls atomic.Int32
	remote := mcp.NewServer(&mcp.Implementation{Name: "loss", Version: "1"}, nil)
	remote.AddTool(&mcp.Tool{Name: "mutate", InputSchema: map[string]any{"type": "object"}}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		calls.Add(1)
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "changed"}}}, nil
	})
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return remote }, &mcp.StreamableHTTPOptions{JSONResponse: true})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			body, _ := io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewReader(body))
			if bytes.Contains(body, []byte(`"tools/call"`)) {
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, r)
				conn, _, e := w.(http.Hijacker).Hijack()
				if e == nil {
					conn.Close()
				}
				return
			}
		}
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	c := f.save(server.URL)
	f.coreCall("mcp.call", map[string]any{"connection_id": c.ID, "name": "mutate", "arguments": map[string]any{}})
	f.tick()
	j := f.job()
	if calls.Load() != 1 || j.Status != "failed" || j.Result["outcome"] != "indeterminate" || j.Result["dispatched"] != true {
		t.Fatalf("lost response %+v calls=%d", j, calls.Load())
	}
	f.runner = NewRunner(f.store, f.core.Store())
	f.tick()
	if calls.Load() != 1 {
		t.Fatal("worker restart retried remote mutation")
	}
	out := f.status(j)
	if !strings.Contains(out, "may have executed") || !strings.Contains(out, "indeterminate") {
		t.Fatal("uncertainty missing in Core", out)
	}
}

func TestPaginationGrantVersionAndCrashRecovery(t *testing.T) {
	f := setup(t)
	var calls atomic.Int32
	remote := mcp.NewServer(&mcp.Implementation{Name: "pages", Version: "1"}, &mcp.ServerOptions{PageSize: 1})
	for _, name := range []string{"a", "z"} {
		remote.AddTool(&mcp.Tool{Name: name, InputSchema: map[string]any{"type": "object"}}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			calls.Add(1)
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "done"}}}, nil
		})
	}
	server := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return remote }, &mcp.StreamableHTTPOptions{JSONResponse: true}))
	defer server.Close()
	c := f.save(server.URL)
	f.coreCall("mcp.list_tools", map[string]any{"connection_id": c.ID})
	f.tick()
	j := f.job()
	cursor, _ := j.Result["next_cursor"].(string)
	if cursor == "" {
		t.Fatalf("missing cursor %+v", j)
	}
	f.child("null")
	f.coreCall("mcp.list_tools", map[string]any{"connection_id": c.ID, "cursor": cursor})
	f.tick()
	j = f.job()
	b, _ := json.Marshal(j.Result)
	if !bytes.Contains(b, []byte(`"name":"z"`)) {
		t.Fatal("second page", string(b))
	}
	f.child("null")
	f.coreCall("mcp.call", map[string]any{"connection_id": c.ID, "name": "z", "arguments": map[string]any{}})
	f.tick()
	j = f.job()
	if j.Status != "done" || calls.Load() != 1 {
		t.Fatalf("lookup through pages %+v", j)
	}
	f.child("null")
	f.coreCall("mcp.call", map[string]any{"connection_id": c.ID, "name": "z", "arguments": map[string]any{}})
	status, _ := f.api("PUT", "/api/mcp-connections/"+c.ID, owner, Input{Name: c.Name, Endpoint: c.Endpoint, Enabled: true, BearerToken: "rotated"})
	if status != 200 {
		t.Fatal(status)
	}
	f.tick()
	j = f.job()
	if j.Status != "failed" || calls.Load() != 1 {
		t.Fatalf("changed grant was used %+v", j)
	}
	f.child("null")
	f.coreCall("mcp.call", map[string]any{"connection_id": c.ID, "name": "z", "arguments": map[string]any{}})
	jobs, _, e := f.core.Store().ClaimJobs(context.Background(), persona, "dead-worker", []string{"mcp"}, time.Minute, 1, "*")
	if e != nil || len(jobs) != 1 {
		t.Fatal(e)
	}
	_, e = f.store.pool.Exec(context.Background(), `UPDATE core_jobs SET claim_expires_at=now()-interval '1 second' WHERE persona_id=$1 AND job_id=$2`, persona, jobs[0].JobID)
	if e != nil {
		t.Fatal(e)
	}
	f.tick()
	j = f.job()
	if j.Status != "lost" || calls.Load() != 1 {
		t.Fatalf("crashed claim replayed %+v", j)
	}
	var count int
	if e := f.store.pool.QueryRow(context.Background(), `SELECT count(*) FROM core_inputs WHERE persona_id=$1 AND input_id=$2`, persona, "job:"+j.JobID).Scan(&count); e != nil || count != 1 {
		t.Fatal("notification", count, e)
	}
}

func TestProtocolRejectionAndPublicDestinationBoundary(t *testing.T) {
	f := setup(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var message map[string]any
		json.NewDecoder(r.Body).Decode(&message)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": message["id"], "result": map[string]any{"protocolVersion": "unsupported-future-version", "capabilities": map[string]any{}, "serverInfo": map[string]any{"name": "bad", "version": "1"}}})
	}))
	defer server.Close()
	c := f.save(server.URL)
	f.coreCall("mcp.list_tools", map[string]any{"connection_id": c.ID})
	f.tick()
	j := f.job()
	if j.Status != "failed" || j.Result["dispatched"] != false {
		t.Fatalf("unsupported protocol %+v", j)
	}
	f.store.allowLoopback = false
	if _, e := f.store.Save(context.Background(), owner, "", Input{Name: "private", Endpoint: server.URL}); !errors.Is(e, ErrInvalid) {
		t.Fatal("http endpoint admitted", e)
	}
	if e := validateSchema(map[string]any{"$ref": "https://127.0.0.1/schema"}, map[string]any{}); e == nil {
		t.Fatal("remote schema resolution allowed")
	}
	// Stored endpoint cannot reach a private network even if configuration was
	// created by a fixture/import: the production dialer checks resolved IPs.
	f.child("null")
	f.coreCall("mcp.list_tools", map[string]any{"connection_id": c.ID})
	f.tick()
	j = f.job()
	if j.Status != "failed" || j.Result["dispatched"] != false {
		t.Fatalf("private destination reached %+v", j)
	}
}

func TestRemoteToolErrorsAndInvalidOutputRemainVisible(t *testing.T) {
	f := setup(t)
	remote := mcp.NewServer(&mcp.Implementation{Name: "errors", Version: "1"}, nil)
	remote.AddTool(&mcp.Tool{Name: "error", InputSchema: map[string]any{"type": "object"}}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: "Remote quota exhausted " + secret}}}, nil
	})
	remote.AddTool(&mcp.Tool{Name: "bad_output", InputSchema: map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object", "required": []string{"value"}}}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{StructuredContent: map[string]any{"other": "already-executed"}, Content: []mcp.Content{&mcp.TextContent{Text: "wrong shape"}}}, nil
	})
	server := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return remote }, &mcp.StreamableHTTPOptions{JSONResponse: true}))
	defer server.Close()
	c := f.save(server.URL)
	var ciphertext []byte
	if e := f.store.pool.QueryRow(context.Background(), `SELECT credential_ciphertext FROM mcp_connections WHERE human_id=$1 AND connection_id=$2`, owner, c.ID).Scan(&ciphertext); e != nil || bytes.Contains(ciphertext, []byte(secret)) {
		t.Fatal("credential sealing", e)
	}
	for _, name := range []string{"error", "bad_output"} {
		f.coreCall("mcp.call", map[string]any{"connection_id": c.ID, "name": name, "arguments": map[string]any{}})
		f.tick()
		j := f.job()
		if j.Status != "failed" || j.Result["outcome"] != "returned" {
			t.Fatalf("remote failure %+v", j)
		}
		out := f.status(j)
		if name == "error" && !strings.Contains(out, "Remote quota exhausted [redacted]") {
			t.Fatal("remote error lost")
		}
		if name == "bad_output" && j.Result["output_schema_valid"] != false {
			t.Fatal("bad output accepted")
		}
	}
}

// Notifications are expendable, but what was dropped must be visible: a
// shortened list that reads as the whole story is a quieter kind of loss.
func TestNoisyProgressDoesNotOmitSmallPrimaryResult(t *testing.T) {
	f := setup(t)
	remote := mcp.NewServer(&mcp.Implementation{Name: "noisy-progress", Version: "1"}, nil)
	noisy := func(count int, text string) func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return func(ctx context.Context, r *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			for i := 0; i < count; i++ {
				_ = r.Session.NotifyProgress(ctx, &mcp.ProgressNotificationParams{ProgressToken: r.Params.Meta["progressToken"], Progress: float64(i), Total: float64(count), Message: strings.Repeat("x", 8192)})
			}
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: text}}, StructuredContent: map[string]any{"value": "retained"}}, nil
		}
	}
	for _, name := range []string{"small_result", "very_noisy"} {
		remote.AddTool(&mcp.Tool{Name: name, InputSchema: map[string]any{"type": "object"}}, noisy(map[string]int{"small_result": 16, "very_noisy": 64}[name], "actual small result"))
	}
	remote.AddTool(&mcp.Tool{Name: "huge_result", InputSchema: map[string]any{"type": "object"}}, noisy(16, strings.Repeat("h", 120<<10)))
	remoteHTTP := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return remote }, nil))
	defer remoteHTTP.Close()
	c := f.save(remoteHTTP.URL)
	call := func(name string) agentstate.Job {
		t.Helper()
		f.child("null")
		f.coreCall("mcp.call", map[string]any{"connection_id": c.ID, "name": name, "arguments": map[string]any{}})
		f.tick()
		job := f.job()
		if raw, _ := json.Marshal(job.Result); len(raw) > resultBoundBytes {
			t.Fatalf("%s exceeded the durable bound: %d bytes", name, len(raw))
		}
		return job
	}
	notes := func(job agentstate.Job) []any {
		list, _ := job.Result["notifications"].([]any)
		for _, entry := range list {
			message, _ := entry.(map[string]any)["message"].(string)
			if len([]rune(message)) > notificationMessageRunes+1 {
				t.Fatalf("unbounded notification message: %d runes", len([]rune(message)))
			}
		}
		return list
	}

	job := call("small_result")
	raw, _ := json.Marshal(job.Result)
	if job.Status != "done" || job.Result["result_omitted"] == true || !bytes.Contains(raw, []byte("retained")) || !bytes.Contains(raw, []byte("actual small result")) {
		t.Fatalf("notifications displaced primary result: %+v", job)
	}
	if len(notes(job)) == 0 || job.Result["notifications_dropped"] != nil || job.Result["notifications_omitted"] != nil {
		t.Fatalf("bounded notifications were shed anyway: %+v", job.Result)
	}

	job = call("very_noisy")
	raw, _ = json.Marshal(job.Result)
	if job.Status != "done" || !bytes.Contains(raw, []byte("actual small result")) {
		t.Fatalf("very noisy call lost its primary result: %+v", job)
	}
	dropped, _ := job.Result["notifications_dropped"].(float64)
	if len(notes(job)) != maxNotifications || dropped < 1 {
		t.Fatalf("shedding at the cap is not visible: %+v", job.Result)
	}

	// Even when the primary result itself cannot be stored, what happened and
	// what was dropped survive in its place.
	job = call("huge_result")
	if job.Status != "done" || job.Result["result_omitted"] != true || job.Result["dispatched"] != true || job.Result["outcome"] != "returned" {
		t.Fatalf("oversized result lost its account of itself: %+v", job.Result)
	}
	dropped, _ = job.Result["notifications_dropped"].(float64)
	if job.Result["notifications"] != nil || job.Result["notifications_omitted"] != true || dropped < 1 {
		t.Fatalf("notifications vanished silently with the result: %+v", job.Result)
	}
}

// jsonb cannot store a NUL. Whether a connection happens to carry a bearer
// token, arguments or environment values says nothing about whether the
// server's answer contains one, so the normalization cannot depend on it.
func TestNULBytesAreNormalizedWithoutAnyConfiguredSecret(t *testing.T) {
	f := setup(t)
	remote := mcp.NewServer(&mcp.Implementation{Name: "nul-bytes", Version: "1"}, nil)
	remote.AddTool(&mcp.Tool{Name: "nul_bytes", InputSchema: map[string]any{"type": "object"}}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{
			Content:           []mcp.Content{&mcp.TextContent{Text: "before\x00after"}},
			StructuredContent: map[string]any{"nested": map[string]any{"key\x00in-map": []any{"value\x00here"}}},
		}, nil
	})
	server := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return remote }, &mcp.StreamableHTTPOptions{JSONResponse: true}))
	defer server.Close()
	// Saved without a credential: this connection has nothing to redact.
	c, e := f.store.Save(context.Background(), owner, "", Input{Name: "No credential fixture", Endpoint: server.URL, Enabled: true})
	if e != nil {
		t.Fatal(e)
	}
	f.coreCall("mcp.call", map[string]any{"connection_id": c.ID, "name": "nul_bytes", "arguments": map[string]any{}})
	f.tick()
	job := f.job()
	if job.Status != "done" || job.Result["outcome"] != "returned" {
		t.Fatalf("a call that really ran was recorded as a loss: %+v", job)
	}
	raw, _ := json.Marshal(job.Result)
	if bytes.Contains(raw, []byte(`\u0000`)) {
		t.Fatalf("NUL survived into the durable result: %s", raw)
	}
	for _, want := range []string{"before�after", "key�in-map", "value�here"} {
		if !bytes.Contains(raw, []byte(want)) {
			t.Fatalf("%q was not normalized in place: %s", want, raw)
		}
	}
}

// A status read that fails is a fact about the database, never evidence that
// the person stopped the job.
func TestStatusReadFailureNeitherDispatchesNorCancels(t *testing.T) {
	f := setup(t)
	var calls atomic.Int32
	remote := mcp.NewServer(&mcp.Implementation{Name: "slow", Version: "1"}, nil)
	remote.AddTool(&mcp.Tool{Name: "slow", InputSchema: map[string]any{"type": "object"}}, func(ctx context.Context, r *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		calls.Add(1)
		select {
		case <-time.After(3500 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "completed"}}}, nil
	})
	server := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return remote }, &mcp.StreamableHTTPOptions{JSONResponse: true}))
	defer server.Close()
	c := f.save(server.URL)
	trusted := f.runner.readStatus
	request := map[string]any{"connection_id": c.ID, "name": "slow", "arguments": map[string]any{}}
	failure := errors.New("injected transient status read failure")
	run := func() agentstate.Job {
		t.Helper()
		f.child("null")
		f.coreCall("mcp.call", request)
		f.tick()
		return f.job()
	}
	reason := func(job agentstate.Job) string {
		if job.Error == nil {
			return ""
		}
		return *job.Error
	}

	// Before dispatch nothing has been sent, so an unreadable status refuses —
	// and says what it is, rather than a cancellation the person never made.
	f.runner.readStatus = func(context.Context, string, string) (string, error) { return "", failure }
	job := run()
	if job.Status != "failed" || job.Result["dispatched"] != false || calls.Load() != 0 || !strings.Contains(reason(job), "could not be read") {
		t.Fatalf("unreadable status before dispatch: %+v %q", job.Result, reason(job))
	}

	// A real cancellation, read through the real store, still stops dispatch.
	var reads atomic.Int32
	f.runner.readStatus = func(ctx context.Context, personaID, jobID string) (string, error) {
		if reads.Add(1) == 1 {
			if _, e := f.core.Store().CancelJob(ctx, personaID, jobID); e != nil {
				t.Error(e)
			}
		}
		return trusted(ctx, personaID, jobID)
	}
	job = run()
	if job.Status != "failed" || job.Result["dispatched"] != false || calls.Load() != 0 || !strings.Contains(reason(job), "cancelled before dispatch") {
		t.Fatalf("cancelled before dispatch: %+v %q", job.Result, reason(job))
	}

	// Once admitted, a failing read must not cancel a healthy call.
	reads.Store(0)
	f.runner.readStatus = func(ctx context.Context, personaID, jobID string) (string, error) {
		if reads.Add(1) > 1 {
			return "", failure
		}
		return trusted(ctx, personaID, jobID)
	}
	job = run()
	if job.Status != "done" || job.Result["outcome"] != "returned" || calls.Load() != 1 {
		t.Fatalf("transient read failure cancelled a running call: %+v %q", job.Result, reason(job))
	}
	if reads.Load() < 3 {
		t.Fatalf("the poller never read status during the call: %d", reads.Load())
	}
}
