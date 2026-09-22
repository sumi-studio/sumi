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
