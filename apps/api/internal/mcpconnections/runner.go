package mcpconnections

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/publicweb"
)

type Runner struct {
	Store  *Store
	Core   *agentstate.Store
	ID     string
	cursor string
}

func NewRunner(s *Store, core *agentstate.Store) *Runner {
	return &Runner{Store: s, Core: core, ID: "mcp:" + uuid.NewString()}
}
func (r *Runner) Run(ctx context.Context) {
	timer := time.NewTicker(time.Second)
	defer timer.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		_ = r.Tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
}

// Tick claims one bounded call at a time. A crash leaves a claim to expire to
// 'lost', never queued again. Only persistence of an observed result is retried.
func (r *Runner) Tick(ctx context.Context) error {
	if _, e := r.Core.SweepExpiredJobs(ctx, []string{"mcp"}, 64); e != nil {
		return e
	}
	personas, e := r.Core.PersonasWithRunnableJobs(ctx, []string{"mcp"}, 1, r.cursor)
	if e != nil {
		return e
	}
	for _, persona := range personas {
		r.cursor = persona
		jobs, _, e := r.Core.ClaimJobs(ctx, persona, r.ID, []string{"mcp"}, 2*time.Minute, 1, "*")
		if e != nil {
			return e
		}
		for _, job := range jobs {
			result, jobError := r.execute(ctx, job)
			status := "done"
			if jobError != "" {
				status = "failed"
			}
			for i := 0; i < 3; i++ {
				_, e = r.Core.CompleteJob(ctx, persona, job.JobID, r.ID, status, result, jobError)
				if e == nil || errors.Is(e, agentstate.ErrJobConflict) || errors.Is(e, agentstate.ErrJobNotClaimed) {
					break
				}
				if ctx.Err() != nil {
					return ctx.Err()
				}
			}
			if e != nil {
				return e
			}
		}
	}
	return nil
}
func (r *Runner) execute(parent context.Context, job agentstate.Job) (result map[string]any, problem string) {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	result = map[string]any{"dispatched": false}
	// Pin the grant and persona ownership until this bounded operation finishes.
	// Revocation waits for an already executing call; once returned, no queued
	// job or new HTTP dispatch using the old grant can run.
	tx, e := r.Store.pool.Begin(ctx)
	if e != nil {
		return result, "MCP authorization could not be checked"
	}
	defer tx.Rollback(context.Background())
	var human, endpoint string
	var ciphertext []byte
	e = tx.QueryRow(ctx, `SELECT c.human_id,c.endpoint,c.credential_ciphertext FROM mcp_connections c JOIN core_personas p ON p.human_id=c.human_id WHERE p.persona_id=$1 AND p.authority='active' AND c.connection_id=$2 AND c.version=$3 AND c.enabled FOR SHARE OF c,p`, job.PersonaID, job.Request["connection_id"], job.Request["version"]).Scan(&human, &endpoint, &ciphertext)
	if e != nil {
		return result, ErrUnavailable.Error()
	}
	n := r.Store.aead.NonceSize()
	if len(ciphertext) < n {
		return result, "MCP credential unavailable"
	}
	aad, _ := json.Marshal([]string{"sumi.mcp.v1", human, fmt.Sprint(job.Request["connection_id"])})
	raw, e := r.Store.aead.Open(nil, ciphertext[:n], ciphertext[n:], aad)
	if e != nil {
		return result, "MCP credential unavailable"
	}
	secret := string(raw)
	defer clear(raw)
	// Do not dispatch a job cancelled after it was claimed. While executing,
	// cancellation closes the request; the outcome may still be indeterminate.
	current, e := r.Core.GetJob(ctx, job.PersonaID, job.JobID)
	if e != nil || current.Status != "running" {
		return result, "MCP call cancelled before dispatch"
	}
	stopPoll := make(chan struct{})
	defer close(stopPoll)
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stopPoll:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				j, e := r.Core.GetJob(ctx, job.PersonaID, job.JobID)
				if e != nil || j.Status != "running" {
					cancel()
					return
				}
			}
		}
	}()
	transport := &http.Transport{Proxy: nil, DisableCompression: true, MaxResponseHeaderBytes: 32 << 10, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 15 * time.Second}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, e := net.SplitHostPort(address)
		if e != nil {
			return nil, e
		}
		ips, e := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if e != nil || len(ips) == 0 {
			return nil, errors.New("MCP destination could not be resolved")
		}
		for _, ip := range ips {
			if !(r.Store.allowLoopback && ip.IsLoopback()) && !publicweb.IsPublicAddress(ip) {
				return nil, errors.New("MCP destination is not public")
			}
		}
		return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, net.JoinHostPort(ips[0].String(), port))
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: authTransport{base: transport, token: secret}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, Timeout: 30 * time.Second}
	var mu sync.Mutex
	notes := []map[string]any{}
	note := func(v map[string]any) {
		mu.Lock()
		defer mu.Unlock()
		if len(notes) < 16 {
			notes = append(notes, v)
		}
	}
	opts := &mcp.ClientOptions{Capabilities: &mcp.ClientCapabilities{}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), ToolListChangedHandler: func(context.Context, *mcp.ToolListChangedRequest) { note(map[string]any{"type": "tools/list_changed"}) }, ProgressNotificationHandler: func(_ context.Context, req *mcp.ProgressNotificationClientRequest) {
		note(map[string]any{"type": "progress", "progress": req.Params.Progress, "total": req.Params.Total, "message": req.Params.Message})
	}}
	session, e := mcp.NewClient(&mcp.Implementation{Name: "sumi", Version: "1"}, opts).Connect(ctx, &mcp.StreamableClientTransport{Endpoint: endpoint, HTTPClient: client, MaxRetries: -1}, nil)
	if e != nil {
		result["error_detail"] = safeError(e, secret)
		return result, "MCP initialization failed (check endpoint, credential, and supported protocol)"
	}
	defer func() {
		// Close drains SDK notification handlers before taking their snapshot.
		// Capturing first races the final progress event against CallTool.
		_ = session.Close()
		mu.Lock()
		if len(notes) > 0 {
			result["notifications"] = notes
		}
		mu.Unlock()
		result = scrub(result, secret).(map[string]any)
		b, _ := json.Marshal(result)
		if len(b) > 60<<10 {
			result = map[string]any{"result_omitted": true, "reason": "remote result exceeds 60 KiB", "dispatched": result["dispatched"], "outcome": result["outcome"]}
		}
	}()
	result["protocol_version"] = session.InitializeResult().ProtocolVersion
	method, _ := job.Request["method"].(string)
	if method == "list_tools" {
		cursor, _ := job.Request["cursor"].(string)
		list, e := session.ListTools(ctx, &mcp.ListToolsParams{Cursor: cursor})
		if e != nil {
			result["error_detail"] = safeError(e, secret)
			return result, "MCP tools/list failed"
		}
		result["tools"] = list.Tools
		result["next_cursor"] = list.NextCursor
		return normalize(result), ""
	}
	// Fetch the schema at invocation, in the same remote session. Cached
	// annotations never lower authority; external schema refs are not fetched.
	name, _ := job.Request["name"].(string)
	var found *mcp.Tool
	cursor := ""
	seen := map[string]bool{}
	for page := 0; page < 32; page++ {
		list, e := session.ListTools(ctx, &mcp.ListToolsParams{Cursor: cursor})
		if e != nil {
			result["error_detail"] = safeError(e, secret)
			return result, "MCP tools/list failed before dispatch"
		}
		for _, tool := range list.Tools {
			if tool.Name == name {
				found = tool
				break
			}
		}
		if found != nil || list.NextCursor == "" {
			break
		}
		if seen[list.NextCursor] {
			return result, "MCP tools/list returned a repeated cursor"
		}
		cursor = list.NextCursor
		seen[cursor] = true
	}
	if found == nil {
		return result, "MCP tool not found in bounded discovery (32 pages)"
	}
	if e := validateSchema(found.InputSchema, job.Request["arguments"]); e != nil {
		return result, "MCP arguments do not satisfy the remote input schema, or schema is unsupported"
	}
	result["dispatched"] = true
	call, e := session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: job.Request["arguments"], Meta: mcp.Meta{"progressToken": job.JobID}})
	if e != nil {
		result["outcome"] = "indeterminate"
		result["error_detail"] = safeError(e, secret)
		return result, "MCP call did not return a valid result; it may have executed. Inspect the remote state before making a new call"
	}
	result["outcome"] = "returned"
	result["call_result"] = call
	if found.OutputSchema != nil && !call.IsError {
		if e := validateSchema(found.OutputSchema, call.StructuredContent); e != nil {
			result["output_schema_valid"] = false
			return normalize(result), "MCP returned output that does not satisfy its schema; the tool already executed"
		}
		result["output_schema_valid"] = true
	}
	if call.IsError {
		return normalize(result), "MCP tool reported an error; inspect call_result"
	}
	return normalize(result), ""
}
func validateSchema(schema, value any) error {
	b, e := json.Marshal(schema)
	if e != nil {
		return e
	}
	var s jsonschema.Schema
	if e = json.Unmarshal(b, &s); e != nil {
		return e
	}
	resolved, e := s.Resolve(nil)
	if e != nil {
		return e
	}
	return resolved.Validate(value)
}
func normalize(v map[string]any) map[string]any {
	b, _ := json.Marshal(v)
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	return out
}
func scrub(v any, secret string) any {
	switch x := v.(type) {
	case string:
		x = strings.ReplaceAll(x, "\x00", "�")
		if secret != "" {
			x = strings.ReplaceAll(x, secret, "[redacted]")
		}
		return x
	case map[string]any:
		o := map[string]any{}
		for k, v := range x {
			o[scrub(k, secret).(string)] = scrub(v, secret)
		}
		return o
	case []any:
		for i := range x {
			x[i] = scrub(x[i], secret)
		}
		return x
	case []map[string]any:
		for i := range x {
			x[i] = scrub(x[i], secret).(map[string]any)
		}
		return x
	}
	return v
}

type authTransport struct {
	base  http.RoundTripper
	token string
}

func (t authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r := req.Clone(req.Context())
	r.Header = req.Header.Clone()
	if t.token != "" {
		r.Header.Set("Authorization", "Bearer "+t.token)
	}
	resp, e := t.base.RoundTrip(r)
	if e == nil {
		resp.Body = &boundedBody{Reader: io.LimitReader(resp.Body, 2<<20), Closer: resp.Body}
	}
	return resp, e
}

type boundedBody struct {
	io.Reader
	io.Closer
}

func safeError(e error, secret string) string {
	s := scrub(e.Error(), secret).(string)
	if len(s) > 1024 {
		s = s[:1024] + "…"
	}
	return s
}
