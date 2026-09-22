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
	// readStatus reads one job's current status. Only a fault test replaces
	// it, to inject a transient read failure; nothing else substitutes the
	// real store. A failed read is never evidence that a job stopped.
	readStatus func(ctx context.Context, personaID, jobID string) (string, error)
}

func NewRunner(s *Store, core *agentstate.Store) *Runner {
	r := &Runner{Store: s, Core: core, ID: "mcp:" + uuid.NewString()}
	r.readStatus = func(ctx context.Context, personaID, jobID string) (string, error) {
		j, e := core.GetJob(ctx, personaID, jobID)
		return j.Status, e
	}
	return r
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
	if _, e := r.Core.SweepExpiredJobs(ctx, []string{r.Store.jobKind()}, 64); e != nil {
		return e
	}
	personas, e := r.Core.PersonasWithRunnableJobs(ctx, []string{r.Store.jobKind()}, 1, r.cursor)
	if e != nil {
		return e
	}
	for _, persona := range personas {
		r.cursor = persona
		jobs, _, e := r.Core.ClaimJobs(ctx, persona, r.ID, []string{r.Store.jobKind()}, 2*time.Minute, 1, "*")
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
	cfg, e := r.Store.configuration(ctx, tx, job.PersonaID, fmt.Sprint(job.Request["connection_id"]), fmt.Sprint(job.Request["version"]))
	if e != nil {
		return result, "MCP connection unavailable or permission revoked"
	}
	secret := cfg.BearerToken
	// Every configured value that must never be persisted, in one list. The
	// traversal that removes them also normalizes NUL, which jsonb cannot
	// store — that normalization is unconditional, because whether a
	// connection happens to carry a bearer, arguments or environment values
	// says nothing about whether the server's answer contains a NUL.
	secrets := append([]string{secret}, cfg.Args...)
	for _, value := range cfg.Env {
		secrets = append(secrets, value)
	}
	defer func() {
		result = persist(normalize(result), secrets)
		result = boundResult(result)
	}()
	// Do not dispatch a job cancelled after it was claimed. Nothing has been
	// sent yet, so an unreadable status refuses rather than guesses — and it
	// is reported as what it is, not as a cancellation the person never made.
	status, e := r.readStatus(ctx, job.PersonaID, job.JobID)
	if e != nil {
		return result, "MCP job status could not be read before dispatch; nothing was sent"
	}
	if status != "running" {
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
				// Once admitted, only a definite stopped job cancels the call.
				// A failed status read is a fact about the database, not about
				// the person's intent: reporting it as cancellation would turn
				// a healthy call into a false indeterminate outcome. The 30s
				// deadline already bounds a call whose status cannot be read.
				status, e := r.readStatus(ctx, job.PersonaID, job.JobID)
				if e == nil && status != "running" {
					cancel()
					return
				}
			}
		}
	}()
	var wire mcp.Transport
	if cfg.Transport == "stdio" {
		if r.Store.local == nil {
			return result, "Local MCP transport unavailable"
		}
		var closeProcess func()
		wire, closeProcess, e = startStdio(ctx, cfg)
		if e != nil {
			return result, "Local MCP process could not start"
		}
		defer closeProcess()
		result["server_started"] = true
	} else {
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
		wire = &mcp.StreamableClientTransport{Endpoint: cfg.Endpoint, HTTPClient: client, MaxRetries: -1}
	}
	var mu sync.Mutex
	notes := []map[string]any{}
	dropped := 0
	// Notifications are expendable, and a noisy server must not grow this
	// without bound: the count and each message are capped. What was dropped
	// is counted, so a shortened list never reads as the whole story.
	note := func(v map[string]any) {
		mu.Lock()
		defer mu.Unlock()
		if len(notes) < maxNotifications {
			notes = append(notes, v)
			return
		}
		dropped++
	}
	opts := &mcp.ClientOptions{Capabilities: &mcp.ClientCapabilities{}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), ToolListChangedHandler: func(context.Context, *mcp.ToolListChangedRequest) { note(map[string]any{"type": "tools/list_changed"}) }, ProgressNotificationHandler: func(_ context.Context, req *mcp.ProgressNotificationClientRequest) {
		note(map[string]any{"type": "progress", "progress": req.Params.Progress, "total": req.Params.Total, "message": boundRunes(req.Params.Message, notificationMessageRunes)})
	}}
	session, e := mcp.NewClient(&mcp.Implementation{Name: "sumi", Version: "1"}, opts).Connect(ctx, wire, nil)
	if e != nil {
		result["error_detail"] = safeError(e, secrets)
		if cfg.Transport == "stdio" {
			return result, "Local MCP initialization failed; server startup may already have had effects"
		}
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
		if dropped > 0 {
			result["notifications_dropped"] = dropped
		}
		mu.Unlock()
	}()
	result["protocol_version"] = session.InitializeResult().ProtocolVersion
	method, _ := job.Request["method"].(string)
	if method == "list_tools" {
		if problem := discoverTools(ctx, session, job.Request, result, secrets); problem != "" {
			return result, problem
		}
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
			result["error_detail"] = safeError(e, secrets)
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
		result["error_detail"] = safeError(e, secrets)
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

// persist applies the transformation a stored result passes through, and is
// the only place that decides what it covers. Discovery measures its pages
// through this same function, so a page's decision and its durable bytes are
// one decision.
//
// The boundary it draws is between what this package minted and what the
// server said. A result's top-level shape is Sumi's own: those key names, and
// the continuation cursor's value, are written here from a page number, an
// offset and a digest — never from remote bytes. Everything beneath them is
// the server's, and is traversed in full, keys included, because a NUL or a
// credential can sit in a remote key just as easily as in a remote value.
//
// Without that line an ordinary configured value rewrites the result itself: a
// short DEBUG value turns "next_cursor" into a cursor that cannot be read back
// and "call_result" into a key no reader is looking for.
func persist(result map[string]any, secrets []string) map[string]any {
	out := map[string]any{}
	for key, value := range result {
		if key == cursorKey {
			out[key] = value
			continue
		}
		out[key] = scrub(value, secrets)
	}
	return out
}

// scrub walks keys and values, removing every configured secret and always
// replacing NUL. The NUL pass is not conditional on there being a secret:
// jsonb cannot store a NUL, so leaving one in would throw away the record of
// a call that really ran and report it as an indeterminate loss instead.
func scrub(v any, secrets []string) any {
	switch x := v.(type) {
	case string:
		x = strings.ReplaceAll(x, "\x00", "�")
		for _, secret := range secrets {
			if secret != "" {
				x = strings.ReplaceAll(x, secret, "[redacted]")
			}
		}
		return x
	case map[string]any:
		o := map[string]any{}
		for k, v := range x {
			o[scrub(k, secrets).(string)] = scrub(v, secrets)
		}
		return o
	case []any:
		for i := range x {
			x[i] = scrub(x[i], secrets)
		}
		return x
	case []map[string]any:
		for i := range x {
			x[i] = scrub(x[i], secrets).(map[string]any)
		}
		return x
	}
	return v
}

// boundRunes keeps an expendable string small without claiming it is whole.
func boundRunes(s string, limit int) string {
	runes := []rune(s)
	if len(runes) <= limit {
		return s
	}
	return string(runes[:limit]) + "…"
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

func safeError(e error, secrets []string) string {
	return boundRunes(scrub(e.Error(), secrets).(string), 1024)
}

const (
	// resultBoundBytes is the durable size of one MCP job result.
	resultBoundBytes = 60 << 10
	// maxNotifications bounds retained progress/list-changed events.
	maxNotifications = 16
	// notificationMessageRunes bounds one retained progress message.
	notificationMessageRunes = 512
)

// boundResult keeps a durable result inside its size, in the order that loses
// the least: expendable notifications go first, so a complete schema or tool
// result survives. A schema is never truncated into a different schema. When
// even the primary result cannot fit, the small facts that tell the secretary
// what happened and how to continue — the cursor, what was omitted and why —
// are carried through instead of disappearing with it.
func boundResult(result map[string]any) map[string]any {
	b, _ := json.Marshal(result)
	if len(b) > resultBoundBytes && result["notifications"] != nil {
		if notes, ok := result["notifications"].([]any); ok {
			result["notifications_dropped"] = countOf(result["notifications_dropped"]) + len(notes)
		}
		delete(result, "notifications")
		result["notifications_omitted"] = true
		b, _ = json.Marshal(result)
	}
	if len(b) <= resultBoundBytes {
		return result
	}
	out := map[string]any{"result_omitted": true, "reason": "primary MCP result exceeds 60 KiB"}
	for _, key := range []string{
		"dispatched", "outcome", "server_started", "protocol_version",
		"next_cursor", "remaining_on_page", "next_names", "next_names_truncated",
		"names_not_found", "scan_truncated", "page_changed", "tools_omitted",
		"notifications_omitted", "notifications_dropped", "output_schema_valid",
	} {
		if v, ok := result[key]; ok {
			out[key] = v
		}
	}
	if _, wasPage := result["tools"]; wasPage {
		// Discovery settles its page against this same bound, so reaching here
		// means something else grew. The page was not delivered, and a cursor
		// past it would skip schemas nothing named: withdraw the continuation
		// and say the request has to be made again.
		delete(out, cursorKey)
		delete(out, "remaining_on_page")
		out["repeat_request"] = true
	}
	// The carried metadata is itself bounded, but a server-chosen cursor plus
	// omission notes must never push the replacement back over the limit.
	for _, key := range []string{"tools_omitted", "next_names", "names_not_found"} {
		if raw, _ := json.Marshal(out); len(raw) <= resultBoundBytes {
			break
		}
		delete(out, key)
		out["omission_metadata_reduced"] = true
	}
	return out
}

func countOf(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case float64:
		return int(n)
	}
	return 0
}
