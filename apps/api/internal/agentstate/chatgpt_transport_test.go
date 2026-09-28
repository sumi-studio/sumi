package agentstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/modelconnections"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

type chatGPTFixture struct {
	s                     *Server
	conns                 *modelconnections.Store
	human, persona, token string
	conn                  modelconnections.Connection
	version               string
	api                   *httptest.Server
	refreshes             atomic.Int32
	beforeRefresh         func()
	revoked               atomic.Bool
}

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func chatGPTTransportFixture(t *testing.T, upstream http.HandlerFunc) *chatGPTFixture {
	t.Helper()
	ctx := context.Background()
	pool := testdb.Create(t)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	f := &chatGPTFixture{s: NewServer(pool, testAdminSecret)}
	issuer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/accounts/deviceauth/usercode":
			io.WriteString(w, `{"device_auth_id":"synthetic-device","user_code":"SYN-1","interval":"1"}`)
		case "/api/accounts/deviceauth/token":
			io.WriteString(w, `{"authorization_code":"synthetic-code","code_verifier":"synthetic-verifier"}`)
		case "/oauth/token":
			if strings.Contains(r.Header.Get("Content-Type"), "json") {
				f.refreshes.Add(1)
				if f.beforeRefresh != nil {
					f.beforeRefresh()
				}
				if f.revoked.Load() {
					w.WriteHeader(401)
					io.WriteString(w, `{"error":"invalid_grant"}`)
					return
				}
			}
			json.NewEncoder(w).Encode(map[string]string{
				"access_token":  fakeJWT("synthetic-account", time.Now().Add(time.Hour), fmt.Sprint(f.refreshes.Load())),
				"refresh_token": fmt.Sprint("synthetic-refresh-", f.refreshes.Load()),
			})
		default:
			t.Errorf("unexpected issuer path: %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(issuer.Close)
	var err error
	f.conns, err = modelconnections.New(pool, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	oauth := modelconnections.NewOAuthClient()
	oauth.Issuer, oauth.Now = issuer.URL, func() time.Time { return now }
	if err := f.conns.EnableChatGPT(oauth); err != nil {
		t.Fatal(err)
	}
	f.s.SetModelConnections(f.conns)
	f.human, f.persona = mustHuman(t, pool), pid(t)
	if _, _, err := f.s.store.EnsurePersona(ctx, f.persona, &f.human, "transport"); err != nil {
		t.Fatal(err)
	}
	login, err := f.conns.BeginChatGPTLogin(ctx, f.human, "synthetic-session", "", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	done, err := f.conns.PollChatGPTLogin(ctx, f.human, "synthetic-session", login.LoginID)
	if err != nil || done.Connection == nil {
		t.Fatalf("synthetic login: %+v %v", done, err)
	}
	f.conn = *done.Connection
	meta, err := f.conns.Describe(ctx, f.human, f.conn.ID)
	if err != nil {
		t.Fatal(err)
	}
	f.version = meta.Version
	f.token = f.s.PersonaToken(f.persona)
	backend := httptest.NewServer(upstream)
	t.Cleanup(backend.Close)
	target, _ := url.Parse(backend.URL)
	f.s.chatGPTHTTP = &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.String() != chatGPTResponsesURL {
			t.Errorf("non-fixed upstream: %s", r.URL)
			return nil, errors.New("non-fixed upstream")
		}
		clone := r.Clone(r.Context())
		u := *r.URL
		u.Scheme, u.Host = target.Scheme, target.Host
		clone.URL = &u
		return backend.Client().Transport.RoundTrip(clone)
	})}
	mux := http.NewServeMux()
	f.s.RegisterRoutes(mux)
	f.api = httptest.NewServer(mux)
	t.Cleanup(f.api.Close)
	return f
}

const transportBody = `{"model":"gpt-6-astra","stream":true,"store":false,"reasoning":{"effort":"medium","context":"all_turns"},"input":[{"role":"user","content":"synthetic prompt"}]}`
const completedStream = "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"done\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n"

func (f *chatGPTFixture) request() chatGPTTransportRequest {
	return chatGPTTransportRequest{ConnectionID: f.conn.ID, Version: f.version, Request: json.RawMessage(transportBody)}
}
func (f *chatGPTFixture) call(t *testing.T, ctx context.Context, token string, body any) *http.Response {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	r, err := http.NewRequestWithContext(ctx, "POST", f.api.URL+"/internal/core/personas/"+f.persona+"/model/chatgpt/responses", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("ChatGPT-Account-ID", "forged-account")
	r.Header.Set("originator", "forged-client")
	r.Header.Set("Cookie", "browser-session=not-authority")
	res, err := f.api.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { res.Body.Close() })
	return res
}

func TestChatGPTTransportMissingContentTypeStream(t *testing.T) {
	f := chatGPTTransportFixture(t, func(http.ResponseWriter, *http.Request) { t.Fatal("unexpected fixture route") })
	f.s.chatGPTHTTP = &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(completedStream))}, nil
	})}
	res := f.call(t, context.Background(), f.token, f.request())
	got, err := io.ReadAll(res.Body)
	if err != nil || res.StatusCode != 200 || string(got) != completedStream || res.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("valid untyped upstream stream was lost: status=%d body=%q err=%v", res.StatusCode, got, err)
	}
}

func TestChatGPTTransportAdmissionAndServerOwnedHeaders(t *testing.T) {
	var calls atomic.Int32
	f := chatGPTTransportFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ey") || r.Header.Get("ChatGPT-Account-ID") != "synthetic-account" || r.Header.Get("originator") != "sumi" || r.Header.Get("User-Agent") != "sumi-secretary/alpha" || r.Header.Get("Cookie") != "" || r.Header.Get("x-openai-internal-codex-responses-lite") != "true" {
			t.Errorf("upstream did not receive exclusively server-owned headers")
		}
		w.Header().Set("Set-Cookie", "private-upstream-cookie")
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, completedStream)
	})
	ctx := context.Background()
	for _, token := range []string{"", "browser-session", f.s.PersonaToken(pid(t))} {
		if res := f.call(t, ctx, token, f.request()); res.StatusCode != 401 {
			t.Fatal("non-Core/cross-persona authority accepted", res.StatusCode)
		}
	}
	for _, field := range []string{"connection", "version", "model", "effort", "stream", "store", "previous_response"} {
		req := f.request()
		switch field {
		case "connection":
			req.ConnectionID = uuid.NewString()
		case "version":
			req.Version = uuid.NewString()
		case "model":
			req.Request = json.RawMessage(strings.Replace(transportBody, "gpt-6-astra", "gpt-6-sol", 1))
		case "effort":
			req.Request = json.RawMessage(strings.Replace(transportBody, "medium", "high", 1))
		case "stream":
			req.Request = json.RawMessage(strings.Replace(transportBody, `"stream":true`, `"stream":false`, 1))
		case "store":
			req.Request = json.RawMessage(strings.Replace(transportBody, `"store":false`, `"store":true`, 1))
		case "previous_response":
			req.Request = json.RawMessage(strings.Replace(transportBody, `"input":`, `"previous_response_id":"another-response","input":`, 1))
		}
		res := f.call(t, ctx, f.token, req)
		if res.StatusCode < 400 {
			t.Fatal("accepted invalid", field)
		}
	}
	if _, err := f.s.store.pool.Exec(ctx, `UPDATE core_personas SET authority='sealed' WHERE persona_id=$1`, f.persona); err != nil {
		t.Fatal(err)
	}
	if res := f.call(t, ctx, f.token, f.request()); res.StatusCode != 409 {
		t.Fatal("inactive accepted")
	}
	if _, err := f.s.store.pool.Exec(ctx, `UPDATE core_personas SET authority='active' WHERE persona_id=$1`, f.persona); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("denied requests reached upstream")
	}
	res := f.call(t, ctx, f.token, f.request())
	data, err := io.ReadAll(res.Body)
	if err != nil || res.StatusCode != 200 || string(data) != completedStream || res.Header.Get("Set-Cookie") != "" || calls.Load() != 1 {
		t.Fatalf("stream %d err=%v", res.StatusCode, err)
	}
	runtime := strings.Repeat("r", 48)
	if err := f.s.SetRuntimeToken(runtime); err != nil {
		t.Fatal(err)
	}
	res = f.call(t, ctx, runtime, f.request())
	io.Copy(io.Discard, res.Body)
	if res.StatusCode != 200 {
		t.Fatal("runtime token rejected")
	}
	if err := f.conns.Select(ctx, f.human, modelconnections.Selection{Kind: "none"}); err != nil {
		t.Fatal(err)
	}
	res = f.call(t, ctx, f.token, f.request())
	if res.StatusCode != 409 || calls.Load() != 2 {
		t.Fatal("nonselected connection accepted")
	}
}

func TestChatGPTTransportExplicit401RefreshAndSanitizedErrors(t *testing.T) {
	var calls atomic.Int32
	var firstAuth string
	f := chatGPTTransportFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			firstAuth = r.Header.Get("Authorization")
			w.WriteHeader(401)
			io.WriteString(w, `{"error":{"code":"token_expired","message":"secret-access-token"}}`)
			return
		}
		if r.Header.Get("Authorization") == firstAuth {
			t.Error("refresh did not replace token")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, completedStream)
	})
	res := f.call(t, context.Background(), f.token, f.request())
	data, _ := io.ReadAll(res.Body)
	digest := res.Header.Get(chatGPTRejectedHeader)
	if res.StatusCode != 401 || !chatGPTDigest.MatchString(digest) || strings.Contains(string(data), "secret") || calls.Load() != 1 || f.refreshes.Load() != 0 {
		t.Fatal("401 must not silently resend or leak error body")
	}
	req := f.request()
	req.Rejected = digest
	res = f.call(t, context.Background(), f.token, req)
	io.Copy(io.Discard, res.Body)
	if res.StatusCode != 200 || calls.Load() != 2 || f.refreshes.Load() != 1 {
		t.Fatal("explicit refresh-and-resend failed")
	}
	res = f.call(t, context.Background(), f.token, req)
	io.Copy(io.Discard, res.Body)
	if f.refreshes.Load() != 1 {
		t.Fatal("stale rejection rotated twice")
	}
}

func TestChatGPTTransportRechecksSelectionAfterRefresh(t *testing.T) {
	var calls atomic.Int32
	f := chatGPTTransportFixture(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(401) })
	res := f.call(t, context.Background(), f.token, f.request())
	io.Copy(io.Discard, res.Body)
	req := f.request()
	req.Rejected = res.Header.Get(chatGPTRejectedHeader)
	entered, release := make(chan struct{}), make(chan struct{})
	f.beforeRefresh = func() { close(entered); <-release }
	done := make(chan int, 1)
	go func() {
		res := f.call(t, context.Background(), f.token, req)
		io.Copy(io.Discard, res.Body)
		done <- res.StatusCode
	}()
	<-entered
	if err := f.conns.Select(context.Background(), f.human, modelconnections.Selection{Kind: "none"}); err != nil {
		t.Fatal(err)
	}
	close(release)
	if status := <-done; status != 409 || calls.Load() != 1 || f.refreshes.Load() != 1 {
		t.Fatal("selection changed during refresh still dispatched", status)
	}
}

func TestChatGPTTransportStreamsAndCancelsUpstream(t *testing.T) {
	cancelled := make(chan struct{})
	release := make(chan struct{})
	f := chatGPTTransportFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
			close(cancelled)
		case <-release:
		}
	})
	t.Cleanup(func() { close(release) })
	res := f.call(t, context.Background(), f.token, f.request())
	buf := make([]byte, len("data: first\n\n"))
	if _, err := io.ReadFull(res.Body, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "data: first\n\n" {
		t.Fatal("did not forward before upstream completion")
	}
	res.Body.Close()
	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("downstream cancellation did not cancel upstream")
	}
}

func TestChatGPTTransportBoundsRedirectAndFailures(t *testing.T) {
	var calls atomic.Int32
	f := chatGPTTransportFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Location", "http://127.0.0.1:1/secret")
		w.WriteHeader(302)
		io.WriteString(w, "private redirect body")
	})
	res := f.call(t, context.Background(), f.token, f.request())
	data, _ := io.ReadAll(res.Body)
	if res.StatusCode != 302 || res.Header.Get("Location") != "" || strings.Contains(string(data), "private") || calls.Load() != 1 {
		t.Fatal("redirect followed or exposed")
	}
	oversize := map[string]any{"connection_id": f.conn.ID, "connection_version": f.version, "request": map[string]any{"input": strings.Repeat("x", 4<<20)}}
	res = f.call(t, context.Background(), f.token, oversize)
	if res.StatusCode != 400 || calls.Load() != 1 {
		t.Fatal("oversize dispatched")
	}
	f.s.chatGPTHTTP = &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New("secret upstream error")
	})}
	res = f.call(t, context.Background(), f.token, f.request())
	data, _ = io.ReadAll(res.Body)
	if res.StatusCode != 502 || res.Header.Get(chatGPTErrorHeader) != "transport_ambiguous" || strings.Contains(string(data), "secret") || calls.Load() != 2 {
		t.Fatal("ambiguous transport retried/leaked")
	}
}

func TestChatGPTTransportCoreToGoToolRound(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Node is required for the Core integration")
	}
	var calls atomic.Int32
	f := chatGPTTransportFixture(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		if calls.Add(1) == 1 {
			io.WriteString(w, "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"reasoning\",\"id\":\"rs_synthetic\",\"encrypted_content\":\"opaque-synthetic\",\"summary\":[]}}\n\n")
			io.WriteString(w, "data: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"id\":\"fc_note\",\"call_id\":\"call_note\",\"name\":\"journal_note\",\"arguments\":\"{\\\"route\\\":\\\"normal\\\",\\\"input\\\":{\\\"text\\\":\\\"synthetic transport note\\\"}}\"}}\n\n")
			io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n")
		} else {
			if !bytes.Contains(b, []byte("opaque-synthetic")) || !bytes.Contains(b, []byte("function_call_output")) {
				t.Error("Core did not replay reasoning and tool result")
			}
			io.WriteString(w, completedStream)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, node, "../../../core/test/chatgpt-transport.integration.ts", f.api.URL, f.token, f.persona)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Core -> Go integration: %v\n%s", err, out)
	}
	t.Log(string(out))
	if calls.Load() != 2 {
		t.Fatal("unexpected model attempts", calls.Load())
	}
}

func TestChatGPTTransportGateRevocationAndUpstreamErrors(t *testing.T) {
	var calls atomic.Int32
	f := chatGPTTransportFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Server", "cloudflare")
		w.WriteHeader(403)
		io.WriteString(w, "<html>private diagnostic and credential-looking bytes</html>")
	})
	f.s.SetModelConnections(modelconnections.MetadataOnly(f.s.store.pool))
	res := f.call(t, context.Background(), f.token, f.request())
	if res.Header.Get(chatGPTErrorHeader) != "model_connection_disabled" || calls.Load() != 0 {
		t.Fatal("disabled gate bypassed")
	}
	f.s.SetModelConnections(f.conns)
	res = f.call(t, context.Background(), f.token, f.request())
	b, _ := io.ReadAll(res.Body)
	if res.StatusCode != 403 || strings.Contains(string(b), "private") || calls.Load() != 1 || f.refreshes.Load() != 0 {
		t.Fatal("403 was changed, leaked or retried")
	}
	f.revoked.Store(true)
	grant, err := f.conns.ResolveChatGPT(context.Background(), f.human, f.conn.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	req := f.request()
	req.Rejected = modelconnections.TokenDigest(grant.AccessToken)
	res = f.call(t, context.Background(), f.token, req)
	if res.Header.Get(chatGPTErrorHeader) != "model_reconnect_required" || calls.Load() != 1 {
		t.Fatal("revoked credential used")
	}
}

func TestChatGPTTransportCancelledRefreshSavesWithoutDispatch(t *testing.T) {
	var calls atomic.Int32
	f := chatGPTTransportFixture(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(401) })
	res := f.call(t, context.Background(), f.token, f.request())
	io.Copy(io.Discard, res.Body)
	req := f.request()
	req.Rejected = res.Header.Get(chatGPTRejectedHeader)
	entered, release := make(chan struct{}), make(chan struct{})
	f.beforeRefresh = func() { close(entered); <-release }
	b, _ := json.Marshal(req)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, _ := http.NewRequestWithContext(ctx, "POST", f.api.URL+"/internal/core/personas/"+f.persona+"/model/chatgpt/responses", bytes.NewReader(b))
	r.Header.Set("Authorization", "Bearer "+f.token)
	done := make(chan error, 1)
	go func() {
		res, err := f.api.Client().Do(r)
		if res != nil {
			res.Body.Close()
		}
		done <- err
	}()
	<-entered
	cancel()
	close(release)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("client cancellation", err)
	}
	grant, err := f.conns.ResolveChatGPT(context.Background(), f.human, f.conn.ID, "")
	if err != nil || modelconnections.TokenDigest(grant.AccessToken) == req.Rejected || f.refreshes.Load() != 1 || calls.Load() != 1 {
		t.Fatal("cancel lost refresh or dispatched another model request", err)
	}
}

type blockedStreamWriter struct {
	header           http.Header
	entered, release chan struct{}
}

func (w *blockedStreamWriter) Header() http.Header { return w.header }
func (*blockedStreamWriter) WriteHeader(int)       {}
func (*blockedStreamWriter) Flush()                {}
func (w *blockedStreamWriter) Write(p []byte) (int, error) {
	close(w.entered)
	<-w.release
	return 0, errors.New("downstream gone")
}

type countedStream struct {
	reads  atomic.Int32
	closed atomic.Bool
}

func (b *countedStream) Read(p []byte) (int, error) {
	b.reads.Add(1)
	copy(p, "data: bounded\n\n")
	return len("data: bounded\n\n"), nil
}
func (b *countedStream) Close() error { b.closed.Store(true); return nil }

func TestChatGPTTransportBackpressureAndOutputBound(t *testing.T) {
	f := chatGPTTransportFixture(t, func(w http.ResponseWriter, r *http.Request) { t.Error("unexpected real fixture request") })
	body := &countedStream{}
	f.s.chatGPTHTTP = &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: body}, nil
	})}
	b, _ := json.Marshal(f.request())
	r := httptest.NewRequest("POST", "/", bytes.NewReader(b))
	r.SetPathValue("persona", f.persona)
	r.Header.Set("Authorization", "Bearer "+f.token)
	w := &blockedStreamWriter{header: make(http.Header), entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() { defer close(done); f.s.chatGPTResponses(w, r) }()
	<-w.entered
	time.Sleep(30 * time.Millisecond)
	if body.reads.Load() != 1 {
		t.Fatal("API buffered upstream while downstream was blocked")
	}
	close(w.release)
	<-done
	if !body.closed.Load() {
		t.Fatal("upstream body not closed after write failure")
	}
	f.s.chatGPTHTTP = &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(bytes.NewReader(make([]byte, chatGPTStreamLimit+1)))}, nil
	})}
	res := f.call(t, context.Background(), f.token, f.request())
	n, err := io.Copy(io.Discard, res.Body)
	if err == nil || n > chatGPTStreamLimit {
		t.Fatalf("output not bounded/aborted: %d %v", n, err)
	}
}

func TestChatGPTTransportNonLiteAndQuotaResponse(t *testing.T) {
	f := chatGPTTransportFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-openai-internal-codex-responses-lite") != "" {
			t.Error("non-lite model received lite header")
		}
		w.Header().Set("Retry-After", "2")
		w.WriteHeader(429)
		io.WriteString(w, `{"error":{"type":"usage_limit_reached","resets_at":1900000000,"message":"private","plan_type":"private"}}`)
	})
	if _, err := f.conns.SetChatGPTSettings(context.Background(), f.human, f.conn.ID, modelconnections.ChatGPTSettings{Model: "gpt-5.5", ReasoningEffort: "high"}); err != nil {
		t.Fatal(err)
	}
	req := f.request()
	req.Request = json.RawMessage(strings.ReplaceAll(strings.ReplaceAll(transportBody, "gpt-6-astra", "gpt-5.5"), "medium", "high"))
	res := f.call(t, context.Background(), f.token, req)
	b, _ := io.ReadAll(res.Body)
	if res.StatusCode != 429 || res.Header.Get("Retry-After") != "2" || !bytes.Contains(b, []byte("usage_limit_reached")) || !bytes.Contains(b, []byte("1900000000")) || bytes.Contains(b, []byte("private")) {
		t.Fatal("quota classification lost or body leaked")
	}
}

func TestChatGPTTransportUpstreamDeadline(t *testing.T) {
	cancelled := make(chan struct{})
	release := make(chan struct{})
	f := chatGPTTransportFixture(t, func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
			close(cancelled)
		case <-release:
		}
	})
	t.Cleanup(func() { close(release) })
	f.s.chatGPTHTTP.Timeout = 50 * time.Millisecond // scale the production 120s HTTP bound
	res := f.call(t, context.Background(), f.token, f.request())
	if res.StatusCode != 502 || res.Header.Get(chatGPTErrorHeader) != "transport_ambiguous" {
		t.Fatal("timeout misclassified")
	}
	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("deadline did not cancel upstream")
	}
}

func TestChatGPTTransportErrorIdentifierShapes(t *testing.T) {
	f := chatGPTTransportFixture(t, func(w http.ResponseWriter, r *http.Request) { t.Error("unexpected fixture call") })
	for _, tc := range []struct{ body, code string }{
		{`{"error":"token_expired"}`, "token_expired"},
		{`{"error":{"code":"secret-token-123","type":"permission_denied"}}`, "permission_denied"},
		{`{"error":{"code":"secret-token-123"}}`, "unrecognized"},
	} {
		f.s.chatGPTHTTP = &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 401, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(tc.body))}, nil
		})}
		res := f.call(t, context.Background(), f.token, f.request())
		b, _ := io.ReadAll(res.Body)
		if !bytes.Contains(b, []byte(tc.code)) || bytes.Contains(b, []byte("secret-token")) {
			t.Fatalf("diagnostic: %s", b)
		}
	}
}
