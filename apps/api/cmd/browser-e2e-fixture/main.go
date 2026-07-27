// browser-e2e-fixture is a test-only API/agent boundary used by the real
// Chrome journey in apps/web. It is intentionally not wired into cmd/server.
package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

var secret = []byte("browser-e2e-session-secret-32-bytes")

func main() {
	dir, err := os.MkdirTemp("", "sumi-browser-e2e-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	store, err := agentevents.OpenCommandStore(filepath.Join(dir, "commands"))
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	gateway, err := agentevents.OpenDurableGateway(filepath.Join(dir, "runtime"), store)
	if err != nil {
		log.Fatal(err)
	}
	gateway.PollInterval = 5 * time.Millisecond
	browserSessions, err := agentevents.NewHMACUserSessionVerifier(secret, "")
	if err != nil {
		log.Fatal(err)
	}

	// Use the same production router as cmd/server so the E2E journey exercises
	// production wiring. We do not expose the agent WebSocket boundary in this
	// fixture; nil TokenVerifier makes it fail-closed.
	mux, browser, _ := agentevents.NewProductionMux(store, gateway, nil, browserSessions, nil, []string{"http://127.0.0.1:4173"})

	fixture := newFixture(store, gateway)
	mux.HandleFunc("GET /__e2e__/session", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: agentevents.BrowserSessionCookie, Value: signSession(), Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode})
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /__e2e__/emit-terminal", fixture.emitTerminal)
	mux.HandleFunc("POST /__e2e__/disconnect", func(w http.ResponseWriter, r *http.Request) {
		browser.CloseBrowserConnections()
		w.WriteHeader(http.StatusNoContent)
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("E2E_FIXTURE=http://%s\n", listener.Addr().String())
	if err := http.Serve(listener, mux); err != nil {
		log.Print(err)
	}
}

type fixture struct {
	store   *agentevents.CommandStore
	gateway *agentevents.DurableGateway
	mu      sync.Mutex
	seq     uint64
	from    uint64
	stage   int
	aborted chan struct{}
	once    sync.Once
}

func newFixture(store *agentevents.CommandStore, gateway *agentevents.DurableGateway) *fixture {
	f := &fixture{store: store, gateway: gateway, aborted: make(chan struct{})}
	go f.run()
	return f
}

func (f *fixture) run() {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		commands, err := f.store.CatchUp(context.Background(), "conversation-1", f.from)
		if err != nil {
			log.Printf("fixture command catch-up: %v", err)
			continue
		}
		for _, command := range commands {
			f.from = command.Seq + 1
			var body struct {
				Type string `json:"type"`
			}
			_ = json.Unmarshal(command.Command, &body)
			f.react(body.Type)
		}
	}
}

func (f *fixture) react(kind string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case kind == "user_message" && f.stage == 0:
		f.stage = 1
		f.volatile(`{"type":"message_update","message_id":"00000000-0000-4000-8000-000000000001","event":{"type":"text_delta","content_index":0,"delta":"streamed assistant"}}`)
		f.durable(`{"type":"tool_execution_start","tool_call_id":"call-1","tool_name":"read_file","args":{}}`)
		f.durable(`{"type":"tool_execution_end","tool_call_id":"call-1","result":"ok","is_error":false}`)
	case kind == "user_message" && f.stage == 1:
		f.stage = 2
		f.durable(`{"type":"steered","mode":"hard"}`)
		f.durable(`{"type":"approval_requested","request":{"id":"request-1","tool_call_id":"call-1","tool_name":"read_file","action":{"reviewable":"read fixture"},"args_summary":"read fixture"}}`)
	case kind == "approval_decision" && f.stage == 2:
		f.stage = 3
		f.volatile(`{"type":"message_update","message_id":"00000000-0000-4000-8000-000000000002","event":{"type":"text_delta","content_index":0,"delta":"abortable stream"}}`)
	case kind == "abort" && f.stage == 3:
		f.stage = 4
		f.once.Do(func() { close(f.aborted) })
	}
}

func (f *fixture) emitTerminal(w http.ResponseWriter, r *http.Request) {
	select {
	case <-f.aborted:
	case <-time.After(2 * time.Second):
		http.Error(w, "abort command was not admitted", http.StatusConflict)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stage != 4 {
		http.Error(w, "terminal already emitted", http.StatusConflict)
		return
	}
	f.stage = 5
	f.durable(`{"type":"message_end","message_id":"00000000-0000-4000-8000-000000000002","message":{"role":"assistant","content":[{"type":"text","text":"Terminal replay","wire_item_index":0}],"model":"fixture","provider":"fixture","origin":{"provider_instance_id":"fixture","protocol":"open_ai_responses","model":"fixture"},"usage":{"input":0,"output":0,"cache_read":0,"cache_write":0,"reasoning":0,"total_tokens":0},"stop_reason":"aborted","error_message":null,"provider_code":null,"interrupted":true,"timestamp":"2026-07-28T00:00:00Z"}}`)
	w.WriteHeader(http.StatusNoContent)
}

func (f *fixture) durable(event string) {
	f.seq++
	seq := f.seq
	err := f.gateway.Receive(context.Background(), agentevents.TokenClaims{TenantID: "tenant", AgentID: "fixture-agent", ConversationID: "conversation-1", Generation: 1}, agentevents.Envelope{Seq: &seq, ConversationID: "conversation-1", Event: json.RawMessage(event)})
	if err != nil {
		log.Printf("fixture durable event: %v", err)
	}
}

func (f *fixture) volatile(event string) {
	err := f.gateway.Receive(context.Background(), agentevents.TokenClaims{TenantID: "tenant", AgentID: "fixture-agent", ConversationID: "conversation-1", Generation: 1}, agentevents.Envelope{ConversationID: "conversation-1", Event: json.RawMessage(event)})
	if err != nil {
		log.Printf("fixture volatile event: %v", err)
	}
}

func signSession() string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload, _ := json.Marshal(map[string]any{"tenant_id": "tenant", "user_id": "user", "conversation_id": "conversation-1", "exp": time.Now().Add(time.Hour).Unix(), "aud": "sumi:web:conversation"})
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(header + "." + encoded))
	return header + "." + encoded + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
