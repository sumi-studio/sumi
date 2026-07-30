// browser-e2e-fixture is a test-only API/agent boundary used by the real
// Chrome journey in apps/web. It is intentionally not wired into cmd/server.
package main

import (
	"context"
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
	dir := os.Getenv("SUMI_E2E_RUNTIME_DIR")
	if dir == "" || !filepath.IsAbs(dir) {
		log.Fatal("SUMI_E2E_RUNTIME_DIR must be an absolute parent-owned directory")
	}
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
	receipt := "browser-e2e-ready"
	if err := gateway.PublishRuntimeState("018f47a2-9b3c-7def-8abc-0123456789ab", 1, &receipt); err != nil {
		log.Fatal(err)
	}
	browserSessions, err := agentevents.NewHMACUserSessionVerifier(secret, "")
	if err != nil {
		log.Fatal(err)
	}
	preissuedSession, err := browserSessions.IssueSession(
		context.Background(),
		agentevents.UserSessionClaims{
			TenantID:           "tenant",
			UserID:             "user",
			PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
		},
		time.Hour,
	)
	if err != nil {
		log.Fatal(err)
	}

	// Use the same production router as cmd/server so the E2E journey exercises
	// production wiring. We do not expose the agent WebSocket boundary in this
	// fixture; nil TokenVerifier makes it fail-closed.
	mux, browser, _, err := agentevents.NewProductionMux(store, gateway, nil, browserSessions, nil, []string{"http://127.0.0.1:4173"})
	if err != nil {
		log.Fatal(err)
	}

	fixture := newFixture(store, gateway)
	mux.HandleFunc("GET /__e2e__/session", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: agentevents.BrowserSessionCookie, Value: preissuedSession, Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode})
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /__e2e__/emit-terminal", fixture.emitTerminal)
	mux.HandleFunc("POST /__e2e__/disconnect", func(w http.ResponseWriter, r *http.Request) {
		browser.CloseBrowserConnections()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /__e2e__/connection-stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(browser.ConnectionStats()); err != nil {
			log.Printf("encode browser connection stats: %v", err)
		}
	})
	mux.HandleFunc("POST /__e2e__/disconnect-and-emit-terminal", func(w http.ResponseWriter, r *http.Request) {
		if err := fixture.waitForAbort(); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		browser.CloseBrowserConnections()
		deadline := time.Now().Add(2 * time.Second)
		for browser.ConnectionStats().Active != 0 {
			if time.Now().After(deadline) {
				http.Error(w, "browser websocket did not disconnect", http.StatusGatewayTimeout)
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		if err := fixture.emitTerminalEvent(); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("E2E_FIXTURE=http://%s\n", listener.Addr().String())
	srv := &http.Server{Handler: mux}
	if err := srv.Serve(listener); err != nil {
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
		commands, err := f.store.CatchUp(context.Background(), "018f47a2-9b3c-7def-8abc-0123456789ab", f.from)
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
		f.durable(`{"type":"agent_start"}`)
		f.durable(`{"type":"turn_start"}`)
		f.volatile(`{"type":"message_update","message_id":"00000000-0000-4000-8000-000000000001","event":{"type":"text_delta","content_index":0,"delta":"streamed assistant"}}`)
		f.durable(`{"type":"tool_execution_start","tool_call_id":"call-1","tool_name":"read_file","args":{}}`)
		f.durable(`{"type":"tool_execution_end","tool_call_id":"call-1","result":"ok","is_error":false}`)
	case kind == "user_message" && f.stage == 1:
		f.stage = 2
		f.durable(`{"type":"steered","mode":"hard"}`)
		f.durable(`{"type":"approval_requested","request":{"id":"request-1","tool_call_id":"call-1","tool_name":"read_file","action":{"reviewable":"read fixture"},"args_summary":"read fixture"}}`)
	case kind == "approval_decision" && f.stage == 2:
		f.stage = 3
		f.durable(`{"type":"message_start","message_id":"00000000-0000-4000-8000-000000000002","message":{"role":"assistant","content":[],"model":"fixture","provider":"fixture","origin":{"provider_instance_id":"fixture","protocol":"open_ai_responses","model":"fixture"},"usage":{"input":0,"output":0,"cache_read":0,"cache_write":0,"reasoning":0,"total_tokens":0},"stop_reason":"stop","error_message":null,"provider_code":null,"interrupted":false,"timestamp":"2026-07-28T00:00:00Z"}}`)
		f.volatile(`{"type":"message_update","message_id":"00000000-0000-4000-8000-000000000002","event":{"type":"text_delta","content_index":0,"delta":"abortable stream"}}`)
	case kind == "abort" && f.stage == 3:
		f.stage = 4
		f.once.Do(func() { close(f.aborted) })
	}
}

func (f *fixture) emitTerminal(w http.ResponseWriter, r *http.Request) {
	if err := f.waitForAbort(); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if err := f.emitTerminalEvent(); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (f *fixture) waitForAbort() error {
	select {
	case <-f.aborted:
	case <-time.After(2 * time.Second):
		return fmt.Errorf("abort command was not admitted")
	}
	return nil
}

func (f *fixture) emitTerminalEvent() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stage != 4 {
		return fmt.Errorf("terminal already emitted")
	}
	f.stage = 5
	f.durable(`{"type":"message_end","message_id":"00000000-0000-4000-8000-000000000002","message":{"role":"assistant","content":[{"type":"text","text":"Terminal replay","wire_item_index":0}],"model":"fixture","provider":"fixture","origin":{"provider_instance_id":"fixture","protocol":"open_ai_responses","model":"fixture"},"usage":{"input":0,"output":0,"cache_read":0,"cache_write":0,"reasoning":0,"total_tokens":0},"stop_reason":"aborted","error_message":null,"provider_code":null,"interrupted":true,"timestamp":"2026-07-28T00:00:00Z"}}`)
	// The ordered durable event after message_end is an E2E replay barrier:
	// once the browser observes it, every earlier replay frame has settled.
	f.durable(`{"type":"turn_end","message":null,"tool_results":[]}`)
	f.durable(`{"type":"agent_end"}`)
	return nil
}

func (f *fixture) durable(event string) {
	f.seq++
	seq := f.seq
	err := f.gateway.Receive(context.Background(), agentevents.TokenClaims{TenantID: "tenant", PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Generation: 1}, agentevents.Envelope{Seq: &seq, PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Event: json.RawMessage(event)})
	if err != nil {
		log.Printf("fixture durable event: %v", err)
	}
}

func (f *fixture) volatile(event string) {
	err := f.gateway.Receive(context.Background(), agentevents.TokenClaims{TenantID: "tenant", PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Generation: 1}, agentevents.Envelope{PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Event: json.RawMessage(event)})
	if err != nil {
		log.Printf("fixture volatile event: %v", err)
	}
}
