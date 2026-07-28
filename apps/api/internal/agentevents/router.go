package agentevents

import (
	"fmt"
	"log"
	"net/http"
)

// NewProductionMux assembles the production API router. All dependencies are
// explicit parameters so tests and the browser E2E fixture can inject the same
// wiring as cmd/server without copying route registration.
//
// A nil TokenVerifier makes /agent/ws fail-closed. A nil UserSessionVerifier
// makes the browser command and WebSocket routes fail-closed. /health is not
// registered by this helper so callers can attach their own health handler.
func NewProductionMux(
	store *CommandStore,
	runtime *DurableGateway,
	tv TokenVerifier,
	sv UserSessionVerifier,
	agentOrigins,
	browserOrigins []string,
) (*http.ServeMux, *BrowserServer, *Server, error) {
	mux := http.NewServeMux()

	agent := newAgentWebSocketHandler(tv, runtime, agentOrigins)
	mux.Handle("GET /agent/ws", agent)

	if store == nil {
		return nil, nil, nil, fmt.Errorf("user command ingress: %w", errCommandAppenderRequired)
	}
	ingress, err := NewUserCommandIngress(store, sv)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("user command ingress: %w", err)
	}
	mux.Handle("POST /conversations/{conversation_id}/commands", ingress)

	browser := NewBrowserServer(sv, store, runtime)
	browser.AllowedOrigins = browserOrigins
	mux.Handle("GET /conversations/{conversation_id}/ws", browser)

	return mux, browser, agent, nil
}

func newAgentWebSocketHandler(tv TokenVerifier, runtime *DurableGateway, allowedOrigins []string) *Server {
	if tv == nil || runtime == nil {
		srv := NewFailClosedServer()
		srv.AllowedOrigins = allowedOrigins
		return srv
	}
	log.Print("agent WS token, generation, durable command/event, and hydration wiring ready")
	srv := NewServer(tv, runtime, runtime, runtime, runtime)
	srv.AllowedOrigins = allowedOrigins
	return srv
}
