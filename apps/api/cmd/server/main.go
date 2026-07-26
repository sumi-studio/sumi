package main

import (
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/handler"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	mux, err := newRouter()
	if err != nil {
		log.Fatal(err)
	}

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	log.Printf("sumi api listening on :%s", port)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func newRouter() (*http.ServeMux, error) {
	tv, err := tokenVerifierFromEnv()
	if err != nil && !errors.Is(err, errTokenSecretMissing) {
		return nil, fmt.Errorf("agent token verifier: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handler.Health)
	cmdDir := os.Getenv("SUMI_COMMAND_LOG_DIR")
	if cmdDir == "" {
		return nil, errors.New("SUMI_COMMAND_LOG_DIR not set")
	}
	store, err := agentevents.OpenCommandStore(cmdDir)
	if err != nil {
		return nil, fmt.Errorf("open command store: %w", err)
	}
	runtimeDir := os.Getenv("SUMI_AGENT_RUNTIME_STATE_DIR")
	runtime, err := agentevents.OpenDurableGateway(runtimeDir, store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("open agent runtime gateway: %w", err)
	}
	mux.Handle("GET /agent/ws", wsHandler(tv, runtime))
	ingress, err := agentevents.NewUserCommandIngress(store, tv)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("create command ingress: %w", err)
	}
	mux.Handle("POST /conversations/{conversation_id}/commands", ingress)

	return mux, nil
}

// wsHandler assembles the production token, generation, durable command/event,
// and hydration adapters. A missing token verifier remains fail-closed, but a
// configured production route never substitutes placeholder T17/T26 seams.
func wsHandler(tv agentevents.TokenVerifier, runtime *agentevents.DurableGateway) http.Handler {
	if tv == nil || runtime == nil {
		srv := agentevents.NewFailClosedServer()
		srv.AllowedOrigins = allowedOriginsFromEnv()
		return srv
	}
	log.Print("agent WS token, generation, durable command/event, and hydration wiring ready")
	srv := agentevents.NewServer(tv, runtime, runtime, runtime, runtime)
	srv.AllowedOrigins = allowedOriginsFromEnv()
	return srv
}

func allowedOriginsFromEnv() []string {
	raw := os.Getenv("SUMI_AGENT_WS_ALLOWED_ORIGINS")
	if raw == "" {
		return nil
	}
	var origins []string
	for _, o := range strings.Split(raw, ",") {
		o = strings.TrimSpace(o)
		if o != "" {
			origins = append(origins, o)
		}
	}
	return origins
}

var errTokenSecretMissing = errors.New("SUMI_AGENT_TOKEN_SECRET not set")

func tokenVerifierFromEnv() (agentevents.TokenVerifier, error) {
	b64 := os.Getenv("SUMI_AGENT_TOKEN_SECRET")
	if b64 == "" {
		return nil, errTokenSecretMissing
	}
	secret, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, err
	}
	audience := os.Getenv("SUMI_AGENT_TOKEN_AUDIENCE")
	return agentevents.NewHMACTokenVerifier(secret, audience)
}
