package main

import (
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

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

	log.Printf("sumi api listening on :%s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal(err)
	}
}

func newRouter() (*http.ServeMux, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handler.Health)
	mux.Handle("GET /agent/ws", wsHandler())

	cmdDir := os.Getenv("SUMI_COMMAND_LOG_DIR")
	if cmdDir == "" {
		return nil, errors.New("SUMI_COMMAND_LOG_DIR not set")
	}
	store, err := agentevents.OpenCommandStore(cmdDir)
	if err != nil {
		return nil, fmt.Errorf("open command store: %w", err)
	}
	ingress, err := agentevents.NewUserCommandIngress(store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("create command ingress: %w", err)
	}
	mux.Handle("POST /conversations/{conversation_id}/commands", ingress)

	return mux, nil
}

// wsHandler wires a real HMAC token verifier when SUMI_AGENT_TOKEN_SECRET is
// configured, and falls back to a fail-closed server while the remaining T17
// (durable source/hydration) and T26 (generation lease) seams are not yet
// injected.
func wsHandler() http.Handler {
	tv, err := tokenVerifierFromEnv()
	if err == nil {
		log.Print("agent WS token verification wired")
		srv := agentevents.NewServerWithTokenVerifier(tv)
		srv.AllowedOrigins = allowedOriginsFromEnv()
		return srv
	}
	if !errors.Is(err, errTokenSecretMissing) {
		log.Fatalf("agent WS token verifier misconfigured: %v", err)
	}
	log.Print("agent WS running fail-closed: T17/T26 production seams not wired")
	srv := agentevents.NewFailClosedServer()
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
