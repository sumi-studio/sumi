package main

import (
	"encoding/base64"
	"errors"
	"log"
	"net/http"
	"os"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/handler"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handler.Health)
	mux.Handle("GET /agent/ws", wsHandler())

	log.Printf("sumi api listening on :%s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal(err)
	}
}

// wsHandler wires a real HMAC token verifier when SUMI_AGENT_TOKEN_SECRET is
// configured, and falls back to a fail-closed server while the remaining T17
// (durable source/hydration) and T26 (generation lease) seams are not yet
// injected.
func wsHandler() http.Handler {
	tv, err := tokenVerifierFromEnv()
	if err == nil {
		log.Print("agent WS token verification wired")
		return agentevents.NewServerWithTokenVerifier(tv)
	}
	if !errors.Is(err, errTokenSecretMissing) {
		log.Printf("agent WS token verifier misconfigured: %v", err)
	}
	log.Print("agent WS running fail-closed: T17/T26 production seams not wired")
	return agentevents.NewFailClosedServer()
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
