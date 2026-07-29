package main

import (
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
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
	sv, err := browserSessionVerifierFromEnv()
	if err != nil && !errors.Is(err, errBrowserSessionSecretMissing) {
		return nil, fmt.Errorf("browser session verifier: %w", err)
	}

	cmdDir := os.Getenv("SUMI_COMMAND_LOG_DIR")
	if cmdDir == "" {
		return nil, errors.New("SUMI_COMMAND_LOG_DIR not set")
	}
	store, err := agentevents.OpenCommandStore(cmdDir)
	if err != nil {
		return nil, fmt.Errorf("open command store: %w", err)
	}
	runtimeDir := os.Getenv("SUMI_AGENT_RUNTIME_STATE_DIR")
	if runtimeDir == "" {
		_ = store.Close()
		return nil, errors.New("SUMI_AGENT_RUNTIME_STATE_DIR not set")
	}
	runtime, err := agentevents.OpenDurableGateway(runtimeDir, store)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("open agent runtime gateway: %w", err)
	}

	mux, _, _, err := agentevents.NewProductionMux(store, runtime, tv, sv, allowedOriginsFromEnv(), browserAllowedOriginsFromEnv())
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	localControl, enabled, err := localControlServerFromEnv(runtime)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("local control fixture: %w", err)
	}
	if enabled {
		if err := localControl.RegisterRoutes(mux); err != nil {
			_ = store.Close()
			return nil, fmt.Errorf("register local control fixture: %w", err)
		}
	}
	mux.HandleFunc("GET /health", handler.Health)
	return mux, nil
}

func allowedOriginsFromEnv() []string {
	return originsFromEnv("SUMI_AGENT_WS_ALLOWED_ORIGINS")
}

func browserAllowedOriginsFromEnv() []string {
	return originsFromEnv("SUMI_BROWSER_WS_ALLOWED_ORIGINS")
}

func originsFromEnv(name string) []string {
	raw := os.Getenv(name)
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
var errBrowserSessionSecretMissing = errors.New("SUMI_BROWSER_SESSION_SECRET not set")

func tokenVerifierFromEnv() (agentevents.TokenVerifier, error) {
	secret, err := tokenSecretFromEnv()
	if err != nil {
		return nil, err
	}
	audience := os.Getenv("SUMI_AGENT_TOKEN_AUDIENCE")
	return agentevents.NewHMACTokenVerifier(secret, audience)
}

func tokenSecretFromEnv() ([]byte, error) {
	b64 := os.Getenv("SUMI_AGENT_TOKEN_SECRET")
	if b64 == "" {
		return nil, errTokenSecretMissing
	}
	return base64.StdEncoding.DecodeString(b64)
}

// localControlServerFromEnv is an explicit local/CI fixture switch. Production
// routing never registers these endpoints unless the switch and the complete
// one-runtime authorization binding are present.
func localControlServerFromEnv(runtime *agentevents.DurableGateway) (*agentevents.LocalControlServer, bool, error) {
	switch enabled := os.Getenv("SUMI_LOCAL_CONTROL_ENABLED"); enabled {
	case "", "0":
		return nil, false, nil
	case "1":
	default:
		return nil, false, errors.New("SUMI_LOCAL_CONTROL_ENABLED must be 0 or 1")
	}

	required := func(name string) (string, error) {
		value := os.Getenv(name)
		if value == "" {
			return "", fmt.Errorf("%s not set", name)
		}
		return value, nil
	}
	bearer, err := required("SUMI_LOCAL_CONTROL_BEARER")
	if err != nil {
		return nil, false, err
	}
	tenantID, err := required("SUMI_LOCAL_CONTROL_TENANT_ID")
	if err != nil {
		return nil, false, err
	}
	personalityAgentID, err := required("SUMI_LOCAL_CONTROL_PERSONALITY_AGENT_ID")
	if err != nil {
		return nil, false, err
	}
	generationRaw, err := required("SUMI_LOCAL_CONTROL_GENERATION")
	if err != nil {
		return nil, false, err
	}
	generation, err := strconv.ParseUint(generationRaw, 10, 64)
	if err != nil {
		return nil, false, fmt.Errorf("parse SUMI_LOCAL_CONTROL_GENERATION: %w", err)
	}
	rpcBootNonce, err := required("SUMI_LOCAL_CONTROL_RPC_BOOT_NONCE")
	if err != nil {
		return nil, false, err
	}
	deliveryRaw, err := required("SUMI_LOCAL_CONTROL_DELIVERY_AUTHORIZATION")
	if err != nil {
		return nil, false, err
	}
	signingSecret, err := tokenSecretFromEnv()
	if err != nil {
		return nil, false, err
	}
	agentAudience := os.Getenv("SUMI_AGENT_TOKEN_AUDIENCE")
	if agentAudience == "" {
		agentAudience = agentevents.DefaultAgentAudience()
	}
	controlAudience := os.Getenv("SUMI_LOCAL_CONTROL_AUDIENCE")
	if controlAudience == "" {
		controlAudience = agentAudience
	}
	if controlAudience != agentAudience {
		return nil, false, errors.New("SUMI_LOCAL_CONTROL_AUDIENCE must match SUMI_AGENT_TOKEN_AUDIENCE")
	}
	control, err := agentevents.NewLocalControlServer(
		runtime,
		signingSecret,
		[]agentevents.LocalRuntimeAuthorization{{
			BearerToken:           bearer,
			TenantID:              tenantID,
			PersonalityAgentID:    personalityAgentID,
			Generation:            generation,
			RPCBootNonce:          rpcBootNonce,
			Audience:              controlAudience,
			DeliveryAuthorization: agentevents.LocalDeliveryAuthorization(deliveryRaw),
		}},
	)
	if err != nil {
		return nil, false, err
	}
	return control, true, nil
}

// browserSessionVerifierFromEnv is deliberately separate from the agent token
// verifier. Browser sessions are HttpOnly cookies scoped to users and
// conversations; agent bearer tokens never enter this route.
func browserSessionVerifierFromEnv() (agentevents.UserSessionVerifier, error) {
	b64 := os.Getenv("SUMI_BROWSER_SESSION_SECRET")
	if b64 == "" {
		return nil, errBrowserSessionSecretMissing
	}
	secret, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, err
	}
	audience := os.Getenv("SUMI_BROWSER_SESSION_AUDIENCE")
	return agentevents.NewHMACUserSessionVerifier(secret, audience)
}
