package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/handler"
	"github.com/sumi-studio/sumi/apps/api/internal/todo"
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
	mux.HandleFunc("GET /health", handler.Health)
	if !todoEnabled() {
		mux.HandleFunc("GET /ready", handler.Ready())
		return mux, nil
	}
	if !todoDevelopmentSessionAuthEnabled() {
		_ = store.Close()
		return nil, errors.New("SUMI_TODO_DEV_SESSION_AUTH must be true while production Todo auth is unavailable")
	}
	if sv == nil {
		_ = store.Close()
		return nil, errors.New("SUMI_BROWSER_SESSION_SECRET is required for Todo development session auth")
	}

	databaseURL := os.Getenv("SUMI_DATABASE_URL")
	if databaseURL == "" {
		_ = store.Close()
		return nil, errors.New("SUMI_DATABASE_URL not set")
	}
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("open todo database pool: %w", err)
	}
	pingContext, cancelPing := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelPing()
	if err := pool.Ping(pingContext); err != nil {
		pool.Close()
		_ = store.Close()
		return nil, fmt.Errorf("ping todo database: %w", err)
	}
	todoService, err := todo.NewService(todo.NewPostgresRepository(pool), os.Getenv("SUMI_DEFAULT_TIMEZONE"))
	if err != nil {
		pool.Close()
		_ = store.Close()
		return nil, fmt.Errorf("create todo service: %w", err)
	}
	todo.NewHandler(todoService, todoDevelopmentSessionPrincipalVerifier{sessions: sv}).Register(mux)
	mux.HandleFunc("GET /ready", handler.Ready(todoDatabaseReadiness(pool)))
	return mux, nil
}

func todoDatabaseReadiness(pool *pgxpool.Pool) func(context.Context) error {
	return func(ctx context.Context) error {
		pingContext, cancelPing := context.WithTimeout(ctx, 2*time.Second)
		defer cancelPing()
		return pool.Ping(pingContext)
	}
}

func todoEnabled() bool {
	return os.Getenv("SUMI_TODO_ENABLED") == "true"
}

func todoDevelopmentSessionAuthEnabled() bool {
	return os.Getenv("SUMI_TODO_DEV_SESSION_AUTH") == "true"
}

// todoDevelopmentSessionPrincipalVerifier deliberately adapts the existing
// conversation-scoped cookie only for the local backend development stack.
// Production Todo routes stay disabled until user-scoped auth is available.
type todoDevelopmentSessionPrincipalVerifier struct {
	sessions agentevents.UserSessionVerifier
}

func (v todoDevelopmentSessionPrincipalVerifier) VerifyRequest(ctx context.Context, request *http.Request) (todo.Principal, error) {
	if v.sessions == nil {
		return todo.Principal{}, errors.New("browser session verifier unavailable")
	}
	cookie, err := request.Cookie(agentevents.BrowserSessionCookie)
	if err != nil {
		return todo.Principal{}, err
	}
	claims, err := v.sessions.VerifySession(ctx, cookie.Value)
	if err != nil {
		return todo.Principal{}, err
	}
	return todo.Principal{UserID: claims.UserID}, nil
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
