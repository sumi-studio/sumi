package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"net/http"
	"os"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/mcpconnections"
)

func wireLocalMCP(pool *pgxpool.Pool, core *agentstate.Server, fm *fmServer, mux *http.ServeMux, persona string) (func(), error) {
	install := os.Getenv("SUMI_LOCAL_ID")
	if install == "" {
		return func() {}, nil
	}
	// Stable across ordinary host restarts, distinct from portable Core state.
	// Changing install identity/admin secret requires local reconfiguration.
	mac := hmac.New(sha256.New, fm.secret)
	mac.Write([]byte("sumi.local-mcp.key.v1:" + install))
	key := mac.Sum(nil)
	defer clear(key)
	host := uuid.NewSHA1(uuid.NameSpaceOID, []byte("sumi.local-mcp.host.v1:"+install)).String()
	store, e := mcpconnections.NewLocal(pool, key, host, persona)
	if e != nil {
		return nil, e
	}
	for name, effect := range store.Effects() {
		if e = core.RegisterToolEffect(name, effect); e != nil {
			return nil, e
		}
	}
	store.RegisterLocalRoutes(mux, func(w http.ResponseWriter, r *http.Request) bool {
		id, ok := fm.scope(w, r)
		if !ok {
			return false
		}
		if id != persona {
			http.Error(w, "Local MCP belongs to another persona", 403)
			return false
		}
		authority, e := core.Store().PersonaAuthority(r.Context(), persona)
		if e != nil || authority != "active" {
			http.Error(w, "Local MCP unavailable for this persona", 403)
			return false
		}
		return true
	})
	runner := mcpconnections.NewRunner(store, core.Store())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); runner.Run(ctx) }()
	return func() { cancel(); <-done }, nil
}
