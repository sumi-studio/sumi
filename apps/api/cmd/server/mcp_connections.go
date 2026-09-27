package main

import (
	"encoding/base64"
	"errors"
	"net/http"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/chatgpt"
	"github.com/sumi-studio/sumi/apps/api/internal/mcpconnections"
)

func wireMCP(pool *pgxpool.Pool, core *agentstate.Server, mux *http.ServeMux, authenticate func(*http.Request) (chatgpt.LoginIdentity, error)) (*mcpconnections.Runner, error) {
	var store *mcpconnections.Store
	// Reuse the server's credential-encryption key; MCP uses a distinct AAD
	// domain so ciphertext cannot be substituted for model credentials.
	raw := strings.TrimSpace(os.Getenv("SUMI_MODEL_CONNECTION_KEY"))
	if raw != "" && pool != nil && core != nil {
		key, e := base64.StdEncoding.DecodeString(raw)
		if e != nil || len(key) != 32 {
			return nil, errors.New("SUMI_MODEL_CONNECTION_KEY must encode 32 bytes")
		}
		defer clear(key)
		store, e = mcpconnections.New(pool, key)
		if e != nil {
			return nil, e
		}
		for name, effect := range store.Effects() {
			if e := core.RegisterToolEffect(name, effect); e != nil {
				return nil, e
			}
		}
	}
	(&mcpconnections.Service{Store: store, Authenticate: authenticate}).RegisterRoutes(mux)
	if store == nil {
		return nil, nil
	}
	return mcpconnections.NewRunner(store, core.Store()), nil
}
func (a *application) startMCP() {
	if a.mcpRunner == nil {
		return
	}
	a.attentionWorkers.Add(1)
	go func() { defer a.attentionWorkers.Done(); a.mcpRunner.Run(a.backgroundCtx) }()
}
