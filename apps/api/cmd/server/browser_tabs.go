package main

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/browsertabs"
	"github.com/sumi-studio/sumi/apps/api/internal/chatgpt"
	"net/http"
)

func wireBrowserTabs(pool *pgxpool.Pool, core *agentstate.Server, mux *http.ServeMux, authenticate func(*http.Request) (chatgpt.LoginIdentity, error)) (*browsertabs.Store, error) {
	var store *browsertabs.Store
	if pool != nil && core != nil {
		store = browsertabs.New(pool, core.Store())
		for name, effect := range store.Effects() {
			if e := core.RegisterToolEffect(name, effect); e != nil {
				return nil, e
			}
		}
	}
	(&browsertabs.Service{Store: store, Authenticate: authenticate}).RegisterRoutes(mux)
	return store, nil
}
func (a *application) startBrowserTabs() {
	if a.browserTabs == nil {
		return
	}
	a.attentionWorkers.Add(1)
	go func() { defer a.attentionWorkers.Done(); a.browserTabs.Run(a.backgroundCtx) }()
}
