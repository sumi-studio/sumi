package main

import (
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/browseridentity"
	"github.com/sumi-studio/sumi/apps/api/internal/browsertabs"
	"github.com/sumi-studio/sumi/apps/api/internal/cloudbrowser"
)

// wireCloudBrowser mounts the Sumi-owned Cloud browser. It is opt-in:
//
//	SUMI_BROWSER_CLOUD_URL   — origin of the browser Worker (wake target)
//	SUMI_BROWSER_CLOUD_TOKEN — shared runtime secret (>=32 chars) the Worker
//	                            presents on /api/cloud-browser-host/* and the
//	                            API presents on wake; viewer tickets are
//	                            signed with a key derived from it
//
// Checkpoints and the optional Jev key are sealed with a key derived from
// SUMI_MODEL_CONNECTION_KEY. Without the three, the person's routes answer
// {configured:false} and Cloud tabs never become available.
func wireCloudBrowser(pool *pgxpool.Pool, tabs *browsertabs.Store, mux *http.ServeMux, authenticate func(*http.Request) (browseridentity.Identity, error)) (*cloudbrowser.Service, error) {
	service := &cloudbrowser.Service{Authenticate: authenticate}
	wake := strings.TrimSpace(os.Getenv("SUMI_BROWSER_CLOUD_URL"))
	token := strings.TrimSpace(os.Getenv("SUMI_BROWSER_CLOUD_TOKEN"))
	if wake == "" && token == "" {
		service.RegisterRoutes(mux)
		return service, nil
	}
	if wake == "" || token == "" {
		return nil, errors.New("SUMI_BROWSER_CLOUD_URL and SUMI_BROWSER_CLOUD_TOKEN must be set together")
	}
	u, err := url.Parse(wake)
	if err != nil || (u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "127.0.0.1" || u.Hostname() == "localhost"))) || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		// Never echo the value: it may carry credentials.
		return nil, errors.New("SUMI_BROWSER_CLOUD_URL must be an https origin (http only on loopback)")
	}
	if len(token) < 32 {
		return nil, errors.New("SUMI_BROWSER_CLOUD_TOKEN must be at least 32 characters")
	}
	raw := strings.TrimSpace(os.Getenv("SUMI_MODEL_CONNECTION_KEY"))
	key, err := base64.StdEncoding.DecodeString(raw)
	if err != nil || len(key) != 32 {
		return nil, errors.New("the Cloud browser seals checkpoints with a key derived from SUMI_MODEL_CONNECTION_KEY, which must encode 32 bytes")
	}
	defer clear(key)
	if pool == nil || tabs == nil {
		return nil, errors.New("the Cloud browser requires the database and the core browser tab tools")
	}
	store, err := cloudbrowser.New(pool, key, token)
	if err != nil {
		return nil, err
	}
	service.Store, service.RuntimeToken, service.WakeURL = store, token, strings.TrimRight(u.String(), "/")
	tabs.Cloud = true
	service.RegisterRoutes(mux)
	return service, nil
}

func (a *application) startCloudBrowser() {
	if a.cloudBrowser == nil || a.cloudBrowser.Store == nil {
		return
	}
	a.attentionWorkers.Add(1)
	go func() { defer a.attentionWorkers.Done(); a.cloudBrowser.Run(a.backgroundCtx) }()
}
