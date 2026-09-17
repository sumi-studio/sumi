// Command state-dev serves only the agentstate core-state contract against a
// Postgres database. It exists for local development and isolated tests — the
// same handler is mounted on the API mux when SUMI_CORE_STATE_TOKEN is set —
// and is the honest shape of the Local deployment's state service: local PG,
// same Go service, same schema. The transfer routes (internal/portable) are
// mounted alongside, under the same admin secret.
//
// Env:
//
//	SUMI_DB_URL            postgres connection string (required)
//	SUMI_CORE_STATE_TOKEN  admin/service bearer; also mints persona tokens (required)
//	SUMI_STATE_LISTEN      listen address (default 127.0.0.1:8180)
//	SUMI_MODEL_CONNECTION_KEY  base64-encoded 32-byte key for the user
//	                         model-connection store; absent = metadata-only
//	SUMI_DB_CREATE=1       create the database named in SUMI_DB_URL if absent
//	SUMI_CORE_RUNTIME_TOKEN  optional runtime credential for a Cloud-shaped
//	                         core host (persona-scoped routes of any persona)
//	SUMI_CORE_WAKE_URL / SUMI_CORE_WAKE_TOKEN  optional: wake that core host
//	                         for personas with work and no live writer
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/fileaccess"
	"github.com/sumi-studio/sumi/apps/api/internal/modelconnections"
	"github.com/sumi-studio/sumi/apps/api/internal/portable"
)

func main() {
	databaseURL := os.Getenv("SUMI_DB_URL")
	if databaseURL == "" {
		log.Fatal("SUMI_DB_URL is required")
	}
	token := os.Getenv("SUMI_CORE_STATE_TOKEN")
	if len(token) < 16 {
		log.Fatal("SUMI_CORE_STATE_TOKEN is required (>=16 chars)")
	}
	listen := os.Getenv("SUMI_STATE_LISTEN")
	if listen == "" {
		listen = "127.0.0.1:8180"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if os.Getenv("SUMI_DB_CREATE") == "1" {
		if err := ensureDatabase(ctx, databaseURL); err != nil {
			log.Fatalf("create database: %v", err)
		}
	}
	pool, err := db.Open(ctx, databaseURL)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool.Pool); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	mux := http.NewServeMux()
	coreState := agentstate.NewServer(pool.Pool, token)
	var conns *modelconnections.Store
	if raw := strings.TrimSpace(os.Getenv("SUMI_MODEL_CONNECTION_KEY")); raw != "" {
		key, err := base64.StdEncoding.DecodeString(raw)
		if err != nil || len(key) != 32 {
			log.Fatal("SUMI_MODEL_CONNECTION_KEY must encode 32 bytes")
		}
		conns, err = modelconnections.New(pool.Pool, key)
		if err != nil {
			log.Fatalf("model connection store: %v", err)
		}
		coreState.SetModelConnections(conns)
	} else {
		conns = modelconnections.MetadataOnly(pool.Pool)
		coreState.SetModelConnections(conns)
	}
	if rt := strings.TrimSpace(os.Getenv(agentstate.RuntimeTokenEnv)); rt != "" {
		if err := coreState.SetRuntimeToken(rt); err != nil {
			log.Fatal(err)
		}
	}
	waker, err := agentstate.RuntimeWakerFromEnv(coreState.Store(), os.Getenv)
	if err != nil {
		log.Fatal(err)
	}
	if waker != nil {
		log.Printf("core wake: sweeping for personas awaiting a runtime; waking %s", waker.Target())
		go waker.Run(context.Background())
	}
	// Canonical workspace files: when a filesvc endpoint is configured the
	// file.* effects are claimable, scoped to the claiming persona — the
	// same wiring cmd/server applies on the public mux. Unset config leaves
	// the tools unregistered (discovery and execution agree).
	filesClient, err := fileaccess.FromEnv(os.Getenv)
	if err != nil {
		log.Fatal(err)
	}
	if filesClient != nil {
		for tool, effect := range fileaccess.FileEffects(filesClient) {
			if err := coreState.RegisterToolEffect(tool, effect); err != nil {
				log.Fatalf("register core file effect %s: %v", tool, err)
			}
		}
		log.Print("core file tools ready (file.* effects scoped to the claiming persona)")
	}
	coreState.RegisterRoutes(mux)
	portable.NewServer(pool.Pool, token).RegisterRoutes(mux)
	// Dev-only fixture seeding: humans are provisioned by the account flow
	// on a real deployment; a local/test placement needs a row to bind a
	// persona to. Admin-token guarded like every internal route.
	mux.HandleFunc("POST /internal/dev/humans", func(w http.ResponseWriter, r *http.Request) {
		h := r.Header.Get("Authorization")
		if h != "Bearer "+token {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		var b struct {
			HumanID string `json:"human_id"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&b); err != nil || !uuidv7Re.MatchString(b.HumanID) {
			http.Error(w, `{"error":"human_id must be a uuidv7"}`, http.StatusBadRequest)
			return
		}
		if _, err := pool.Exec(r.Context(),
			`INSERT INTO humans (human_id) VALUES ($1) ON CONFLICT DO NOTHING`, b.HumanID); err != nil {
			http.Error(w, `{"error":"insert failed"}`, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"human_id":"` + b.HumanID + `"}`))
	})
	// Dev-only fixture seeding for model connections: the real product
	// flow validates transport (https, non-loopback) before saving; a
	// local/test placement must be able to point a seeded connection at a
	// loopback stub. SaveUnchecked still seals the credential through the
	// armed store, so a working binding needs SUMI_MODEL_CONNECTION_KEY.
	mux.HandleFunc("POST /internal/dev/model-connections", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		var b struct {
			HumanID         string            `json:"human_id"`
			ConnectionID    string            `json:"connection_id"`
			Name            string            `json:"name"`
			Preset          string            `json:"preset"`
			BaseURL         string            `json:"base_url"`
			Model           string            `json:"model"`
			APIKey          string            `json:"api_key"`
			ExtraHeaders    map[string]string `json:"extra_headers"`
			MaxOutputTokens *int              `json:"max_output_tokens"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&b); err != nil || !uuidv7Re.MatchString(b.HumanID) {
			http.Error(w, `{"error":"human_id must be a uuidv7"}`, http.StatusBadRequest)
			return
		}
		var key *string
		if b.APIKey != "" {
			key = &b.APIKey
		}
		c, err := conns.SaveUnchecked(r.Context(), b.HumanID, b.ConnectionID, modelconnections.Input{
			Name: b.Name, Preset: b.Preset, BaseURL: b.BaseURL, Model: b.Model, APIKey: key, ExtraHeaders: b.ExtraHeaders,
			MaxOutputTokens: b.MaxOutputTokens,
		})
		if err != nil {
			http.Error(w, `{"error":`+strconv.Quote(err.Error())+`}`, http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"connection_id": c.ID})
	})
	mux.HandleFunc("POST /internal/dev/model-selections", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		var b struct {
			HumanID      string `json:"human_id"`
			Kind         string `json:"kind"`
			ConnectionID string `json:"connection_id"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&b); err != nil || !uuidv7Re.MatchString(b.HumanID) {
			http.Error(w, `{"error":"human_id must be a uuidv7"}`, http.StatusBadRequest)
			return
		}
		if err := conns.Select(r.Context(), b.HumanID, modelconnections.Selection{
			Kind: b.Kind, ConnectionID: b.ConnectionID,
		}); err != nil {
			http.Error(w, `{"error":`+strconv.Quote(err.Error())+`}`, http.StatusBadRequest)
			return
		}
		// Mirror the production wiring (modelConnectionService.Changed →
		// usageService.FundingChanged): a selection change re-resolves
		// funding, so this human's budget-parked inputs resume — the next
		// attempt may admit under a different source.
		if _, err := coreState.Store().ResumeWaitsForHuman(r.Context(), b.HumanID); err != nil {
			http.Error(w, `{"error":`+strconv.Quote(err.Error())+`}`, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	// Dev-only fixture seeding for usage budgets and Sumi funding grants.
	// The product surface configures budgets through the authenticated
	// /api/usage routes; tests need the same rows without a browser
	// session, so these are admin-token guarded like every internal route.
	mux.HandleFunc("PUT /internal/dev/usage-budgets", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		var b struct {
			FundingKind       string `json:"funding_kind"`
			FundingID         string `json:"funding_id"`
			LimitMinor        int64  `json:"limit_minor"`
			Currency          string `json:"currency"`
			RateInputPerMTok  int64  `json:"rate_input_per_mtok"`
			RateOutputPerMTok int64  `json:"rate_output_per_mtok"`
			PricingRevision   string `json:"pricing_revision"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&b); err != nil {
			http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
			return
		}
		budget, err := coreState.Store().SetBudgetAdmin(r.Context(), b.FundingKind, b.FundingID, agentstate.UsageBudget{
			LimitMinor:        b.LimitMinor,
			Currency:          b.Currency,
			RateInputPerMTok:  b.RateInputPerMTok,
			RateOutputPerMTok: b.RateOutputPerMTok,
			PricingRevision:   b.PricingRevision,
		})
		if err != nil {
			http.Error(w, `{"error":`+strconv.Quote(err.Error())+`}`, http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"budget": budget})
	})
	mux.HandleFunc("DELETE /internal/dev/usage-budgets", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		var b struct {
			FundingKind string `json:"funding_kind"`
			FundingID   string `json:"funding_id"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&b); err != nil {
			http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
			return
		}
		if err := coreState.Store().ClearBudgetAdmin(r.Context(), b.FundingKind, b.FundingID); err != nil {
			http.Error(w, `{"error":`+strconv.Quote(err.Error())+`}`, http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /internal/dev/usage-grants", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		var b struct {
			FundingID string `json:"funding_id"`
			HumanID   string `json:"human_id"`
			Label     string `json:"label"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&b); err != nil || !uuidv7Re.MatchString(b.HumanID) {
			http.Error(w, `{"error":"human_id must be a uuidv7"}`, http.StatusBadRequest)
			return
		}
		if _, err := pool.Exec(r.Context(), `
			INSERT INTO usage_funding_grants (funding_id, human_id, label)
			VALUES ($1, $2, $3)
			ON CONFLICT (funding_id) DO UPDATE SET human_id = $2, label = $3, revoked_at = NULL`,
			b.FundingID, b.HumanID, b.Label); err != nil {
			http.Error(w, `{"error":`+strconv.Quote(err.Error())+`}`, http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"funding_id": b.FundingID})
	})
	mux.HandleFunc("DELETE /internal/dev/usage-grants/{id}", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		if _, err := pool.Exec(r.Context(),
			`UPDATE usage_funding_grants SET revoked_at = now() WHERE funding_id = $1 AND revoked_at IS NULL`,
			r.PathValue("id")); err != nil {
			http.Error(w, `{"error":`+strconv.Quote(err.Error())+`}`, http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	log.Printf("state-dev listening on http://%s (persona-scoped core state)", listen)
	log.Fatal(http.ListenAndServe(listen, mux))
}

var (
	dbNameRe = regexp.MustCompile(`^[a-z0-9_]{1,63}$`)
	uuidv7Re = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
)

// ensureDatabase creates the target database through the server's
// maintenance database, so a fresh Local placement or test run needs no
// separate psql step.
func ensureDatabase(ctx context.Context, databaseURL string) error {
	cfg, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return err
	}
	name := cfg.Database
	if !dbNameRe.MatchString(name) {
		return fmt.Errorf("database name %q must match %s", name, dbNameRe)
	}
	cfg.Database = "postgres"
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	var exists bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, name).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}
	_, err = conn.Exec(ctx, `CREATE DATABASE "`+name+`"`)
	return err
}
