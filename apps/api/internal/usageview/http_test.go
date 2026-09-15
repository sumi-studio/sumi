package usageview

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/chatgpt"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

func pid(t *testing.T) string {
	t.Helper()
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuidv7: %v", err)
	}
	return id.String()
}

type fixture struct {
	srv  *httptest.Server
	pool *pgxpool.Pool
	// asHuman selects which session the next request authenticates as.
	asHuman *string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	pool := testdb.Create(t)
	if err := db.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	f := &fixture{pool: pool, asHuman: new(string)}
	svc := &Service{
		Store: agentstate.NewStore(pool),
		Authenticate: func(*http.Request) (chatgpt.LoginIdentity, error) {
			if *f.asHuman == "" {
				return chatgpt.LoginIdentity{}, http.ErrNoCookie
			}
			return chatgpt.LoginIdentity{
				HumanID:   *f.asHuman,
				SessionID: "s-1",
				Authorize: func(ctx context.Context, fn func(context.Context) error) error {
					return fn(ctx)
				},
			}, nil
		},
	}
	mux := http.NewServeMux()
	svc.RegisterRoutes(mux)
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fixture) human(t *testing.T) string {
	t.Helper()
	id := pid(t)
	if _, err := f.pool.Exec(context.Background(),
		"INSERT INTO humans(human_id) VALUES($1)", id); err != nil {
		t.Fatalf("insert human: %v", err)
	}
	return id
}

func (f *fixture) connection(t *testing.T, humanID string) string {
	t.Helper()
	connID := pid(t)
	if _, err := f.pool.Exec(context.Background(), `
		INSERT INTO model_api_connections
			(human_id, connection_id, name, preset, base_url, model,
			 credential_ciphertext, version)
		VALUES ($1, $2::uuid, 'My API', 'openai-chat',
			'https://provider.example/v1', 'fixture-model', 'key', $3::uuid)`,
		humanID, connID, pid(t)); err != nil {
		t.Fatalf("insert connection: %v", err)
	}
	return connID
}

func (f *fixture) persona(t *testing.T, humanID string) string {
	t.Helper()
	id := pid(t)
	if _, err := f.pool.Exec(context.Background(), `
		INSERT INTO core_personas (persona_id, human_id) VALUES ($1, $2)`,
		id, humanID); err != nil {
		t.Fatalf("insert persona: %v", err)
	}
	return id
}

func (f *fixture) call(t *testing.T, method, path, body string) (int, map[string]any) {
	t.Helper()
	var reader *strings.Reader
	if body != "" {
		reader = strings.NewReader(body)
	} else {
		reader = strings.NewReader("")
	}
	req, err := http.NewRequest(method, f.srv.URL+path, reader)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer res.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(res.Body).Decode(&out)
	return res.StatusCode, out
}

const budgetBody = `{"limit_minor":100,"currency":"USD",` +
	`"rate_input_per_mtok":1000000,"rate_output_per_mtok":2000000,` +
	`"pricing_revision":"fixture-rates-v1"}`

// The overview lists the human's own connections and live grants, with
// budgets and totals; unauthenticated requests are refused.
func TestUsageOverviewAuthAndSources(t *testing.T) {
	f := newFixture(t)
	human := f.human(t)
	conn := f.connection(t, human)
	f.persona(t, human)
	if _, err := f.pool.Exec(context.Background(), `
		INSERT INTO usage_funding_grants (funding_id, human_id, label)
		VALUES ('sumi-alpha', $1, 'Sumi allocation')`, human); err != nil {
		t.Fatalf("grant: %v", err)
	}
	// Another human's connection must never appear here.
	other := f.human(t)
	f.connection(t, other)

	if status, _ := f.call(t, "GET", "/api/usage", ""); status != 401 {
		t.Fatalf("unauthenticated overview status=%d, want 401", status)
	}
	*f.asHuman = human
	status, body := f.call(t, "GET", "/api/usage", "")
	if status != 200 {
		t.Fatalf("overview status=%d body=%v", status, body)
	}
	sources, _ := body["sources"].([]any)
	if len(sources) != 2 {
		t.Fatalf("sources=%v, want the connection and the grant", sources)
	}
	var connView, grantView map[string]any
	for _, s := range sources {
		v := s.(map[string]any)
		switch v["kind"] {
		case "connection":
			connView = v
		case "sumi":
			grantView = v
		}
	}
	if connView == nil || connView["id"] != conn || connView["model"] != "fixture-model" {
		t.Fatalf("connection source: %v", connView)
	}
	if grantView == nil || grantView["id"] != "sumi-alpha" || grantView["grant"] != true {
		t.Fatalf("grant source: %v", grantView)
	}
	if _, ok := body["waits"].([]any); !ok {
		t.Fatalf("waits missing: %v", body)
	}
}

// Budget set/clear is owner-only: another human's connection is
// forbidden, a 'sumi' allocation's budget is the funder's, never the
// grantee's self-service.
func TestUsageBudgetAuthorization(t *testing.T) {
	f := newFixture(t)
	human := f.human(t)
	conn := f.connection(t, human)
	other := f.human(t)
	otherConn := f.connection(t, other)
	if _, err := f.pool.Exec(context.Background(), `
		INSERT INTO usage_funding_grants (funding_id, human_id, label)
		VALUES ('sumi-alpha', $1, 'Sumi allocation')`, human); err != nil {
		t.Fatalf("grant: %v", err)
	}
	*f.asHuman = human

	// Own connection: set, then clear.
	if status, body := f.call(t, "PUT",
		"/api/usage/funding/connection/"+conn+"/budget", budgetBody); status != 200 {
		t.Fatalf("set own budget status=%d body=%v", status, body)
	}
	if status, _ := f.call(t, "DELETE",
		"/api/usage/funding/connection/"+conn+"/budget", ""); status != 204 {
		t.Fatalf("clear own budget status=%d", status)
	}
	// Another human's connection: 403 (it exists, just not theirs).
	if status, _ := f.call(t, "PUT",
		"/api/usage/funding/connection/"+otherConn+"/budget", budgetBody); status != 403 {
		t.Fatalf("other's budget status=%d, want 403", status)
	}
	// A grantee cannot configure the Sumi allocation's budget — it is the
	// funder's knob.
	if status, _ := f.call(t, "PUT",
		"/api/usage/funding/sumi/sumi-alpha/budget", budgetBody); status != 403 {
		t.Fatalf("sumi budget status=%d, want 403", status)
	}
	// A connection that does not exist anywhere: 404.
	if status, _ := f.call(t, "PUT",
		"/api/usage/funding/connection/"+pid(t)+"/budget", budgetBody); status != 404 {
		t.Fatalf("unknown funding status=%d, want 404", status)
	}
	// Malformed bodies and unknown fields are rejected.
	if status, _ := f.call(t, "PUT",
		"/api/usage/funding/connection/"+conn+"/budget",
		`{"limit_minor":1,"currency":"USD","bogus":true}`); status != 400 {
		t.Fatalf("unknown-field budget status=%d, want 400", status)
	}
	if status, _ := f.call(t, "PUT",
		"/api/usage/funding/connection/"+conn+"/budget",
		budgetBody+` {}`); status != 400 {
		t.Fatalf("trailing-json budget status=%d, want 400", status)
	}
}

// Facts are filtered to the owned funding source; another human's ledger
// is unreachable.
func TestUsageFactsAuthorization(t *testing.T) {
	f := newFixture(t)
	human := f.human(t)
	conn := f.connection(t, human)
	pa := f.persona(t, human)
	other := f.human(t)
	otherConn := f.connection(t, other)
	otherPa := f.persona(t, other)

	store := agentstate.NewStore(f.pool)
	ctx := context.Background()
	in, out := int64(10), int64(4)
	for _, rec := range []struct {
		pa, fact, conn string
	}{{pa, "f-mine", conn}, {otherPa, "f-theirs", otherConn}} {
		if _, _, err := store.RecordUsage(ctx, rec.pa, agentstate.UsageRecordRequest{
			FactID: rec.fact, Kind: "model_call", Phase: "turn",
			Funding:     agentstate.FundingRef{Kind: "connection", ID: rec.conn},
			Status:      "reported",
			InputTokens: &in, OutputTokens: &out, Quantities: map[string]any{},
		}); err != nil {
			t.Fatalf("seed fact %s: %v", rec.fact, err)
		}
	}
	*f.asHuman = human
	status, body := f.call(t, "GET",
		"/api/usage/funding/connection/"+conn+"/facts", "")
	if status != 200 {
		t.Fatalf("own facts status=%d body=%v", status, body)
	}
	facts, _ := body["facts"].([]any)
	if len(facts) != 1 || facts[0].(map[string]any)["fact_id"] != "f-mine" {
		t.Fatalf("own facts: %v", facts)
	}
	if status, _ := f.call(t, "GET",
		"/api/usage/funding/connection/"+otherConn+"/facts", ""); status != 403 {
		t.Fatalf("other's facts status=%d, want 403", status)
	}
}
