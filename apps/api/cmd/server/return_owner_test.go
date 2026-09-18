package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/returnsession"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

// TestReturnOwnerAdapterAndPolicyGate exercises the PRODUCTION owner-proof
// adapter (ownerSessionProof: signed browser cookie + origin + CSRF) wired
// to a service whose file policy is undecided — the real deployed shape
// while the user's file decision is open. The auth is the genuine HMAC
// verifier, only the signing secret is a test key.
func TestReturnOwnerAdapterAndPolicyGate(t *testing.T) {
	pool := testdb.Create(t)
	if err := db.Migrate(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	human, persona := uuid.Must(uuid.NewV7()).String(), uuid.Must(uuid.NewV7()).String()
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO humans (human_id) VALUES ($1)`, human); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO core_personas (persona_id, human_id, display_name, authority)
		 VALUES ($1, $2, 'sec', 'active')`, persona, human); err != nil {
		t.Fatal(err)
	}
	sessions, err := agentevents.NewHMACUserSessionVerifier(
		testSessionSecret, "", testBrowserSessionRevocationStore(t))
	if err != nil {
		t.Fatal(err)
	}
	cookie, err := sessions.IssueSession(context.Background(), agentevents.UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             human,
		PersonalityAgentID: persona,
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	// Production shape: routes mounted, Config{} — undecided file policy.
	svc := returnsession.New(pool, returnsession.Config{})
	srv := httptest.NewServer(nil)
	t.Cleanup(srv.Close)
	server, err := returnsession.NewServer(svc,
		ownerSessionProof{sessions: sessions, origins: []string{testBrowserOrigin}}, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	srv.Config.Handler = mux

	csrf := base64.RawURLEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	call := func(method, path string, origin string, withCSRF, withCookie bool) (int, []byte) {
		t.Helper()
		req, err := http.NewRequest(method, srv.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		if withCookie {
			req.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: cookie})
		}
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		if withCSRF {
			req.Header.Set("X-CSRF-Token", csrf)
			req.AddCookie(&http.Cookie{Name: agentevents.BrowserCSRFCookie, Value: csrf})
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		raw, _ := io.ReadAll(res.Body)
		return res.StatusCode, raw
	}

	// Mutating calls require the full browser proof: cookie + origin + CSRF.
	if code, _ := call(http.MethodPost, "/api/secretary-return/sessions", testBrowserOrigin, true, false); code != http.StatusUnauthorized {
		t.Fatalf("no cookie: %d", code)
	}
	if code, _ := call(http.MethodPost, "/api/secretary-return/sessions", "https://evil.example", true, true); code != http.StatusUnauthorized {
		t.Fatalf("foreign origin: %d", code)
	}
	if code, _ := call(http.MethodPost, "/api/secretary-return/sessions", testBrowserOrigin, false, true); code != http.StatusUnauthorized {
		t.Fatalf("no CSRF: %d", code)
	}
	// Fully authenticated, still refused: the file policy is undecided and
	// the refusal carries the machine marker the web client renders.
	code, raw := call(http.MethodPost, "/api/secretary-return/sessions", testBrowserOrigin, true, true)
	if code != http.StatusConflict {
		t.Fatalf("authenticated create: %d %s", code, raw)
	}
	var e struct {
		Error string `json:"error"`
		Code  string `json:"code"`
	}
	if err := json.Unmarshal(raw, &e); err != nil || e.Code != "file_policy_undecided" {
		t.Fatalf("policy marker: %s", raw)
	}
	// Nothing moved: the persona is still active, no session row exists.
	var authority string
	if err := pool.QueryRow(context.Background(),
		`SELECT authority FROM core_personas WHERE persona_id = $1`, persona).Scan(&authority); err != nil || authority != "active" {
		t.Fatalf("authority: %v %s", err, authority)
	}
	var open int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM return_sessions WHERE persona_id = $1`, persona).Scan(&open); err != nil || open != 0 {
		t.Fatalf("sessions: %v %d", err, open)
	}
	// The owner's session read needs only the cookie (a GET — no CSRF) and
	// honestly answers that no session exists.
	if code, _ := call(http.MethodGet, "/api/secretary-return/session", "", false, true); code != http.StatusNotFound {
		t.Fatalf("owner read: %d", code)
	}
}
