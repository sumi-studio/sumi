package main

// Real-entrypoint e2e for the file workspace slice: the production
// newApplicationFromEnv constructor — the same path cmd/server's main()
// runs — boots against a real Postgres (migrations applied by the app
// itself) and the fixture's real filesvc. A seeded human/secretary pair
// (koseki auto-register, real employment + direct-chat installation rows)
// drives the session-scoped /files/* routes with a genuinely signed
// cookie, and the secretary's file.* effects execute through the real
// /internal/core HTTP contract with the admin state token.
//
// Gated on SUMI_ENTRYPOINT_E2E=1 plus:
//   SUMI_TEST_DB_URL      maintenance Postgres DSN (a temp db is created)
//   FILEACCESS_E2E_URL    reachable filesvc base URL
//   FILEACCESS_E2E_TOKEN  filesvc token with a * scope grant
// Run inside the fixture origin container where filesvc and PG are
// reachable.

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

func TestEntrypointFileSliceE2E(t *testing.T) {
	if os.Getenv("SUMI_ENTRYPOINT_E2E") != "1" {
		t.Skip("SUMI_ENTRYPOINT_E2E != 1; skipping real-entrypoint e2e")
	}
	filesURL := strings.TrimSpace(os.Getenv("FILEACCESS_E2E_URL"))
	filesToken := strings.TrimSpace(os.Getenv("FILEACCESS_E2E_TOKEN"))
	maintenanceDSN := strings.TrimSpace(os.Getenv("SUMI_TEST_DB_URL"))
	if filesURL == "" || filesToken == "" || maintenanceDSN == "" {
		t.Skip("FILEACCESS_E2E_URL/FILEACCESS_E2E_TOKEN/SUMI_TEST_DB_URL unset")
	}
	ctx := context.Background()

	// Isolated database; the real constructor runs db.Migrate on it.
	maint, err := pgxpool.New(ctx, maintenanceDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer maint.Close()
	suffix := make([]byte, 4)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatal(err)
	}
	dbName := "sumi_entry_" + hex.EncodeToString(suffix)
	if _, err := maint.Exec(ctx, fmt.Sprintf(`CREATE DATABASE "%s"`, dbName)); err != nil {
		t.Fatalf("create db: %v", err)
	}
	defer maint.Exec(context.Background(), fmt.Sprintf(`DROP DATABASE IF EXISTS "%s" WITH (FORCE)`, dbName))
	appDSN := maintenanceDSN[:strings.LastIndex(maintenanceDSN, "/")+1] + dbName
	if i := strings.Index(maintenanceDSN, "?"); i >= 0 {
		appDSN = maintenanceDSN[:strings.LastIndex(maintenanceDSN[:i], "/")+1] + dbName + maintenanceDSN[i:]
	}

	secret := []byte("entrypoint-e2e-session-secret-32!!")
	secretB64 := base64.StdEncoding.EncodeToString(secret)
	coreToken := "entrypoint-e2e-core-admin-token"
	tokenFile := t.TempDir() + "/filesvc-token"
	if err := os.WriteFile(tokenFile, []byte(filesToken), 0o600); err != nil {
		t.Fatal(err)
	}
	// The runtime gateway pins a private 0700 directory owned by the
	// process euid with no entries — create it exactly.
	runtimeDir := t.TempDir() + "/rt"
	if err := os.Mkdir(runtimeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for k, v := range map[string]string{
		"SUMI_DB_URL":                      appDSN,
		"SUMI_COMMAND_LOG_DIR":             t.TempDir(),
		"SUMI_AGENT_RUNTIME_STATE_DIR":     runtimeDir,
		"SUMI_BROWSER_SESSION_SECRET":      secretB64,
		"SUMI_BROWSER_SESSION_AUDIENCE":    "e2e-browser",
		"SUMI_BROWSER_WS_ALLOWED_ORIGINS":  "https://app.example",
		"SUMI_CORE_STATE_TOKEN":            coreToken,
		"SUMI_FILESVC_URL":                 filesURL,
		"SUMI_FILESVC_TOKEN_FILE":          tokenFile,
		"SUMI_MESSAGING_ATTACHMENTS_STORE": "disabled",
	} {
		t.Setenv(k, v)
	}

	app, err := newApplicationFromEnv()
	if err != nil {
		t.Fatalf("newApplicationFromEnv: %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })
	srv := httptest.NewServer(app.publicMux)
	t.Cleanup(srv.Close)

	// Seed a real human + secretary + employment + direct-chat installation
	// through the same store the sign-in flow uses.
	ks := koseki.NewWithWrappingKeyID(app.database.Pool, "e2e-wrap-key")
	reg, err := ks.AutoRegisterWithDisplayName(ctx, "firebase", "e2e-uid-"+dbName, "Entrypoint E2E")
	if err != nil {
		t.Fatalf("auto-register: %v", err)
	}
	var installationID string
	var epoch int64
	if err := app.database.Pool.QueryRow(ctx, `
		SELECT installation_id::text, authority_epoch
		FROM app_installations
		WHERE owner_kind = 'human' AND owner_id = $1 AND app_id = 'direct-chat'`,
		reg.HumanID).Scan(&installationID, &epoch); err != nil {
		t.Fatalf("read installation: %v", err)
	}
	reg2, err := ks.AutoRegisterWithDisplayName(ctx, "firebase", "e2e-uid2-"+dbName, "Entrypoint Other")
	if err != nil {
		t.Fatalf("auto-register other: %v", err)
	}
	var installation2 string
	var epoch2 int64
	if err := app.database.Pool.QueryRow(ctx, `
		SELECT installation_id::text, authority_epoch
		FROM app_installations
		WHERE owner_kind = 'human' AND owner_id = $1 AND app_id = 'direct-chat'`,
		reg2.HumanID).Scan(&installation2, &epoch2); err != nil {
		t.Fatalf("read other installation: %v", err)
	}

	issuer, err := agentevents.NewHMACBrowserSessionIssuer(secret, "e2e-browser")
	if err != nil {
		t.Fatal(err)
	}
	mint := func(humanID, paid string) *http.Cookie {
		signed, err := issuer.IssueSession(ctx, agentevents.UserSessionClaims{
			TenantID: "e2e", UserID: humanID, PersonalityAgentID: paid,
		}, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		return &http.Cookie{Name: agentevents.BrowserSessionCookie, Value: signed}
	}
	cookieA := mint(reg.HumanID, reg.AgentID)
	cookieB := mint(reg2.HumanID, reg2.AgentID)

	scopeQ := fmt.Sprintf("installation_id=%s&authority_epoch=%d", installationID, epoch)
	do := func(cookie *http.Cookie, method, target string, hdrs map[string]string, body io.Reader) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, srv.URL+target, body)
		req.RequestURI = ""
		if cookie != nil {
			req.AddCookie(cookie)
		}
		for k, v := range hdrs {
			req.Header.Set(k, v)
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, target, err)
		}
		defer resp.Body.Close()
		data, _ := io.ReadAll(resp.Body)
		w := httptest.NewRecorder()
		w.Code = resp.StatusCode
		w.Body.Write(data)
		for k := range resp.Header {
			w.Header().Set(k, resp.Header.Get(k))
		}
		return w
	}
	runID := "entry" + dbName[strings.LastIndex(dbName, "_")+1:]

	// Unauthenticated is refused before any scope work.
	if w := do(nil, http.MethodGet, "/files/list?"+scopeQ, nil, nil); w.Code != http.StatusUnauthorized {
		t.Fatalf("no cookie: want 401, got %d %s", w.Code, w.Body)
	}
	// Person A writes through the real session boundary.
	w := do(cookieA, http.MethodPut,
		"/files/write?"+scopeQ+"&path="+url.QueryEscape("entry/hello.txt"),
		map[string]string{"If-Version": "none", "X-Idempotency-Key": runID + ":w1"},
		strings.NewReader("from real server"))
	if w.Code != 200 {
		t.Fatalf("person write: %d %s", w.Code, w.Body)
	}
	// Replayed keyed request returns the receipt through the real route.
	w2 := do(cookieA, http.MethodPut,
		"/files/write?"+scopeQ+"&path="+url.QueryEscape("entry/hello.txt"),
		map[string]string{"If-Version": "none", "X-Idempotency-Key": runID + ":w1"},
		strings.NewReader("from real server"))
	if w2.Code != 200 || !strings.Contains(w2.Body.String(), `"replayed":true`) {
		t.Fatalf("browser keyed replay: %d %s", w2.Code, w2.Body)
	}
	if w = do(cookieA, http.MethodGet,
		"/files/read?"+scopeQ+"&path="+url.QueryEscape("entry/hello.txt"), nil, nil); w.Code != 200 ||
		w.Body.String() != "from real server" {
		t.Fatalf("person read: %d %q", w.Code, w.Body)
	}
	// Person B cannot borrow A's installation: the composite authority
	// check denies before any scope resolution.
	if w = do(cookieB, http.MethodGet,
		"/files/read?"+scopeQ+"&path="+url.QueryEscape("entry/hello.txt"), nil, nil); w.Code != 403 {
		t.Fatalf("foreign session on A's installation must deny: %d %s", w.Code, w.Body)
	}
	// Under B's own installation the request authorizes — but B's session
	// resolves to B's PAID scope, where A's file is simply absent (404).
	scopeQ2 := fmt.Sprintf("installation_id=%s&authority_epoch=%d", installation2, epoch2)
	if w = do(cookieB, http.MethodGet,
		"/files/read?"+scopeQ2+"&path="+url.QueryEscape("entry/hello.txt"), nil, nil); w.Code != 404 {
		t.Fatalf("foreign session must see its own scope only: %d %s", w.Code, w.Body)
	}
	// A bad epoch denies at the real authority boundary.
	if w = do(cookieA, http.MethodGet,
		fmt.Sprintf("/files/read?installation_id=%s&authority_epoch=%d&path=entry/hello.txt",
			installationID, epoch+1), nil, nil); w.Code != 403 {
		t.Fatalf("stale epoch must deny: %d %s", w.Code, w.Body)
	}

	// Core side through the real /internal/core contract: the secretary's
	// persona is the seeded AgentID; the admin token drives the lifecycle.
	core := func(method, target string, body string) (int, map[string]any) {
		req, _ := http.NewRequest(method, srv.URL+"/internal/core/personas"+target,
			strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+coreToken)
		req.Header.Set("Content-Type", "application/json")
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatalf("core %s %s: %v", method, target, err)
		}
		defer resp.Body.Close()
		var out map[string]any
		data, _ := io.ReadAll(resp.Body)
		_ = json.Unmarshal(data, &out)
		return resp.StatusCode, out
	}
	code, out := core(http.MethodPost, "", fmt.Sprintf(
		`{"persona_id":%q,"human_id":%q}`, reg.AgentID, reg.HumanID))
	if code != 201 && code != 200 {
		t.Fatalf("create persona: %d %v", code, out)
	}
	// Tool discovery through the real wiring must list the file effects.
	code, out = core(http.MethodGet, "/"+reg.AgentID+"/tools", "")
	if code != 200 {
		t.Fatalf("list tools: %d %v", code, out)
	}
	tools, _ := out["tools"].([]any)
	seen := map[string]bool{}
	for _, tool := range tools {
		if m, ok := tool.(map[string]any); ok {
			seen[fmt.Sprint(m["tool"])] = true
		} else {
			seen[fmt.Sprint(tool)] = true
		}
	}
	for _, want := range []string{"file.stat", "file.list", "file.read", "file.write", "file.mkdir", "file.remove"} {
		if !seen[want] {
			t.Fatalf("tool discovery missing %s: %v", want, out["tools"])
		}
	}
	// A full claim executes the real file.write effect against filesvc.
	code, out = core(http.MethodPost, "/"+reg.AgentID+"/writer/acquire",
		`{"holder_id":"e2e","ttl_ms":60000}`)
	if code != 200 {
		t.Fatalf("acquire writer: %d %v", code, out)
	}
	gen, _ := out["generation"].(float64)
	code, out = core(http.MethodPost, "/"+reg.AgentID+"/inputs",
		fmt.Sprintf(`{"input_id":%q,"kind":"message","payload":{"text":"write"}}`, runID+":in"))
	if code != 200 && code != 201 {
		t.Fatalf("submit input: %d %v", code, out)
	}
	code, out = core(http.MethodPost, "/"+reg.AgentID+"/turns/load",
		fmt.Sprintf(`{"generation":%v,"turn_id":%q,"context_limit":10}`, gen, runID+":t1"))
	if code != 200 {
		t.Fatalf("load turn: %d %v", code, out)
	}
	code, out = core(http.MethodPost, "/"+reg.AgentID+"/turns/plan",
		fmt.Sprintf(`{"generation":%v,"turn_id":%q,"round":0,"calls":[{"tool":"file.write","route":"normal","request":{"path":"entry/secretary.txt","content_text":"written by core claim"}}]}`, gen, runID+":t1"))
	if code != 200 {
		t.Fatalf("save plan: %d %v", code, out)
	}
	code, out = core(http.MethodPost, "/"+reg.AgentID+"/operations/claim",
		fmt.Sprintf(`{"generation":%v,"operation_id":%q,"turn_id":%q,"tool":"file.write","call_index":0,"request":{"path":"entry/secretary.txt","content_text":"written by core claim"}}`,
			gen, runID+":op1", runID+":t1"))
	if code != 200 {
		t.Fatalf("claim file.write: %d %v", code, out)
	}
	op, _ := out["operation"].(map[string]any)
	resp, _ := op["response"].(map[string]any)
	if op["status"] != "done" || resp["written"] != true {
		t.Fatalf("file.write claim did not execute: %v", out)
	}
	// The person's session reads what the secretary's effect wrote — the
	// full product slice through one real server process.
	w = do(cookieA, http.MethodGet,
		"/files/read?"+scopeQ+"&path="+url.QueryEscape("entry/secretary.txt"), nil, nil)
	if w.Code != 200 || w.Body.String() != "written by core claim" {
		t.Fatalf("person reads secretary write through real server: %d %q", w.Code, w.Body)
	}
	// Claim replay: identical re-claim returns the recorded operation —
	// no second upstream mutation.
	code, out = core(http.MethodPost, "/"+reg.AgentID+"/operations/claim",
		fmt.Sprintf(`{"generation":%v,"operation_id":%q,"turn_id":%q,"tool":"file.write","call_index":0,"request":{"path":"entry/secretary.txt","content_text":"written by core claim"}}`,
			gen, runID+":op1", runID+":t1"))
	if code != 200 || out["fresh"] != false {
		t.Fatalf("claim replay: %d %v", code, out)
	}
}
