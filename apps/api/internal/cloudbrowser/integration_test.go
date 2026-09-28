package cloudbrowser

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/browseridentity"
	"github.com/sumi-studio/sumi/apps/api/internal/browsertabs"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

const (
	owner   = "0198f0f4-9b72-7000-8000-000000000801"
	other   = "0198f0f4-9b72-7000-8000-000000000802"
	persona = "0198f0f4-9b72-7000-8000-000000000803"
	foreign = "0198f0f4-9b72-7000-8000-000000000804"
	runtime = "cloud-browser-test-runtime-token-0123456789"
)

type fixture struct {
	t       *testing.T
	store   *Store
	service *Service
	tabs    *browsertabs.Store
	core    *agentstate.Server
	url     string
}

func setup(t *testing.T) *fixture {
	pool := testdb.Create(t)
	ctx := context.Background()
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{owner, other} {
		if _, err := pool.Exec(ctx, `INSERT INTO humans(human_id)VALUES($1)`, id); err != nil {
			t.Fatal(err)
		}
	}
	core := agentstate.NewServer(pool, "cloud-browser-fixture-admin-credential")
	for p, h := range map[string]string{persona: owner, foreign: other} {
		if _, _, err := core.Store().EnsurePersona(ctx, p, &h, "Browser secretary"); err != nil {
			t.Fatal(err)
		}
	}
	tabs := browsertabs.New(pool, core.Store())
	tabs.Cloud = true
	for name, effect := range tabs.Effects() {
		if err := core.RegisterToolEffect(name, effect); err != nil {
			t.Fatal(err)
		}
	}
	key := bytes.Repeat([]byte{7}, 32)
	store, err := New(pool, key, runtime)
	if err != nil {
		t.Fatal(err)
	}
	auth := func(r *http.Request) (browseridentity.Identity, error) {
		h := r.Header.Get("Test-Human")
		if h != owner && h != other {
			return browseridentity.Identity{}, fmt.Errorf("unauthorized")
		}
		return browseridentity.Identity{HumanID: h, Authorize: func(ctx context.Context, effect func(context.Context) error) error { return effect(ctx) }}, nil
	}
	service := &Service{Store: store, Authenticate: auth, RuntimeToken: runtime}
	mux := http.NewServeMux()
	core.RegisterRoutes(mux)
	(&browsertabs.Service{Store: tabs, Authenticate: auth}).RegisterRoutes(mux)
	service.RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return &fixture{t, store, service, tabs, core, server.URL}
}

func (f *fixture) call(method, path, auth string, in any, headers ...string) (int, map[string]any) {
	f.t.Helper()
	var body io.Reader
	if in != nil {
		raw, _ := json.Marshal(in)
		body = bytes.NewReader(raw)
	}
	req, _ := http.NewRequest(method, f.url+path, body)
	switch auth {
	case owner, other:
		req.Header.Set("Test-Human", auth)
	case "":
	default:
		req.Header.Set("Authorization", "Bearer "+auth)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	defer res.Body.Close()
	out := map[string]any{}
	_ = json.NewDecoder(res.Body).Decode(&out)
	return res.StatusCode, out
}

func (f *fixture) profile() string {
	f.t.Helper()
	status, out := f.call("POST", "/api/cloud-browser/profiles", owner, map[string]any{"persona_id": persona})
	if status != 200 {
		f.t.Fatalf("create profile %d %v", status, out)
	}
	return out["profile"].(map[string]any)["profile_id"].(string)
}

func (f *fixture) grant(profile, tab string, write bool) string {
	f.t.Helper()
	status, out := f.call("POST", "/api/cloud-browser/profiles/"+profile+"/grants", owner, map[string]any{"tab_id": tab, "name": "Cloud tab", "allow_actions": write})
	if status != 200 {
		f.t.Fatalf("grant %d %v", status, out)
	}
	return out["grant"].(map[string]any)["attachment_id"].(string)
}

func (f *fixture) begin(profile string, fresh bool, incarnation int64) (int, HostSession) {
	f.t.Helper()
	req, _ := http.NewRequest("POST", f.url+"/api/cloud-browser-host/profiles/"+profile+"/begin", strings.NewReader(fmt.Sprintf(`{"fresh":%v,"incarnation":%d}`, fresh, incarnation)))
	req.Header.Set("Authorization", "Bearer "+runtime)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	defer res.Body.Close()
	var s HostSession
	_ = json.NewDecoder(res.Body).Decode(&s)
	return res.StatusCode, s
}

func (f *fixture) browserTabs() []map[string]any {
	f.t.Helper()
	tx, err := f.store.Pool.Begin(context.Background())
	if err != nil {
		f.t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	out, err := f.tabs.Effects()["browser.tabs"].Apply(context.Background(), tx, persona, uuid.NewString()+":tool:0", map[string]any{})
	if err != nil {
		f.t.Fatal(err)
	}
	list := []map[string]any{}
	for _, tab := range out["tabs"].([]map[string]any) {
		list = append(list, tab)
	}
	return list
}

func (f *fixture) enqueue(attachment, method string) string {
	f.t.Helper()
	ctx := context.Background()
	tx, err := f.store.Pool.Begin(ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	req := map[string]any{"attachment_id": attachment}
	if method == "goal" {
		req["goal"] = "Open the docs page"
	}
	out, err := f.tabs.Effects()["browser."+method].Apply(ctx, tx, persona, uuid.NewString()+":tool:0", req)
	if err != nil {
		f.t.Fatalf("enqueue %s: %v", method, err)
	}
	if err := tx.Commit(ctx); err != nil {
		f.t.Fatal(err)
	}
	return out["job"].(map[string]any)["job_id"].(string)
}

func TestProfileGrantAvailabilityAndJev(t *testing.T) {
	f := setup(t)
	profile := f.profile()
	if again := f.profile(); again != profile {
		t.Fatalf("one profile per secretary: %s then %s", profile, again)
	}
	if status, _ := f.call("POST", "/api/cloud-browser/profiles", other, map[string]any{"persona_id": persona}); status != 403 {
		t.Fatalf("another human's secretary: %d", status)
	}
	if status, _ := f.call("POST", "/api/cloud-browser/profiles/"+profile+"/viewer-ticket", other, nil); status != 404 {
		t.Fatalf("another human's ticket: %d", status)
	}
	tab := uuid.NewString()
	attachment := f.grant(profile, tab, true)
	list := f.browserTabs()
	if len(list) != 1 || list[0]["attachment_id"] != attachment || list[0]["available"] != true {
		t.Fatalf("a sleeping Cloud tab is available (wakeable): %v", list)
	}
	if layers := list[0]["operation_layers"].(map[string]any); layers["jev"] != "not_configured" || layers["direct"] != true {
		t.Fatalf("direct path without a Jev key: %v", layers)
	}
	if status, out := f.call("PUT", "/api/cloud-browser/jev-key", owner, map[string]any{"api_key": "jev-synthetic-key-123"}); status != 200 || out["jev"].(map[string]any)["configured"] != true {
		t.Fatalf("set jev key %d %v", status, out)
	}
	_, state := f.call("GET", "/api/cloud-browser", owner, nil)
	if raw, _ := json.Marshal(state); bytes.Contains(raw, []byte("jev-synthetic-key-123")) {
		t.Fatal("the Jev key is write-only to people")
	}
	if layers := f.browserTabs()[0]["operation_layers"].(map[string]any); layers["jev"] != "available" {
		t.Fatalf("jev with a stored key: %v", layers)
	}
	var sealed []byte
	if err := f.store.Pool.QueryRow(context.Background(), `SELECT sealed FROM cloud_browser_jev_credentials WHERE human_id=$1`, owner).Scan(&sealed); err != nil || bytes.Contains(sealed, []byte("jev-synthetic")) {
		t.Fatalf("the key is stored sealed: %v", err)
	}
	status, s := f.begin(profile, true, 0)
	if status != 200 || s.JevKey != "jev-synthetic-key-123" || s.Incarnation != 1 || len(s.Attachments) != 1 {
		t.Fatalf("begin %d incarnation=%d attachments=%d jev=%v", status, s.Incarnation, len(s.Attachments), s.JevKey != "")
	}
	// A running browser offers Jev only once it declares the key it received.
	hostToken := s.Attachments[0].HostToken
	for _, declared := range []bool{false, true} {
		if status, _ := f.call("POST", "/api/browser-host/tabs/"+attachment+"/poll", hostToken, map[string]bool{"jev": declared}); status != 200 {
			t.Fatalf("poll %d", status)
		}
		want := map[bool]string{false: "not_configured", true: "available"}[declared]
		if layers := f.browserTabs()[0]["operation_layers"].(map[string]any); layers["jev"] != want {
			t.Fatalf("live browser declaring jev=%v: %v", declared, layers)
		}
	}
	if status, out := f.call("POST", "/api/cloud-browser-host/profiles/"+profile+"/jev-rejected", runtime, map[string]any{"key_version": s.JevKeyVersion}); status != 200 || out["rejected"] != true {
		t.Fatalf("jev rejected %d %v", status, out)
	}
	if layers := f.browserTabs()[0]["operation_layers"].(map[string]any); layers["jev"] != "not_configured" {
		t.Fatalf("a rejected key is not offered: %v", layers)
	}
	// Local hosts cannot claim the Cloud tab namespace.
	status, _ = f.call("POST", "/api/browser-tabs", owner, map[string]any{"persona_id": persona, "name": "x", "tab": map[string]string{"runtimeId": profile, "profileId": "cloud", "tabId": uuid.NewString()}, "allow_actions": true})
	if status != 400 {
		t.Fatalf("local attach into the cloud namespace: %d", status)
	}
}

func TestIncarnationsTokensAndSnapshots(t *testing.T) {
	f := setup(t)
	profile := f.profile()
	tab := uuid.NewString()
	attachment := f.grant(profile, tab, true)
	status, s1 := f.begin(profile, true, 0)
	if status != 200 || s1.Incarnation != 1 || s1.Snapshot != nil {
		t.Fatalf("first begin %d %+v", status, s1)
	}
	token1 := s1.Attachments[0].HostToken
	if status, _ := f.call("POST", "/api/browser-host/tabs/"+attachment+"/poll", token1, map[string]bool{"jev": false}); status != 200 {
		t.Fatalf("rotated token polls: %d", status)
	}
	payload := json.RawMessage(`{"version":1,"cookies":[{"name":"sid","value":"secret-cookie-value"}],"tabs":[{"slot":"` + tab + `","url":"http://app.sumi-fixture.test/"}]}`)
	save := func(incarnation, seq int64) int {
		status, _ := f.call("POST", "/api/cloud-browser-host/profiles/"+profile+"/snapshot", runtime, map[string]any{"incarnation": incarnation, "seq": seq, "version": 1, "tab_ids": []string{tab}, "snapshot": payload})
		return status
	}
	if save(1, 1) != 200 || save(1, 1) != 409 || save(0, 2) != 409 || save(1, 2) != 200 {
		t.Fatal("checkpoints are monotonic per current incarnation")
	}
	var sealed []byte
	f.store.Pool.QueryRow(context.Background(), `SELECT snapshot FROM cloud_browser_profiles WHERE profile_id=$1`, profile).Scan(&sealed)
	if bytes.Contains(sealed, []byte("secret-cookie-value")) {
		t.Fatal("checkpoint must be sealed")
	}
	// Reconnect to the live browser: same incarnation, tokens rotate, no snapshot.
	if status, _ := f.begin(profile, false, 7); status != 409 {
		t.Fatalf("reconnect with a wrong incarnation: %d", status)
	}
	status, s2 := f.begin(profile, false, 1)
	if status != 200 || s2.Incarnation != 1 || s2.SnapshotSeq != 2 || !bytes.Contains(s2.Snapshot, []byte("secret-cookie-value")) {
		t.Fatalf("reconnect %d %+v", status, s2)
	}
	if status, _ := f.call("POST", "/api/browser-host/tabs/"+attachment+"/poll", token1, nil); status != 403 {
		t.Fatalf("the previous host instance's token is dead: %d", status)
	}
	// Restore: a new incarnation receives the canonical checkpoint.
	status, s3 := f.begin(profile, true, 0)
	if status != 200 || s3.Incarnation != 2 || s3.SnapshotSeq != 2 || !bytes.Contains(s3.Snapshot, []byte("secret-cookie-value")) {
		t.Fatalf("restore begin %d incarnation=%d seq=%d", status, s3.Incarnation, s3.SnapshotSeq)
	}
	if save(1, 3) != 409 {
		t.Fatal("a checkpoint from the older incarnation is stale")
	}
	token3 := s3.Attachments[0].HostToken
	// Refresh keeps known grants' tokens (in-flight receipts still land) and
	// delivers new grants.
	tab2 := uuid.NewString()
	attachment2 := f.grant(profile, tab2, false)
	status, out := f.call("POST", "/api/cloud-browser-host/profiles/"+profile+"/refresh", runtime, map[string]any{"incarnation": 2, "known": []string{attachment}})
	if status != 200 {
		t.Fatalf("refresh %d", status)
	}
	raw, _ := json.Marshal(out)
	var refreshed HostSession
	json.Unmarshal(raw, &refreshed)
	if len(refreshed.Attachments) != 1 || refreshed.Attachments[0].Attachment.ID != attachment2 {
		t.Fatalf("refresh delivers only the new grant: %+v", refreshed.Attachments)
	}
	if status, _ := f.call("POST", "/api/browser-host/tabs/"+attachment+"/poll", token3, nil); status != 200 {
		t.Fatalf("known grant keeps its token: %d", status)
	}
	// Revocation ends the grant for its host.
	if status, _ := f.call("DELETE", "/api/cloud-browser/profiles/"+profile+"/grants/"+attachment, owner, nil); status != 200 {
		t.Fatal("revoke")
	}
	if status, _ := f.call("POST", "/api/browser-host/tabs/"+attachment+"/poll", token3, nil); status != 403 {
		t.Fatalf("revoked grant: %d", status)
	}
	// Reset forgets the checkpoint and revokes everything; the host closes.
	if status, _ := f.call("DELETE", "/api/cloud-browser/profiles/"+profile, owner, nil); status != 200 {
		t.Fatal("reset")
	}
	if status, s := f.begin(profile, true, 0); status != 200 || s.Enabled || s.Snapshot != nil || len(s.Attachments) != 0 {
		t.Fatalf("begin after reset %d %+v", status, s)
	}
	if fresh := f.profile(); fresh == profile {
		t.Fatal("a reset profile is not reused")
	}
}

func TestHostRouteAuthentication(t *testing.T) {
	f := setup(t)
	profile := f.profile()
	for _, c := range []struct {
		token  string
		header []string
	}{{"wrong-token-wrong-token-wrong-token-00", nil}, {runtime, []string{"Origin", "https://sumi.example"}}, {"", nil}} {
		if status, _ := f.call("POST", "/api/cloud-browser-host/profiles/"+profile+"/begin", c.token, map[string]any{"fresh": true}, c.header...); status != 403 {
			t.Fatalf("host route accepted token=%v %v: %d", c.token != "", c.header, status)
		}
	}
	if status, _ := f.call("GET", "/api/cloud-browser", "", nil); status != 403 {
		t.Fatal("person routes need a session")
	}
}

func TestTicketBindsOwnerProfileAndSerializer(t *testing.T) {
	f := setup(t)
	profile := f.profile()
	status, out := f.call("POST", "/api/cloud-browser/profiles/"+profile+"/viewer-ticket", owner, nil)
	if status != 200 {
		t.Fatalf("ticket %d", status)
	}
	parts := strings.Split(out["ticket"].(string), ".")
	if len(parts) != 3 || parts[0] != "sbt1" {
		t.Fatalf("ticket shape %v", len(parts))
	}
	m := hmac.New(sha256.New, derive([]byte(runtime), "sumi.cloud-browser.viewer-ticket.v1"))
	m.Write([]byte(parts[1]))
	if base64.RawURLEncoding.EncodeToString(m.Sum(nil)) != parts[2] {
		t.Fatal("ticket signature")
	}
	raw, _ := base64.RawURLEncoding.DecodeString(parts[1])
	var ticket Ticket
	json.Unmarshal(raw, &ticket)
	if ticket.HumanID != owner || ticket.PersonaID != persona || ticket.ProfileID != profile || ticket.Serial != SnapshotVersion || time.UnixMilli(ticket.Expires).After(time.Now().Add(61*time.Second)) {
		t.Fatalf("ticket claims %+v", ticket)
	}
}

func TestWakeOnlyForWorkAndLiveRefresh(t *testing.T) {
	f := setup(t)
	var mu sync.Mutex
	wakes := []string{}
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+runtime {
			w.WriteHeader(403)
			return
		}
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		wakes = append(wakes, r.URL.Path+" "+string(body))
		mu.Unlock()
	}))
	defer worker.Close()
	f.service.WakeURL = worker.URL
	profile := f.profile()
	attachment := f.grant(profile, uuid.NewString(), true)
	// A grant on a sleeping profile wakes nothing: the next start delivers it.
	if n := f.service.Sweep(context.Background()); n != 0 {
		t.Fatalf("idle profile woken %d times", n)
	}
	f.enqueue(attachment, "observe")
	if n := f.service.Sweep(context.Background()); n != 1 {
		t.Fatalf("queued work wakes: %d", n)
	}
	if n := f.service.Sweep(context.Background()); n != 0 {
		t.Fatal("repeated wakes are spaced")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(wakes) != 1 || wakes[0] != "/profiles/"+profile+"/wake {\"work\":true}" {
		t.Fatalf("wakes %v", wakes)
	}
}
