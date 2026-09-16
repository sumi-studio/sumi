package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"github.com/sumi-studio/sumi/apps/api/internal/messaging"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

// Firebase is the only external boundary replaced here. Session signing,
// rotation/revocation, cookie routing, and device persistence are real.
type pushLoginIdentity struct{}

func (pushLoginIdentity) VerifyIDToken(_ context.Context, token string) (agentevents.FirebaseIdentity, error) {
	identity := agentevents.FirebaseIdentity{UID: token}
	// "google:<subject>" carries the provider sign-in shape a real Google token
	// verifies into; the bare form stays a direct exchange token.
	if subject, ok := strings.CutPrefix(token, "google:"); ok {
		identity.UID = "firebase-" + subject
		identity.SignInProvider = "google.com"
		identity.ProviderSubjects = map[string][]string{"google.com": {subject}}
	}
	return identity, nil
}
func (pushLoginIdentity) ResolveIdentity(_ context.Context, identity agentevents.FirebaseIdentity) (agentevents.UserSessionClaims, error) {
	return agentevents.UserSessionClaims{TenantID: "push-test", UserID: identity.UID, PersonalityAgentID: testLocalControlPAID}, nil
}

func TestBrowserPushDeviceLoginRefreshSwitchAndLogout(t *testing.T) {
	ctx := context.Background()
	pool := testdb.Create(t)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	registry := koseki.New(pool)
	humanA, err := registry.MintHuman(ctx)
	if err != nil {
		t.Fatal(err)
	}
	humanB, err := registry.MintHuman(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Both Humans sign in through the real provider auth flow, so each needs
	// the registry state a provisioned account would have: a Secretary plus
	// the Firebase and Google credentials the flow resolves by. POST
	// /auth/session is the no-flows fallback and is absent once flow routes
	// are registered, exactly as in production.
	seed := func(humanID, providerSubject string) {
		t.Helper()
		if _, err := registry.MintSecretary(ctx, humanID); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO credentials (provider, external_subject, human_id)
			VALUES ('firebase', $1, $2), ('google.com', $3, $2)`,
			"firebase-"+providerSubject, humanID, providerSubject); err != nil {
			t.Fatal(err)
		}
	}
	const providerSubjectA = "google-subject-a"
	const providerSubjectB = "google-subject-b"
	seed(humanA, providerSubjectA)
	seed(humanB, providerSubjectB)
	commands, err := agentevents.OpenCommandStore(privateRuntimeDir(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = commands.Close() })
	gateway, err := agentevents.OpenDurableGateway(privateRuntimeDir(t), commands)
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := agentevents.NewHMACUserSessionVerifier(testSessionSecret, "", gateway)
	if err != nil {
		t.Fatal(err)
	}
	auth, err := agentevents.NewBrowserAuthServer(pushLoginIdentity{}, pushLoginIdentity{}, sessions, []string{testBrowserOrigin}, true)
	if err != nil {
		t.Fatal(err)
	}
	auth.Flows = newKosekiAuthFlowController(registry, "push-test", nil)
	auth.PushDevices = messaging.New(pool, nil, nil)
	mux := http.NewServeMux()
	auth.RegisterRoutes(mux)
	jar, _ := cookiejar.New(nil)
	request := func(requestCtx context.Context, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		method := http.MethodPost
		if body == "" {
			method = http.MethodGet
		}
		u, _ := url.Parse(testBrowserOrigin + path)
		r := httptest.NewRequest(method, u.String(), strings.NewReader(body)).WithContext(requestCtx)
		r.Header.Set("Origin", testBrowserOrigin)
		r.Header.Set("Content-Type", "application/json")
		for _, cookie := range jar.Cookies(u) {
			r.AddCookie(cookie)
			if cookie.Name == agentevents.BrowserCSRFCookie {
				r.Header.Set("X-CSRF-Token", cookie.Value)
			}
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		jar.SetCookies(u, w.Result().Cookies())
		return w
	}
	startFlow := func() (string, string) {
		t.Helper()
		raw := make([]byte, 32)
		if _, err := rand.Read(raw); err != nil {
			t.Fatal(err)
		}
		nonce := base64.RawURLEncoding.EncodeToString(raw)
		started := request(ctx, "/auth/flows", `{"intent":"sign_in","provider":"google.com","continuation":"/","nonce":"`+nonce+`"}`)
		if started.Code != http.StatusCreated {
			t.Fatal(started.Code, started.Body.String())
		}
		var flow struct {
			FlowID string `json:"flow_id"`
		}
		if err := json.Unmarshal(started.Body.Bytes(), &flow); err != nil || flow.FlowID == "" {
			t.Fatalf("flow start: %v %s", err, started.Body.String())
		}
		return flow.FlowID, nonce
	}
	resolveFlow := func(flowID, nonce, idToken, switchFrom string) *httptest.ResponseRecorder {
		t.Helper()
		payload, _ := json.Marshal(map[string]string{
			"flow_id": flowID, "nonce": nonce, "id_token": idToken,
			"switch_from_user_id": switchFrom,
		})
		return request(ctx, "/auth/flows/resolve", string(payload))
	}
	login := func(idToken string) *httptest.ResponseRecorder {
		t.Helper()
		if w := request(ctx, "/auth/csrf", ""); w.Code != http.StatusOK {
			t.Fatal(w.Code, w.Body.String())
		}
		flowID, nonce := startFlow()
		w := resolveFlow(flowID, nonce, idToken, "")
		if w.Code != http.StatusOK {
			t.Fatal(w.Code, w.Body.String())
		}
		return w
	}
	// switchLogin signs a different Human in through the supported account-
	// switch path: the provider flow's resolve is first refused by session
	// admission while another Human is active, then recovered by replaying
	// the same committed flow once the person names the Human being replaced.
	switchLogin := func(idToken, currentHuman string) {
		t.Helper()
		if w := request(ctx, "/auth/csrf", ""); w.Code != http.StatusOK {
			t.Fatal(w.Code, w.Body.String())
		}
		flowID, nonce := startFlow()
		if w := resolveFlow(flowID, nonce, idToken, ""); w.Code != http.StatusConflict {
			t.Fatalf("provider switch must be refused without consent: %d %s", w.Code, w.Body.String())
		}
		if w := resolveFlow(flowID, nonce, idToken, currentHuman); w.Code != http.StatusOK {
			t.Fatalf("consented provider switch failed: %d %s", w.Code, w.Body.String())
		}
	}
	deviceID := func() string {
		t.Helper()
		u, _ := url.Parse(testBrowserOrigin + "/messaging/push-subscriptions")
		r := httptest.NewRequest(http.MethodPost, u.String(), nil)
		for _, cookie := range jar.Cookies(u) {
			r.AddCookie(cookie)
		}
		id, err := agentevents.BrowserPushDeviceID(r)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	deviceOwner := func(id string) string {
		t.Helper()
		var owner string
		if err := pool.QueryRow(ctx, `SELECT human_id FROM push_devices WHERE device_id=$1`, id).Scan(&owner); err != nil {
			t.Fatal(err)
		}
		return owner
	}
	deviceGone := func(id string) {
		t.Helper()
		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM push_devices WHERE device_id=$1`, id).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatal("retired device can still receive notifications")
		}
	}
	first := login("google:" + providerSubjectA)
	var sessionAge, pushAge int
	for _, cookie := range first.Result().Cookies() {
		switch cookie.Name {
		case agentevents.BrowserSessionCookie:
			sessionAge = cookie.MaxAge
		case agentevents.BrowserPushDeviceCookie:
			pushAge = cookie.MaxAge
			if !cookie.HttpOnly || !cookie.Secure || cookie.Path != "/" || cookie.SameSite != http.SameSiteLaxMode {
				t.Fatal("unsafe push cookie scope")
			}
		}
	}
	if sessionAge != 15*60 || pushAge != 30*24*60*60 {
		t.Fatal("push persistence changed HTTP login lifetime", sessionAge, pushAge)
	}
	firstID := deviceID()
	if deviceOwner(firstID) != humanA {
		t.Fatal("device assigned to a different Human")
	}
	login("google:" + providerSubjectA)
	if deviceID() != firstID {
		t.Fatal("ordinary login refresh retired device")
	}
	switchLogin("google:"+providerSubjectB, humanA)
	secondID := deviceID()
	if secondID == firstID || deviceOwner(secondID) != humanB {
		t.Fatal("account switch reused the old recipient")
	}
	deviceGone(firstID)

	// A closed application loses its short session cookie before its device
	// cookie. Logout must still revoke Push without a valid HTTP credential.
	u, _ := url.Parse(testBrowserOrigin + "/")
	jar.SetCookies(u, []*http.Cookie{{Name: agentevents.BrowserSessionCookie, Path: "/", MaxAge: -1}})
	if w := request(ctx, "/auth/logout", "{}"); w.Code != http.StatusNoContent {
		t.Fatal(w.Code, w.Body.String())
	}
	deviceGone(secondID)
	for _, cookie := range jar.Cookies(u) {
		if cookie.Name == agentevents.BrowserPushDeviceCookie {
			t.Fatal("device cookie survived logout")
		}
	}

	// If persistence cannot finish revocation, retain the only device handle
	// and return an error. A retry must actually revoke it, not claim success.
	login("google:" + providerSubjectA)
	thirdID := deviceID()
	lock, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(ctx)
	if _, err := lock.Exec(ctx, `SELECT 1 FROM push_devices WHERE device_id=$1 FOR UPDATE`, thirdID); err != nil {
		t.Fatal(err)
	}
	blockedCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	w := request(blockedCtx, "/auth/logout", "{}")
	cancel()
	if w.Code != http.StatusServiceUnavailable || len(w.Result().Cookies()) != 0 {
		t.Fatal("failed revoke discarded cookies", w.Code)
	}
	if deviceID() != thirdID {
		t.Fatal("failed logout lost retry handle")
	}
	status := request(ctx, "/auth/session", "")
	var sessionStatus struct {
		Authenticated bool `json:"authenticated"`
	}
	if err := json.Unmarshal(status.Body.Bytes(), &sessionStatus); err != nil {
		t.Fatal(err)
	}
	if status.Code != http.StatusOK || !sessionStatus.Authenticated {
		t.Fatal("failed device revoke removed authenticated UI and logout retry")
	}
	if err := lock.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if w := request(ctx, "/auth/logout", "{}"); w.Code != http.StatusNoContent {
		t.Fatal(w.Code, w.Body.String())
	}
	deviceGone(thirdID)
}
