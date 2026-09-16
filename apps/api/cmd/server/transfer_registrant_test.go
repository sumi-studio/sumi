package main

// The production RegistrantProof adapter, exercised at the real HTTP
// boundary: origin allowlist, CSRF double-submit, the browser epoch the flow
// was bound to, and a live verified create-account flow — each boundary is
// broken in turn to prove the adapter trusts nothing else.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"github.com/sumi-studio/sumi/apps/api/internal/transfersession"
)

const registrantTestOrigin = "https://register.example.test"

func registrantToken(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

// registrantFlow builds a real sign-in flow that resolved to the
// create-account confirmation for an unknown credential — the exact flow
// state the transfer endpoints accept as proof. epochCookie is the raw
// browser epoch value; the flow stores only its hash.
func registrantFlow(t *testing.T, pool *pgxpool.Pool, store *koseki.Store, uid, epochCookie string) (flowID, nonce string) {
	t.Helper()
	ctx := context.Background()
	nonce = registrantToken(t)
	epochSum := sha256.Sum256([]byte(epochCookie))
	normalized, err := koseki.NormalizeEmail(uid + "@example.com")
	if err != nil {
		t.Fatal(err)
	}
	// Invite-only signup: a real enrollment invitation issued through the
	// store's own operator path.
	issuer := uuid.Must(uuid.NewV7()).String()
	if _, err := pool.Exec(ctx,
		`INSERT INTO humans(human_id,display_name) VALUES($1,'transfer test issuer')`, issuer); err != nil {
		t.Fatalf("issuer: %v", err)
	}
	_, inviteToken, err := store.IssueEnrollmentInvite(ctx, issuer, normalized, time.Hour)
	if err != nil {
		t.Fatalf("issue invite: %v", err)
	}
	flow, err := store.StartAuthFlow(ctx, koseki.StartAuthFlowRequest{
		InviteToken: inviteToken,
		Intent:      koseki.IntentSignIn, Channel: koseki.ChannelEmailLink,
		ExpectedProvider: "password", NormalizedEmail: normalized,
		Continuation: "/direct-chat", Nonce: nonce, TTL: 10 * time.Minute,
		BrowserEpochHash: base64.RawURLEncoding.EncodeToString(epochSum[:]),
	})
	if err != nil {
		t.Fatalf("start flow: %v", err)
	}
	pending, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, koseki.VerifiedIdentity{
		FirebaseUID: uid, NormalizedEmail: normalized, EmailVerified: true, SignInProvider: "password",
	})
	if err != nil {
		t.Fatalf("resolve proof: %v", err)
	}
	if pending.Status != "confirmation_required" || pending.ConfirmationAction != koseki.ActionCreateAccount {
		t.Fatalf("flow is not a live create-account confirmation: %+v", pending)
	}
	return flow.FlowID, nonce
}

// registrantRequest issues one POST with the browser boundaries the
// adapter checks; empty epoch/CSRF/origin values omit that boundary.
func registrantRequest(t *testing.T, srv *httptest.Server, path, epoch, csrf, origin string, body io.Reader) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+path, body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	if csrf != "" {
		req.Header.Set("X-CSRF-Token", csrf)
		req.AddCookie(&http.Cookie{Name: agentevents.BrowserCSRFCookie, Value: csrf})
	}
	if epoch != "" {
		req.AddCookie(&http.Cookie{Name: agentevents.BrowserEpochCookie, Value: epoch})
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	return res.StatusCode, raw
}

func TestRegistrantProofAdapterBoundaries(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
	svc := transfersession.New(pool, transfersession.Config{})
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	server, err := transfersession.NewServer(svc,
		registrantFlowProof{store: store, origins: []string{registrantTestOrigin}}, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	server.RegisterRoutes(mux)

	uid := "registrant-" + registrantToken(t)[:12]
	epoch := registrantToken(t)
	csrf := registrantToken(t)
	flowID, nonce := registrantFlow(t, pool, store, uid, epoch)
	creds := `{"flow_id":"` + flowID + `","nonce":"` + nonce + `"}`

	// The full boundary admits the session and returns the move URL on the
	// configured public base, never the request Host.
	code, raw := registrantRequest(t, srv, "/api/secretary-transfer/sessions", epoch, csrf, registrantTestOrigin, strings.NewReader(creds))
	if code != http.StatusCreated {
		t.Fatalf("create: %d %s", code, raw)
	}
	var created struct {
		MoveURL string `json:"move_url"`
	}
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(created.MoveURL, srv.URL+"/api/secretary-transfer/sessions/") ||
		!strings.Contains(created.MoveURL, "#grant=") {
		t.Fatalf("move URL %q is not the configured base + fragment grant", created.MoveURL)
	}

	for name, mutate := range map[string]struct {
		epoch, csrf, origin string
	}{
		"wrong epoch": {registrantToken(t), csrf, registrantTestOrigin},
		"no epoch":    {"", csrf, registrantTestOrigin},
		"bad origin":  {epoch, csrf, "https://evil.example.test"},
		"no origin":   {epoch, csrf, ""},
		"no csrf":     {epoch, "", registrantTestOrigin},
	} {
		// A fresh flow for a different credential each time: the first
		// legitimate session stays open under uid and must not be readable.
		otherUID := "other-" + registrantToken(t)[:12]
		otherEpoch := registrantToken(t)
		otherFlow, otherNonce := registrantFlow(t, pool, store, otherUID, otherEpoch)
		body := `{"flow_id":"` + otherFlow + `","nonce":"` + otherNonce + `"}`
		usedEpoch, usedCSRF, usedOrigin := mutate.epoch, mutate.csrf, mutate.origin
		if usedEpoch == epoch {
			usedEpoch = otherEpoch
		}
		if code, raw := registrantRequest(t, srv, "/api/secretary-transfer/sessions", usedEpoch, usedCSRF, usedOrigin, strings.NewReader(body)); code != http.StatusUnauthorized {
			t.Fatalf("%s boundary admitted: %d %s", name, code, raw)
		}
	}

	// A request-body subject is never proof.
	bodySubject := `{"flow_id":"` + flowID + `","nonce":"` + nonce + `","firebase_uid":"` + uid + `"}`
	if code, _ := registrantRequest(t, srv, "/api/secretary-transfer/sessions", epoch, csrf, registrantTestOrigin, strings.NewReader(bodySubject)); code != http.StatusBadRequest {
		t.Fatalf("body-supplied subject admitted: %d", code)
	}

	// A flow whose proof belongs to a different browser jar is rejected even
	// when its nonce is right.
	if code, _ := registrantRequest(t, srv, "/api/secretary-transfer/sessions", registrantToken(t), csrf, registrantTestOrigin, strings.NewReader(creds)); code != http.StatusUnauthorized {
		t.Fatalf("foreign jar replayed the flow: %d", code)
	}
}

// The deployment switch: env unset leaves the whole surface off — no mount,
// no sweep, and the registration store's feature flag stays nil so no
// account-creation path ever consults transfer_sessions. Env set wires one
// service instance into the routes and the store so the claim consult and
// the mounted surface agree.
func TestSecretaryTransferEnvContract(t *testing.T) {
	pool := kosekiResolverTestPool(t)

	t.Setenv(transferPublicBaseURLEnv, "")
	disabled := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
	mount, err := secretaryTransferFromEnv(pool, disabled, []string{registrantTestOrigin})
	if err != nil || mount != nil {
		t.Fatalf("disabled mount: %+v %v", mount, err)
	}
	if disabled.Transfers != nil {
		t.Fatal("a disabled surface still wired a claim service into the store")
	}

	t.Setenv(transferPublicBaseURLEnv, "https://move.example.test")
	enabled := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
	mount, err = secretaryTransferFromEnv(pool, enabled, []string{registrantTestOrigin})
	if err != nil || mount == nil || mount.service == nil || mount.server == nil {
		t.Fatalf("enabled mount: %+v %v", mount, err)
	}
	if enabled.Transfers != mount.service {
		t.Fatal("the claim consult and the mounted routes disagree on the service")
	}
}
