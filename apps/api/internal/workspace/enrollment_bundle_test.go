package workspace

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

func bundleSetup(t *testing.T) (testWorld, *koseki.Store, Workspace) {
	t.Helper()
	w := newTestWorld(t)
	w.store.EnrollmentAdmin = func(id string) bool { return id == w.humanA.ID }
	registry := koseki.NewWithWrappingKeyID(w.pool, "test-wrapping/v1")
	registry.EnrollmentWorkspaceAuthority = w.store
	space, err := w.store.CreateWorkspace(context.Background(), "Bundle workspace", w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	return w, registry, space
}
func bundleNonce() string {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}
func bundleStart(t *testing.T, ctx context.Context, r *koseki.Store, token string) (koseki.AuthFlow, string) {
	t.Helper()
	n := bundleNonce()
	f, err := r.StartAuthFlow(ctx, koseki.StartAuthFlowRequest{InviteToken: token, Intent: koseki.IntentSignUp, Channel: koseki.ChannelProvider, ExpectedProvider: "google.com", Continuation: "/", Nonce: n, TTL: time.Minute * 10})
	if err != nil {
		t.Fatal(err)
	}
	return f, n
}
func bundleProof(uid string) koseki.VerifiedIdentity {
	return koseki.VerifiedIdentity{FirebaseUID: uid, SignInProvider: "google.com", ProviderSubject: uid, NormalizedEmail: "target@example.com", EmailVerified: true}
}
func TestEnrollmentBundleReservesOnlyThenExplicitJoin(t *testing.T) {
	ctx := context.Background()
	w, r, space := bundleSetup(t)
	v, err := w.store.CreateEnrollmentBundle(ctx, space.WorkspaceID, w.humanA, "target@example.com", r)
	if err != nil {
		t.Fatal(err)
	}
	preview, err := w.store.PreviewInvite(ctx, v.Code)
	if err != nil || !preview.RequiresEmailVerification {
		t.Fatalf("preview: %+v %v", preview, err)
	}
	f, n := bundleStart(t, ctx, r, v.Enrollment.Token)
	created, err := r.ResolveAuthProof(ctx, f.FlowID, n, bundleProof("bundle-new"))
	if err != nil {
		t.Fatal(err)
	}
	members, err := w.store.Members(ctx, space.WorkspaceID, w.humanA)
	if err != nil || len(members) != 1 {
		t.Fatalf("signup autojoined: %+v %v", members, err)
	}
	preview, err = w.store.PreviewInvite(ctx, v.Code)
	if err != nil || preview.RequiresEmailVerification {
		t.Fatalf("reserved recipient requires duplicate proof: %+v %v", preview, err)
	}
	if _, err = w.store.RedeemInvite(ctx, v.Code, w.humanB); !errors.Is(err, ErrInviteUnavailable) {
		t.Fatalf("other human stole reserved join: %v", err)
	}
	member, err := w.store.RedeemInvite(ctx, v.Code, participant.Human(created.HumanID))
	if err != nil {
		t.Fatal(err)
	}
	replay, err := w.store.RedeemInvite(ctx, v.Code, participant.Human(created.HumanID))
	if err != nil || replay.WorkspaceMemberID != member.WorkspaceMemberID {
		t.Fatalf("join retry: %v", err)
	}
}
func TestEnrollmentBundleExistingJoinRetiresGrantAndBindsEmailIdentity(t *testing.T) {
	ctx := context.Background()
	w, r, space := bundleSetup(t)
	v, err := w.store.CreateEnrollmentBundle(ctx, space.WorkspaceID, w.humanA, "target@example.com", r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.pool.Exec(ctx, `INSERT INTO credentials(provider,external_subject,human_id) VALUES('firebase','existing-b',$1)`, w.humanB.ID); err != nil {
		t.Fatal(err)
	}
	for _, proof := range []EnrollmentRecipientProof{{}, {FirebaseUID: "existing-b", Email: "target@example.com"}, {FirebaseUID: "another-user", Email: "target@example.com", EmailVerified: true}} {
		if _, err = w.store.RedeemInviteWithEnrollmentProof(ctx, v.Code, w.humanB, proof); !errors.Is(err, ErrInviteEmailVerification) {
			t.Fatalf("unverified/wrong identity accepted: %v", err)
		}
	}
	if _, err = w.store.RedeemInviteWithEnrollmentProof(ctx, v.Code, w.humanB, EnrollmentRecipientProof{FirebaseUID: "existing-b", Email: "target@example.com", EmailVerified: true}); err != nil {
		t.Fatal(err)
	}
	if _, err = r.InspectEnrollmentInvite(ctx, v.Enrollment.Token); !errors.Is(err, koseki.ErrEnrollmentInvite) {
		t.Fatalf("existing join left reusable enrollment: %v", err)
	}
	if _, err = w.store.RedeemInvite(ctx, v.Code, w.humanC); !errors.Is(err, ErrInviteUnavailable) {
		t.Fatalf("another join: %v", err)
	}
}
func TestEnrollmentBundleBothIssuancePermissionsAndAtomicity(t *testing.T) {
	ctx := context.Background()
	w, r, space := bundleSetup(t)
	if _, err := w.store.CreateEnrollmentBundle(ctx, space.WorkspaceID, w.humanB, "", r); !errors.Is(err, ErrForbidden) {
		t.Fatalf("nonadmin issued: %v", err)
	}
	w.store.EnrollmentAdmin = func(string) bool { return true }
	if _, err := w.store.CreateEnrollmentBundle(ctx, space.WorkspaceID, w.humanB, "", r); err == nil {
		t.Fatal("admin without Workspace manage permission issued")
	}
	_, err := w.pool.Exec(ctx, `CREATE FUNCTION fail_bundle_invite() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected workspace invite failure'; END $$; CREATE TRIGGER fail_bundle_invite BEFORE INSERT ON workspace_invites FOR EACH ROW EXECUTE FUNCTION fail_bundle_invite()`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.store.CreateEnrollmentBundle(ctx, space.WorkspaceID, w.humanA, "", r); err == nil {
		t.Fatal("injected failure ignored")
	}
	var count int
	if err = w.pool.QueryRow(ctx, `SELECT count(*) FROM enrollment_invites`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("orphan enrollment grant: %d %v", count, err)
	}
}
func TestEnrollmentBundleRevokeEitherRemainingGrant(t *testing.T) {
	for _, side := range []string{"workspace", "enrollment", "enrollment_after_signup"} {
		t.Run(side, func(t *testing.T) {
			ctx := context.Background()
			w, r, space := bundleSetup(t)
			v, err := w.store.CreateEnrollmentBundle(ctx, space.WorkspaceID, w.humanA, "", r)
			if err != nil {
				t.Fatal(err)
			}
			recipient := w.humanB
			if side == "enrollment_after_signup" {
				f, n := bundleStart(t, ctx, r, v.Enrollment.Token)
				u, err := r.ResolveAuthProof(ctx, f.FlowID, n, bundleProof("revoked-new"))
				if err != nil {
					t.Fatal(err)
				}
				recipient = participant.Human(u.HumanID)
			}
			if side == "workspace" {
				err = w.store.RevokeInvite(ctx, space.WorkspaceID, v.InviteID, w.humanA)
			} else {
				err = r.RevokeEnrollmentInvite(ctx, v.Enrollment.ID)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err = r.InspectEnrollmentInvite(ctx, v.Enrollment.Token); !errors.Is(err, koseki.ErrEnrollmentInvite) {
				t.Fatalf("live enrollment after revoke: %v", err)
			}
			if _, err = w.store.RedeemInvite(ctx, v.Code, recipient); !errors.Is(err, ErrInviteUnavailable) {
				t.Fatalf("live join after revoke: %v", err)
			}
		})
	}
}
func TestEnrollmentBundleConcurrentExistingJoinOrNewSignupHasOneRecipient(t *testing.T) {
	ctx := context.Background()
	w, r, space := bundleSetup(t)
	v, err := w.store.CreateEnrollmentBundle(ctx, space.WorkspaceID, w.humanA, "", r)
	if err != nil {
		t.Fatal(err)
	}
	f, n := bundleStart(t, ctx, r, v.Enrollment.Token)
	var wg sync.WaitGroup
	wg.Add(2)
	var signupErr, joinErr error
	var created koseki.AuthFlow
	go func() {
		defer wg.Done()
		created, signupErr = r.ResolveAuthProof(ctx, f.FlowID, n, bundleProof("race-bundle"))
	}()
	go func() { defer wg.Done(); _, joinErr = w.store.RedeemInvite(ctx, v.Code, w.humanB) }()
	wg.Wait()
	if (signupErr == nil) == (joinErr == nil) {
		t.Fatalf("must admit exactly one recipient signup=%v join=%v", signupErr, joinErr)
	}
	if signupErr == nil {
		if _, err = w.store.RedeemInvite(ctx, v.Code, participant.Human(created.HumanID)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestEnrollmentBundleAlreadyMemberAcceptRetiresGrant(t *testing.T) {
	ctx := context.Background()
	w, r, space := bundleSetup(t)
	prior, err := w.store.CreateInvite(ctx, space.WorkspaceID, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	member, err := w.store.RedeemInvite(ctx, prior.Code, w.humanB)
	if err != nil {
		t.Fatal(err)
	}
	v, err := w.store.CreateEnrollmentBundle(ctx, space.WorkspaceID, w.humanA, "", r)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := w.store.RedeemInvite(ctx, v.Code, w.humanB)
	if err != nil || accepted.WorkspaceMemberID != member.WorkspaceMemberID {
		t.Fatalf("existing membership acceptance: %+v %v", accepted, err)
	}
	if _, err = r.InspectEnrollmentInvite(ctx, v.Enrollment.Token); !errors.Is(err, koseki.ErrEnrollmentInvite) {
		t.Fatalf("unused grant left active: %v", err)
	}
	if _, err = w.store.RedeemInvite(ctx, v.Code, w.humanC); !errors.Is(err, ErrInviteUnavailable) {
		t.Fatalf("second recipient admitted: %v", err)
	}
	retry, err := w.store.RedeemInvite(ctx, v.Code, w.humanB)
	if err != nil || retry.WorkspaceMemberID != member.WorkspaceMemberID {
		t.Fatalf("exact retry: %v", err)
	}
}

func TestEnrollmentBundleHTTPUsesVerifiedIdentityForExplicitJoin(t *testing.T) {
	ctx := context.Background()
	w, r, space := bundleSetup(t)
	sessions := &transportTestSessions{claims: agentevents.UserSessionClaims{UserID: w.humanA.ID}}
	server := NewServer(w.store, applicationapps.New(w.pool, w.store), sessions)
	server.AllowedOrigins = []string{"https://sumi.test"}
	server.EnrollmentIssuer = r
	server.VerifyEnrollmentRecipient = func(_ context.Context, _ agentevents.UserSessionClaims, token string) (EnrollmentRecipientProof, error) {
		if token != "verified-b" {
			return EnrollmentRecipientProof{}, errors.New("invalid proof")
		}
		return EnrollmentRecipientProof{FirebaseUID: "verified-b", Email: "target@example.com", EmailVerified: true}, nil
	}
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	response := browserCall(mux, "POST", "/workspaces/"+space.WorkspaceID+"/invites", `{"include_enrollment":true,"email":"target@example.com"}`)
	if response.Code != 201 {
		t.Fatalf("create=%d %s", response.Code, response.Body.String())
	}
	var invite inviteWire
	if err := json.Unmarshal(response.Body.Bytes(), &invite); err != nil {
		t.Fatal(err)
	}
	if invite.Enrollment == nil || len(invite.Enrollment.Token) != 43 {
		t.Fatal("missing one-time enrollment token")
	}
	previewBody, _ := json.Marshal(map[string]string{"code": invite.Code})
	response = browserCall(mux, "POST", "/workspace-invites/preview", string(previewBody))
	var preview invitePreviewWire
	if err := json.Unmarshal(response.Body.Bytes(), &preview); err != nil {
		t.Fatal(err)
	}
	if response.Code != 200 || !preview.RequiresEmailVerification {
		t.Fatalf("preview=%d %+v", response.Code, preview)
	}
	sessions.claims.UserID = w.humanB.ID
	if _, err := w.pool.Exec(ctx, `INSERT INTO credentials(provider,external_subject,human_id) VALUES('firebase','verified-b',$1)`, w.humanB.ID); err != nil {
		t.Fatal(err)
	}
	response = browserCall(mux, "POST", "/workspace-invites/redeem", string(previewBody))
	if response.Code != 403 {
		t.Fatalf("missing email proof=%d", response.Code)
	}
	payload, _ := json.Marshal(map[string]string{"code": invite.Code, "id_token": "invalid"})
	response = browserCall(mux, "POST", "/workspace-invites/redeem", string(payload))
	if response.Code != 403 {
		t.Fatalf("invalid identity=%d", response.Code)
	}
	payload, _ = json.Marshal(map[string]string{"code": invite.Code, "id_token": "verified-b"})
	response = browserCall(mux, "POST", "/workspace-invites/redeem", string(payload))
	if response.Code != 200 {
		t.Fatalf("verified explicit join=%d %s", response.Code, response.Body.String())
	}
	if _, err := r.InspectEnrollmentInvite(ctx, invite.Enrollment.Token); !errors.Is(err, koseki.ErrEnrollmentInvite) {
		t.Fatalf("grant still reusable after join: %v", err)
	}
}
