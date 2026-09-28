package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"github.com/sumi-studio/sumi/apps/api/internal/transfersession"
)

func githubTestProver(t *testing.T, handler http.HandlerFunc) *githubAPIEmailProver {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	prover := newGitHubAPIEmailProver()
	prover.origin = server.URL
	return prover
}

func TestGitHubEmailEvidenceReadsOnlyTheProvenAccount(t *testing.T) {
	ctx := context.Background()
	emails := `[{"email":"Primary@Example.com","verified":true,"primary":true,"visibility":"private"},
		{"email":"pending@example.com","verified":false,"primary":false,"visibility":null}]`
	prover := githubTestProver(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer gho_valid" || r.Header.Get("X-GitHub-Api-Version") != "2022-11-28" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/user":
			_, _ = w.Write([]byte(`{"id":4242,"login":"invitee","email":null}`))
		case "/user/emails":
			_, _ = w.Write([]byte(emails))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	for _, tt := range []struct {
		email string
		want  githubEmailEvidence
	}{
		{"primary@example.com", githubEmailVerified},
		{"pending@example.com", githubEmailUnverified},
		{"elsewhere@example.com", githubEmailAbsent},
	} {
		got, err := prover.EmailEvidence(ctx, "gho_valid", "4242", tt.email)
		if err != nil || got != tt.want {
			t.Fatalf("%s: evidence=%v err=%v", tt.email, got, err)
		}
	}
	// A token for another GitHub account never speaks for this sign-in.
	if _, err := prover.EmailEvidence(ctx, "gho_valid", "7", "primary@example.com"); !errors.Is(err, errGitHubSubjectMismatch) {
		t.Fatalf("other account token: %v", err)
	}
	if _, err := prover.EmailEvidence(ctx, "gho_valid", "not-numeric", "primary@example.com"); !errors.Is(err, errGitHubSubjectMismatch) {
		t.Fatalf("non-numeric subject: %v", err)
	}
	if _, err := prover.EmailEvidence(ctx, "gho_revoked", "4242", "primary@example.com"); !errors.Is(err, errGitHubProofRejected) {
		t.Fatalf("revoked token: %v", err)
	}
}

func TestGitHubEmailEvidenceClassifiesOutagesAndNeverFollowsRedirects(t *testing.T) {
	ctx := context.Background()
	var leaked atomic.Int32
	elsewhere := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { leaked.Add(1) }))
	defer elsewhere.Close()
	for _, tt := range []struct {
		name    string
		handler http.HandlerFunc
		want    error
	}{
		{"missing scope", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/user" {
				_, _ = w.Write([]byte(`{"id":1}`))
				return
			}
			w.WriteHeader(http.StatusNotFound)
		}, errGitHubProofRejected},
		{"rate limited", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.WriteHeader(http.StatusForbidden)
		}, errGitHubUnavailable},
		{"secondary rate limited", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-RateLimit-Remaining", "4999")
			w.Header().Set("Retry-After", "60")
			w.WriteHeader(http.StatusForbidden)
		}, errGitHubUnavailable},
		{"server error", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadGateway)
		}, errGitHubUnavailable},
		{"oversized", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"id":1,"bio":"` + strings.Repeat("x", githubMaxResponseBytes) + `"}`))
		}, errGitHubUnavailable},
		{"malformed", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`<html>`))
		}, errGitHubUnavailable},
		{"redirect", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, elsewhere.URL+"/user", http.StatusFound)
		}, errGitHubUnavailable},
	} {
		prover := githubTestProver(t, tt.handler)
		if _, err := prover.EmailEvidence(ctx, "gho_token", "1", "a@example.com"); !errors.Is(err, tt.want) {
			t.Fatalf("%s: %v, want %v", tt.name, err, tt.want)
		}
	}
	if leaked.Load() != 0 {
		t.Fatal("a redirect carried the GitHub token to another origin")
	}
}

func TestGitHubEmailEvidenceReadsLaterPagesWithoutInventingMismatch(t *testing.T) {
	for _, found := range []bool{true, false} {
		prover := githubTestProver(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/user" {
				_, _ = w.Write([]byte(`{"id":1}`))
				return
			}
			if found && r.URL.Query().Get("page") == "2" {
				_, _ = w.Write([]byte(`[{"email":"invited@example.com","verified":true}]`))
				return
			}
			emails := make([]map[string]any, 100)
			for i := range emails {
				emails[i] = map[string]any{"email": "other@example.com", "verified": true}
			}
			_ = json.NewEncoder(w).Encode(emails)
		})
		got, err := prover.EmailEvidence(context.Background(), "gho_valid", "1", "invited@example.com")
		if found && (err != nil || got != githubEmailVerified) {
			t.Fatalf("later-page email: evidence=%v err=%v", got, err)
		}
		if !found && !errors.Is(err, errGitHubUnavailable) {
			t.Fatalf("incomplete search must not report absence: evidence=%v err=%v", got, err)
		}
	}
}

type fakeGitHubEmailProver struct {
	calls    int
	token    string
	subject  string
	evidence githubEmailEvidence
	err      error
}

func (f *fakeGitHubEmailProver) EmailEvidence(_ context.Context, token, subject, _ string) (githubEmailEvidence, error) {
	f.calls++
	if f.err != nil {
		return githubEmailAbsent, f.err
	}
	if token != f.token {
		return githubEmailAbsent, errGitHubProofRejected
	}
	if subject != f.subject {
		return githubEmailAbsent, errGitHubSubjectMismatch
	}
	return f.evidence, nil
}

type githubInvitationFixture struct {
	ctx        context.Context
	store      *koseki.Store
	controller *kosekiAuthFlowController
	github     *fakeGitHubEmailProver
	invite     string
}

func newGitHubInvitationFixture(t *testing.T, withTransfers bool) githubInvitationFixture {
	t.Helper()
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	store := koseki.New(pool)
	if withTransfers {
		store.Transfers = transfersession.New(pool, transfersession.Config{})
	}
	controller := newKosekiAuthFlowController(store, "local", nil)
	github := &fakeGitHubEmailProver{token: "gho_invitee", subject: "4242", evidence: githubEmailVerified}
	controller.githubEmails = github
	issuer := uuid.Must(uuid.NewV7()).String()
	if _, err := pool.Exec(ctx, `INSERT INTO humans(human_id,display_name) VALUES($1,'issuer')`, issuer); err != nil {
		t.Fatal(err)
	}
	_, invite, err := store.IssueEnrollmentInvite(ctx, issuer, "invitee@example.com", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return githubInvitationFixture{ctx: ctx, store: store, controller: controller, github: github, invite: invite}
}

func (f githubInvitationFixture) start(t *testing.T, provider string) (string, string) {
	t.Helper()
	nonce := controllerNonce(t)
	started, err := f.controller.Start(f.ctx, agentevents.StartBrowserAuthFlowRequest{
		InviteToken: f.invite, Intent: "sign_up", Provider: provider, Continuation: "/", Nonce: nonce,
	})
	if err != nil {
		t.Fatal(err)
	}
	return started.FlowID, nonce
}

func (f githubInvitationFixture) resolve(flowID, nonce, token, email, subject string) (agentevents.BrowserAuthFlowResult, error) {
	return f.controller.Resolve(f.ctx, agentevents.ResolveBrowserAuthFlowRequest{
		FlowID: flowID, Nonce: nonce, ProviderAccessToken: token,
	}, agentevents.FirebaseIdentity{
		// Firebase reports GitHub addresses as unverified.
		UID: "github-firebase-uid", SignInProvider: "github.com", Email: email, EmailVerified: false,
		ProviderSubjects: map[string][]string{"github.com": {subject}}, IssuedAt: time.Now(),
	})
}

func (f githubInvitationFixture) assertInviteLive(t *testing.T, want bool) {
	t.Helper()
	_, err := f.store.InspectEnrollmentInvite(f.ctx, f.invite)
	if (err == nil) != want {
		t.Fatalf("invitation live=%v, want %v", err == nil, want)
	}
}

// Every refused GitHub proof leaves the invitation and the flow as they were:
// the same flow completes once GitHub's answer is presented.
func TestGitHubInvitationRefusalsAreActionableAndConsumeNothing(t *testing.T) {
	f := newGitHubInvitationFixture(t, false)
	flowID, nonce := f.start(t, "github.com")
	for _, tt := range []struct {
		name, token, email, subject string
		evidence                    githubEmailEvidence
		githubErr                   error
		want                        error
	}{
		{"no token", "", "invitee@example.com", "4242", githubEmailVerified, nil,
			agentevents.ErrBrowserEnrollmentEmailProofRequired},
		{"token refused", "gho_stale", "invitee@example.com", "4242", githubEmailVerified, nil,
			agentevents.ErrBrowserEnrollmentEmailProofRequired},
		{"github outage", "gho_invitee", "invitee@example.com", "4242", githubEmailVerified, errGitHubUnavailable,
			agentevents.ErrBrowserEnrollmentEmailProofUnavailable},
		{"another account's token", "gho_invitee", "invitee@example.com", "999", githubEmailVerified, nil,
			agentevents.ErrBrowserAuthFlowProof},
		{"unverified on github", "gho_invitee", "invitee@example.com", "4242", githubEmailUnverified, nil,
			agentevents.ErrBrowserEnrollmentEmailUnverified},
		{"address not on account", "gho_invitee", "invitee@example.com", "4242", githubEmailAbsent, nil,
			agentevents.ErrBrowserEnrollmentEmailMismatch},
		{"different address", "gho_invitee", "someone@example.com", "4242", githubEmailAbsent, nil,
			agentevents.ErrBrowserEnrollmentEmailMismatch},
	} {
		f.github.evidence, f.github.err = tt.evidence, tt.githubErr
		if _, err := f.resolve(flowID, nonce, tt.token, tt.email, tt.subject); !errors.Is(err, tt.want) {
			t.Fatalf("%s: %v, want %v", tt.name, err, tt.want)
		}
		f.assertInviteLive(t, true)
	}
	f.github.err, f.github.evidence = nil, githubEmailVerified
	result, err := f.resolve(flowID, nonce, "gho_invitee", "invitee@example.com", "4242")
	if err != nil || result.Outcome != "account_created" || result.HumanID == "" {
		t.Fatalf("verified GitHub registration: %+v %v", result, err)
	}
	f.assertInviteLive(t, false)
}

// GitHub's verified list — not the profile email Firebase copied — decides,
// so an invited secondary address registers too.
func TestGitHubInvitationAcceptsInvitedSecondaryAddress(t *testing.T) {
	f := newGitHubInvitationFixture(t, false)
	flowID, nonce := f.start(t, "github.com")
	result, err := f.resolve(flowID, nonce, "gho_invitee", "primary@example.com", "4242")
	if err != nil || result.Outcome != "account_created" {
		t.Fatalf("secondary verified address: %+v %v", result, err)
	}
	f.assertInviteLive(t, false)
}

// With the secretary-move choice mounted, the proof is judged once at resolve
// and recorded; the confirmation's token refresh and the confirm itself need
// no GitHub token and never ask GitHub again.
func TestGitHubInvitationProofSurvivesConfirmationWithoutToken(t *testing.T) {
	f := newGitHubInvitationFixture(t, true)
	flowID, nonce := f.start(t, "github.com")
	resolved, err := f.resolve(flowID, nonce, "gho_invitee", "invitee@example.com", "4242")
	if err != nil || resolved.Outcome != "confirmation_required" || resolved.NextAction != "create_account" {
		t.Fatalf("resolve: %+v %v", resolved, err)
	}
	f.assertInviteLive(t, true)
	calls := f.github.calls
	refreshed, err := f.resolve(flowID, nonce, "", "invitee@example.com", "4242")
	if err != nil || refreshed.Outcome != "confirmation_required" {
		t.Fatalf("confirmation refresh: %+v %v", refreshed, err)
	}
	confirmed, err := f.controller.Confirm(f.ctx, agentevents.ConfirmBrowserAuthFlowRequest{
		FlowID: flowID, Nonce: nonce, Action: "create_account",
	})
	if err != nil || confirmed.Outcome != "account_created" {
		t.Fatalf("confirm: %+v %v", confirmed, err)
	}
	if f.github.calls != calls {
		t.Fatalf("GitHub asked again after the proof was recorded: %d calls", f.github.calls)
	}
	f.assertInviteLive(t, false)
}

// Only a GitHub sign-in that an email-bound invitation still judges consults
// GitHub; a Google sign-in never forwards a presented token.
func TestGitHubProofIsNotConsultedOutsideGitHubInvitationProof(t *testing.T) {
	f := newGitHubInvitationFixture(t, false)
	flowID, nonce := f.start(t, "google.com")
	result, err := f.controller.Resolve(f.ctx, agentevents.ResolveBrowserAuthFlowRequest{
		FlowID: flowID, Nonce: nonce, ProviderAccessToken: "gho_invitee",
	}, agentevents.FirebaseIdentity{
		UID: "google-uid", SignInProvider: "google.com", Email: "invitee@example.com", EmailVerified: true,
		ProviderSubjects: map[string][]string{"google.com": {"google-subject"}}, IssuedAt: time.Now(),
	})
	if err != nil || result.Outcome != "account_created" {
		t.Fatalf("google registration: %+v %v", result, err)
	}
	if f.github.calls != 0 {
		t.Fatalf("Google sign-in consulted GitHub %d times", f.github.calls)
	}
}
