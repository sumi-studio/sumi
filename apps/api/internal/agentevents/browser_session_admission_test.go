package agentevents

import (
	"context"
	"errors"
	"testing"
	"time"
)

var (
	testFlowIDA   = "0198f0f4-9b72-7000-8000-0000000000a1"
	testFlowIDB   = "0198f0f4-9b72-7000-8000-0000000000b2"
	testEpochA    = hashBrowserEpochValue("epoch-value-a")
	testEpochB    = hashBrowserEpochValue("epoch-value-b")
	testHumanA    = "0198f0f4-9b72-7000-8000-00000000aaa1"
	testHumanB    = "0198f0f4-9b72-7000-8000-00000000bbb2"
	testHumanC    = "0198f0f4-9b72-7000-8000-00000000ccc3"
	testClaimsFor = func(humanID string) UserSessionClaims {
		return UserSessionClaims{
			TenantID:           "tenant-1",
			UserID:             humanID,
			PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
		}
	}
)

func newAdmissionTestVerifier(t *testing.T) *HMACUserSessionVerifier {
	t.Helper()
	v, err := NewHMACUserSessionVerifier(
		testSessionSecret,
		"",
		newTestBrowserSessionRevocationStore(),
	)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// A flow closed by discard can never issue again — its admission is refused
// and the session it already minted is revoked.
func TestAdmitBrowserSessionRefusesClosedFlow(t *testing.T) {
	v := newAdmissionTestVerifier(t)
	_, signed, _, err := v.AdmitSession(
		context.Background(),
		BrowserSessionAdmission{
			FlowID:    testFlowIDA,
			FlowEpoch: testEpochA,
			Epoch:     testEpochA,
		},
		testClaimsFor(testHumanA),
		time.Minute,
	)
	if err != nil {
		t.Fatalf("initial admission: %v", err)
	}

	retired, err := v.DiscardBrowserFlow(
		context.Background(),
		testFlowIDA,
		time.Now().Add(browserFlowClosureHorizon),
	)
	if err != nil {
		t.Fatalf("discard: %v", err)
	}
	if len(retired) != 1 {
		t.Fatalf("discard retired %v sessions, want 1", retired)
	}
	if _, err := v.VerifySession(context.Background(), signed); err == nil {
		t.Fatal("discarded flow's session still verifies")
	}

	_, _, _, err = v.AdmitSession(
		context.Background(),
		BrowserSessionAdmission{
			FlowID:    testFlowIDA,
			FlowEpoch: testEpochA,
			Epoch:     testEpochA,
		},
		testClaimsFor(testHumanA),
		time.Minute,
	)
	if !errors.Is(err, errBrowserFlowClosed) {
		t.Fatalf("closed flow admission = %v, want flow closed", err)
	}
}

// Logout closes the browser epoch: a proof that commits after the logout's
// flow enumeration can never issue, because both the request's epoch and the
// epoch the flow was started under are refused.
func TestAdmitBrowserSessionRefusesClosedEpoch(t *testing.T) {
	v := newAdmissionTestVerifier(t)
	_, signed, _, err := v.AdmitSession(
		context.Background(),
		BrowserSessionAdmission{Epoch: testEpochA},
		testClaimsFor(testHumanA),
		time.Minute,
	)
	if err != nil {
		t.Fatalf("initial admission: %v", err)
	}
	_, _, _, err = v.RevokeSessionsForLogout(
		context.Background(),
		[]string{signed},
		[]string{testEpochA},
		nil,
	)
	if err != nil {
		t.Fatalf("logout: %v", err)
	}

	// The presented epoch is closed.
	if _, _, _, err := v.AdmitSession(
		context.Background(),
		BrowserSessionAdmission{Epoch: testEpochA},
		testClaimsFor(testHumanA),
		time.Minute,
	); !errors.Is(err, errBrowserFlowClosed) {
		t.Fatalf("closed-epoch admission = %v, want refused", err)
	}
	// A flow started under the closed epoch cannot issue even when the
	// request carries a fresh epoch cookie — the proof committed after
	// logout's enumeration but its bound epoch is still closed.
	if _, _, _, err := v.AdmitSession(
		context.Background(),
		BrowserSessionAdmission{
			FlowID:    testFlowIDA,
			FlowEpoch: testEpochA,
			Epoch:     testEpochB,
		},
		testClaimsFor(testHumanA),
		time.Minute,
	); !errors.Is(err, errBrowserFlowClosed) {
		t.Fatalf("flow bound to closed epoch = %v, want refused", err)
	}
	// Unrelated work on a fresh epoch stays usable.
	if _, _, _, err := v.AdmitSession(
		context.Background(),
		BrowserSessionAdmission{Epoch: testEpochB},
		testClaimsFor(testHumanA),
		time.Minute,
	); err != nil {
		t.Fatalf("fresh epoch admission: %v", err)
	}
}

// The epoch's live tip — not the request's cookie — is the current Human
// authority. Replacing it requires switchFrom naming that Human.
func TestAdmitBrowserSessionCrossHumanRequiresExplicitSwitch(t *testing.T) {
	v := newAdmissionTestVerifier(t)
	_, signedA, _, err := v.AdmitSession(
		context.Background(),
		BrowserSessionAdmission{
			FlowID: testFlowIDA,
			Epoch:  testEpochA,
		},
		testClaimsFor(testHumanA),
		time.Minute,
	)
	if err != nil {
		t.Fatalf("human A admission: %v", err)
	}

	// No switch acknowledgement: refused.
	if _, _, _, err := v.AdmitSession(
		context.Background(),
		BrowserSessionAdmission{Epoch: testEpochA},
		testClaimsFor(testHumanB),
		time.Minute,
	); !errors.Is(err, ErrBrowserSessionActive) {
		t.Fatalf("different-human admission = %v, want session_active", err)
	}
	// Naming the wrong Human is not consent.
	if _, _, _, err := v.AdmitSession(
		context.Background(),
		BrowserSessionAdmission{Epoch: testEpochA, SwitchFrom: testHumanC},
		testClaimsFor(testHumanB),
		time.Minute,
	); !errors.Is(err, ErrBrowserSessionActive) {
		t.Fatalf("wrong switch_from admission = %v, want session_active", err)
	}
	if _, err := v.VerifySession(context.Background(), signedA); err != nil {
		t.Fatalf("refused switches retired the live session: %v", err)
	}

	// Naming the actual authority replaces it and cancels the retired
	// Human's pending flow authority so a late completion cannot reissue.
	_, signedB, outcome, err := v.AdmitSession(
		context.Background(),
		BrowserSessionAdmission{Epoch: testEpochA, SwitchFrom: testHumanA},
		testClaimsFor(testHumanB),
		time.Minute,
	)
	if err != nil {
		t.Fatalf("explicit switch: %v", err)
	}
	if len(outcome.RetiredSessionIDs) != 1 {
		t.Fatalf("switch retired %v, want the old session", outcome.RetiredSessionIDs)
	}
	if len(outcome.ClosedFlowIDs) != 1 || outcome.ClosedFlowIDs[0] != testFlowIDA {
		t.Fatalf("switch closed flows %v, want %q", outcome.ClosedFlowIDs, testFlowIDA)
	}
	if _, err := v.VerifySession(context.Background(), signedA); err == nil {
		t.Fatal("switched-away session still verifies")
	}
	if _, err := v.VerifySession(context.Background(), signedB); err != nil {
		t.Fatalf("switch successor invalid: %v", err)
	}
	// Same-Human reissue is ordinary: no switch acknowledgement needed.
	if _, _, _, err := v.AdmitSession(
		context.Background(),
		BrowserSessionAdmission{Epoch: testEpochA},
		testClaimsFor(testHumanB),
		time.Minute,
	); err != nil {
		t.Fatalf("same-human reissue: %v", err)
	}
}

// A later explicit choice stands: after the epoch's tip moved to Human B, a
// replay of Human A's retired cookie is consumed authority and fails closed,
// and a fresh flow completion for a third Human still needs consent.
func TestAdmitBrowserSessionLaterChoiceStandsOverStaleWork(t *testing.T) {
	v := newAdmissionTestVerifier(t)
	_, signedA, _, err := v.AdmitSession(
		context.Background(),
		BrowserSessionAdmission{Epoch: testEpochA},
		testClaimsFor(testHumanA),
		time.Minute,
	)
	if err != nil {
		t.Fatalf("human A admission: %v", err)
	}
	_, signedB, _, err := v.AdmitSession(
		context.Background(),
		BrowserSessionAdmission{Epoch: testEpochA, SwitchFrom: testHumanA},
		testClaimsFor(testHumanB),
		time.Minute,
	)
	if err != nil {
		t.Fatalf("explicit switch to B: %v", err)
	}

	// Replaying A's retired cookie is refused rather than treated as a fresh
	// jar that may mint a session beside B's.
	claimsA, err := v.VerifySessionLocal(context.Background(), signedA)
	if err != nil {
		t.Fatalf("local verify of retired cookie: %v", err)
	}
	identityA := claimsA.BrowserSessionIdentity()
	if _, _, _, err := v.AdmitSession(
		context.Background(),
		BrowserSessionAdmission{
			Presented:   &identityA,
			PresentedBy: testHumanA,
			Epoch:       testEpochA,
		},
		testClaimsFor(testHumanA),
		time.Minute,
	); err == nil {
		t.Fatal("retired cookie replay minted a session")
	}
	if _, err := v.VerifySession(context.Background(), signedB); err != nil {
		t.Fatalf("stale replay disturbed the later session: %v", err)
	}

	// A third Human completing on the same jar still requires consent.
	if _, _, _, err := v.AdmitSession(
		context.Background(),
		BrowserSessionAdmission{Epoch: testEpochA, FlowID: testFlowIDB},
		testClaimsFor(testHumanC),
		time.Minute,
	); !errors.Is(err, ErrBrowserSessionActive) {
		t.Fatalf("third-human completion = %v, want session_active", err)
	}
}

// Logout revokes the presented session's lineage, closes the presented epoch
// and every epoch the jar's retired sessions were admitted under, and closes
// the nonce-listed pending flows — all in one durable transition.
func TestRevokeSessionsForLogoutClosesJarAuthority(t *testing.T) {
	v := newAdmissionTestVerifier(t)
	_, signed, _, err := v.AdmitSession(
		context.Background(),
		BrowserSessionAdmission{
			FlowID: testFlowIDA,
			Epoch:  testEpochA,
		},
		testClaimsFor(testHumanA),
		time.Minute,
	)
	if err != nil {
		t.Fatalf("admission: %v", err)
	}

	retain := time.Now().Add(browserFlowClosureHorizon).Unix()
	closedFlows, closedEpochs, retiredSessions, err := v.RevokeSessionsForLogout(
		context.Background(),
		[]string{signed},
		[]string{testEpochA},
		map[string]time.Time{
			testFlowIDB: time.Unix(retain, 0),
		},
	)
	if err != nil {
		t.Fatalf("logout: %v", err)
	}
	if len(retiredSessions) != 1 {
		t.Fatalf("logout retired %v, want the live session", retiredSessions)
	}
	if _, err := v.VerifySession(context.Background(), signed); err == nil {
		t.Fatal("logged-out session still verifies")
	}
	foundFlow := map[string]bool{}
	for _, flowID := range closedFlows {
		foundFlow[flowID] = true
	}
	if !foundFlow[testFlowIDA] || !foundFlow[testFlowIDB] {
		t.Fatalf("logout closed flows %v, want issuing + listed", closedFlows)
	}
	foundEpoch := map[string]bool{}
	for _, epoch := range closedEpochs {
		foundEpoch[epoch] = true
	}
	if !foundEpoch[testEpochA] {
		t.Fatalf("logout closed epochs %v, want presented epoch", closedEpochs)
	}
	// Closed listed flow cannot issue afterwards.
	if _, _, _, err := v.AdmitSession(
		context.Background(),
		BrowserSessionAdmission{FlowID: testFlowIDB, Epoch: testEpochB},
		testClaimsFor(testHumanA),
		time.Minute,
	); !errors.Is(err, errBrowserFlowClosed) {
		t.Fatalf("listed flow after logout = %v, want closed", err)
	}
}

// Discard is scoped: it revokes only what the named flow minted and leaves
// the jar's other sessions and flows alone.
func TestDiscardBrowserFlowRevokesOnlyItsOwnSessions(t *testing.T) {
	v := newAdmissionTestVerifier(t)
	_, signedA, _, err := v.AdmitSession(
		context.Background(),
		BrowserSessionAdmission{FlowID: testFlowIDA, Epoch: testEpochA},
		testClaimsFor(testHumanA),
		time.Minute,
	)
	if err != nil {
		t.Fatalf("flow A admission: %v", err)
	}
	// A different jar (different epoch) signed into Human B.
	_, signedB, _, err := v.AdmitSession(
		context.Background(),
		BrowserSessionAdmission{Epoch: testEpochB},
		testClaimsFor(testHumanB),
		time.Minute,
	)
	if err != nil {
		t.Fatalf("epoch B admission: %v", err)
	}

	retired, err := v.DiscardBrowserFlow(
		context.Background(),
		testFlowIDA,
		time.Now().Add(browserFlowClosureHorizon),
	)
	if err != nil {
		t.Fatalf("discard: %v", err)
	}
	if len(retired) != 1 {
		t.Fatalf("discard retired %v, want flow A's session", retired)
	}
	if _, err := v.VerifySession(context.Background(), signedA); err == nil {
		t.Fatal("discarded flow's session still verifies")
	}
	if _, err := v.VerifySession(context.Background(), signedB); err != nil {
		t.Fatalf("discard disturbed another jar's session: %v", err)
	}
	// Discarding an already-closed flow is idempotent.
	if _, err := v.DiscardBrowserFlow(
		context.Background(),
		testFlowIDA,
		time.Now().Add(browserFlowClosureHorizon),
	); err != nil {
		t.Fatalf("idempotent discard: %v", err)
	}
}
