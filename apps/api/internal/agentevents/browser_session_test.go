package agentevents

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

var testSessionSecret = []byte("browser-session-secret-32-bytes!!")

type testBrowserSessionRevocationStore struct {
	mu        sync.RWMutex
	state     browserSessionRevocationState
	max       int
	checkErr  error
	revokeErr error
}

func newTestBrowserSessionRevocationStore() *testBrowserSessionRevocationStore {
	return &testBrowserSessionRevocationStore{
		state: newBrowserSessionRevocationState(),
		max:   maxRevokedSessions,
	}
}

func openSharedBrowserSessionGateways(
	t *testing.T,
) (*CommandStore, *DurableGateway, *DurableGateway) {
	t.Helper()
	store, err := OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	runtimeDir := t.TempDir()
	first, err := OpenDurableGateway(runtimeDir, store)
	if err != nil {
		t.Fatal(err)
	}
	second, err := OpenDurableGateway(runtimeDir, store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = first.runtimeDir.Close()
		_ = second.runtimeDir.Close()
	})
	return store, first, second
}

func waitBrowserSessionTestSignal(
	t *testing.T,
	signal <-chan struct{},
	label string,
) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func (s *testBrowserSessionRevocationStore) CheckBrowserSession(
	_ context.Context,
	sessionID string,
	expiresAt time.Time,
	now time.Time,
) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.checkErr != nil {
		return s.checkErr
	}
	return checkBrowserSessionState(
		s.state,
		sessionID,
		expiresAt.Unix(),
		now.Unix(),
	)
}

func (s *testBrowserSessionRevocationStore) AuthorizeBrowserSession(
	_ context.Context,
	sessionID string,
	expiresAt time.Time,
	now time.Time,
	operation func() error,
) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.checkErr != nil {
		return s.checkErr
	}
	if err := checkBrowserSessionState(
		s.state,
		sessionID,
		expiresAt.Unix(),
		now.Unix(),
	); err != nil {
		return err
	}
	return operation()
}

func (s *testBrowserSessionRevocationStore) RevokeBrowserSession(
	_ context.Context,
	sessionID string,
	expiresAt time.Time,
	now time.Time,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.revokeErr != nil {
		return s.revokeErr
	}
	next := cloneBrowserSessionRevocationState(s.state)
	cleanupBrowserSessionRevocations(&next, now.Unix())
	if err := revokeBrowserSessionState(
		&next,
		sessionID,
		expiresAt.Unix(),
		s.max,
	); err != nil {
		return err
	}
	if err := validateBrowserSessionRevocationState(next); err != nil {
		return err
	}
	s.state = next
	return nil
}

func (s *testBrowserSessionRevocationStore) RotateBrowserSession(
	_ context.Context,
	currentSessionID string,
	currentExpiresAt time.Time,
	successorSessionID string,
	successorExpiresAt time.Time,
	now time.Time,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.revokeErr != nil {
		return s.revokeErr
	}
	next := cloneBrowserSessionRevocationState(s.state)
	cleanupBrowserSessionRevocations(&next, now.Unix())
	if err := rotateBrowserSessionState(
		&next,
		currentSessionID,
		currentExpiresAt.Unix(),
		successorSessionID,
		successorExpiresAt.Unix(),
		s.max,
	); err != nil {
		return err
	}
	if err := validateBrowserSessionRevocationState(next); err != nil {
		return err
	}
	s.state = next
	return nil
}

type testSessionClaims struct {
	TenantID           string `json:"tenant_id"`
	UserID             string `json:"user_id"`
	PersonalityAgentID string `json:"personality_agent_id"`
	Iat                int64  `json:"iat"`
	Exp                int64  `json:"exp"`
	Aud                string `json:"aud"`
	SID                string `json:"sid"`
}

func signTestSession(t *testing.T, secret []byte, claims testSessionClaims) string {
	t.Helper()
	if claims.Iat == 0 && claims.Exp != 0 {
		claims.Iat = claims.Exp - int64(maxBrowserSessionTTL/time.Second)
	}
	if claims.SID == "" {
		claims.SID = base64.RawURLEncoding.EncodeToString(make([]byte, browserSessionIDBytes))
	}
	return signRawTestSession(t, secret, claims)
}

func signRawTestSession(t *testing.T, secret []byte, claims testSessionClaims) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString(mustJSON(t, claims))
	signingInput := header + "." + payload
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(signingInput))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return signingInput + "." + sig
}

func TestHMACUserSessionVerifierRequiresBoundedLifecycleClaims(t *testing.T) {
	v, err := NewHMACUserSessionVerifier(testSessionSecret, "", newTestBrowserSessionRevocationStore())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 31, 1, 0, 0, 0, time.UTC)
	v.now = func() time.Time { return now }
	valid := testSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
		Iat:                now.Add(-time.Minute).Unix(),
		Exp:                now.Add(time.Minute).Unix(),
		Aud:                defaultBrowserAudience,
		SID:                base64.RawURLEncoding.EncodeToString(make([]byte, browserSessionIDBytes)),
	}

	cases := []struct {
		name   string
		mutate func(*testSessionClaims)
	}{
		{name: "missing iat", mutate: func(c *testSessionClaims) { c.Iat = 0 }},
		{name: "future iat", mutate: func(c *testSessionClaims) { c.Iat = now.Add(time.Second).Unix() }},
		{name: "nonpositive lifetime", mutate: func(c *testSessionClaims) { c.Exp = c.Iat }},
		{name: "overlong lifetime", mutate: func(c *testSessionClaims) { c.Exp = c.Iat + int64(maxBrowserSessionTTL/time.Second) + 1 }},
		{name: "expired", mutate: func(c *testSessionClaims) { c.Exp = now.Unix() }},
		{name: "missing sid", mutate: func(c *testSessionClaims) { c.SID = "" }},
		{name: "short sid", mutate: func(c *testSessionClaims) {
			c.SID = base64.RawURLEncoding.EncodeToString(make([]byte, browserSessionIDBytes-1))
		}},
		{name: "padded sid", mutate: func(c *testSessionClaims) {
			c.SID = base64.URLEncoding.EncodeToString(make([]byte, browserSessionIDBytes))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			claims := valid
			tc.mutate(&claims)
			if _, err := v.VerifySession(context.Background(), signRawTestSession(t, testSessionSecret, claims)); err == nil {
				t.Fatal("expected lifecycle claims to be rejected")
			}
		})
	}
}

func TestHMACUserSessionVerifierRevocationIsAnAdmissionBarrier(t *testing.T) {
	v, err := NewHMACUserSessionVerifier(testSessionSecret, "", newTestBrowserSessionRevocationStore())
	if err != nil {
		t.Fatal(err)
	}
	session, err := v.IssueSession(context.Background(), UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := v.VerifySession(context.Background(), session)
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	authorized := make(chan error, 1)
	go func() {
		authorized <- v.AuthorizeSession(context.Background(), claims, func() error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started
	revoked := make(chan error, 1)
	go func() {
		_, err := v.RevokeSession(context.Background(), session)
		revoked <- err
	}()
	select {
	case err := <-revoked:
		t.Fatalf("revocation crossed an in-flight admission: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	if err := <-authorized; err != nil {
		t.Fatalf("authorized operation: %v", err)
	}
	if err := <-revoked; err != nil {
		t.Fatalf("revoke session: %v", err)
	}
	if _, err := v.VerifySession(context.Background(), session); err == nil {
		t.Fatal("revoked session still verified")
	}
	called := false
	if err := v.AuthorizeSession(context.Background(), claims, func() error {
		called = true
		return nil
	}); err == nil || called {
		t.Fatal("revoked session admitted a new operation")
	}
}

func TestHMACUserSessionVerifierBoundsAndReclaimsRevocations(t *testing.T) {
	revocations := newTestBrowserSessionRevocationStore()
	v, err := NewHMACUserSessionVerifier(
		testSessionSecret,
		"",
		revocations,
	)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 31, 1, 0, 0, 0, time.UTC)
	v.now = func() time.Time { return now }
	revocations.max = 1
	claims := UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
	}
	first, err := v.IssueSession(context.Background(), claims, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	second, err := v.IssueSession(context.Background(), claims, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.RevokeSession(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if _, err := v.RevokeSession(context.Background(), second); !errors.Is(err, errRevocationCapacity) {
		t.Fatalf("got %v, want revocation capacity error", err)
	}
	now = now.Add(time.Minute + time.Second)
	if _, err := v.RevokeSession(context.Background(), second); err != nil {
		t.Fatalf("expired revocation was not reclaimed: %v", err)
	}
}

func TestHMACUserSessionVerifierBoundsAndReclaimsRotationLineage(t *testing.T) {
	revocations := newTestBrowserSessionRevocationStore()
	revocations.max = 2
	verifier, err := NewHMACUserSessionVerifier(
		testSessionSecret,
		"",
		revocations,
	)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 31, 1, 0, 0, 0, time.UTC)
	verifier.now = func() time.Time { return now }
	claims := UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
	}
	current, err := verifier.IssueSession(
		context.Background(),
		claims,
		2*time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, successor, valid, err := verifier.RotateSession(
		context.Background(),
		current,
		claims,
		time.Minute,
	)
	if err != nil || !valid {
		t.Fatalf("first rotation: valid=%v err=%v", valid, err)
	}
	successorClaims, err := verifier.VerifySession(
		context.Background(),
		successor,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, valid, err := verifier.RotateSession(
		context.Background(),
		successor,
		claims,
		time.Minute,
	); !valid || !errors.Is(err, errRevocationCapacity) {
		t.Fatalf("bounded lineage rotation: valid=%v err=%v", valid, err)
	}
	if _, err := verifier.VerifySession(
		context.Background(),
		successor,
	); err != nil {
		t.Fatalf("failed rotation invalidated live successor: %v", err)
	}

	now = successorClaims.expiresAt.Add(2*time.Minute + time.Second)
	unrelated, err := verifier.IssueSession(
		context.Background(),
		claims,
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.RevokeSession(
		context.Background(),
		unrelated,
	); err != nil {
		t.Fatalf("expired lineage was not reclaimed: %v", err)
	}
	revocations.mu.RLock()
	count := browserSessionStateCount(revocations.state)
	revocations.mu.RUnlock()
	if count != 1 {
		t.Fatalf("revocation state count = %d, want 1 after lineage cleanup", count)
	}
}

func TestDurableBrowserSessionRevocationSurvivesRestartAndIsShared(t *testing.T) {
	commandDir := t.TempDir()
	runtimeDir := t.TempDir()
	store, err := OpenCommandStore(commandDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	firstGateway, err := OpenDurableGateway(runtimeDir, store)
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewHMACUserSessionVerifier(
		testSessionSecret,
		"",
		firstGateway,
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewHMACUserSessionVerifier(
		testSessionSecret,
		"",
		firstGateway,
	)
	if err != nil {
		t.Fatal(err)
	}
	session, err := first.IssueSession(context.Background(), UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.VerifySession(context.Background(), session); err != nil {
		t.Fatalf("second manager rejected live session: %v", err)
	}
	if _, err := first.RevokeSession(context.Background(), session); err != nil {
		t.Fatalf("revoke session: %v", err)
	}
	if _, err := second.VerifySession(context.Background(), session); !errors.Is(
		err,
		errBrowserSessionRevoked,
	) {
		t.Fatalf("shared manager verify = %v, want revoked", err)
	}
	if err := firstGateway.runtimeDir.Close(); err != nil {
		t.Fatalf("close first gateway runtime directory: %v", err)
	}

	restartedGateway, err := OpenDurableGateway(runtimeDir, store)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewHMACUserSessionVerifier(
		testSessionSecret,
		"",
		restartedGateway,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.VerifySession(context.Background(), session); !errors.Is(
		err,
		errBrowserSessionRevoked,
	) {
		t.Fatalf("restarted manager verify = %v, want revoked", err)
	}
}

func TestDurableBrowserSessionRevocationRejectsCorruptionButAcceptsLegacyState(
	t *testing.T,
) {
	_, gateway, _ := openSharedBrowserSessionGateways(t)
	verifier, err := NewHMACUserSessionVerifier(
		testSessionSecret,
		"",
		gateway,
	)
	if err != nil {
		t.Fatal(err)
	}
	session, err := verifier.IssueSession(
		context.Background(),
		UserSessionClaims{
			TenantID:           "tenant-1",
			UserID:             "user-1",
			PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
		},
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	statePath := gateway.browserSessionRevocationPath()
	if err := os.WriteFile(
		statePath,
		[]byte(`{"version":2,"entries":{},"lineages":null}`),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.VerifySession(
		context.Background(),
		session,
	); err == nil {
		t.Fatal("explicit null lineage state accepted a signed cookie")
	}
	if err := os.WriteFile(
		statePath,
		[]byte(`{"version":2,"entries":{}}`),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.VerifySession(
		context.Background(),
		session,
	); err == nil {
		t.Fatal("version 2 state missing lineages accepted a signed cookie")
	}
	if err := os.WriteFile(
		statePath,
		[]byte(`{"version":1,"entries":{}}`),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.VerifySession(
		context.Background(),
		session,
	); err != nil {
		t.Fatalf("legacy revocation state rejected a valid signed cookie: %v", err)
	}
}

func TestDurableBrowserSessionRotationSurvivesExpiredAncestorCleanupAndRestart(
	t *testing.T,
) {
	store, firstGateway, secondGateway := openSharedBrowserSessionGateways(t)
	first, err := NewHMACUserSessionVerifier(
		testSessionSecret,
		"",
		firstGateway,
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewHMACUserSessionVerifier(
		testSessionSecret,
		"",
		secondGateway,
	)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 31, 2, 0, 0, 0, time.UTC)
	first.now = func() time.Time { return now }
	second.now = func() time.Time { return now }
	claims := UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
	}
	ancestor, err := first.IssueSession(
		context.Background(),
		claims,
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(30 * time.Second)
	ancestorClaims, successor, valid, err := first.RotateSession(
		context.Background(),
		ancestor,
		claims,
		3*time.Minute,
	)
	if err != nil || !valid {
		t.Fatalf("rotate ancestor: valid=%v err=%v", valid, err)
	}

	now = now.Add(31 * time.Second)
	if !now.After(ancestorClaims.expiresAt) {
		t.Fatal("test clock did not advance past ancestor expiry")
	}
	_, latest, valid, err := second.RotateSession(
		context.Background(),
		successor,
		claims,
		3*time.Minute,
	)
	if err != nil || !valid {
		t.Fatalf("rotate live successor after ancestor expiry: valid=%v err=%v", valid, err)
	}

	if err := firstGateway.runtimeDir.Close(); err != nil {
		t.Fatalf("close first gateway runtime directory: %v", err)
	}
	if err := secondGateway.runtimeDir.Close(); err != nil {
		t.Fatalf("close second gateway runtime directory: %v", err)
	}
	restartedGateway, err := OpenDurableGateway(firstGateway.dir, store)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewHMACUserSessionVerifier(
		testSessionSecret,
		"",
		restartedGateway,
	)
	if err != nil {
		t.Fatal(err)
	}
	restarted.now = func() time.Time { return now }
	if _, err := restarted.VerifySession(
		context.Background(),
		latest,
	); err != nil {
		t.Fatalf("restarted verifier rejected latest successor: %v", err)
	}
}

func TestDurableBrowserSessionRotationThenLogoutRevokesSuccessorAcrossRestart(
	t *testing.T,
) {
	store, firstGateway, secondGateway := openSharedBrowserSessionGateways(t)
	first, err := NewHMACUserSessionVerifier(
		testSessionSecret,
		"",
		firstGateway,
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewHMACUserSessionVerifier(
		testSessionSecret,
		"",
		secondGateway,
	)
	if err != nil {
		t.Fatal(err)
	}
	claims := UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
	}
	current, err := first.IssueSession(
		context.Background(),
		claims,
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	rotationEntered := make(chan struct{})
	releaseRotation := make(chan struct{})
	defer func() {
		select {
		case <-releaseRotation:
		default:
			close(releaseRotation)
		}
	}()
	firstGateway.browserSessionMutationHook = func(
		kind browserSessionMutationKind,
	) {
		if kind == browserSessionMutationRotate {
			close(rotationEntered)
			<-releaseRotation
		}
	}
	type rotationResult struct {
		successor string
		valid     bool
		err       error
	}
	rotationDone := make(chan rotationResult, 1)
	go func() {
		_, successor, valid, err := first.RotateSession(
			context.Background(),
			current,
			claims,
			2*time.Minute,
		)
		rotationDone <- rotationResult{
			successor: successor,
			valid:     valid,
			err:       err,
		}
	}()
	waitBrowserSessionTestSignal(t, rotationEntered, "rotation lock")

	type logoutResult struct {
		valid bool
		err   error
	}
	logoutAttempted := make(chan struct{})
	var logoutAttemptedOnce sync.Once
	secondGateway.browserSessionLockAttemptHook = func() {
		logoutAttemptedOnce.Do(func() { close(logoutAttempted) })
	}
	logoutDone := make(chan logoutResult, 1)
	go func() {
		_, valid, err := second.RevokeSessionForLogout(
			context.Background(),
			current,
		)
		logoutDone <- logoutResult{valid: valid, err: err}
	}()
	waitBrowserSessionTestSignal(t, logoutAttempted, "concurrent logout lock attempt")
	close(releaseRotation)

	var rotated rotationResult
	select {
	case rotated = <-rotationDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for rotation")
	}
	if rotated.err != nil || !rotated.valid || rotated.successor == "" {
		t.Fatalf("rotation result = %+v", rotated)
	}
	select {
	case loggedOut := <-logoutDone:
		if loggedOut.err != nil || !loggedOut.valid {
			t.Fatalf("logout result = %+v", loggedOut)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for logout")
	}
	if _, err := second.VerifySession(
		context.Background(),
		rotated.successor,
	); !errors.Is(err, errBrowserSessionRevoked) {
		t.Fatalf("successor after ancestor logout = %v, want revoked", err)
	}

	if err := firstGateway.runtimeDir.Close(); err != nil {
		t.Fatalf("close first gateway runtime directory: %v", err)
	}
	if err := secondGateway.runtimeDir.Close(); err != nil {
		t.Fatalf("close second gateway runtime directory: %v", err)
	}
	restartedGateway, err := OpenDurableGateway(firstGateway.dir, store)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewHMACUserSessionVerifier(
		testSessionSecret,
		"",
		restartedGateway,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.VerifySession(
		context.Background(),
		rotated.successor,
	); !errors.Is(err, errBrowserSessionRevoked) {
		t.Fatalf("restarted successor check = %v, want revoked", err)
	}
}

func TestDurableBrowserSessionLogoutThenRotationRejectsSuccessor(
	t *testing.T,
) {
	_, firstGateway, secondGateway := openSharedBrowserSessionGateways(t)
	first, err := NewHMACUserSessionVerifier(
		testSessionSecret,
		"",
		firstGateway,
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewHMACUserSessionVerifier(
		testSessionSecret,
		"",
		secondGateway,
	)
	if err != nil {
		t.Fatal(err)
	}
	claims := UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
	}
	current, err := first.IssueSession(
		context.Background(),
		claims,
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	logoutEntered := make(chan struct{})
	releaseLogout := make(chan struct{})
	defer func() {
		select {
		case <-releaseLogout:
		default:
			close(releaseLogout)
		}
	}()
	firstGateway.browserSessionMutationHook = func(
		kind browserSessionMutationKind,
	) {
		if kind == browserSessionMutationRevoke {
			close(logoutEntered)
			<-releaseLogout
		}
	}
	type logoutResult struct {
		valid bool
		err   error
	}
	logoutDone := make(chan logoutResult, 1)
	go func() {
		_, valid, err := first.RevokeSessionForLogout(
			context.Background(),
			current,
		)
		logoutDone <- logoutResult{valid: valid, err: err}
	}()
	waitBrowserSessionTestSignal(t, logoutEntered, "logout lock")

	type rotationResult struct {
		successor string
		valid     bool
		err       error
	}
	rotationAttempted := make(chan struct{})
	var rotationAttemptedOnce sync.Once
	secondGateway.browserSessionLockAttemptHook = func() {
		rotationAttemptedOnce.Do(func() { close(rotationAttempted) })
	}
	rotationDone := make(chan rotationResult, 1)
	go func() {
		_, successor, valid, err := second.RotateSession(
			context.Background(),
			current,
			claims,
			time.Minute,
		)
		rotationDone <- rotationResult{
			successor: successor,
			valid:     valid,
			err:       err,
		}
	}()
	waitBrowserSessionTestSignal(t, rotationAttempted, "concurrent rotation lock attempt")
	close(releaseLogout)

	select {
	case loggedOut := <-logoutDone:
		if loggedOut.err != nil || !loggedOut.valid {
			t.Fatalf("logout result = %+v", loggedOut)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for logout")
	}
	select {
	case rotated := <-rotationDone:
		if !rotated.valid ||
			rotated.successor != "" ||
			!errors.Is(rotated.err, errBrowserSessionRetired) {
			t.Fatalf("rotation result = %+v, want retired", rotated)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for rotation")
	}
}

func TestDurableBrowserSessionRevocationFailuresFailClosed(t *testing.T) {
	commandDir := t.TempDir()
	runtimeDir := t.TempDir()
	store, err := OpenCommandStore(commandDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	gateway, err := OpenDurableGateway(runtimeDir, store)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := NewHMACUserSessionVerifier(
		testSessionSecret,
		"",
		gateway,
	)
	if err != nil {
		t.Fatal(err)
	}
	claims := UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
	}
	first, err := verifier.IssueSession(context.Background(), claims, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	gateway.writeAtomic = func(string, []byte, os.FileMode) error {
		return errors.New("injected revocation write failure")
	}
	if _, err := verifier.RevokeSession(context.Background(), first); err == nil {
		t.Fatal("revocation write failure was reported as success")
	}

	gateway.writeAtomic = writeFileAtomic
	gateway.MaxBrowserSessionRevocations = 1
	if _, err := verifier.RevokeSession(context.Background(), first); err != nil {
		t.Fatalf("revoke first session: %v", err)
	}
	second, err := verifier.IssueSession(context.Background(), claims, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.RevokeSession(context.Background(), second); !errors.Is(
		err,
		errRevocationCapacity,
	) {
		t.Fatalf("capacity failure = %v, want %v", err, errRevocationCapacity)
	}

	gateway.writeAtomic = func(string, []byte, os.FileMode) error {
		return errors.New("not used")
	}
	if err := os.Chmod(gateway.browserSessionRevocationPath(), 0o000); err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.VerifySession(context.Background(), second); err == nil {
		t.Fatal("revocation-store read failure accepted a signed cookie")
	}
}

func TestHMACUserSessionVerifierAcceptsValidSession(t *testing.T) {
	v, err := NewHMACUserSessionVerifier(testSessionSecret, "", newTestBrowserSessionRevocationStore())
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	session := signTestSession(t, testSessionSecret, testSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
		Exp:                time.Now().Add(time.Hour).Unix(),
		Aud:                defaultBrowserAudience,
	})

	claims, err := v.VerifySession(context.Background(), session)
	if err != nil {
		t.Fatalf("verify valid session: %v", err)
	}
	if claims.TenantID != "tenant-1" || claims.UserID != "user-1" || claims.PersonalityAgentID != "018f47a2-9b3c-7def-8abc-0123456789ab" {
		t.Fatalf("unexpected claims: %+v", claims)
	}
}

func TestHMACUserSessionVerifierIssuesItsOwnVerifiableSession(t *testing.T) {
	v, err := NewHMACUserSessionVerifier(testSessionSecret, "", newTestBrowserSessionRevocationStore())
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}
	want := UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
	}
	session, err := v.IssueSession(context.Background(), want, 5*time.Minute)
	if err != nil {
		t.Fatalf("issue session: %v", err)
	}
	got, err := v.VerifySession(context.Background(), session)
	if err != nil {
		t.Fatalf("verify issued session: %v", err)
	}
	if got.TenantID != want.TenantID ||
		got.UserID != want.UserID ||
		got.PersonalityAgentID != want.PersonalityAgentID {
		t.Fatalf("got %+v, want %+v", got, want)
	}

	if _, err := v.IssueSession(context.Background(), want, time.Hour+time.Second); err == nil {
		t.Fatal("expected overlong session TTL to be rejected")
	}
}

func TestHMACUserSessionVerifierScopesOpaqueAuthorityBinding(t *testing.T) {
	now := time.Date(2026, 7, 31, 1, 0, 0, 0, time.UTC)
	v, err := NewHMACUserSessionVerifier(testSessionSecret, "", newTestBrowserSessionRevocationStore())
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}
	v.now = func() time.Time { return now }
	binding := UserSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
		authorityBindingID: strings.Repeat("caller-authored", 4),
	}

	issueAndVerify := func(
		t *testing.T,
		verifier *HMACUserSessionVerifier,
		claims UserSessionClaims,
	) UserSessionClaims {
		t.Helper()
		session, issueErr := verifier.IssueSession(
			context.Background(),
			claims,
			5*time.Minute,
		)
		if issueErr != nil {
			t.Fatalf("issue session: %v", issueErr)
		}
		verified, verifyErr := verifier.VerifySession(
			context.Background(),
			session,
		)
		if verifyErr != nil {
			t.Fatalf("verify session: %v", verifyErr)
		}
		return verified
	}

	first := issueAndVerify(t, v, binding)
	bindingWithDifferentCallerID := binding
	bindingWithDifferentCallerID.authorityBindingID = "different-caller-value"
	second := issueAndVerify(t, v, bindingWithDifferentCallerID)
	if first.authorityBindingID != second.authorityBindingID {
		t.Fatalf(
			"same signed authority binding changed across session issuance: %q != %q",
			first.authorityBindingID,
			second.authorityBindingID,
		)
	}
	if first.authorityBindingID == binding.authorityBindingID {
		t.Fatal("session issuer accepted a caller-authored authority binding ID")
	}
	if !validBrowserAuthorityBindingID(first.authorityBindingID) {
		t.Fatalf("authority binding ID is not canonical: %q", first.authorityBindingID)
	}

	for _, tc := range []struct {
		name   string
		mutate func(*UserSessionClaims)
	}{
		{
			name:   "tenant",
			mutate: func(claims *UserSessionClaims) { claims.TenantID = "tenant-2" },
		},
		{
			name:   "human principal",
			mutate: func(claims *UserSessionClaims) { claims.UserID = "user-2" },
		},
		{
			name: "target personality agent",
			mutate: func(claims *UserSessionClaims) {
				claims.PersonalityAgentID = "018f47a2-9b3c-7def-8abc-0123456789ac"
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := binding
			tc.mutate(&changed)
			got := issueAndVerify(t, v, changed)
			if got.authorityBindingID == first.authorityBindingID {
				t.Fatalf("changed %s reused authority binding ID", tc.name)
			}
		})
	}

	rotated, err := NewHMACUserSessionVerifier(
		[]byte("rotated-browser-session-secret-32-bytes!"),
		"",
		newTestBrowserSessionRevocationStore(),
	)
	if err != nil {
		t.Fatalf("new rotated verifier: %v", err)
	}
	rotated.now = func() time.Time { return now }
	afterRotation := issueAndVerify(t, rotated, binding)
	if afterRotation.authorityBindingID == first.authorityBindingID {
		t.Fatal("key rotation must cause a safe authority binding reset")
	}
}

func TestHMACUserSessionVerifierRejectsWrongAudience(t *testing.T) {
	v, err := NewHMACUserSessionVerifier(
		testSessionSecret,
		"sumi:web:conversation",
		newTestBrowserSessionRevocationStore(),
	)
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	session := signTestSession(t, testSessionSecret, testSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
		Exp:                time.Now().Add(time.Hour).Unix(),
		Aud:                "other-audience",
	})

	if _, err := v.VerifySession(context.Background(), session); err == nil {
		t.Fatal("expected wrong audience session to be rejected")
	}
}

func TestHMACUserSessionVerifierRejectsDuplicateKeysUnknownFieldsAndWrongTyp(t *testing.T) {
	v, err := NewHMACUserSessionVerifier(testSessionSecret, "", newTestBrowserSessionRevocationStore())
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	cases := []struct {
		name   string
		header string
		claims string
	}{
		{
			name:   "duplicate header keys",
			header: `{"alg":"HS256","alg":"HS256","typ":"JWT"}`,
			claims: `{"tenant_id":"tenant-1","user_id":"user-1","personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","exp":` + fmt.Sprintf("%d", time.Now().Add(time.Hour).Unix()) + `,"aud":"` + defaultBrowserAudience + `"}`,
		},
		{
			name:   "duplicate claims keys",
			header: `{"alg":"HS256","typ":"JWT"}`,
			claims: `{"tenant_id":"tenant-1","tenant_id":"tenant-1","user_id":"user-1","personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","exp":` + fmt.Sprintf("%d", time.Now().Add(time.Hour).Unix()) + `,"aud":"` + defaultBrowserAudience + `"}`,
		},
		{
			name:   "unknown header field",
			header: `{"alg":"HS256","typ":"JWT","extra":"field"}`,
			claims: `{"tenant_id":"tenant-1","user_id":"user-1","personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","exp":` + fmt.Sprintf("%d", time.Now().Add(time.Hour).Unix()) + `,"aud":"` + defaultBrowserAudience + `"}`,
		},
		{
			name:   "wrong typ",
			header: `{"alg":"HS256","typ":"session"}`,
			claims: `{"tenant_id":"tenant-1","user_id":"user-1","personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","exp":` + fmt.Sprintf("%d", time.Now().Add(time.Hour).Unix()) + `,"aud":"` + defaultBrowserAudience + `"}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			header := base64.RawURLEncoding.EncodeToString([]byte(tc.header))
			claims := base64.RawURLEncoding.EncodeToString([]byte(tc.claims))
			signingInput := header + "." + claims
			mac := hmac.New(sha256.New, testSessionSecret)
			mac.Write([]byte(signingInput))
			sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
			session := signingInput + "." + sig

			if _, err := v.VerifySession(context.Background(), session); err == nil {
				t.Fatal("expected malformed session to be rejected")
			}
		})
	}
}

func TestHMACUserSessionVerifierRejectsTamperedSignature(t *testing.T) {
	v, err := NewHMACUserSessionVerifier(testSessionSecret, "", newTestBrowserSessionRevocationStore())
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	session := signTestSession(t, testSessionSecret, testSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
		Exp:                time.Now().Add(time.Hour).Unix(),
		Aud:                defaultBrowserAudience,
	})
	session = session + "x"

	if _, err := v.VerifySession(context.Background(), session); err == nil {
		t.Fatal("expected tampered session to be rejected")
	}
}

func TestHMACUserSessionVerifierRejectsPaddedInput(t *testing.T) {
	// browser sessions use raw base64url; padded signatures should be trimmed
	// defensively before HMAC comparison.
	v, err := NewHMACUserSessionVerifier(testSessionSecret, "", newTestBrowserSessionRevocationStore())
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	session := signTestSession(t, testSessionSecret, testSessionClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
		Exp:                time.Now().Add(time.Hour).Unix(),
		Aud:                defaultBrowserAudience,
	})
	parts := strings.Split(session, ".")
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatalf("decode signature: %v", err)
	}
	parts[2] = base64.URLEncoding.EncodeToString(sig)
	session = strings.Join(parts, ".")

	if _, err := v.VerifySession(context.Background(), session); err != nil {
		t.Fatalf("padded signature must verify: %v", err)
	}
}
