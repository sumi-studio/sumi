package agentevents

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log"
	"mime"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	BrowserCSRFCookie = "sumi_csrf"
	// BrowserEpochCookie is an HttpOnly browser-jar authority marker. Its
	// server-held hash names "this cookie jar" so session admission can
	// compare a flow's issuing browser against the jar's current Human even
	// when the request's own session cookie is absent or already stale.
	BrowserEpochCookie       = "sumi_browser"
	browserEpochBytes        = 32
	defaultBrowserSessionTTL = 15 * time.Minute
	maxAuthRequestBytes      = 16 * 1024
	maxFirebaseIDTokenBytes  = 12 * 1024
	csrfTokenBytes           = 32
)

// FirebaseIdentity contains only the stable server-verified Firebase
// principal needed for Sumi identity binding.
type FirebaseIdentity struct {
	UID              string
	TenantID         string
	DisplayName      string
	Email            string
	EmailVerified    bool
	SignInProvider   string
	ProviderSubjects map[string][]string
	AuthTime         time.Time
	IssuedAt         time.Time
}

// FirebaseIDTokenVerifier verifies a Firebase client ID token server-side.
// Production uses the Firebase Admin SDK; tests use a deterministic fake.
type FirebaseIDTokenVerifier interface {
	VerifyIDToken(ctx context.Context, idToken string) (FirebaseIdentity, error)
}

// IdentityBindingResolver maps a verified external principal to Sumi's
// server-owned authorization binding. Browsers never author these claims.
type IdentityBindingResolver interface {
	ResolveIdentity(ctx context.Context, identity FirebaseIdentity) (UserSessionClaims, error)
}

// HumanProfileReader exposes only the canonical Human-owned presentation name
// needed by the authenticated session bootstrap. It does not expose external
// identity metadata or agent ownership internals.
type HumanProfileReader interface {
	HumanDisplayName(ctx context.Context, humanID string) (string, error)
}

var (
	// ErrDirectChatAuthorizationDenied deliberately collapses Employer and
	// exact AppInstallation mismatches at the browser boundary.
	ErrDirectChatAuthorizationDenied = errors.New("direct-chat authorization denied")
	// ErrDirectChatAuthorizationUnavailable distinguishes an unbound or failed
	// authority store from a valid-but-denied capability.
	ErrDirectChatAuthorizationUnavailable = errors.New("direct-chat authorization unavailable")
)

// DirectChatAuthorizer commits one Current-Employer + exact enabled
// participant-owned direct-chat AppInstallation snapshot before a private
// operation starts. The caller's process-lifetime lifecycle permit then holds
// that authority ordering across the effect; PostgreSQL locks do not. The
// installation identity and epoch are transport control metadata and never
// become command or provenance data.
type DirectChatAuthorizer interface {
	AuthorizeDirectChat(
		ctx context.Context,
		humanID,
		personalityAgentID,
		installationID string,
		authorityEpoch int64,
	) error
}

// DirectChatSpawner lazily starts an agent runtime on 呼びかけ (ADR 0010). The
// EnsureRunning context bounds provisioning only; successful runtime lifetime
// belongs to the provisioner. Calls for one agent must be idempotent. A nil
// spawner disables lazy spawn (the agent is assumed already running).
type DirectChatSpawner interface {
	EnsureRunning(ctx context.Context, agentID string) error
	Touch(agentID string)
}

// holdRuntimeAdmission coordinates the production idle reaper with durable
// append. External/test spawners without an idle reaper need no such hold.
func holdRuntimeAdmission(spawner DirectChatSpawner, agentID string) (func(), error) {
	if holder, ok := spawner.(interface{ HoldAdmission(string) (func(), error) }); ok {
		release, err := holder.HoldAdmission(agentID)
		if err != nil {
			return nil, errBrowserRuntimeUnavailable
		}
		return release, nil
	}
	return func() {}, nil
}

// StaticIdentityBindingResolver is the deliberately narrow hackathon binding:
// exactly one configured Firebase UID maps to exactly one server-owned Sumi
// principal. Every other external identity is denied.
type StaticIdentityBindingResolver struct {
	firebaseUID      string
	firebaseTenantID string
	claims           UserSessionClaims
}

func NewStaticIdentityBindingResolver(
	firebaseUID string,
	claims UserSessionClaims,
) (*StaticIdentityBindingResolver, error) {
	return NewStaticIdentityBindingResolverForTenant(firebaseUID, "", claims)
}

// NewStaticIdentityBindingResolverForTenant explicitly binds both the
// Firebase UID and, when non-empty, its Identity Platform tenant. An
// unconfigured tenant binding accepts only non-tenant Firebase tokens.
func NewStaticIdentityBindingResolverForTenant(
	firebaseUID string,
	firebaseTenantID string,
	claims UserSessionClaims,
) (*StaticIdentityBindingResolver, error) {
	if firebaseUID == "" || len(firebaseUID) > 128 {
		return nil, errors.New("Firebase UID binding must be between 1 and 128 bytes")
	}
	if len(firebaseTenantID) > 128 {
		return nil, errors.New("Firebase tenant binding must not exceed 128 bytes")
	}
	if !provenanceIDRegexp.MatchString(claims.TenantID) ||
		!provenanceIDRegexp.MatchString(claims.UserID) {
		return nil, errors.New("identity binding has invalid tenant or user ID")
	}
	if err := ValidatePersonalityAgentID(claims.PersonalityAgentID); err != nil {
		return nil, errors.New("identity binding has invalid personality-agent ID")
	}
	return &StaticIdentityBindingResolver{
		firebaseUID:      firebaseUID,
		firebaseTenantID: firebaseTenantID,
		claims:           claims,
	}, nil
}

func (r *StaticIdentityBindingResolver) ResolveIdentity(
	ctx context.Context,
	identity FirebaseIdentity,
) (UserSessionClaims, error) {
	select {
	case <-ctx.Done():
		return UserSessionClaims{}, ctx.Err()
	default:
	}
	if identity.UID != r.firebaseUID || identity.TenantID != r.firebaseTenantID {
		return UserSessionClaims{}, errors.New("Firebase identity is not bound")
	}
	return r.claims, nil
}

type browserSessionManager interface {
	BrowserSessionLifecycle
}

type BrowserSessionConnectionCloser interface {
	CloseBrowserSession(sessionID string)
}

// BrowserAuthServer exchanges verified Firebase identities for the same
// opaque HttpOnly session consumed by targetless direct-chat routes.
type BrowserAuthServer struct {
	EnrollmentInvitations EnrollmentInvitationStore
	EnrollmentAdmins      map[string]bool
	authAllocations       authAllocationLimiter
	Firebase              FirebaseIDTokenVerifier
	Bindings              IdentityBindingResolver
	Sessions              browserSessionManager
	AllowedOrigins        []string
	SecureCookies         bool
	SessionTTL            time.Duration
	Connections           BrowserSessionConnectionCloser
	PushDevices           BrowserPushDevices
	Flows                 BrowserAuthFlowController
	EmailFlows            BrowserEmailAuthController
	Profiles              HumanProfileReader
	random                io.Reader
	sessionMu             sync.Mutex
}

func NewBrowserAuthServer(
	firebase FirebaseIDTokenVerifier,
	bindings IdentityBindingResolver,
	sessions browserSessionManager,
	allowedOrigins []string,
	secureCookies bool,
) (*BrowserAuthServer, error) {
	if firebase == nil || bindings == nil || sessions == nil {
		return nil, errors.New("browser auth requires Firebase verification, identity binding, and session management")
	}
	if len(allowedOrigins) == 0 {
		return nil, errors.New("browser auth requires at least one allowed origin")
	}
	for _, origin := range allowedOrigins {
		if strings.TrimSpace(origin) == "" || origin == "*" {
			return nil, errors.New("browser auth origins must be non-empty and exact")
		}
	}
	return &BrowserAuthServer{
		Firebase:       firebase,
		Bindings:       bindings,
		Sessions:       sessions,
		AllowedOrigins: append([]string(nil), allowedOrigins...),
		SecureCookies:  secureCookies,
		SessionTTL:     defaultBrowserSessionTTL,
		random:         rand.Reader,
	}, nil
}

// RegisterRoutes attaches the browser authentication boundary. Callers should
// omit registration entirely when authentication is not configured.
func (s *BrowserAuthServer) RegisterRoutes(mux *http.ServeMux) {
	if s.EnrollmentInvitations != nil {
		s.registerEnrollmentRoutes(mux)
	}
	mux.HandleFunc("GET /auth/csrf", s.serveCSRF)
	if s.Flows == nil {
		mux.HandleFunc("POST /auth/session", s.serveSessionExchange)
	} else {
		mux.HandleFunc("POST /auth/flows", s.serveStartAuthFlow)
		mux.HandleFunc("POST /auth/flows/resolve", s.serveResolveAuthFlow)
		mux.HandleFunc("POST /auth/flows/confirm", s.serveConfirmAuthFlow)
		mux.HandleFunc("POST /auth/flows/status", s.serveAuthFlowStatus)
		mux.HandleFunc("POST /auth/flows/discard", s.serveDiscardAuthFlow)
		mux.HandleFunc("POST /auth/providers/operations", s.serveStartProviderOperation)
		mux.HandleFunc("POST /auth/providers/operations/complete", s.serveCompleteProviderOperation)
		mux.HandleFunc("POST /auth/providers/operations/fail", s.serveFailProviderOperation)
		mux.HandleFunc("POST /auth/providers/operations/status", s.serveProviderOperationStatus)
		if s.EmailFlows != nil {
			mux.HandleFunc("POST /auth/email/verify", s.serveVerifyEmailCode)
			mux.HandleFunc("POST /auth/email/resend", s.serveResendEmailCode)
			mux.HandleFunc("POST /auth/email/status", s.serveEmailFlowStatus)
			mux.HandleFunc("POST /auth/email/link/inspect", s.serveInspectEmailLink)
			mux.HandleFunc("POST /auth/email/link/complete", s.serveCompleteEmailLink)
		}
	}
	mux.HandleFunc("GET /auth/session", s.serveSessionStatus)
	mux.HandleFunc("POST /auth/logout", s.serveLogout)
}

func (s *BrowserAuthServer) serveCSRF(w http.ResponseWriter, r *http.Request) {
	if !s.allowSafeReadOrigin(w, r) {
		return
	}
	tokenBytes := make([]byte, csrfTokenBytes)
	if _, err := io.ReadFull(s.random, tokenBytes); err != nil {
		writeBrowserAuthError(w, http.StatusServiceUnavailable, "authentication unavailable")
		return
	}
	token := base64.RawURLEncoding.EncodeToString(tokenBytes)
	http.SetCookie(w, s.csrfCookie(token, 0))
	s.expireLegacyCSRFCookie(w)
	// The CSRF bootstrap is also where a browser jar first identifies itself;
	// minting the epoch here covers flows started before any session exists.
	_, _ = s.browserEpochHash(w, r, true)
	writeBrowserAuthJSON(w, http.StatusOK, map[string]string{"csrf_token": token})
}

func (s *BrowserAuthServer) serveSessionExchange(w http.ResponseWriter, r *http.Request) {
	if !s.allowAuthAllocation(w, r) {
		return
	}
	if !s.allowOrigin(w, r) || !s.requireCSRF(w, r) {
		return
	}
	if len(r.CookiesNamed(BrowserSessionCookie)) > 1 {
		writeBrowserAuthError(w, http.StatusBadRequest, "duplicate session cookies")
		return
	}
	if !hasJSONContentType(r.Header.Get("Content-Type")) {
		writeBrowserAuthError(w, http.StatusUnsupportedMediaType, "application/json required")
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxAuthRequestBytes))
	if err != nil {
		writeBrowserAuthError(w, http.StatusRequestEntityTooLarge, "request too large")
		return
	}
	if err := checkDuplicateKeys(body); err != nil {
		writeBrowserAuthError(w, http.StatusBadRequest, "invalid request")
		return
	}
	var request struct {
		IDToken string `json:"id_token"`
	}
	if err := unmarshalStrict(body, &request); err != nil ||
		request.IDToken == "" ||
		len(request.IDToken) > maxFirebaseIDTokenBytes {
		writeBrowserAuthError(w, http.StatusBadRequest, "invalid request")
		return
	}

	identity, err := s.Firebase.VerifyIDToken(r.Context(), request.IDToken)
	if err != nil || identity.UID == "" {
		writeBrowserAuthError(w, http.StatusUnauthorized, "authentication failed")
		return
	}
	claims, err := s.Bindings.ResolveIdentity(r.Context(), identity)
	if err != nil {
		writeBrowserAuthError(w, http.StatusForbidden, "account is not authorized")
		return
	}
	if err := s.establishSession(w, r, claims, "", ""); err != nil {
		s.writeSessionIssuanceError(w, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
}

// establishSession is the single session-issuance boundary. The successor's
// lineage and provenance (flow, epoch, Human) commit to the durable store
// under the exclusive lock BEFORE the token is signed, so a logout, discard,
// or explicit switch that linearizes first always wins. A successor for a
// different Human than the jar's current authority requires the caller's
// explicit switchFrom to name that authority — the request's own cookie is
// only a fallback witness, the epoch's live session is authoritative.
func (s *BrowserAuthServer) establishSession(
	w http.ResponseWriter,
	r *http.Request,
	claims UserSessionClaims,
	flowID string,
	switchFromUserID string,
) error {
	if s.PushDevices != nil && len(r.CookiesNamed(BrowserPushDeviceCookie)) > 1 {
		return errors.New("duplicate push device cookies")
	}
	if len(r.CookiesNamed(BrowserSessionCookie)) > 1 {
		return errors.New("duplicate session cookies")
	}
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	ttl := s.SessionTTL
	if ttl == 0 {
		ttl = defaultBrowserSessionTTL
	}
	var presented *BrowserSessionIdentity
	presentedBy := ""
	if existing, cookieErr := uniqueBrowserSessionCookie(r); cookieErr == nil {
		if verified, verifyErr := s.Sessions.VerifySessionLocal(r.Context(), existing.Value); verifyErr == nil {
			identity := verified.BrowserSessionIdentity()
			presented = &identity
			presentedBy = verified.UserID
		}
	}
	epochHash, err := s.browserEpochHash(w, r, true)
	if err != nil {
		return err
	}
	admission := BrowserSessionAdmission{
		Presented:   presented,
		PresentedBy: presentedBy,
		Epoch:       epochHash,
		SwitchFrom:  switchFromUserID,
		FlowID:      flowID,
	}
	if flowID != "" && s.Flows != nil {
		admission.FlowEpoch, err = s.Flows.AuthFlowEpoch(r.Context(), flowID)
		if err != nil {
			return err
		}
	}
	admitted, signed, outcome, err := s.Sessions.AdmitSession(
		r.Context(), admission, claims, ttl)
	if err != nil {
		return err
	}
	if s.Connections != nil {
		for _, sessionID := range outcome.RetiredSessionIDs {
			s.Connections.CloseBrowserSession(sessionID)
		}
	}
	if len(outcome.ClosedFlowIDs) > 0 && s.Flows != nil {
		// The closed-flow barrier already committed in the session store;
		// this mirrors it for status/proof reporting only.
		_ = s.Flows.CloseFlows(r.Context(), outcome.ClosedFlowIDs)
	}
	if err := s.refreshPushDevice(w, r, admitted.UserID); err != nil {
		return err
	}
	http.SetCookie(w, s.sessionCookie(signed, int(ttl/time.Second)))
	return nil
}

// writeSessionIssuanceError separates the deliberate refusals a client can
// recover from — a closed flow (discard the pending state) and a different
// live Human (ask or switch explicitly) — from storage failures.
func (s *BrowserAuthServer) writeSessionIssuanceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errBrowserFlowClosed):
		writeBrowserAuthError(w, http.StatusGone, "flow_closed")
	case errors.Is(err, ErrBrowserSessionActive):
		writeBrowserAuthError(w, http.StatusConflict, "session_active")
	default:
		writeBrowserAuthError(w, http.StatusServiceUnavailable, "authentication unavailable")
	}
}

func (s *BrowserAuthServer) serveSessionStatus(w http.ResponseWriter, r *http.Request) {
	if !s.allowSafeReadOrigin(w, r) {
		return
	}
	cookie, err := uniqueBrowserSessionCookie(r)
	if err != nil {
		if errors.Is(err, errBrowserSessionDuplicate) {
			writeBrowserAuthError(w, http.StatusBadRequest, "duplicate session cookies")
			return
		}
		writeBrowserAuthJSON(w, http.StatusOK, map[string]bool{"authenticated": false})
		return
	}
	claims, err := s.Sessions.VerifySession(r.Context(), cookie.Value)
	if err != nil {
		if errors.Is(err, errBrowserSessionInvalid) || errors.Is(err, errBrowserSessionRevoked) {
			writeBrowserAuthJSON(w, http.StatusOK, map[string]bool{"authenticated": false})
		} else {
			// Failure to check revocation is not proof of session loss.
			// Keep authorization closed without telling clients to erase drafts.
			writeBrowserAuthError(w, http.StatusServiceUnavailable, "authentication unavailable")
		}
		return
	}
	if !validBrowserAuthorityBindingID(claims.authorityBindingID) {
		writeBrowserAuthError(w, http.StatusServiceUnavailable, "authentication unavailable")
		return
	}
	displayName := ""
	if s.Profiles != nil {
		displayName, err = s.Profiles.HumanDisplayName(r.Context(), claims.UserID)
		if err != nil {
			writeBrowserAuthError(w, http.StatusServiceUnavailable, "authentication unavailable")
			return
		}
	}
	writeBrowserAuthJSON(w, http.StatusOK, struct {
		Authenticated      bool   `json:"authenticated"`
		AuthorityBindingID string `json:"authority_binding_id"`
		User               struct {
			ID          string `json:"id"`
			DisplayName string `json:"display_name"`
		} `json:"user"`
	}{
		Authenticated:      true,
		AuthorityBindingID: claims.authorityBindingID,
		User: struct {
			ID          string `json:"id"`
			DisplayName string `json:"display_name"`
		}{ID: claims.UserID, DisplayName: displayName},
	})
}

func (s *BrowserAuthServer) serveLogout(w http.ResponseWriter, r *http.Request) {
	if !s.allowOrigin(w, r) || !s.requireCSRF(w, r) {
		return
	}
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	// Retain HTTP authority if device revocation fails, so the user keeps the
	// logout control and can retry. A removed device cannot be registered by an
	// in-flight application request; only a later authenticated exchange can
	// create a new device, and exchanges are serialized by sessionMu.
	if err := s.revokePushDevice(r); err != nil {
		writeBrowserAuthError(w, http.StatusServiceUnavailable, "notification revocation unavailable")
		return
	}
	epochHash, err := s.browserEpochHash(w, r, false)
	if err != nil {
		writeBrowserAuthError(w, http.StatusBadRequest, "invalid browser epoch")
		return
	}
	// The browser lists the pending flows it still knows about (nonce
	// authenticated). This covers flows bound to an epoch cookie this jar no
	// longer presents — enumeration by epoch alone cannot find those.
	var listedFlows []struct {
		FlowID string `json:"flow_id"`
		Nonce  string `json:"nonce"`
	}
	if hasJSONContentType(r.Header.Get("Content-Type")) {
		var request struct {
			Flows []struct {
				FlowID string `json:"flow_id"`
				Nonce  string `json:"nonce"`
			} `json:"flows"`
		}
		body, readErr := io.ReadAll(http.MaxBytesReader(w, r.Body, maxAuthRequestBytes))
		if readErr != nil {
			writeBrowserAuthError(w, http.StatusRequestEntityTooLarge, "request too large")
			return
		}
		if len(body) > 0 {
			if checkDuplicateKeys(body) != nil || unmarshalStrict(body, &request) != nil {
				writeBrowserAuthError(w, http.StatusBadRequest, "invalid request")
				return
			}
			listedFlows = request.Flows
		}
	}
	if len(listedFlows) > 16 {
		writeBrowserAuthError(w, http.StatusBadRequest, "invalid request")
		return
	}
	horizon := time.Now().Add(browserFlowClosureHorizon)
	extraFlows := make(map[string]time.Time)
	epochs := make(map[string]struct{})
	if epochHash != "" {
		epochs[epochHash] = struct{}{}
	}
	if s.Flows != nil {
		for _, listed := range listedFlows {
			if !validAuthFlowID(listed.FlowID) {
				continue
			}
			// Only the nonce authority may name a flow for closure; anything
			// else is ignored rather than failing the logout.
			ref, refErr := s.Flows.AuthFlowForNonce(r.Context(), listed.FlowID, listed.Nonce)
			if refErr != nil {
				continue
			}
			retain := ref.ExpiresAt.Add(6 * time.Minute)
			if retain.After(horizon) {
				retain = horizon
			}
			extraFlows[ref.FlowID] = retain
			if ref.EpochHash != "" {
				epochs[ref.EpochHash] = struct{}{}
			}
		}
	}
	var cookies []string
	for _, cookie := range r.CookiesNamed(BrowserSessionCookie) {
		cookies = append(cookies, cookie.Value)
	}
	// Closing every epoch this jar has used — the presented cookie, the
	// epochs its retired sessions were admitted under, and the epochs its
	// listed flows were bound to — plus the flows themselves is what makes
	// logout irreversible by authentication work started before it: a
	// pending or just-completed flow can neither prove nor issue a session
	// after this commits, and any session it already signed stays revoked.
	epochList := make([]string, 0, len(epochs))
	for epoch := range epochs {
		epochList = append(epochList, epoch)
	}
	closedFlows, closedEpochs, retiredSessions, err := s.Sessions.RevokeSessionsForLogout(
		r.Context(), cookies, epochList, extraFlows)
	if err != nil {
		// A durable failure must keep the credentials for a retry; claiming
		// logout succeeded while sessions stay live would be a lie.
		writeBrowserAuthError(w, http.StatusServiceUnavailable, "authentication unavailable")
		return
	}
	if s.Connections != nil {
		for _, sessionID := range retiredSessions {
			s.Connections.CloseBrowserSession(sessionID)
		}
	}
	if s.Flows != nil {
		// The session-store barrier already holds; the mirror marks every
		// closed flow in PostgreSQL so status and proof paths report the
		// closure. Enumerating each closed epoch also mirrors flows whose
		// closure was implied by their epoch rather than listed directly.
		mirror := make(map[string]struct{}, len(closedFlows))
		for _, flowID := range closedFlows {
			mirror[flowID] = struct{}{}
		}
		for _, epoch := range closedEpochs {
			refs, listErr := s.Flows.OpenBrowserFlows(r.Context(), epoch)
			if listErr != nil {
				log.Printf("browser logout: enumerate closed epoch flows: %v", listErr)
				continue
			}
			for _, ref := range refs {
				mirror[ref.FlowID] = struct{}{}
			}
		}
		if len(mirror) > 0 {
			flowIDs := make([]string, 0, len(mirror))
			for flowID := range mirror {
				flowIDs = append(flowIDs, flowID)
			}
			if err := s.Flows.CloseFlows(r.Context(), flowIDs); err != nil {
				log.Printf("browser logout: mirror flow closure failed: %v", err)
			}
		}
	}
	if s.PushDevices != nil || len(r.CookiesNamed(BrowserPushDeviceCookie)) > 0 {
		http.SetCookie(w, s.pushDeviceCookie("", -1))
	}
	http.SetCookie(w, s.sessionCookie("", -1))
	http.SetCookie(w, s.epochCookie("", -1))
	http.SetCookie(w, s.csrfCookie("", -1))
	s.expireLegacyCSRFCookie(w)
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
}

// serveDiscardAuthFlow is the nonce-authenticated, flow-scoped counterpart of
// logout: it cancels exactly one stale flow — closing its issuance authority
// and revoking sessions it already minted — without disturbing the jar's
// current session. Both the session store and the PostgreSQL mirror are
// idempotent, so a lost response is safely retried.
func (s *BrowserAuthServer) serveDiscardAuthFlow(w http.ResponseWriter, r *http.Request) {
	if !s.allowOrigin(w, r) || !s.requireCSRF(w, r) {
		return
	}
	var request struct {
		FlowID string `json:"flow_id"`
		Nonce  string `json:"nonce"`
	}
	if !decodeAuthJSON(w, r, &request) {
		return
	}
	if !validAuthFlowID(request.FlowID) || request.Nonce == "" || len(request.Nonce) > 64 {
		writeBrowserAuthError(w, http.StatusBadRequest, "invalid request")
		return
	}
	ref, err := s.Flows.AuthFlowForNonce(r.Context(), request.FlowID, request.Nonce)
	if err != nil {
		if errors.Is(err, ErrBrowserAuthFlowInvalid) {
			// Unknown flow: nothing to discard; treat as already done.
			w.Header().Set("Cache-Control", "no-store")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeFlowError(w, err)
		return
	}
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	retiredSessions, err := s.Sessions.DiscardBrowserFlow(
		r.Context(), request.FlowID, ref.ExpiresAt.Add(6*time.Minute))
	if err != nil {
		writeBrowserAuthError(w, http.StatusServiceUnavailable, "authentication unavailable")
		return
	}
	if s.Connections != nil {
		for _, sessionID := range retiredSessions {
			s.Connections.CloseBrowserSession(sessionID)
		}
	}
	if err := s.Flows.CloseFlows(r.Context(), []string{request.FlowID}); err != nil {
		// The session-store barrier committed; the mirror failing means status
		// reads may still look live, so tell the client to retry.
		writeBrowserAuthError(w, http.StatusServiceUnavailable, "authentication unavailable")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
}

func (s *BrowserAuthServer) allowOrigin(w http.ResponseWriter, r *http.Request) bool {
	if !browserOriginAllowed(r, s.AllowedOrigins) {
		writeBrowserAuthError(w, http.StatusForbidden, "origin not allowed")
		return false
	}
	return true
}

// Browsers do not consistently send Origin on same-origin GET requests.
// Missing Origin is acceptable only for these safe reads; any supplied Origin
// must still match exactly. State-changing POST routes always require Origin.
func (s *BrowserAuthServer) allowSafeReadOrigin(w http.ResponseWriter, r *http.Request) bool {
	if len(r.Header.Values("Origin")) == 0 {
		return true
	}
	return s.allowOrigin(w, r)
}

func (s *BrowserAuthServer) requireCSRF(w http.ResponseWriter, r *http.Request) bool {
	if !BrowserCSRFValid(r) {
		writeBrowserAuthError(w, http.StatusForbidden, "invalid CSRF token")
		return false
	}
	return true
}

func validCSRFToken(token string) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	return err == nil && len(decoded) == csrfTokenBytes
}

var authFlowIDRegexp = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func validAuthFlowID(flowID string) bool {
	return authFlowIDRegexp.MatchString(flowID)
}

func hashBrowserEpochValue(value string) string {
	sum := sha256.Sum256([]byte(value))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func validBrowserEpochValue(value string) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	return err == nil && len(decoded) == browserEpochBytes &&
		base64.RawURLEncoding.EncodeToString(decoded) == value
}

func validBrowserEpochHash(hash string) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(hash)
	return err == nil && len(decoded) == sha256.Size &&
		base64.RawURLEncoding.EncodeToString(decoded) == hash
}

// RequestBrowserEpochHash reads the browser epoch cookie without minting:
// the hash of the jar this request came from, or "" when the jar holds none.
// Adjacent browser-bound surfaces (secretary transfer) compare it to the
// epoch hash recorded on an auth flow instead of trusting request-body
// identity.
func RequestBrowserEpochHash(r *http.Request) (string, error) {
	cookies := r.CookiesNamed(BrowserEpochCookie)
	if len(cookies) == 1 && validBrowserEpochValue(cookies[0].Value) {
		return hashBrowserEpochValue(cookies[0].Value), nil
	}
	if len(cookies) > 1 {
		return "", errors.New("duplicate browser epoch cookies")
	}
	return "", nil
}

// browserEpochHash returns the hash of this jar's epoch cookie. With mint it
// replaces a missing, malformed, or closed cookie on this response — a jar
// still holding an epoch that logout closed must not bind new flows to dead
// authority. Without mint a missing epoch is simply "". An epoch value alone
// authorizes nothing — it only names the jar for admission and closure.
func (s *BrowserAuthServer) browserEpochHash(w http.ResponseWriter, r *http.Request, mint bool) (string, error) {
	cookies := r.CookiesNamed(BrowserEpochCookie)
	if len(cookies) == 1 && validBrowserEpochValue(cookies[0].Value) {
		epochHash := hashBrowserEpochValue(cookies[0].Value)
		if !mint {
			return epochHash, nil
		}
		// A closed epoch can never carry new work again; replace it instead
		// of letting the jar wedge on dead authority for the whole horizon.
		if err := s.Sessions.CheckBrowserEpochUsable(r.Context(), epochHash); err == nil {
			return epochHash, nil
		}
	}
	if !mint {
		if len(cookies) > 1 {
			return "", errors.New("duplicate browser epoch cookies")
		}
		return "", nil
	}
	epochBytes := make([]byte, browserEpochBytes)
	if _, err := io.ReadFull(s.random, epochBytes); err != nil {
		return "", errors.New("browser epoch unavailable")
	}
	value := base64.RawURLEncoding.EncodeToString(epochBytes)
	http.SetCookie(w, s.epochCookie(value, 0))
	return hashBrowserEpochValue(value), nil
}

func (s *BrowserAuthServer) epochCookie(value string, maxAge int) *http.Cookie {
	cookie := &http.Cookie{
		Name:     BrowserEpochCookie,
		Value:    value,
		Path:     "/",
		HttpOnly: true,
		Secure:   s.SecureCookies,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   maxAge,
	}
	if maxAge < 0 {
		cookie.Expires = time.Unix(1, 0)
	}
	return cookie
}

func hasJSONContentType(value string) bool {
	mediaType, _, err := mime.ParseMediaType(value)
	return err == nil && mediaType == "application/json"
}

func (s *BrowserAuthServer) sessionCookie(value string, maxAge int) *http.Cookie {
	cookie := &http.Cookie{
		Name:     BrowserSessionCookie,
		Value:    value,
		Path:     "/",
		HttpOnly: true,
		Secure:   s.SecureCookies,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   maxAge,
	}
	if maxAge < 0 {
		cookie.Expires = time.Unix(1, 0)
	}
	return cookie
}

func (s *BrowserAuthServer) csrfCookie(value string, maxAge int) *http.Cookie {
	cookie := &http.Cookie{
		Name:     BrowserCSRFCookie,
		Value:    value,
		Path:     "/",
		HttpOnly: false,
		Secure:   s.SecureCookies,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   maxAge,
	}
	if maxAge < 0 {
		cookie.Expires = time.Unix(1, 0)
	}
	return cookie
}

// Retire the former auth-only cookie so browsers do not send two cookies with
// the same name to /auth after the application-wide cookie is issued.
func (s *BrowserAuthServer) expireLegacyCSRFCookie(w http.ResponseWriter) {
	cookie := s.csrfCookie("", -1)
	cookie.Path = "/auth"
	http.SetCookie(w, cookie)
}

func writeBrowserAuthError(w http.ResponseWriter, status int, message string) {
	writeBrowserAuthJSON(w, status, map[string]string{"error": message})
}

func writeBrowserAuthJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
