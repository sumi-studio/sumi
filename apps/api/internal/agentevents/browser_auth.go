package agentevents

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	BrowserCSRFCookie        = "sumi_csrf"
	defaultBrowserSessionTTL = 15 * time.Minute
	maxAuthRequestBytes      = 16 * 1024
	maxFirebaseIDTokenBytes  = 12 * 1024
	csrfTokenBytes           = 32
)

// FirebaseIdentity contains only the stable server-verified Firebase
// principal needed for Sumi identity binding.
type FirebaseIdentity struct {
	UID      string
	TenantID string
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

// DirectChatAuthorizer gates the 私信 Surface (raw direct chat) on the calling
// Human being the current Employer of the target agent (ADR 0009 §5). A nil
// authorizer permits all verified sessions, preserving the legacy single-user
// contract for tests and the static binding fallback.
type DirectChatAuthorizer interface {
	AuthorizeDirectChat(ctx context.Context, humanID, personalityAgentID string) error
}

// ResearchConsentStore reads and writes a Human's 研究協力 consent decision
// (ADR 0009 §6). "decided" distinguishes an explicit decline from "never asked".
type ResearchConsentStore interface {
	ResearchConsentState(ctx context.Context, humanID string) (decided bool, granted bool, err error)
	SetResearchConsent(ctx context.Context, humanID string, granted bool) error
}

// ContentLogger observes durably appended commands for the 研究協力-gated content
// log and always-on metadata telemetry (ADR 0009 §6). It is called after a
// command is appended; failures must not affect the command path.
type ContentLogger interface {
	LogCommand(ctx context.Context, humanID, agentID, commandID string, seq uint64, content []byte) error
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
	Firebase       FirebaseIDTokenVerifier
	Bindings       IdentityBindingResolver
	Sessions       browserSessionManager
	AllowedOrigins []string
	SecureCookies  bool
	SessionTTL     time.Duration
	Connections    BrowserSessionConnectionCloser
	// Consents optionally serves the 研究協力 consent UI contract. When nil,
	// the consent routes are not registered.
	Consents ResearchConsentStore
	random   io.Reader
	sessionMu sync.Mutex
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
	mux.HandleFunc("GET /auth/csrf", s.serveCSRF)
	mux.HandleFunc("POST /auth/session", s.serveSessionExchange)
	mux.HandleFunc("GET /auth/session", s.serveSessionStatus)
	mux.HandleFunc("POST /auth/logout", s.serveLogout)
	if s.Consents != nil {
		mux.HandleFunc("GET /auth/consent", s.serveConsentStatus)
		mux.HandleFunc("POST /auth/consent", s.serveConsentUpdate)
	}
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
	writeBrowserAuthJSON(w, http.StatusOK, map[string]string{"csrf_token": token})
}

func (s *BrowserAuthServer) serveSessionExchange(w http.ResponseWriter, r *http.Request) {
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
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	ttl := s.SessionTTL
	if ttl == 0 {
		ttl = defaultBrowserSessionTTL
	}
	var session string
	if existing, cookieErr := uniqueBrowserSessionCookie(r); cookieErr == nil {
		retired, replacement, valid, rotateErr := s.Sessions.RotateSession(
			r.Context(),
			existing.Value,
			claims,
			ttl,
		)
		if rotateErr != nil {
			writeBrowserAuthError(w, http.StatusServiceUnavailable, "authentication unavailable")
			return
		}
		if valid {
			session = replacement
			if s.Connections != nil {
				s.Connections.CloseBrowserSession(retired.sessionID)
			}
		}
	}
	if session == "" {
		session, err = s.Sessions.IssueSession(r.Context(), claims, ttl)
		if err != nil {
			writeBrowserAuthError(w, http.StatusServiceUnavailable, "authentication unavailable")
			return
		}
	}
	http.SetCookie(w, s.sessionCookie(session, int(ttl/time.Second)))
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
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
		writeBrowserAuthJSON(w, http.StatusOK, map[string]bool{"authenticated": false})
		return
	}
	if !validBrowserAuthorityBindingID(claims.authorityBindingID) {
		writeBrowserAuthError(w, http.StatusServiceUnavailable, "authentication unavailable")
		return
	}
	writeBrowserAuthJSON(w, http.StatusOK, struct {
		Authenticated      bool   `json:"authenticated"`
		AuthorityBindingID string `json:"authority_binding_id"`
		User               struct {
			ID string `json:"id"`
		} `json:"user"`
	}{
		Authenticated:      true,
		AuthorityBindingID: claims.authorityBindingID,
		User: struct {
			ID string `json:"id"`
		}{ID: claims.UserID},
	})
}

func (s *BrowserAuthServer) serveLogout(w http.ResponseWriter, r *http.Request) {
	if !s.allowOrigin(w, r) || !s.requireCSRF(w, r) {
		return
	}
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	for _, cookie := range r.CookiesNamed(BrowserSessionCookie) {
		revoked, valid, revokeErr := s.Sessions.RevokeSessionForLogout(
			r.Context(),
			cookie.Value,
		)
		if revokeErr != nil {
			// A valid signed credential whose durable lineage cannot be
			// checked or revoked must remain in the browser for a retry.
			writeBrowserAuthError(w, http.StatusServiceUnavailable, "authentication unavailable")
			return
		}
		if valid {
			if s.Connections != nil {
				s.Connections.CloseBrowserSession(revoked.sessionID)
			}
		}
	}
	http.SetCookie(w, s.sessionCookie("", -1))
	http.SetCookie(w, s.csrfCookie("", -1))
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
}

func (s *BrowserAuthServer) serveConsentStatus(w http.ResponseWriter, r *http.Request) {
	if !s.allowSafeReadOrigin(w, r) {
		return
	}
	claims, ok := s.requireVerifiedClaims(w, r)
	if !ok {
		return
	}
	decided, granted, err := s.Consents.ResearchConsentState(r.Context(), claims.UserID)
	if err != nil {
		writeBrowserAuthError(w, http.StatusServiceUnavailable, "consent unavailable")
		return
	}
	writeBrowserAuthJSON(w, http.StatusOK, map[string]bool{
		"decided":  decided,
		"granted":  granted,
	})
}

func (s *BrowserAuthServer) serveConsentUpdate(w http.ResponseWriter, r *http.Request) {
	if !s.allowOrigin(w, r) || !s.requireCSRF(w, r) {
		return
	}
	claims, ok := s.requireVerifiedClaims(w, r)
	if !ok {
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
		Grant bool `json:"grant"`
	}
	if err := unmarshalStrict(body, &request); err != nil {
		writeBrowserAuthError(w, http.StatusBadRequest, "invalid request")
		return
	}
	if err := s.Consents.SetResearchConsent(r.Context(), claims.UserID, request.Grant); err != nil {
		writeBrowserAuthError(w, http.StatusServiceUnavailable, "consent unavailable")
		return
	}
	writeBrowserAuthJSON(w, http.StatusOK, map[string]bool{
		"decided": true,
		"granted": request.Grant,
	})
}

// requireVerifiedClaims extracts and verifies the browser session, writing an
// unauthorized response on failure. It returns the claims and true on success.
func (s *BrowserAuthServer) requireVerifiedClaims(w http.ResponseWriter, r *http.Request) (UserSessionClaims, bool) {
	cookie, err := uniqueBrowserSessionCookie(r)
	if err != nil {
		if errors.Is(err, errBrowserSessionDuplicate) {
			writeBrowserAuthError(w, http.StatusBadRequest, "duplicate session cookies")
			return UserSessionClaims{}, false
		}
		writeBrowserAuthError(w, http.StatusUnauthorized, "missing session")
		return UserSessionClaims{}, false
	}
	claims, err := s.Sessions.VerifySession(r.Context(), cookie.Value)
	if err != nil {
		writeBrowserAuthError(w, http.StatusUnauthorized, "invalid session")
		return UserSessionClaims{}, false
	}
	return claims, true
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
	headers := r.Header.Values("X-CSRF-Token")
	if len(headers) != 1 {
		writeBrowserAuthError(w, http.StatusForbidden, "invalid CSRF token")
		return false
	}
	cookies := r.CookiesNamed(BrowserCSRFCookie)
	if len(cookies) != 1 {
		writeBrowserAuthError(w, http.StatusForbidden, "invalid CSRF token")
		return false
	}
	headerToken := headers[0]
	cookieToken := cookies[0].Value
	if !validCSRFToken(headerToken) || !validCSRFToken(cookieToken) ||
		subtle.ConstantTimeCompare([]byte(headerToken), []byte(cookieToken)) != 1 {
		writeBrowserAuthError(w, http.StatusForbidden, "invalid CSRF token")
		return false
	}
	return true
}

func validCSRFToken(token string) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	return err == nil && len(decoded) == csrfTokenBytes
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
		Path:     "/auth",
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
