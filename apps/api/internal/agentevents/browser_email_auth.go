package agentevents

import (
	"context"
	"errors"
	"net/http"
	"time"
)

type EmailFlowRequest struct {
	FlowID string `json:"flow_id"`
	Nonce  string `json:"nonce"`
}

type VerifyEmailCodeRequest struct {
	FlowID string `json:"flow_id"`
	Nonce  string `json:"nonce"`
	Code   string `json:"code"`
}

type InspectEmailLinkRequest struct {
	ChallengeID string `json:"challenge_id"`
	Token       string `json:"token"`
	Nonce       string `json:"nonce,omitempty"`
}

type CompleteEmailLinkRequest struct {
	ChallengeID string `json:"challenge_id"`
	Token       string `json:"token"`
	Nonce       string `json:"nonce"`
	Adopt       bool   `json:"adopt"`
	// BrowserEpochHash is set by the server from the epoch cookie so an
	// adoption rebinds the flow to the adopting jar.
	BrowserEpochHash string `json:"-"`
}

type EmailChallengeResult struct {
	FlowID             string    `json:"flow_id"`
	FlowStatus         string    `json:"flow_status"`
	Email              string    `json:"email"`
	FlowExpiresAt      time.Time `json:"flow_expires_at"`
	ChallengeExpiresAt time.Time `json:"challenge_expires_at"`
	AttemptsRemaining  int       `json:"attempts_remaining"`
	Delivery           string    `json:"delivery"`
	ResendAvailableAt  time.Time `json:"resend_available_at"`
	// HumanID names the Human a completed flow resolved to, so a poller can
	// compare it against the browser's current session identity instead of
	// treating any authenticated session as this flow's outcome.
	HumanID string `json:"human_id,omitempty"`
}

// EmailProofResult carries a short-lived Firebase custom token for the
// principal bound to the proved mailbox. Only the flow authority receives it.
type EmailProofResult struct {
	FlowID      string `json:"flow_id"`
	Intent      string `json:"intent"`
	CustomToken string `json:"custom_token"`
	// HumanID names the Human this proof will resolve to when one is bound,
	// so the browser can compare it against its current session identity.
	HumanID string `json:"human_id,omitempty"`
}

type EmailLinkInspectionResult struct {
	FlowID      string    `json:"flow_id"`
	Intent      string    `json:"intent"`
	Email       string    `json:"email"`
	State       string    `json:"state"`
	SameBrowser bool      `json:"same_browser"`
	Session     string    `json:"session"`
	ExpiresAt   time.Time `json:"expires_at"`
}

// BrowserEmailAuthController owns Sumi mailbox proof. It never establishes a
// session: the custom-token sign-in still resolves through the flow routes.
// The optional session claims identify the currently signed-in Human for
// same-Human verification and inspection labeling; they never authorize a
// session change by themselves.
type BrowserEmailAuthController interface {
	VerifyEmailCode(ctx context.Context, request VerifyEmailCodeRequest, session *UserSessionClaims) (EmailProofResult, error)
	ResendEmailCode(ctx context.Context, request EmailFlowRequest) (EmailChallengeResult, error)
	EmailFlowStatus(ctx context.Context, request EmailFlowRequest) (EmailChallengeResult, error)
	InspectEmailLink(ctx context.Context, request InspectEmailLinkRequest, session *UserSessionClaims) (EmailLinkInspectionResult, error)
	CompleteEmailLink(ctx context.Context, request CompleteEmailLinkRequest, session *UserSessionClaims) (EmailProofResult, error)
}

var (
	ErrBrowserEmailCodeLocked         = errors.New("email code locked")
	ErrBrowserEmailSuperseded         = errors.New("email challenge superseded")
	ErrBrowserEmailExpired            = errors.New("email challenge expired")
	ErrBrowserEmailConsumed           = errors.New("email challenge consumed")
	ErrBrowserEmailLinkInvalid        = errors.New("email link invalid")
	ErrBrowserEmailAdoptionRequired   = errors.New("email link adoption required")
	ErrBrowserEmailContinuedElsewhere = errors.New("email flow continued in another browser")
	ErrBrowserEmailUnverifiedAccount  = errors.New("email belongs to an unverified bound account")
	ErrBrowserEmailUnavailable        = errors.New("email sign-in unavailable")
	ErrBrowserSessionActive           = errors.New("browser session active")
)

type BrowserEmailCodeMismatchError struct {
	AttemptsRemaining int
}

func (e *BrowserEmailCodeMismatchError) Error() string { return "email code mismatch" }

// BrowserEmailUnverifiedAccountError names the sign-in providers still linked
// to the bound account, so the refusal points at a method that works. It is
// returned only after the mailbox proof.
type BrowserEmailUnverifiedAccountError struct {
	SignInProviders []string
}

func (e *BrowserEmailUnverifiedAccountError) Error() string {
	return ErrBrowserEmailUnverifiedAccount.Error()
}
func (e *BrowserEmailUnverifiedAccountError) Unwrap() error { return ErrBrowserEmailUnverifiedAccount }

type BrowserEmailSendLimitedError struct {
	RetryAt time.Time
}

func (e *BrowserEmailSendLimitedError) Error() string { return "email send limited" }

func validEmailFlowIdentity(flowID, nonce string) bool {
	return flowID != "" && len(flowID) <= 64 && nonce != "" && len(nonce) <= 64
}

func validEmailLinkIdentity(challengeID, token string) bool {
	return challengeID != "" && len(challengeID) <= 64 && token != "" && len(token) <= 64
}

func (s *BrowserAuthServer) serveVerifyEmailCode(w http.ResponseWriter, r *http.Request) {
	if !s.allowAuthAllocation(w, r) || !s.allowOrigin(w, r) || !s.requireCSRF(w, r) {
		return
	}
	var request VerifyEmailCodeRequest
	if !decodeAuthJSON(w, r, &request) {
		return
	}
	if !validEmailFlowIdentity(request.FlowID, request.Nonce) || len(request.Code) > 32 {
		writeBrowserAuthError(w, http.StatusBadRequest, "invalid request")
		return
	}
	session, err := s.currentSessionClaims(r)
	if err != nil {
		writeBrowserAuthError(w, http.StatusServiceUnavailable, "authentication unavailable")
		return
	}
	result, err := s.EmailFlows.VerifyEmailCode(r.Context(), request, session)
	if err != nil {
		writeFlowError(w, err)
		return
	}
	writeBrowserAuthJSON(w, http.StatusOK, result)
}

func (s *BrowserAuthServer) serveResendEmailCode(w http.ResponseWriter, r *http.Request) {
	if !s.allowAuthAllocation(w, r) || !s.allowOrigin(w, r) || !s.requireCSRF(w, r) {
		return
	}
	var request EmailFlowRequest
	if !decodeAuthJSON(w, r, &request) {
		return
	}
	if !validEmailFlowIdentity(request.FlowID, request.Nonce) {
		writeBrowserAuthError(w, http.StatusBadRequest, "invalid request")
		return
	}
	result, err := s.EmailFlows.ResendEmailCode(r.Context(), request)
	if err != nil {
		writeFlowError(w, err)
		return
	}
	writeBrowserAuthJSON(w, http.StatusOK, result)
}

// serveEmailFlowStatus is read-only and polled while the code form is open.
func (s *BrowserAuthServer) serveEmailFlowStatus(w http.ResponseWriter, r *http.Request) {
	if !s.allowOrigin(w, r) || !s.requireCSRF(w, r) {
		return
	}
	var request EmailFlowRequest
	if !decodeAuthJSON(w, r, &request) {
		return
	}
	if !validEmailFlowIdentity(request.FlowID, request.Nonce) {
		writeBrowserAuthError(w, http.StatusBadRequest, "invalid request")
		return
	}
	result, err := s.EmailFlows.EmailFlowStatus(r.Context(), request)
	if err != nil {
		writeFlowError(w, err)
		return
	}
	writeBrowserAuthJSON(w, http.StatusOK, result)
}

func (s *BrowserAuthServer) serveInspectEmailLink(w http.ResponseWriter, r *http.Request) {
	if !s.allowAuthAllocation(w, r) || !s.allowOrigin(w, r) || !s.requireCSRF(w, r) {
		return
	}
	var request InspectEmailLinkRequest
	if !decodeAuthJSON(w, r, &request) {
		return
	}
	if !validEmailLinkIdentity(request.ChallengeID, request.Token) || len(request.Nonce) > 64 {
		writeBrowserAuthError(w, http.StatusBadRequest, "invalid request")
		return
	}
	session, err := s.currentSessionClaims(r)
	if err != nil {
		writeBrowserAuthError(w, http.StatusServiceUnavailable, "authentication unavailable")
		return
	}
	result, err := s.EmailFlows.InspectEmailLink(r.Context(), request, session)
	if err != nil {
		writeFlowError(w, err)
		return
	}
	writeBrowserAuthJSON(w, http.StatusOK, result)
}

// serveCompleteEmailLink proves the mailbox; it never issues a session. The
// session claims feed same-Human inspection and verification. A different
// Human may only take over the jar through the resolve step's explicit
// switch, so same-Human proof beside a live session stays allowed.
func (s *BrowserAuthServer) serveCompleteEmailLink(w http.ResponseWriter, r *http.Request) {
	if !s.allowAuthAllocation(w, r) || !s.allowOrigin(w, r) || !s.requireCSRF(w, r) {
		return
	}
	var request CompleteEmailLinkRequest
	if !decodeAuthJSON(w, r, &request) {
		return
	}
	if !validEmailLinkIdentity(request.ChallengeID, request.Token) || request.Nonce == "" || len(request.Nonce) > 64 {
		writeBrowserAuthError(w, http.StatusBadRequest, "invalid request")
		return
	}
	session, err := s.currentSessionClaims(r)
	if err != nil {
		writeBrowserAuthError(w, http.StatusServiceUnavailable, "authentication unavailable")
		return
	}
	epochHash, err := s.browserEpochHash(w, r, true)
	if err != nil {
		writeBrowserAuthError(w, http.StatusServiceUnavailable, "authentication unavailable")
		return
	}
	request.BrowserEpochHash = epochHash
	result, err := s.EmailFlows.CompleteEmailLink(r.Context(), request, session)
	if err != nil {
		writeFlowError(w, err)
		return
	}
	writeBrowserAuthJSON(w, http.StatusOK, result)
}

// currentSessionClaims returns nil without error when no valid session exists.
func (s *BrowserAuthServer) currentSessionClaims(r *http.Request) (*UserSessionClaims, error) {
	cookie, err := uniqueBrowserSessionCookie(r)
	if err != nil {
		if errors.Is(err, errBrowserSessionDuplicate) {
			return nil, err
		}
		return nil, nil
	}
	claims, err := s.Sessions.VerifySession(r.Context(), cookie.Value)
	if err != nil {
		if errors.Is(err, errBrowserSessionInvalid) || errors.Is(err, errBrowserSessionRevoked) {
			return nil, nil
		}
		return nil, err
	}
	return &claims, nil
}
