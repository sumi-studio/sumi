package agentevents

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"
)

type StartBrowserAuthFlowRequest struct {
	Intent       string `json:"intent"`
	Provider     string `json:"provider"`
	Email        string `json:"email,omitempty"`
	Continuation string `json:"continuation"`
	Nonce        string `json:"nonce"`
}

type BrowserAuthFlowResult struct {
	FlowID       string            `json:"flow_id,omitempty"`
	Outcome      string            `json:"outcome"`
	NextAction   string            `json:"next_action,omitempty"`
	Continuation string            `json:"continuation,omitempty"`
	ExpiresAt    time.Time         `json:"expires_at,omitempty"`
	Claims       UserSessionClaims `json:"-"`
}

type ResolveBrowserAuthFlowRequest struct {
	FlowID  string `json:"flow_id"`
	Nonce   string `json:"nonce"`
	IDToken string `json:"id_token"`
}

type ConfirmBrowserAuthFlowRequest struct {
	FlowID string `json:"flow_id"`
	Nonce  string `json:"nonce"`
	Action string `json:"action"`
}

type StartProviderOperationRequest struct {
	Provider     string `json:"provider"`
	Operation    string `json:"operation"`
	DecisionPath string `json:"decision_path"`
	Nonce        string `json:"nonce"`
	IDToken      string `json:"id_token"`
}

type CompleteProviderOperationRequest struct {
	OperationID string `json:"operation_id"`
	Nonce       string `json:"nonce"`
	IDToken     string `json:"id_token"`
}

type FailProviderOperationRequest struct {
	OperationID string `json:"operation_id"`
	Nonce       string `json:"nonce"`
	Outcome     string `json:"outcome"`
}

type ProviderOperationStatusRequest struct {
	OperationID string `json:"operation_id"`
	Nonce       string `json:"nonce"`
}

type ProviderOperationResult struct {
	OperationID              string    `json:"operation_id,omitempty"`
	Outcome                  string    `json:"outcome"`
	ClientOperation          string    `json:"client_operation,omitempty"`
	CreatedAt                time.Time `json:"created_at,omitempty"`
	CompletionTokenNotBefore time.Time `json:"completion_token_not_before,omitempty"`
	ExpiresAt                time.Time `json:"expires_at,omitempty"`
	NoticeRequired           bool      `json:"notice_required,omitempty"`
}

type ProviderOperationStatusResult struct {
	OperationID              string     `json:"operation_id"`
	Provider                 string     `json:"provider"`
	Operation                string     `json:"operation"`
	Status                   string     `json:"status"`
	Outcome                  string     `json:"outcome"`
	ClientOperation          string     `json:"client_operation,omitempty"`
	CreatedAt                time.Time  `json:"created_at"`
	CompletionTokenNotBefore time.Time  `json:"completion_token_not_before"`
	ExpiresAt                time.Time  `json:"expires_at"`
	CompletedAt              *time.Time `json:"completed_at,omitempty"`
	NoticeRequired           bool       `json:"notice_required"`
}

var (
	ErrBrowserAuthFlowInvalid         = errors.New("invalid authentication flow")
	ErrBrowserAuthFlowExpired         = errors.New("authentication flow expired")
	ErrBrowserAuthFlowConsumed        = errors.New("authentication flow consumed")
	ErrBrowserAuthFlowProof           = errors.New("authentication proof mismatch")
	ErrBrowserAuthRecentReauth        = errors.New("recent reauthentication required")
	ErrBrowserAuthLastMethod          = errors.New("last login method")
	ErrBrowserAuthProviderPending     = errors.New("provider operation pending")
	ErrBrowserAuthProviderUnavailable = errors.New("provider operation unavailable")
)

// BrowserAuthFlowController owns persisted intent/proof transitions. It never
// receives a provider OAuth credential; linkWithCredential remains a bounded
// Firebase browser-SDK operation and completion is proven by a refreshed ID
// token.
type BrowserAuthFlowController interface {
	Start(ctx context.Context, request StartBrowserAuthFlowRequest) (BrowserAuthFlowResult, error)
	Resolve(ctx context.Context, request ResolveBrowserAuthFlowRequest, identity FirebaseIdentity) (BrowserAuthFlowResult, error)
	Confirm(ctx context.Context, request ConfirmBrowserAuthFlowRequest) (BrowserAuthFlowResult, error)
	Status(ctx context.Context, request ConfirmBrowserAuthFlowRequest) (BrowserAuthFlowResult, error)
	StartProviderOperation(ctx context.Context, claims UserSessionClaims, request StartProviderOperationRequest, identity FirebaseIdentity) (ProviderOperationResult, error)
	CompleteProviderOperation(ctx context.Context, claims UserSessionClaims, request CompleteProviderOperationRequest, identity FirebaseIdentity) (ProviderOperationResult, error)
	FailProviderOperation(ctx context.Context, claims UserSessionClaims, request FailProviderOperationRequest) (ProviderOperationResult, error)
	StatusProviderOperation(ctx context.Context, claims UserSessionClaims, request ProviderOperationStatusRequest) (ProviderOperationStatusResult, error)
}

func (s *BrowserAuthServer) serveStartAuthFlow(w http.ResponseWriter, r *http.Request) {
	if !s.allowOrigin(w, r) || !s.requireCSRF(w, r) {
		return
	}
	var request StartBrowserAuthFlowRequest
	if !decodeAuthJSON(w, r, &request) {
		return
	}
	result, err := s.Flows.Start(r.Context(), request)
	if err != nil {
		writeFlowError(w, err)
		return
	}
	writeBrowserAuthJSON(w, http.StatusCreated, result)
}

func (s *BrowserAuthServer) serveResolveAuthFlow(w http.ResponseWriter, r *http.Request) {
	if !s.allowOrigin(w, r) || !s.requireCSRF(w, r) {
		return
	}
	var request ResolveBrowserAuthFlowRequest
	if !decodeAuthJSON(w, r, &request) {
		return
	}
	if request.IDToken == "" || len(request.IDToken) > maxFirebaseIDTokenBytes {
		writeBrowserAuthError(w, http.StatusBadRequest, "invalid request")
		return
	}
	identity, err := s.Firebase.VerifyIDToken(r.Context(), request.IDToken)
	if err != nil || identity.UID == "" {
		writeBrowserAuthError(w, http.StatusUnauthorized, "authentication failed")
		return
	}
	result, err := s.Flows.Resolve(r.Context(), request, identity)
	if err != nil {
		writeFlowError(w, err)
		return
	}
	if result.Outcome == "signed_in" || result.Outcome == "account_created" {
		if err := s.establishSession(w, r, result.Claims); err != nil {
			writeBrowserAuthError(w, http.StatusServiceUnavailable, "authentication unavailable")
			return
		}
	}
	writeBrowserAuthJSON(w, http.StatusOK, result)
}

func (s *BrowserAuthServer) serveConfirmAuthFlow(w http.ResponseWriter, r *http.Request) {
	if !s.allowOrigin(w, r) || !s.requireCSRF(w, r) {
		return
	}
	var request ConfirmBrowserAuthFlowRequest
	if !decodeAuthJSON(w, r, &request) {
		return
	}
	result, err := s.Flows.Confirm(r.Context(), request)
	if err != nil {
		writeFlowError(w, err)
		return
	}
	if err := s.establishSession(w, r, result.Claims); err != nil {
		writeBrowserAuthError(w, http.StatusServiceUnavailable, "authentication unavailable")
		return
	}
	writeBrowserAuthJSON(w, http.StatusOK, result)
}

func (s *BrowserAuthServer) serveAuthFlowStatus(w http.ResponseWriter, r *http.Request) {
	if !s.allowOrigin(w, r) || !s.requireCSRF(w, r) {
		return
	}
	var request ConfirmBrowserAuthFlowRequest
	if !decodeAuthJSON(w, r, &request) {
		return
	}
	if request.Action != "" {
		writeBrowserAuthError(w, http.StatusBadRequest, "invalid request")
		return
	}
	result, err := s.Flows.Status(r.Context(), request)
	if err != nil {
		writeFlowError(w, err)
		return
	}
	// Status is read-only recovery. Even a completed flow cannot mint another
	// browser session after its proof has been consumed.
	result.Claims = UserSessionClaims{}
	writeBrowserAuthJSON(w, http.StatusOK, result)
}

func (s *BrowserAuthServer) authenticatedClaims(w http.ResponseWriter, r *http.Request) (UserSessionClaims, bool) {
	cookie, err := uniqueBrowserSessionCookie(r)
	if err != nil {
		writeBrowserAuthError(w, http.StatusUnauthorized, "authentication required")
		return UserSessionClaims{}, false
	}
	claims, err := s.Sessions.VerifySession(r.Context(), cookie.Value)
	if err != nil {
		writeBrowserAuthError(w, http.StatusUnauthorized, "authentication required")
		return UserSessionClaims{}, false
	}
	return claims, true
}

func (s *BrowserAuthServer) serveStartProviderOperation(w http.ResponseWriter, r *http.Request) {
	if !s.allowOrigin(w, r) || !s.requireCSRF(w, r) {
		return
	}
	claims, ok := s.authenticatedClaims(w, r)
	if !ok {
		return
	}
	var request StartProviderOperationRequest
	if !decodeAuthJSON(w, r, &request) {
		return
	}
	if request.IDToken == "" || len(request.IDToken) > maxFirebaseIDTokenBytes {
		writeBrowserAuthError(w, http.StatusBadRequest, "invalid request")
		return
	}
	identity, err := s.Firebase.VerifyIDToken(r.Context(), request.IDToken)
	if err != nil {
		writeBrowserAuthError(w, http.StatusUnauthorized, "authentication failed")
		return
	}
	result, err := s.Flows.StartProviderOperation(r.Context(), claims, request, identity)
	if err != nil {
		writeFlowError(w, err)
		return
	}
	writeBrowserAuthJSON(w, http.StatusCreated, result)
}

func (s *BrowserAuthServer) serveCompleteProviderOperation(w http.ResponseWriter, r *http.Request) {
	if !s.allowOrigin(w, r) || !s.requireCSRF(w, r) {
		return
	}
	claims, ok := s.authenticatedClaims(w, r)
	if !ok {
		return
	}
	var request CompleteProviderOperationRequest
	if !decodeAuthJSON(w, r, &request) {
		return
	}
	if request.IDToken == "" || len(request.IDToken) > maxFirebaseIDTokenBytes {
		writeBrowserAuthError(w, http.StatusBadRequest, "invalid request")
		return
	}
	identity, err := s.Firebase.VerifyIDToken(r.Context(), request.IDToken)
	if err != nil {
		writeBrowserAuthError(w, http.StatusUnauthorized, "authentication failed")
		return
	}
	result, err := s.Flows.CompleteProviderOperation(r.Context(), claims, request, identity)
	if err != nil {
		writeFlowError(w, err)
		return
	}
	writeBrowserAuthJSON(w, http.StatusOK, result)
}

func (s *BrowserAuthServer) serveFailProviderOperation(w http.ResponseWriter, r *http.Request) {
	if !s.allowOrigin(w, r) || !s.requireCSRF(w, r) {
		return
	}
	claims, ok := s.authenticatedClaims(w, r)
	if !ok {
		return
	}
	var request FailProviderOperationRequest
	if !decodeAuthJSON(w, r, &request) {
		return
	}
	result, err := s.Flows.FailProviderOperation(r.Context(), claims, request)
	if err != nil {
		writeFlowError(w, err)
		return
	}
	writeBrowserAuthJSON(w, http.StatusOK, result)
}

func (s *BrowserAuthServer) serveProviderOperationStatus(w http.ResponseWriter, r *http.Request) {
	if !s.allowOrigin(w, r) || !s.requireCSRF(w, r) {
		return
	}
	claims, ok := s.authenticatedClaims(w, r)
	if !ok {
		return
	}
	var request ProviderOperationStatusRequest
	if !decodeAuthJSON(w, r, &request) {
		return
	}
	if request.OperationID == "" || request.Nonce == "" {
		writeBrowserAuthError(w, http.StatusBadRequest, "invalid request")
		return
	}
	result, err := s.Flows.StatusProviderOperation(r.Context(), claims, request)
	if err != nil {
		writeFlowError(w, err)
		return
	}
	writeBrowserAuthJSON(w, http.StatusOK, result)
}

func decodeAuthJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	if !hasJSONContentType(r.Header.Get("Content-Type")) {
		writeBrowserAuthError(w, http.StatusUnsupportedMediaType, "application/json required")
		return false
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxAuthRequestBytes))
	if err != nil {
		writeBrowserAuthError(w, http.StatusRequestEntityTooLarge, "request too large")
		return false
	}
	if checkDuplicateKeys(body) != nil || unmarshalStrict(body, target) != nil {
		writeBrowserAuthError(w, http.StatusBadRequest, "invalid request")
		return false
	}
	return true
}

func writeFlowError(w http.ResponseWriter, err error) {
	status, code := http.StatusBadRequest, "invalid_flow"
	switch {
	case errors.Is(err, ErrBrowserAuthFlowExpired):
		status, code = http.StatusGone, "flow_expired"
	case errors.Is(err, ErrBrowserAuthFlowConsumed):
		status, code = http.StatusConflict, "flow_consumed"
	case errors.Is(err, ErrBrowserAuthFlowProof):
		status, code = http.StatusForbidden, "proof_mismatch"
	case errors.Is(err, ErrBrowserAuthRecentReauth):
		status, code = http.StatusForbidden, "recent_reauth_required"
	case errors.Is(err, ErrBrowserAuthLastMethod):
		status, code = http.StatusConflict, "last_login_method"
	case errors.Is(err, ErrBrowserAuthProviderPending):
		status, code = http.StatusConflict, "provider_operation_pending"
	case errors.Is(err, ErrBrowserAuthProviderUnavailable):
		status, code = http.StatusServiceUnavailable, "provider_unavailable"
	}
	writeBrowserAuthJSON(w, status, map[string]string{"error": code})
}
