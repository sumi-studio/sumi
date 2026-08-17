package agentevents

// This file implements the authenticated loopback control plane used only by
// local development and CI. It deliberately does not claim the production
// workload-identity issuer or cross-VM central registry required by issue #80.

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	LocalCredentialIssuePath                   = "/local-control/v1/runtime-credentials:issue"
	LocalRuntimeStatePublishPath               = "/local-control/v1/runtime-state:publish"
	defaultLocalCredentialTTL                  = 60 * time.Second
	maxLocalControlBodyBytes             int64 = 32 * 1024
	maxLocalControlDurableStateBytes           = 8 * 1024 * 1024
	maxLocalControlRecords                     = 1024
	localControlStateVersion                   = 3
	localControlIntegrityVersion               = 1
	maxLocalControlPreviousIntegrityKeys       = 2
	maxOpaqueRuntimeIDBytes                    = 128
)

type LocalDeliveryAuthorization string

const (
	LocalDeliveryRaw           LocalDeliveryAuthorization = "raw"
	LocalDeliveryRedactionOnly LocalDeliveryAuthorization = "redaction_only"
)

type LocalRuntimePublicationState string

const (
	LocalRuntimeNotReady LocalRuntimePublicationState = "not_ready"
	LocalRuntimeReady    LocalRuntimePublicationState = "ready"
)

type LocalRuntimePublicationReason string

const (
	LocalRuntimeStartup  LocalRuntimePublicationReason = "startup"
	LocalRuntimeHydrated LocalRuntimePublicationReason = "hydrated"
	LocalRuntimeShutdown LocalRuntimePublicationReason = "shutdown"
)

// LocalRuntimeAuthorization is provisioned by the local/CI harness. One
// high-entropy bearer authenticates exactly one PAID/generation/RPC-boot epoch
// and one administrative token context. It is never derived from request JSON.
type LocalRuntimeAuthorization struct {
	BearerToken           string
	TenantID              string
	PersonalityAgentID    string
	Generation            uint64
	RPCBootNonce          string
	Audience              string
	DeliveryAuthorization LocalDeliveryAuthorization
}

type LocalCredentialIssueRequest struct {
	RequestID          string `json:"request_id"`
	PersonalityAgentID string `json:"personality_agent_id"`
	Generation         uint64 `json:"generation"`
	RPCBootNonce       string `json:"rpc_boot_nonce"`
	Audience           string `json:"audience"`
}

type LocalCredentialIssueResponse struct {
	RequestID             string                     `json:"request_id"`
	PersonalityAgentID    string                     `json:"personality_agent_id"`
	Generation            uint64                     `json:"generation"`
	RPCBootNonce          string                     `json:"rpc_boot_nonce"`
	Audience              string                     `json:"audience"`
	ExpiresAtUnix         int64                      `json:"expires_at_unix"`
	DeliveryAuthorization LocalDeliveryAuthorization `json:"delivery_authorization"`
	Token                 string                     `json:"token"`
}

type LocalRuntimeStatePublication struct {
	PublicationID            string                        `json:"publication_id"`
	PersonalityAgentID       string                        `json:"personality_agent_id"`
	Generation               uint64                        `json:"generation"`
	RPCBootNonce             string                        `json:"rpc_boot_nonce"`
	ExpectedRevision         *uint64                       `json:"expected_revision"`
	State                    LocalRuntimePublicationState  `json:"state"`
	HydrationReceiptIdentity *string                       `json:"hydration_receipt_identity"`
	Reason                   LocalRuntimePublicationReason `json:"reason"`
}

type LocalRuntimeStateAck struct {
	PublicationID            string                       `json:"publication_id"`
	PersonalityAgentID       string                       `json:"personality_agent_id"`
	Generation               uint64                       `json:"generation"`
	RPCBootNonce             string                       `json:"rpc_boot_nonce"`
	Revision                 uint64                       `json:"revision"`
	State                    LocalRuntimePublicationState `json:"state"`
	HydrationReceiptIdentity *string                      `json:"hydration_receipt_identity"`
}

type localCredentialRecord struct {
	Request               LocalCredentialIssueRequest `json:"request"`
	ExpiresAtUnix         int64                       `json:"expires_at_unix"`
	DeliveryAuthorization LocalDeliveryAuthorization  `json:"delivery_authorization"`
	IntegrityBinding      string                      `json:"integrity_binding"`
}

type localPublicationRecord struct {
	Request LocalRuntimeStatePublication `json:"request"`
	Ack     LocalRuntimeStateAck         `json:"ack"`
}

type localControlDurableState struct {
	Version            uint8                             `json:"version"`
	RPCBootNonce       string                            `json:"rpc_boot_nonce"`
	Revision           uint64                            `json:"revision"`
	State              LocalRuntimePublicationState      `json:"state"`
	Reason             LocalRuntimePublicationReason     `json:"reason"`
	Publications       map[string]localPublicationRecord `json:"publications"`
	CredentialRequests map[string]localCredentialRecord  `json:"credential_requests"`
	Integrity          *localControlStateIntegrity       `json:"integrity,omitempty"`
}

type localControlStateIntegrity struct {
	Version uint8  `json:"version"`
	KeyID   string `json:"key_id"`
	MAC     string `json:"mac"`
}

type localControlIntegrityKey struct {
	ID  string
	Key []byte
}

type localControlIntegrityKeyring struct {
	Current  localControlIntegrityKey
	Previous map[string]localControlIntegrityKey
}

// localRuntimeAuthorizationRegistry linearizes authorization changes by PAID.
// A read lease is held for the full authenticated request so a completed
// replacement/removal is a hard fence: no request using the retired epoch can
// still mutate state or receive a credential after the writer returns.
type localRuntimeAuthorizationRegistry struct {
	mu     sync.RWMutex
	byPAID map[string]LocalRuntimeAuthorization
}

// LocalControlServer owns the local/CI issuer and state-publication handlers.
// The HMAC signing key remains only in this Go process.
type LocalControlServer struct {
	gateway                 *DurableGateway
	signingSecret           []byte
	authorizationMutationMu sync.Mutex
	authorizations          localRuntimeAuthorizationRegistry
	tokenTTL                time.Duration
	now                     func() time.Time
	extensionMu             sync.RWMutex
	extensions              map[string]localControlExtension
}

type localControlExtension struct {
	handler       LocalAuthorizedHandler
	stagedHandler LocalStagedAuthorizedHandler
}

// LocalAuthorizedHandler is an extension mounted on the PAID-bound local
// control transport. The authorization is derived from the bearer and bound
// Unix socket, never from request JSON, and its epoch lease is held until the
// handler returns.
type LocalAuthorizedHandler func(http.ResponseWriter, *http.Request, LocalRuntimeAuthorization)

// LocalAuthorizationAdmission reacquires the exact PAID/process epoch that
// authenticated a staged request and holds its read lease only while op runs.
// It returns admitted=false after a replacement or removal. Application code
// uses it around short preflight/finalization mutations, never body or blob
// I/O.
type LocalAuthorizationAdmission func(op func() error) (admitted bool, err error)

// LocalStagedAuthorizedHandler is for bounded requests whose body or external
// I/O must not pin a runtime authorization generation. The handler starts with
// the usual lease held so it can perform an application preflight, then calls
// release before consuming the body. Every durable mutation must subsequently
// run through admit, which accepts only the exact original authorization
// epoch.
type LocalStagedAuthorizedHandler func(
	http.ResponseWriter,
	*http.Request,
	LocalRuntimeAuthorization,
	func(),
	LocalAuthorizationAdmission,
)

// RegisterAuthorizedRoute adds one authenticated local-control extension.
func (s *LocalControlServer) RegisterAuthorizedRoute(pattern string, handler LocalAuthorizedHandler) error {
	if s == nil || handler == nil || pattern == "" {
		return errors.New("local control authorized route is invalid")
	}
	return s.registerExtension(pattern, localControlExtension{handler: handler})
}

// RegisterStagedAuthorizedRoute adds an authenticated extension that can
// release its initial epoch lease while it performs bounded body or blob I/O.
func (s *LocalControlServer) RegisterStagedAuthorizedRoute(pattern string, handler LocalStagedAuthorizedHandler) error {
	if s == nil || handler == nil || pattern == "" {
		return errors.New("local control staged authorized route is invalid")
	}
	return s.registerExtension(pattern, localControlExtension{stagedHandler: handler})
}

func (s *LocalControlServer) registerExtension(pattern string, extension localControlExtension) error {
	s.extensionMu.Lock()
	defer s.extensionMu.Unlock()
	if s.extensions == nil {
		s.extensions = make(map[string]localControlExtension)
	}
	if _, exists := s.extensions[pattern]; exists {
		return errors.New("local control authorized route is already registered")
	}
	s.extensions[pattern] = extension
	return nil
}

func NewLocalControlServer(
	gateway *DurableGateway,
	signingSecret []byte,
	authorizations []LocalRuntimeAuthorization,
) (*LocalControlServer, error) {
	return newLocalControlServer(gateway, signingSecret, nil, authorizations)
}

// NewLocalControlServerWithPreviousSigningSecrets enables a bounded overlap
// window for durable integrity-key rotation. Tokens are issued only with
// signingSecret; previousSigningSecrets verify old runtime/lease records and
// are never used for new signatures.
func NewLocalControlServerWithPreviousSigningSecrets(
	gateway *DurableGateway,
	signingSecret []byte,
	previousSigningSecrets [][]byte,
	authorizations []LocalRuntimeAuthorization,
) (*LocalControlServer, error) {
	return newLocalControlServer(gateway, signingSecret, previousSigningSecrets, authorizations)
}

func newLocalControlServer(
	gateway *DurableGateway,
	signingSecret []byte,
	previousSigningSecrets [][]byte,
	authorizations []LocalRuntimeAuthorization,
) (*LocalControlServer, error) {
	if gateway == nil {
		return nil, errors.New("local control durable gateway is required")
	}
	if len(signingSecret) < 32 {
		return nil, errors.New("local control token HMAC secret must be at least 32 bytes")
	}
	if len(previousSigningSecrets) > maxLocalControlPreviousIntegrityKeys {
		return nil, fmt.Errorf(
			"at most %d previous local control signing secrets are supported",
			maxLocalControlPreviousIntegrityKeys,
		)
	}
	previousIntegrityKeys := make([][]byte, len(previousSigningSecrets))
	for i, previousSecret := range previousSigningSecrets {
		if len(previousSecret) < 32 {
			return nil, fmt.Errorf("previous local control signing secret %d must be at least 32 bytes", i)
		}
		previousIntegrityKeys[i] = deriveLocalControlIntegrityKey(previousSecret)
	}
	if err := gateway.revalidateRuntimeDirectory(); err != nil {
		return nil, err
	}

	normalized := make([]LocalRuntimeAuthorization, len(authorizations))
	seenPAID := make(map[string]struct{}, len(authorizations))
	for i, authorization := range authorizations {
		var err error
		authorization, err = normalizeLocalRuntimeAuthorization(authorization)
		if err != nil {
			return nil, fmt.Errorf("local runtime authorization %d: %w", i, err)
		}
		if len(signingSecret) == len(authorization.BearerToken) &&
			subtle.ConstantTimeCompare(signingSecret, []byte(authorization.BearerToken)) == 1 {
			return nil, errors.New("local control bearer and token signing secret must be distinct")
		}
		if _, exists := seenPAID[authorization.PersonalityAgentID]; exists {
			return nil, errors.New("each personality agent must have exactly one local runtime authorization")
		}
		for j := 0; j < i; j++ {
			if bearerTokensEqual(authorization.BearerToken, normalized[j].BearerToken) {
				return nil, errors.New("local runtime bearer tokens must be unique")
			}
		}
		seenPAID[authorization.PersonalityAgentID] = struct{}{}
		normalized[i] = authorization
	}

	integrityKey := deriveLocalControlIntegrityKey(signingSecret)
	owners := make([]string, 0, len(normalized))
	for _, authorization := range normalized {
		owners = append(owners, authorization.PersonalityAgentID)
	}
	if err := gateway.installLocalControlIntegrityKeyring(integrityKey, previousIntegrityKeys, owners); err != nil {
		return nil, err
	}
	checkedState := make(map[string]struct{}, len(normalized))
	for _, authorization := range normalized {
		if _, checked := checkedState[authorization.PersonalityAgentID]; checked {
			continue
		}
		if err := gateway.resignIntegrityStates(
			context.Background(),
			authorization.PersonalityAgentID,
		); err != nil {
			return nil, fmt.Errorf("repair local control integrity state: %w", err)
		}
		state, err := gateway.state(context.Background(), authorization.PersonalityAgentID)
		if err != nil {
			return nil, fmt.Errorf("validate existing local control runtime state: %w", err)
		}
		if state.present && state.LocalControl == nil {
			return nil, errors.New("existing runtime state is not owned by local control")
		}
		checkedState[authorization.PersonalityAgentID] = struct{}{}
	}

	return &LocalControlServer{
		gateway:       gateway,
		signingSecret: append([]byte(nil), signingSecret...),
		authorizations: localRuntimeAuthorizationRegistry{
			byPAID: authorizationsByPAID(normalized),
		},
		tokenTTL: defaultLocalCredentialTTL,
		now:      time.Now,
	}, nil
}

func authorizationsByPAID(authorizations []LocalRuntimeAuthorization) map[string]LocalRuntimeAuthorization {
	byPAID := make(map[string]LocalRuntimeAuthorization, len(authorizations))
	for _, authorization := range authorizations {
		byPAID[authorization.PersonalityAgentID] = authorization
	}
	return byPAID
}

func normalizeLocalRuntimeAuthorization(
	authorization LocalRuntimeAuthorization,
) (LocalRuntimeAuthorization, error) {
	if err := validateLocalRuntimeAuthorization(authorization); err != nil {
		return LocalRuntimeAuthorization{}, err
	}
	if authorization.Audience == "" {
		authorization.Audience = defaultAgentAudience
	}
	return authorization, nil
}

func bearerTokensEqual(left, right string) bool {
	return len(left) == len(right) &&
		subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

// InstallLocalRuntimeAuthorization atomically installs or replaces the one
// current authorization epoch for authorization.PersonalityAgentID. Callers
// prepare the allocator/runtime first, then publish that coherent result here.
// The signing and durable-integrity keyring remain process-owned and are never
// replaced by this operation.
func (s *LocalControlServer) InstallLocalRuntimeAuthorization(
	ctx context.Context,
	authorization LocalRuntimeAuthorization,
) error {
	if s == nil || s.gateway == nil {
		return errors.New("local control server is not initialized")
	}
	if ctx == nil {
		return errors.New("local runtime authorization context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	normalized, err := normalizeLocalRuntimeAuthorization(authorization)
	if err != nil {
		return err
	}
	if len(s.signingSecret) == len(normalized.BearerToken) &&
		subtle.ConstantTimeCompare(s.signingSecret, []byte(normalized.BearerToken)) == 1 {
		return errors.New("local control bearer and token signing secret must be distinct")
	}
	s.authorizationMutationMu.Lock()
	defer s.authorizationMutationMu.Unlock()
	if err := s.authorizations.checkInstall(normalized); err != nil {
		return err
	}
	state, err := s.gateway.state(ctx, normalized.PersonalityAgentID)
	if err != nil {
		return fmt.Errorf("validate existing local control runtime state: %w", err)
	}
	if state.present && state.LocalControl == nil {
		return errors.New("existing runtime state is not owned by local control")
	}

	// Ownership and any previous-key repair must be ready before the epoch is
	// externally reachable. Removing an authorization intentionally does not
	// remove this process-owned integrity fence from durable state.
	ownerAdded, err := s.gateway.addLocalControlOwner(normalized.PersonalityAgentID)
	if err != nil {
		return err
	}
	installed := false
	defer func() {
		if ownerAdded && !installed {
			s.gateway.removeLocalControlOwner(normalized.PersonalityAgentID)
		}
	}()
	if err := s.gateway.resignIntegrityStates(ctx, normalized.PersonalityAgentID); err != nil {
		return fmt.Errorf("repair local control integrity state: %w", err)
	}
	state, err = s.gateway.state(ctx, normalized.PersonalityAgentID)
	if err != nil {
		return fmt.Errorf("validate existing local control runtime state: %w", err)
	}
	if state.present && state.LocalControl == nil {
		return errors.New("existing runtime state is not owned by local control")
	}
	if state.present {
		control := state.LocalControl
		switch {
		case normalized.Generation < state.Generation:
			return errors.New("local runtime authorization generation is older than durable state")
		case normalized.Generation == state.Generation && normalized.RPCBootNonce != control.RPCBootNonce:
			return errors.New("local runtime authorization reuses a generation with a different RPC boot nonce")
		case normalized.Generation == state.Generation && control.Reason == LocalRuntimeShutdown:
			return errors.New("local runtime authorization cannot revive a terminal epoch")
		case normalized.Generation > state.Generation && control.Reason != LocalRuntimeShutdown:
			// Fence an orphaned Ready record before the replacement epoch is
			// reachable. This covers an API restart after the root runtime died.
			s.authorizations.removeEpoch(
				normalized.PersonalityAgentID,
				state.Generation,
				control.RPCBootNonce,
			)
			if err := s.publishControlPlaneShutdown(ctx, normalized.PersonalityAgentID, state); err != nil {
				return err
			}
		}
	}
	if err := s.authorizations.install(normalized); err != nil {
		return err
	}
	installed = true
	return nil
}

// RemoveLocalRuntimeAuthorization atomically fences the current epoch for one
// PAID. It is idempotent and does not retire process signing/integrity keys.
func (s *LocalControlServer) RemoveLocalRuntimeAuthorization(personalityAgentID string) error {
	if s == nil || s.gateway == nil {
		return errors.New("local control server is not initialized")
	}
	if err := ValidatePersonalityAgentID(personalityAgentID); err != nil {
		return err
	}
	s.authorizationMutationMu.Lock()
	defer s.authorizationMutationMu.Unlock()
	s.authorizations.remove(personalityAgentID)
	return nil
}

// FenceLocalRuntimeAuthorization retires exactly one PAID/process epoch and
// atomically drives any Ready state owned by that epoch to terminal NotReady.
// A delayed cleanup from an older process can therefore never revoke or
// overwrite a replacement generation.
func (s *LocalControlServer) FenceLocalRuntimeAuthorization(
	ctx context.Context,
	personalityAgentID string,
	generation uint64,
	rpcBootNonce string,
) error {
	if s == nil || s.gateway == nil {
		return errors.New("local control server is not initialized")
	}
	if ctx == nil {
		return errors.New("local runtime authorization context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidatePersonalityAgentID(personalityAgentID); err != nil {
		return err
	}
	if generation > maxProcessGeneration {
		return errors.New("local runtime generation is outside the process-generation domain")
	}
	if err := validateOpaqueRuntimeID(rpcBootNonce, "RPC boot nonce"); err != nil {
		return err
	}

	s.authorizationMutationMu.Lock()
	defer s.authorizationMutationMu.Unlock()
	// An API restart may have lost the in-memory authorization while the root
	// provisioner and durable Ready state survive. Remove the exact epoch when
	// present, but always reconcile matching durable state below.
	s.authorizations.removeEpoch(personalityAgentID, generation, rpcBootNonce)
	state, err := s.gateway.state(ctx, personalityAgentID)
	if err != nil {
		return fmt.Errorf("read local runtime state while fencing epoch: %w", err)
	}
	if !state.present || state.LocalControl == nil ||
		state.Generation != generation || state.LocalControl.RPCBootNonce != rpcBootNonce ||
		state.LocalControl.Reason == LocalRuntimeShutdown {
		return nil
	}
	return s.publishControlPlaneShutdown(ctx, personalityAgentID, state)
}

func (s *LocalControlServer) publishControlPlaneShutdown(
	ctx context.Context,
	personalityAgentID string,
	state runtimeState,
) error {
	if !state.present || state.LocalControl == nil || state.LocalControl.Reason == LocalRuntimeShutdown {
		return nil
	}
	revision := state.LocalControl.Revision
	digest := sha256.Sum256([]byte(fmt.Sprintf(
		"sumi-control-plane-fence-v1\x00%s\x00%d\x00%s",
		personalityAgentID,
		state.Generation,
		state.LocalControl.RPCBootNonce,
	)))
	_, err := s.publishRuntimeState(ctx, LocalRuntimeStatePublication{
		PublicationID:      "control-plane-fence-" + hex.EncodeToString(digest[:16]),
		PersonalityAgentID: personalityAgentID,
		Generation:         state.Generation,
		RPCBootNonce:       state.LocalControl.RPCBootNonce,
		ExpectedRevision:   &revision,
		State:              LocalRuntimeNotReady,
		Reason:             LocalRuntimeShutdown,
	})
	if err != nil {
		return fmt.Errorf("publish terminal local runtime fence: %w", err)
	}
	return nil
}

func (r *localRuntimeAuthorizationRegistry) checkInstall(
	authorization LocalRuntimeAuthorization,
) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for personalityAgentID, current := range r.byPAID {
		if personalityAgentID != authorization.PersonalityAgentID &&
			bearerTokensEqual(current.BearerToken, authorization.BearerToken) {
			return errors.New("local runtime bearer tokens must be unique")
		}
	}
	return nil
}

func (r *localRuntimeAuthorizationRegistry) install(authorization LocalRuntimeAuthorization) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.byPAID == nil {
		r.byPAID = make(map[string]LocalRuntimeAuthorization)
	}
	for personalityAgentID, current := range r.byPAID {
		if personalityAgentID != authorization.PersonalityAgentID &&
			bearerTokensEqual(current.BearerToken, authorization.BearerToken) {
			return errors.New("local runtime bearer tokens must be unique")
		}
	}
	r.byPAID[authorization.PersonalityAgentID] = authorization
	return nil
}

func (r *localRuntimeAuthorizationRegistry) removeEpoch(
	personalityAgentID string,
	generation uint64,
	rpcBootNonce string,
) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, exists := r.byPAID[personalityAgentID]
	if !exists || current.Generation != generation || current.RPCBootNonce != rpcBootNonce {
		return false
	}
	delete(r.byPAID, personalityAgentID)
	return true
}

func (r *localRuntimeAuthorizationRegistry) remove(personalityAgentID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.byPAID, personalityAgentID)
}

// admitExact runs op under the registry read lock only if the exact
// authorization that authenticated a staged request is still the current one
// for its PAID. A replaced or removed epoch reports admitted=false.
func (r *localRuntimeAuthorizationRegistry) admitExact(
	expected LocalRuntimeAuthorization,
	op func() error,
) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	current, exists := r.byPAID[expected.PersonalityAgentID]
	if !exists || !localRuntimeAuthorizationsEqual(current, expected) {
		return false, nil
	}
	return true, op()
}

func localRuntimeAuthorizationsEqual(left, right LocalRuntimeAuthorization) bool {
	return bearerTokensEqual(left.BearerToken, right.BearerToken) &&
		left.TenantID == right.TenantID &&
		left.PersonalityAgentID == right.PersonalityAgentID &&
		left.Generation == right.Generation &&
		left.RPCBootNonce == right.RPCBootNonce &&
		left.Audience == right.Audience &&
		left.DeliveryAuthorization == right.DeliveryAuthorization
}

func (r *localRuntimeAuthorizationRegistry) acquire(
	bearerToken string,
	boundPersonalityAgentID string,
) (LocalRuntimeAuthorization, func(), bool) {
	r.mu.RLock()
	for _, authorization := range r.byPAID {
		if bearerTokensEqual(bearerToken, authorization.BearerToken) {
			if boundPersonalityAgentID != "" &&
				authorization.PersonalityAgentID != boundPersonalityAgentID {
				r.mu.RUnlock()
				return LocalRuntimeAuthorization{}, nil, false
			}
			return authorization, r.mu.RUnlock, true
		}
	}
	r.mu.RUnlock()
	return LocalRuntimeAuthorization{}, nil, false
}

// RegisterRoutes attaches the local control endpoints to an explicitly chosen
// mux. NewProductionMux never calls this method.
func (s *LocalControlServer) RegisterRoutes(mux *http.ServeMux) error {
	if s == nil || s.gateway == nil {
		return errors.New("local control server is not initialized")
	}
	if mux == nil {
		return errors.New("local control mux is required")
	}
	mux.HandleFunc("POST "+LocalCredentialIssuePath, s.handleCredentialIssue)
	mux.HandleFunc("POST "+LocalRuntimeStatePublishPath, s.handleRuntimeStatePublish)
	s.extensionMu.RLock()
	defer s.extensionMu.RUnlock()
	for pattern, extension := range s.extensions {
		extension := extension
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			authorization, release, ok := s.authorize(w, r)
			if !ok {
				return
			}
			var releaseOnce sync.Once
			releaseInitial := func() { releaseOnce.Do(release) }
			defer releaseInitial()
			if extension.stagedHandler != nil {
				extension.stagedHandler(
					w,
					r,
					authorization,
					releaseInitial,
					func(op func() error) (bool, error) {
						releaseInitial()
						return s.authorizations.admitExact(authorization, op)
					},
				)
				return
			}
			extension.handler(w, r, authorization)
		})
	}
	return nil
}

type localControlBoundPersonalityAgentIDKey struct{}

// HandlerForLocalRuntime returns the same local-control routes bound to one
// PAID execution boundary. It is intended for that PAID's trusted Unix socket:
// a request arriving there cannot authenticate as or publish state for a
// different PAID, even if it possesses that other runtime's bearer.
func (s *LocalControlServer) HandlerForLocalRuntime(
	personalityAgentID string,
) (http.Handler, error) {
	if err := ValidatePersonalityAgentID(personalityAgentID); err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	if err := s.RegisterRoutes(mux); err != nil {
		return nil, err
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(
			r.Context(),
			localControlBoundPersonalityAgentIDKey{},
			personalityAgentID,
		)
		localRequest := r.Clone(ctx)
		// Unix-domain HTTP requests do not have an IP RemoteAddr. The socket
		// listener is the trusted loopback transport for this handler.
		localRequest.RemoteAddr = "127.0.0.1:0"
		mux.ServeHTTP(w, localRequest)
	}), nil
}

func (s *LocalControlServer) handleCredentialIssue(w http.ResponseWriter, r *http.Request) {
	authorization, release, ok := s.authorize(w, r)
	if !ok {
		return
	}
	defer release()
	var request LocalCredentialIssueRequest
	if !decodeLocalControlRequest(w, r, &request) {
		return
	}
	if err := validateCredentialIssueRequest(request); err != nil {
		writeLocalControlError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	if !authorizationMatchesCredential(authorization, request) {
		writeLocalControlError(w, http.StatusForbidden, "authorization_scope_mismatch")
		return
	}

	response, err := s.issueCredential(r.Context(), authorization, request)
	if err != nil {
		writeLocalControlOperationError(w, err)
		return
	}
	writeLocalControlJSON(w, http.StatusOK, response)
}

func (s *LocalControlServer) handleRuntimeStatePublish(w http.ResponseWriter, r *http.Request) {
	authorization, release, ok := s.authorize(w, r)
	if !ok {
		return
	}
	defer release()
	var publication LocalRuntimeStatePublication
	if !decodeLocalControlRequest(w, r, &publication) {
		return
	}
	if err := validateRuntimeStatePublication(publication); err != nil {
		writeLocalControlError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	if !authorizationMatchesPublication(authorization, publication) {
		writeLocalControlError(w, http.StatusForbidden, "authorization_scope_mismatch")
		return
	}

	ack, err := s.publishRuntimeState(r.Context(), publication)
	if err != nil {
		writeLocalControlOperationError(w, err)
		return
	}
	writeLocalControlJSON(w, http.StatusOK, ack)
}

func (s *LocalControlServer) authorize(
	w http.ResponseWriter,
	r *http.Request,
) (LocalRuntimeAuthorization, func(), bool) {
	if !requestIsLoopback(r) {
		writeLocalControlError(w, http.StatusForbidden, "loopback_required")
		return LocalRuntimeAuthorization{}, nil, false
	}
	values := r.Header.Values("Authorization")
	if len(values) != 1 {
		writeLocalControlError(w, http.StatusUnauthorized, "invalid_authorization")
		return LocalRuntimeAuthorization{}, nil, false
	}
	token, ok := bearerToken(values[0])
	if !ok {
		writeLocalControlError(w, http.StatusUnauthorized, "invalid_authorization")
		return LocalRuntimeAuthorization{}, nil, false
	}
	boundPersonalityAgentID, _ := r.Context().Value(
		localControlBoundPersonalityAgentIDKey{},
	).(string)
	authorization, release, ok := s.authorizations.acquire(token, boundPersonalityAgentID)
	if ok {
		return authorization, release, true
	}
	writeLocalControlError(w, http.StatusUnauthorized, "invalid_authorization")
	return LocalRuntimeAuthorization{}, nil, false
}

func requestIsLoopback(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func decodeLocalControlRequest(w http.ResponseWriter, r *http.Request, destination any) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeLocalControlError(w, http.StatusUnsupportedMediaType, "application_json_required")
		return false
	}
	reader := http.MaxBytesReader(w, r.Body, maxLocalControlBodyBytes)
	raw, err := io.ReadAll(reader)
	if err != nil {
		writeLocalControlError(w, http.StatusBadRequest, "invalid_request")
		return false
	}
	if err := checkDuplicateKeys(raw); err != nil {
		writeLocalControlError(w, http.StatusBadRequest, "invalid_request")
		return false
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		writeLocalControlError(w, http.StatusBadRequest, "invalid_request")
		return false
	}
	var required []string
	switch destination.(type) {
	case *LocalCredentialIssueRequest:
		required = []string{"request_id", "personality_agent_id", "generation", "rpc_boot_nonce", "audience"}
	case *LocalRuntimeStatePublication:
		required = []string{"publication_id", "personality_agent_id", "generation", "rpc_boot_nonce", "state", "reason"}
	}
	for _, field := range required {
		value, present := object[field]
		if !present || bytesEqualJSONNull(value) {
			writeLocalControlError(w, http.StatusBadRequest, "invalid_request")
			return false
		}
	}
	if err := unmarshalStrict(raw, destination); err != nil {
		writeLocalControlError(w, http.StatusBadRequest, "invalid_request")
		return false
	}
	return true
}

func bytesEqualJSONNull(value []byte) bool {
	return strings.TrimSpace(string(value)) == "null"
}

func validateLocalRuntimeAuthorization(authorization LocalRuntimeAuthorization) error {
	if len(authorization.BearerToken) < 32 || len(authorization.BearerToken) > 1024 {
		return errors.New("bearer token must contain 32..=1024 visible ASCII bytes")
	}
	for i := 0; i < len(authorization.BearerToken); i++ {
		if authorization.BearerToken[i] < 0x21 || authorization.BearerToken[i] > 0x7e {
			return errors.New("bearer token must contain 32..=1024 visible ASCII bytes")
		}
	}
	if !provenanceIDRegexp.MatchString(authorization.TenantID) {
		return errors.New("tenant id is invalid")
	}
	if err := ValidatePersonalityAgentID(authorization.PersonalityAgentID); err != nil {
		return err
	}
	if authorization.Generation > maxProcessGeneration {
		return fmt.Errorf("generation %d exceeds process generation range", authorization.Generation)
	}
	if err := validateOpaqueRuntimeID(authorization.RPCBootNonce, "RPC boot nonce"); err != nil {
		return err
	}
	if authorization.Audience != "" && !provenanceIDRegexp.MatchString(authorization.Audience) {
		return errors.New("audience is invalid")
	}
	if !validDeliveryAuthorization(authorization.DeliveryAuthorization) {
		return errors.New("delivery authorization must be raw or redaction_only")
	}
	return nil
}

func validateCredentialIssueRequest(request LocalCredentialIssueRequest) error {
	if err := validateOpaqueRuntimeID(request.RequestID, "request id"); err != nil {
		return err
	}
	if err := ValidatePersonalityAgentID(request.PersonalityAgentID); err != nil {
		return err
	}
	if request.Generation > maxProcessGeneration {
		return fmt.Errorf("generation %d exceeds process generation range", request.Generation)
	}
	if err := validateOpaqueRuntimeID(request.RPCBootNonce, "RPC boot nonce"); err != nil {
		return err
	}
	if !provenanceIDRegexp.MatchString(request.Audience) {
		return errors.New("audience is invalid")
	}
	return nil
}

func validateRuntimeStatePublication(publication LocalRuntimeStatePublication) error {
	if err := validateOpaqueRuntimeID(publication.PublicationID, "publication id"); err != nil {
		return err
	}
	if err := ValidatePersonalityAgentID(publication.PersonalityAgentID); err != nil {
		return err
	}
	if publication.Generation > maxProcessGeneration {
		return fmt.Errorf("generation %d exceeds process generation range", publication.Generation)
	}
	if err := validateOpaqueRuntimeID(publication.RPCBootNonce, "RPC boot nonce"); err != nil {
		return err
	}
	if publication.HydrationReceiptIdentity != nil {
		if err := validateOpaqueRuntimeID(*publication.HydrationReceiptIdentity, "hydration receipt identity"); err != nil {
			return err
		}
	}
	switch publication.Reason {
	case LocalRuntimeStartup:
		if publication.State != LocalRuntimeNotReady || publication.HydrationReceiptIdentity != nil {
			return errors.New("startup must publish not_ready without a receipt")
		}
	case LocalRuntimeHydrated:
		if publication.State != LocalRuntimeReady || publication.HydrationReceiptIdentity == nil {
			return errors.New("hydrated must publish ready with a receipt")
		}
	case LocalRuntimeShutdown:
		if publication.State != LocalRuntimeNotReady || publication.HydrationReceiptIdentity != nil {
			return errors.New("shutdown must publish not_ready without a receipt")
		}
	default:
		return errors.New("invalid runtime publication reason")
	}
	return nil
}

func validateOpaqueRuntimeID(value, kind string) error {
	if len(value) == 0 || len(value) > maxOpaqueRuntimeIDBytes {
		return fmt.Errorf("%s must contain 1..=%d bytes", kind, maxOpaqueRuntimeIDBytes)
	}
	return nil
}

func validDeliveryAuthorization(value LocalDeliveryAuthorization) bool {
	return value == LocalDeliveryRaw || value == LocalDeliveryRedactionOnly
}

func authorizationMatchesCredential(
	authorization LocalRuntimeAuthorization,
	request LocalCredentialIssueRequest,
) bool {
	return authorization.PersonalityAgentID == request.PersonalityAgentID &&
		authorization.Generation == request.Generation &&
		authorization.RPCBootNonce == request.RPCBootNonce &&
		authorization.Audience == request.Audience
}

func authorizationMatchesPublication(
	authorization LocalRuntimeAuthorization,
	publication LocalRuntimeStatePublication,
) bool {
	return authorization.PersonalityAgentID == publication.PersonalityAgentID &&
		authorization.Generation == publication.Generation &&
		authorization.RPCBootNonce == publication.RPCBootNonce
}

var (
	errLocalControlConflict      = errors.New("local control idempotency conflict")
	errLocalControlStaleEpoch    = errors.New("local control stale epoch")
	errLocalControlCAS           = errors.New("local control revision conflict")
	errLocalControlInvalidState  = errors.New("local control invalid state transition")
	errLocalControlCapacity      = errors.New("local control durable idempotency capacity exhausted")
	errLocalControlUninitialized = errors.New("local control runtime epoch is not initialized")
)

func (s *LocalControlServer) issueCredential(
	ctx context.Context,
	authorization LocalRuntimeAuthorization,
	request LocalCredentialIssueRequest,
) (LocalCredentialIssueResponse, error) {
	var response LocalCredentialIssueResponse
	err := s.gateway.updateLocalControlRuntimeState(ctx, request.PersonalityAgentID, func(state *runtimeState) (bool, error) {
		if !state.present || state.LocalControl == nil {
			return false, errLocalControlUninitialized
		}
		control := state.LocalControl
		if state.Generation != request.Generation || control.RPCBootNonce != request.RPCBootNonce {
			return false, errLocalControlStaleEpoch
		}
		if control.Reason == LocalRuntimeShutdown {
			return false, errLocalControlStaleEpoch
		}
		// A credential request is idempotent only while the token it names can
		// still be used. HMACTokenVerifier rejects exp <= now, so those records
		// cannot answer a useful retry and must not consume capacity forever.
		now := s.now()
		pruned := false
		for requestID, record := range control.CredentialRequests {
			if record.ExpiresAtUnix <= now.Unix() {
				delete(control.CredentialRequests, requestID)
				pruned = true
			}
		}
		if record, exists := control.CredentialRequests[request.RequestID]; exists {
			expectedBinding := s.credentialRecordBinding(
				authorization,
				record.Request,
				record.ExpiresAtUnix,
				record.DeliveryAuthorization,
			)
			if !credentialIssueRequestEqual(record.Request, request) ||
				record.IntegrityBinding != expectedBinding {
				return false, errLocalControlConflict
			}
			var err error
			response, err = s.buildCredentialResponse(
				authorization,
				request,
				record.ExpiresAtUnix,
				record.DeliveryAuthorization,
			)
			if err != nil {
				return false, err
			}
			return pruned, nil
		}
		if len(control.CredentialRequests) >= maxLocalControlRecords {
			return false, errLocalControlCapacity
		}

		expiresAt := now.Add(s.tokenTTL).Unix()
		if expiresAt <= now.Unix() {
			return false, errors.New("local credential expiry did not advance")
		}
		var err error
		response, err = s.buildCredentialResponse(
			authorization,
			request,
			expiresAt,
			authorization.DeliveryAuthorization,
		)
		if err != nil {
			return false, err
		}
		control.CredentialRequests[request.RequestID] = localCredentialRecord{
			Request:               request,
			ExpiresAtUnix:         expiresAt,
			DeliveryAuthorization: authorization.DeliveryAuthorization,
			IntegrityBinding: s.credentialRecordBinding(
				authorization,
				request,
				expiresAt,
				authorization.DeliveryAuthorization,
			),
		}
		return true, nil
	})
	return response, err
}

func (s *LocalControlServer) buildCredentialResponse(
	authorization LocalRuntimeAuthorization,
	request LocalCredentialIssueRequest,
	expiresAt int64,
	deliveryAuthorization LocalDeliveryAuthorization,
) (LocalCredentialIssueResponse, error) {
	token, err := signAgentToken(s.signingSecret, tokenClaims{
		TenantID:           authorization.TenantID,
		PersonalityAgentID: request.PersonalityAgentID,
		Generation:         request.Generation,
		Exp:                expiresAt,
		Aud:                request.Audience,
	})
	if err != nil {
		return LocalCredentialIssueResponse{}, err
	}
	return LocalCredentialIssueResponse{
		RequestID:             request.RequestID,
		PersonalityAgentID:    request.PersonalityAgentID,
		Generation:            request.Generation,
		RPCBootNonce:          request.RPCBootNonce,
		Audience:              request.Audience,
		ExpiresAtUnix:         expiresAt,
		DeliveryAuthorization: deliveryAuthorization,
		Token:                 token,
	}, nil
}

func (s *LocalControlServer) publishRuntimeState(
	ctx context.Context,
	publication LocalRuntimeStatePublication,
) (LocalRuntimeStateAck, error) {
	var ack LocalRuntimeStateAck
	err := s.gateway.updateLocalControlRuntimeState(ctx, publication.PersonalityAgentID, func(state *runtimeState) (bool, error) {
		if state.present && state.LocalControl == nil {
			return false, errLocalControlInvalidState
		}

		if state.present {
			control := state.LocalControl
			if publication.Generation < state.Generation ||
				(publication.Generation == state.Generation &&
					publication.RPCBootNonce != control.RPCBootNonce) {
				return false, errLocalControlStaleEpoch
			}
			if record, exists := control.Publications[publication.PublicationID]; exists {
				if !runtimeStatePublicationEqual(record.Request, publication) {
					return false, errLocalControlConflict
				}
				if publication.Generation != state.Generation ||
					publication.RPCBootNonce != control.RPCBootNonce {
					return false, errLocalControlStaleEpoch
				}
				ack = record.Ack
				return false, nil
			}
			if len(control.Publications) >= maxLocalControlRecords {
				return false, errLocalControlCapacity
			}
		}

		if !state.present {
			if publication.Reason != LocalRuntimeStartup || publication.ExpectedRevision != nil {
				return false, errLocalControlCAS
			}
			state.Generation = publication.Generation
			state.HydrationReceiptIdentity = nil
			state.LocalControl = &localControlDurableState{
				Version:            localControlStateVersion,
				RPCBootNonce:       publication.RPCBootNonce,
				Revision:           1,
				State:              LocalRuntimeNotReady,
				Reason:             LocalRuntimeStartup,
				Publications:       make(map[string]localPublicationRecord),
				CredentialRequests: make(map[string]localCredentialRecord),
			}
		} else if publication.Generation > state.Generation {
			if publication.Reason != LocalRuntimeStartup || publication.ExpectedRevision != nil {
				return false, errLocalControlCAS
			}
			if publication.RPCBootNonce == state.LocalControl.RPCBootNonce {
				return false, errLocalControlInvalidState
			}
			state.Generation = publication.Generation
			state.HydrationReceiptIdentity = nil
			state.LocalControl = &localControlDurableState{
				Version:            localControlStateVersion,
				RPCBootNonce:       publication.RPCBootNonce,
				Revision:           1,
				State:              LocalRuntimeNotReady,
				Reason:             LocalRuntimeStartup,
				Publications:       make(map[string]localPublicationRecord),
				CredentialRequests: make(map[string]localCredentialRecord),
			}
		} else {
			control := state.LocalControl
			if publication.ExpectedRevision == nil || *publication.ExpectedRevision != control.Revision {
				return false, errLocalControlCAS
			}
			if control.Reason == LocalRuntimeShutdown || publication.Reason == LocalRuntimeStartup {
				return false, errLocalControlInvalidState
			}
			if control.Revision == math.MaxUint64 {
				return false, errLocalControlCapacity
			}
			switch publication.Reason {
			case LocalRuntimeHydrated:
				if control.State != LocalRuntimeNotReady ||
					control.Reason != LocalRuntimeStartup ||
					control.Revision == 0 {
					return false, errLocalControlInvalidState
				}
				state.HydrationReceiptIdentity = cloneStringPointer(publication.HydrationReceiptIdentity)
				control.State = LocalRuntimeReady
				control.Reason = LocalRuntimeHydrated
			case LocalRuntimeShutdown:
				state.HydrationReceiptIdentity = nil
				control.State = LocalRuntimeNotReady
				control.Reason = LocalRuntimeShutdown
			default:
				return false, errLocalControlInvalidState
			}
			control.Revision++
		}

		control := state.LocalControl
		ack = LocalRuntimeStateAck{
			PublicationID:            publication.PublicationID,
			PersonalityAgentID:       publication.PersonalityAgentID,
			Generation:               publication.Generation,
			RPCBootNonce:             publication.RPCBootNonce,
			Revision:                 control.Revision,
			State:                    publication.State,
			HydrationReceiptIdentity: cloneStringPointer(publication.HydrationReceiptIdentity),
		}
		control.Publications[publication.PublicationID] = localPublicationRecord{
			Request: publication,
			Ack:     ack,
		}
		return true, nil
	})
	return ack, err
}

func cloneStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func credentialIssueRequestEqual(left, right LocalCredentialIssueRequest) bool {
	return left == right
}

func runtimeStatePublicationEqual(left, right LocalRuntimeStatePublication) bool {
	return left.PublicationID == right.PublicationID &&
		left.PersonalityAgentID == right.PersonalityAgentID &&
		left.Generation == right.Generation &&
		left.RPCBootNonce == right.RPCBootNonce &&
		uint64PointerEqual(left.ExpectedRevision, right.ExpectedRevision) &&
		left.State == right.State &&
		stringPointerEqual(left.HydrationReceiptIdentity, right.HydrationReceiptIdentity) &&
		left.Reason == right.Reason
}

func uint64PointerEqual(left, right *uint64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func (s *LocalControlServer) credentialRecordBinding(
	authorization LocalRuntimeAuthorization,
	request LocalCredentialIssueRequest,
	expiresAt int64,
	deliveryAuthorization LocalDeliveryAuthorization,
) string {
	mac := hmac.New(sha256.New, s.signingSecret)
	writeBoundField := func(value string) {
		_, _ = mac.Write([]byte{byte(len(value) >> 24), byte(len(value) >> 16), byte(len(value) >> 8), byte(len(value))})
		_, _ = mac.Write([]byte(value))
	}
	writeBoundField("sumi-local-control-authorization/v1")
	writeBoundField(authorization.TenantID)
	writeBoundField(authorization.PersonalityAgentID)
	writeBoundField(fmt.Sprintf("%d", authorization.Generation))
	writeBoundField(authorization.RPCBootNonce)
	writeBoundField(authorization.Audience)
	writeBoundField(string(deliveryAuthorization))
	writeBoundField(request.RequestID)
	writeBoundField(request.PersonalityAgentID)
	writeBoundField(fmt.Sprintf("%d", request.Generation))
	writeBoundField(request.RPCBootNonce)
	writeBoundField(request.Audience)
	writeBoundField(fmt.Sprintf("%d", expiresAt))
	return hex.EncodeToString(mac.Sum(nil))
}

func validateLocalControlRuntimeState(personalityAgentID string, state runtimeState) error {
	control := state.LocalControl
	if control == nil {
		return nil
	}
	if err := validateLocalControlStateVersion(control); err != nil {
		return err
	}
	if err := validateOpaqueRuntimeID(control.RPCBootNonce, "RPC boot nonce"); err != nil {
		return err
	}
	if control.Revision == 0 {
		return errors.New("local control revision must be non-zero")
	}
	if control.Publications == nil || control.CredentialRequests == nil {
		return errors.New("local control idempotency maps must be present")
	}
	if control.Integrity == nil ||
		control.Integrity.Version != localControlIntegrityVersion ||
		len(control.Integrity.MAC) != sha256.Size*2 {
		return errors.New("invalid local control state integrity metadata")
	}
	if len(control.Integrity.KeyID) != 32 {
		return errors.New("invalid local control state integrity key identifier")
	}
	if _, err := hex.DecodeString(control.Integrity.KeyID); err != nil {
		return errors.New("invalid local control state integrity key identifier")
	}
	if _, err := hex.DecodeString(control.Integrity.MAC); err != nil {
		return errors.New("invalid local control state integrity metadata")
	}
	if len(control.Publications) > maxLocalControlRecords ||
		len(control.CredentialRequests) > maxLocalControlRecords {
		return errors.New("local control idempotency capacity exceeded")
	}
	switch control.Reason {
	case LocalRuntimeStartup:
		if control.State != LocalRuntimeNotReady || state.HydrationReceiptIdentity != nil {
			return errors.New("startup state must be not_ready without a receipt")
		}
	case LocalRuntimeHydrated:
		if control.State != LocalRuntimeReady || state.HydrationReceiptIdentity == nil {
			return errors.New("hydrated state must be ready with a receipt")
		}
		if err := validateOpaqueRuntimeID(*state.HydrationReceiptIdentity, "hydration receipt identity"); err != nil {
			return err
		}
	case LocalRuntimeShutdown:
		if control.State != LocalRuntimeNotReady || state.HydrationReceiptIdentity != nil {
			return errors.New("shutdown state must be not_ready without a receipt")
		}
	default:
		return errors.New("invalid local control state reason")
	}
	if uint64(len(control.Publications)) != control.Revision {
		return errors.New("durable runtime publication history is not a complete revision prefix")
	}
	byRevision := make(map[uint64]localPublicationRecord, len(control.Publications))
	for id, record := range control.Publications {
		if id != record.Request.PublicationID ||
			record.Request.PersonalityAgentID != personalityAgentID ||
			!runtimeStateAckMatchesPublication(record.Ack, record.Request) {
			return errors.New("invalid durable runtime publication record")
		}
		if err := validateRuntimeStatePublication(record.Request); err != nil {
			return fmt.Errorf("invalid durable runtime publication record: %w", err)
		}
		if record.Ack.Revision > control.Revision {
			return errors.New("durable runtime publication revision exceeds current revision")
		}
		if record.Ack.Generation > state.Generation ||
			(record.Ack.Generation == state.Generation &&
				record.Ack.RPCBootNonce != control.RPCBootNonce) {
			return errors.New("durable runtime publication is outside the current generation history")
		}
		if _, exists := byRevision[record.Ack.Revision]; exists {
			return errors.New("durable runtime publications reuse a revision")
		}
		byRevision[record.Ack.Revision] = record
	}
	for revision := uint64(1); revision <= control.Revision; revision++ {
		record, exists := byRevision[revision]
		if !exists {
			return errors.New("durable runtime publication history has a revision gap")
		}
		if revision == 1 {
			if record.Request.Reason != LocalRuntimeStartup ||
				record.Request.ExpectedRevision != nil {
				return errors.New("first durable runtime publication must start an epoch")
			}
			continue
		}
		previous := byRevision[revision-1]
		switch record.Request.Reason {
		case LocalRuntimeStartup:
			if record.Request.ExpectedRevision != nil ||
				record.Request.Generation <= previous.Ack.Generation ||
				record.Request.RPCBootNonce == previous.Ack.RPCBootNonce {
				return errors.New("durable runtime rollover is not a fresh higher epoch")
			}
		case LocalRuntimeHydrated:
			if record.Request.ExpectedRevision == nil ||
				*record.Request.ExpectedRevision != revision-1 ||
				record.Request.Generation != previous.Ack.Generation ||
				record.Request.RPCBootNonce != previous.Ack.RPCBootNonce ||
				previous.Request.Reason != LocalRuntimeStartup ||
				previous.Ack.State != LocalRuntimeNotReady {
				return errors.New("durable Ready publication is not bound to its startup revision")
			}
		case LocalRuntimeShutdown:
			if record.Request.ExpectedRevision == nil ||
				*record.Request.ExpectedRevision != revision-1 ||
				record.Request.Generation != previous.Ack.Generation ||
				record.Request.RPCBootNonce != previous.Ack.RPCBootNonce ||
				previous.Request.Reason == LocalRuntimeShutdown {
				return errors.New("durable shutdown publication is not bound to the current epoch")
			}
		default:
			return errors.New("invalid durable runtime publication reason")
		}
	}
	current := byRevision[control.Revision]
	if current.Ack.Generation != state.Generation ||
		current.Ack.RPCBootNonce != control.RPCBootNonce ||
		current.Ack.State != control.State ||
		!stringPointerEqual(current.Ack.HydrationReceiptIdentity, state.HydrationReceiptIdentity) {
		return errors.New("current local control state does not match its publication")
	}
	for id, record := range control.CredentialRequests {
		if id != record.Request.RequestID ||
			record.Request.PersonalityAgentID != personalityAgentID ||
			record.Request.Generation > state.Generation ||
			(record.Request.Generation == state.Generation &&
				record.Request.RPCBootNonce != control.RPCBootNonce) ||
			record.ExpiresAtUnix <= 0 ||
			!validDeliveryAuthorization(record.DeliveryAuthorization) ||
			len(record.IntegrityBinding) != sha256.Size*2 {
			return errors.New("invalid durable credential request record")
		}
		if _, err := hex.DecodeString(record.IntegrityBinding); err != nil {
			return errors.New("invalid durable credential integrity binding")
		}
		if err := validateCredentialIssueRequest(record.Request); err != nil {
			return fmt.Errorf("invalid durable credential request record: %w", err)
		}
	}
	return nil
}

func validateLocalControlStateVersion(control *localControlDurableState) error {
	if control.Version != localControlStateVersion {
		return fmt.Errorf(
			"unsupported local control state version %d; delete the state file to reset it",
			control.Version,
		)
	}
	return nil
}

func runtimeStateAckMatchesPublication(ack LocalRuntimeStateAck, publication LocalRuntimeStatePublication) bool {
	return ack.PublicationID == publication.PublicationID &&
		ack.PersonalityAgentID == publication.PersonalityAgentID &&
		ack.Generation == publication.Generation &&
		ack.RPCBootNonce == publication.RPCBootNonce &&
		ack.Revision != 0 &&
		ack.State == publication.State &&
		stringPointerEqual(ack.HydrationReceiptIdentity, publication.HydrationReceiptIdentity)
}

func (g *DurableGateway) updateLocalControlRuntimeState(
	ctx context.Context,
	personalityAgentID string,
	update func(*runtimeState) (bool, error),
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidatePersonalityAgentID(personalityAgentID); err != nil {
		return err
	}
	lock, err := g.openRuntimeLock(personalityAgentID)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := flockContext(ctx, lock.Fd(), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock local control registry: %w", err)
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()
	if err := ctx.Err(); err != nil {
		return err
	}

	state, err := g.state(ctx, personalityAgentID)
	if err != nil {
		return err
	}
	write, err := update(&state)
	if err != nil {
		return err
	}
	if !write && !state.needsResign {
		return nil
	}
	return g.persistSignedLocalControlRuntimeState(personalityAgentID, &state)
}

func (g *DurableGateway) persistSignedLocalControlRuntimeState(
	personalityAgentID string,
	state *runtimeState,
) error {
	if err := g.signLocalControlRuntimeState(state); err != nil {
		return fmt.Errorf("sign local control runtime state: %w", err)
	}
	state.needsResign = false
	if err := validateLocalControlRuntimeState(personalityAgentID, *state); err != nil {
		return fmt.Errorf("validate local control runtime state: %w", err)
	}
	raw, err := json.Marshal(*state)
	if err != nil {
		return fmt.Errorf("encode local control runtime state: %w", err)
	}
	if err := writeFileAtomic(g.statePath(personalityAgentID), raw, 0o600); err != nil {
		return fmt.Errorf("persist local control runtime state: %w", err)
	}
	return nil
}

func deriveLocalControlIntegrityKey(signingSecret []byte) []byte {
	mac := hmac.New(sha256.New, signingSecret)
	_, _ = mac.Write([]byte("sumi-local-control-state-integrity-key/v1"))
	return mac.Sum(nil)
}

func deriveLocalControlIntegrityKeyID(key []byte) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte("sumi-local-control-state-integrity-key-id/v1"))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(key)
	// A 128-bit identifier is non-secret and sufficiently collision resistant.
	return hex.EncodeToString(hash.Sum(nil)[:16])
}

func (g *DurableGateway) installLocalControlIntegrityKey(key []byte, owners []string) error {
	return g.installLocalControlIntegrityKeyring(key, nil, owners)
}

func newLocalControlIntegrityKey(key []byte) (localControlIntegrityKey, error) {
	if len(key) != sha256.Size {
		return localControlIntegrityKey{}, errors.New("invalid local control integrity key")
	}
	return localControlIntegrityKey{
		ID:  deriveLocalControlIntegrityKeyID(key),
		Key: append([]byte(nil), key...),
	}, nil
}

func localControlIntegrityKeyringsEqual(
	leftCurrent localControlIntegrityKey,
	leftPrevious map[string]localControlIntegrityKey,
	rightCurrent localControlIntegrityKey,
	rightPrevious map[string]localControlIntegrityKey,
) bool {
	if leftCurrent.ID != rightCurrent.ID ||
		subtle.ConstantTimeCompare(leftCurrent.Key, rightCurrent.Key) != 1 ||
		len(leftPrevious) != len(rightPrevious) {
		return false
	}
	for id, left := range leftPrevious {
		right, ok := rightPrevious[id]
		if !ok || subtle.ConstantTimeCompare(left.Key, right.Key) != 1 {
			return false
		}
	}
	return true
}

func (g *DurableGateway) installLocalControlIntegrityKeyring(
	currentKey []byte,
	previousKeys [][]byte,
	owners []string,
) error {
	if len(previousKeys) > maxLocalControlPreviousIntegrityKeys {
		return fmt.Errorf(
			"at most %d previous local control integrity keys are supported",
			maxLocalControlPreviousIntegrityKeys,
		)
	}
	current, err := newLocalControlIntegrityKey(currentKey)
	if err != nil {
		return err
	}
	previous := make(map[string]localControlIntegrityKey, len(previousKeys))
	for _, raw := range previousKeys {
		key, err := newLocalControlIntegrityKey(raw)
		if err != nil {
			return err
		}
		if key.ID == current.ID || subtle.ConstantTimeCompare(key.Key, current.Key) == 1 {
			return errors.New("current local control integrity key cannot also be previous")
		}
		if existing, duplicate := previous[key.ID]; duplicate {
			if subtle.ConstantTimeCompare(existing.Key, key.Key) == 1 {
				return errors.New("duplicate previous local control integrity key")
			}
			return errors.New("ambiguous local control integrity key identifier")
		}
		previous[key.ID] = key
	}

	g.localControlIntegrityMu.Lock()
	defer g.localControlIntegrityMu.Unlock()
	if g.localControlIntegrityCurrent.ID == "" {
		g.localControlIntegrityCurrent = current
		g.localControlIntegrityPrevious = previous
	} else if !localControlIntegrityKeyringsEqual(
		g.localControlIntegrityCurrent,
		g.localControlIntegrityPrevious,
		current,
		previous,
	) {
		return errors.New("a different local control integrity keyring is already installed")
	}
	if g.localControlOwners == nil {
		g.localControlOwners = make(map[string]struct{}, len(owners))
	}
	for _, personalityAgentID := range owners {
		g.localControlOwners[personalityAgentID] = struct{}{}
	}
	return nil
}

func (g *DurableGateway) addLocalControlOwner(personalityAgentID string) (bool, error) {
	if err := ValidatePersonalityAgentID(personalityAgentID); err != nil {
		return false, err
	}
	g.localControlIntegrityMu.Lock()
	defer g.localControlIntegrityMu.Unlock()
	if g.localControlIntegrityCurrent.ID == "" {
		return false, errors.New("local control integrity keyring is not installed")
	}
	if g.localControlOwners == nil {
		g.localControlOwners = make(map[string]struct{})
	}
	if _, exists := g.localControlOwners[personalityAgentID]; exists {
		return false, nil
	}
	g.localControlOwners[personalityAgentID] = struct{}{}
	return true, nil
}

func (g *DurableGateway) removeLocalControlOwner(personalityAgentID string) {
	g.localControlIntegrityMu.Lock()
	defer g.localControlIntegrityMu.Unlock()
	delete(g.localControlOwners, personalityAgentID)
}

func (g *DurableGateway) localControlIntegrityKeyringSnapshot() (localControlIntegrityKeyring, bool) {
	g.localControlIntegrityMu.RLock()
	defer g.localControlIntegrityMu.RUnlock()
	if g.localControlIntegrityCurrent.ID == "" {
		return localControlIntegrityKeyring{}, false
	}
	ring := localControlIntegrityKeyring{
		Current: localControlIntegrityKey{
			ID:  g.localControlIntegrityCurrent.ID,
			Key: append([]byte(nil), g.localControlIntegrityCurrent.Key...),
		},
		Previous: make(map[string]localControlIntegrityKey, len(g.localControlIntegrityPrevious)),
	}
	for id, key := range g.localControlIntegrityPrevious {
		ring.Previous[id] = localControlIntegrityKey{
			ID:  key.ID,
			Key: append([]byte(nil), key.Key...),
		}
	}
	return ring, true
}

func (g *DurableGateway) localControlOwns(personalityAgentID string) bool {
	g.localControlIntegrityMu.RLock()
	defer g.localControlIntegrityMu.RUnlock()
	_, owns := g.localControlOwners[personalityAgentID]
	return owns
}

func localControlStateMAC(key []byte, state runtimeState) ([]byte, error) {
	if state.LocalControl == nil {
		return nil, errors.New("local control state is required")
	}
	unsignedControl := *state.LocalControl
	unsignedControl.Integrity = nil
	unsignedState := state
	unsignedState.LocalControl = &unsignedControl
	raw, err := json.Marshal(unsignedState)
	if err != nil {
		return nil, fmt.Errorf("encode local control state for integrity: %w", err)
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte("sumi-local-control-runtime-state/v1"))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write(raw)
	return mac.Sum(nil), nil
}

func (g *DurableGateway) signLocalControlRuntimeState(state *runtimeState) error {
	keyring, ok := g.localControlIntegrityKeyringSnapshot()
	if !ok {
		return errors.New("local control integrity key is not installed")
	}
	mac, err := localControlStateMAC(keyring.Current.Key, *state)
	if err != nil {
		return err
	}
	state.LocalControl.Integrity = &localControlStateIntegrity{
		Version: localControlIntegrityVersion,
		KeyID:   keyring.Current.ID,
		MAC:     hex.EncodeToString(mac),
	}
	return nil
}

func verifyLocalControlIntegrity(
	keyring localControlIntegrityKeyring,
	integrity *localControlStateIntegrity,
	macForKey func([]byte) ([]byte, error),
) (bool, error) {
	if integrity == nil ||
		integrity.Version != localControlIntegrityVersion ||
		len(integrity.MAC) != sha256.Size*2 {
		return false, errors.New("invalid local control state integrity")
	}
	actual, err := hex.DecodeString(integrity.MAC)
	if err != nil || len(actual) != sha256.Size {
		return false, errors.New("invalid local control state integrity")
	}
	if len(integrity.KeyID) != 32 {
		return false, errors.New("invalid local control integrity key identifier")
	}
	if _, err := hex.DecodeString(integrity.KeyID); err != nil {
		return false, errors.New("invalid local control integrity key identifier")
	}
	var key localControlIntegrityKey
	switch {
	case integrity.KeyID == keyring.Current.ID:
		key = keyring.Current
	default:
		var ok bool
		key, ok = keyring.Previous[integrity.KeyID]
		if !ok {
			return false, errors.New("unknown local control integrity key identifier")
		}
	}
	expected, err := macForKey(key.Key)
	if err != nil {
		return false, err
	}
	if subtle.ConstantTimeCompare(actual, expected) != 1 {
		return false, errors.New("invalid local control state integrity")
	}
	return key.ID != keyring.Current.ID, nil
}

func (g *DurableGateway) verifyLocalControlRuntimeStateIntegrity(state runtimeState) (bool, error) {
	if state.LocalControl == nil {
		return false, nil
	}
	keyring, ok := g.localControlIntegrityKeyringSnapshot()
	if !ok {
		return false, errors.New("local control integrity key is not installed")
	}
	needsResign, err := verifyLocalControlIntegrity(
		keyring,
		state.LocalControl.Integrity,
		func(key []byte) ([]byte, error) {
			return localControlStateMAC(key, state)
		},
	)
	if err != nil {
		return false, err
	}
	return needsResign, nil
}

func (g *DurableGateway) localControlLockPath(personalityAgentID string) string {
	return filepath.Join(g.dir, "runtime-"+safeFileID(personalityAgentID)+".lock")
}

func signAgentToken(secret []byte, claims tokenClaims) (string, error) {
	if len(secret) < 32 {
		return "", errors.New("token HMAC secret must be at least 32 bytes")
	}
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("marshal token claims: %w", err)
	}
	claimsPart := base64.RawURLEncoding.EncodeToString(claimsJSON)
	signingInput := header + "." + claimsPart
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(signingInput))
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func writeLocalControlOperationError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errLocalControlConflict),
		errors.Is(err, errLocalControlStaleEpoch),
		errors.Is(err, errLocalControlCAS),
		errors.Is(err, errLocalControlInvalidState),
		errors.Is(err, errLocalControlUninitialized):
		writeLocalControlError(w, http.StatusConflict, "conflict")
	case errors.Is(err, errLocalControlCapacity):
		writeLocalControlError(w, http.StatusInsufficientStorage, "capacity_exhausted")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		writeLocalControlError(w, http.StatusRequestTimeout, "request_cancelled")
	default:
		writeLocalControlError(w, http.StatusInternalServerError, "internal_error")
	}
}

func writeLocalControlError(w http.ResponseWriter, status int, code string) {
	writeLocalControlJSON(w, status, struct {
		Error string `json:"error"`
	}{Error: code})
}

func writeLocalControlJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
