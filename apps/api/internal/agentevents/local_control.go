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
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const (
	LocalCredentialIssuePath               = "/local-control/v1/runtime-credentials:issue"
	LocalRuntimeStatePublishPath           = "/local-control/v1/runtime-state:publish"
	defaultLocalCredentialTTL              = 60 * time.Second
	maxLocalControlBodyBytes         int64 = 32 * 1024
	maxLocalControlDurableStateBytes       = 8 * 1024 * 1024
	maxLocalControlRecords                 = 1024
	localControlStateVersion               = 1
	localControlIntegrityVersion           = 1
	maxOpaqueRuntimeIDBytes                = 128
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
	MAC     string `json:"mac"`
}

// LocalControlServer owns the local/CI issuer and state-publication handlers.
// The HMAC signing key remains only in this Go process.
type LocalControlServer struct {
	gateway        *DurableGateway
	signingSecret  []byte
	authorizations []LocalRuntimeAuthorization
	tokenTTL       time.Duration
	now            func() time.Time
}

func NewLocalControlServer(
	gateway *DurableGateway,
	signingSecret []byte,
	authorizations []LocalRuntimeAuthorization,
) (*LocalControlServer, error) {
	if gateway == nil {
		return nil, errors.New("local control durable gateway is required")
	}
	if len(signingSecret) < 32 {
		return nil, errors.New("local control token HMAC secret must be at least 32 bytes")
	}
	if len(authorizations) == 0 {
		return nil, errors.New("at least one local runtime authorization is required")
	}
	if err := validateLocalControlStateDirectory(gateway.dir); err != nil {
		return nil, err
	}

	normalized := make([]LocalRuntimeAuthorization, len(authorizations))
	seenBearer := make(map[string]struct{}, len(authorizations))
	seenEpoch := make(map[string]struct{}, len(authorizations))
	for i, authorization := range authorizations {
		if err := validateLocalRuntimeAuthorization(authorization); err != nil {
			return nil, fmt.Errorf("local runtime authorization %d: %w", i, err)
		}
		if len(signingSecret) == len(authorization.BearerToken) &&
			subtle.ConstantTimeCompare(signingSecret, []byte(authorization.BearerToken)) == 1 {
			return nil, errors.New("local control bearer and token signing secret must be distinct")
		}
		if authorization.Audience == "" {
			authorization.Audience = defaultAgentAudience
		}
		if _, exists := seenBearer[authorization.BearerToken]; exists {
			return nil, errors.New("local runtime bearer tokens must be unique")
		}
		epoch := fmt.Sprintf(
			"%s\x00%d\x00%s",
			authorization.PersonalityAgentID,
			authorization.Generation,
			authorization.RPCBootNonce,
		)
		if _, exists := seenEpoch[epoch]; exists {
			return nil, errors.New("each local runtime epoch must have exactly one authorization")
		}
		seenBearer[authorization.BearerToken] = struct{}{}
		seenEpoch[epoch] = struct{}{}
		normalized[i] = authorization
	}

	integrityKey := deriveLocalControlIntegrityKey(signingSecret)
	owners := make([]string, 0, len(normalized))
	for _, authorization := range normalized {
		owners = append(owners, authorization.PersonalityAgentID)
	}
	if err := gateway.installLocalControlIntegrityKey(integrityKey, owners); err != nil {
		return nil, err
	}
	checkedState := make(map[string]struct{}, len(normalized))
	for _, authorization := range normalized {
		if _, checked := checkedState[authorization.PersonalityAgentID]; checked {
			continue
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
		gateway:        gateway,
		signingSecret:  append([]byte(nil), signingSecret...),
		authorizations: normalized,
		tokenTTL:       defaultLocalCredentialTTL,
		now:            time.Now,
	}, nil
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
	return nil
}

func (s *LocalControlServer) handleCredentialIssue(w http.ResponseWriter, r *http.Request) {
	authorization, ok := s.authorize(w, r)
	if !ok {
		return
	}
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
	authorization, ok := s.authorize(w, r)
	if !ok {
		return
	}
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

func (s *LocalControlServer) authorize(w http.ResponseWriter, r *http.Request) (LocalRuntimeAuthorization, bool) {
	if !requestIsLoopback(r) {
		writeLocalControlError(w, http.StatusForbidden, "loopback_required")
		return LocalRuntimeAuthorization{}, false
	}
	values := r.Header.Values("Authorization")
	if len(values) != 1 {
		writeLocalControlError(w, http.StatusUnauthorized, "invalid_authorization")
		return LocalRuntimeAuthorization{}, false
	}
	token, ok := bearerToken(values[0])
	if !ok {
		writeLocalControlError(w, http.StatusUnauthorized, "invalid_authorization")
		return LocalRuntimeAuthorization{}, false
	}
	for _, authorization := range s.authorizations {
		if len(token) == len(authorization.BearerToken) &&
			subtle.ConstantTimeCompare([]byte(token), []byte(authorization.BearerToken)) == 1 {
			return authorization, true
		}
	}
	writeLocalControlError(w, http.StatusUnauthorized, "invalid_authorization")
	return LocalRuntimeAuthorization{}, false
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
			return false, nil
		}
		if len(control.CredentialRequests) >= maxLocalControlRecords {
			return false, errLocalControlCapacity
		}

		now := s.now()
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
			if state.LocalControl.Revision == math.MaxUint64 {
				return false, errLocalControlCapacity
			}
			state.Generation = publication.Generation
			state.HydrationReceiptIdentity = nil
			state.LocalControl.RPCBootNonce = publication.RPCBootNonce
			state.LocalControl.Revision++
			state.LocalControl.State = LocalRuntimeNotReady
			state.LocalControl.Reason = LocalRuntimeStartup
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
	if control.Version != localControlStateVersion {
		return fmt.Errorf("unsupported local control state version %d", control.Version)
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
	lock, err := openLocalControlLock(g.localControlLockPath(personalityAgentID))
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
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
	if err != nil || !write {
		return err
	}
	if err := g.signLocalControlRuntimeState(&state); err != nil {
		return fmt.Errorf("sign local control runtime state: %w", err)
	}
	if err := validateLocalControlRuntimeState(personalityAgentID, state); err != nil {
		return fmt.Errorf("validate local control runtime state: %w", err)
	}
	raw, err := json.Marshal(state)
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

func (g *DurableGateway) installLocalControlIntegrityKey(key []byte, owners []string) error {
	if len(key) != sha256.Size {
		return errors.New("invalid local control integrity key")
	}
	g.localControlIntegrityMu.Lock()
	defer g.localControlIntegrityMu.Unlock()
	if len(g.localControlIntegrityKey) == 0 {
		g.localControlIntegrityKey = append([]byte(nil), key...)
	} else if subtle.ConstantTimeCompare(g.localControlIntegrityKey, key) != 1 {
		return errors.New("a different local control integrity key is already installed")
	}
	if g.localControlOwners == nil {
		g.localControlOwners = make(map[string]struct{}, len(owners))
	}
	for _, personalityAgentID := range owners {
		g.localControlOwners[personalityAgentID] = struct{}{}
	}
	return nil
}

func (g *DurableGateway) localControlIntegrityKeySnapshot() ([]byte, bool) {
	g.localControlIntegrityMu.RLock()
	defer g.localControlIntegrityMu.RUnlock()
	if len(g.localControlIntegrityKey) == 0 {
		return nil, false
	}
	return append([]byte(nil), g.localControlIntegrityKey...), true
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
	key, ok := g.localControlIntegrityKeySnapshot()
	if !ok {
		return errors.New("local control integrity key is not installed")
	}
	mac, err := localControlStateMAC(key, *state)
	if err != nil {
		return err
	}
	state.LocalControl.Integrity = &localControlStateIntegrity{
		Version: localControlIntegrityVersion,
		MAC:     hex.EncodeToString(mac),
	}
	return nil
}

func (g *DurableGateway) verifyLocalControlRuntimeStateIntegrity(state runtimeState) error {
	if state.LocalControl == nil {
		return nil
	}
	key, ok := g.localControlIntegrityKeySnapshot()
	if !ok {
		return errors.New("local control integrity key is not installed")
	}
	integrity := state.LocalControl.Integrity
	if integrity == nil ||
		integrity.Version != localControlIntegrityVersion ||
		len(integrity.MAC) != sha256.Size*2 {
		return errors.New("invalid local control state integrity")
	}
	actual, err := hex.DecodeString(integrity.MAC)
	if err != nil || len(actual) != sha256.Size {
		return errors.New("invalid local control state integrity")
	}
	expected, err := localControlStateMAC(key, state)
	if err != nil {
		return err
	}
	if subtle.ConstantTimeCompare(actual, expected) != 1 {
		return errors.New("invalid local control state integrity")
	}
	return nil
}

func openLocalControlLock(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open local control registry lock: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("inspect local control registry lock: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		_ = file.Close()
		return nil, errors.New("local control registry lock must be a private regular file")
	}
	return file, nil
}

func validateLocalControlStateDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect local control state directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return errors.New("local control state directory must be a private non-symlink directory")
	}
	return nil
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
