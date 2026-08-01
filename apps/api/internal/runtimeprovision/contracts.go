// Package runtimeprovision defines the privileged host provisioner boundary.
//
// The API and agent roles speak this typed protocol over a root-managed Unix
// socket. Only a Backend implementation behind the daemon may reach a machine
// runtime such as Docker or, in the future, Firecracker.
package runtimeprovision

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const (
	ProtocolVersion      = 1
	MaxProcessGeneration = uint64(1<<63 - 1)
	MaxOpaqueBytes       = 256
)

var canonicalPAID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// Phase is the host-observed lifecycle of one process generation.
type Phase string

const (
	PhaseUnknown  Phase = "unknown"
	PhasePrepared Phase = "prepared"
	PhaseActive   Phase = "active"
)

// PrepareRequest asks the privileged backend to allocate exactly one new
// process generation without starting runtime, executor, or broker.
type PrepareRequest struct {
	Version            int    `json:"version"`
	PersonalityAgentID string `json:"personality_agent_id"`
	IdempotencyKey     string `json:"idempotency_key"`
}

// PreparedEpoch is the complete authority returned by a successful prepare.
// Callers must treat OpaquePreparedHandle as opaque and return it unchanged.
type PreparedEpoch struct {
	PersonalityAgentID   string `json:"personality_agent_id"`
	Generation           uint64 `json:"generation"`
	RPCBootNonce         string `json:"rpc_boot_nonce"`
	OpaquePreparedHandle string `json:"opaque_prepared_handle"`
}

type ActivateRequest struct {
	Version int `json:"version"`
	PreparedEpoch
	// Activation contains runtime credentials and endpoints. It is never
	// persisted by Service and is not required during Prepare.
	Activation ActivationConfig `json:"activation"`
}

// ActivationConfig is backend-neutral runtime boot configuration. Keeping it
// typed prevents an API caller from injecting the root daemon's environment.
type ActivationConfig struct {
	GatewayURL                      string `json:"gateway_url"`
	LocalControlBearer              string `json:"local_control_bearer"`
	LocalControlBearerExpiresAtUnix int64  `json:"local_control_bearer_expires_at_unix"`
	LocalControlServerUID           uint32 `json:"local_control_server_uid"`
	LocalControlSocketGID           uint32 `json:"local_control_socket_gid"`
	AgentWrappingKey                string `json:"agent_wrapping_key"`
	AgentWrappingKeyID              string `json:"agent_wrapping_key_id"`
	ApprovalSecretDigestKey         string `json:"approval_secret_digest_key"`
	ProviderAPIKey                  string `json:"provider_api_key"`
	ModelPreset                     string `json:"model_preset,omitempty"`
	ModelID                         string `json:"model_id,omitempty"`
	AllowInsecureLoopbackGateway    bool   `json:"allow_insecure_loopback_gateway,omitempty"`
	LogFilter                       string `json:"log_filter,omitempty"`
}

type AbortRequest struct {
	Version int `json:"version"`
	PreparedEpoch
}

type InspectRequest struct {
	Version            int    `json:"version"`
	PersonalityAgentID string `json:"personality_agent_id"`
}

type StopRequest struct {
	Version            int    `json:"version"`
	PersonalityAgentID string `json:"personality_agent_id"`
}

type ReconcileRequest struct {
	Version            int    `json:"version"`
	PersonalityAgentID string `json:"personality_agent_id"`
}

type Inspection struct {
	PersonalityAgentID string         `json:"personality_agent_id"`
	Phase              Phase          `json:"phase"`
	Epoch              *PreparedEpoch `json:"epoch,omitempty"`
}

type OperationResponse struct {
	Inspection Inspection `json:"inspection"`
}

// Namespace is the only stable host naming input exposed by this package.
// Human, tenant, Workspace, organization, and browser-session identities are
// deliberately absent.
type Namespace struct {
	Project      string `json:"project"`
	VolumePrefix string `json:"volume_prefix"`
	IPCPrefix    string `json:"ipc_prefix"`
}

func NamespaceFor(personalityAgentID string) (Namespace, error) {
	if err := ValidatePersonalityAgentID(personalityAgentID); err != nil {
		return Namespace{}, err
	}
	compact := ""
	for _, char := range personalityAgentID {
		if char != '-' {
			compact += string(char)
		}
	}
	project := "sumi-" + compact
	return Namespace{
		Project:      project,
		VolumePrefix: project + "_",
		IPCPrefix:    project + "_",
	}, nil
}

func ValidatePersonalityAgentID(value string) error {
	if !canonicalPAID.MatchString(value) {
		return errors.New("personality_agent_id must be a canonical lowercase UUIDv7")
	}
	return nil
}

func validateVersion(version int) error {
	if version != ProtocolVersion {
		return fmt.Errorf("unsupported protocol version %d", version)
	}
	return nil
}

func validateOpaque(name, value string) error {
	if len(value) == 0 || len(value) > MaxOpaqueBytes {
		return fmt.Errorf("%s must contain 1..=%d bytes", name, MaxOpaqueBytes)
	}
	return nil
}

func (request PrepareRequest) Validate() error {
	if err := validateVersion(request.Version); err != nil {
		return err
	}
	if err := ValidatePersonalityAgentID(request.PersonalityAgentID); err != nil {
		return err
	}
	return validateOpaque("idempotency_key", request.IdempotencyKey)
}

func (epoch PreparedEpoch) Validate() error {
	if err := ValidatePersonalityAgentID(epoch.PersonalityAgentID); err != nil {
		return err
	}
	if epoch.Generation > MaxProcessGeneration {
		return errors.New("generation is outside the process-generation domain")
	}
	if err := validateOpaque("rpc_boot_nonce", epoch.RPCBootNonce); err != nil {
		return err
	}
	return validateOpaque("opaque_prepared_handle", epoch.OpaquePreparedHandle)
}

func (request ActivateRequest) Validate() error {
	if err := validateVersion(request.Version); err != nil {
		return err
	}
	if err := request.PreparedEpoch.Validate(); err != nil {
		return err
	}
	return request.Activation.Validate()
}

func (config ActivationConfig) Validate() error {
	for name, value := range map[string]string{
		"gateway_url":                config.GatewayURL,
		"local_control_bearer":       config.LocalControlBearer,
		"agent_wrapping_key":         config.AgentWrappingKey,
		"agent_wrapping_key_id":      config.AgentWrappingKeyID,
		"approval_secret_digest_key": config.ApprovalSecretDigestKey,
		"provider_api_key":           config.ProviderAPIKey,
	} {
		if err := validateOpaque(name, value); err != nil {
			return err
		}
		if strings.ContainsAny(value, "\x00\r\n") {
			return fmt.Errorf("%s must not contain NUL or a line ending", name)
		}
	}
	if config.LocalControlBearerExpiresAtUnix <= 0 {
		return errors.New("local_control_bearer_expires_at_unix must be positive")
	}
	if config.LocalControlServerUID == 0 {
		return errors.New("local_control_server_uid must be non-root")
	}
	if config.LocalControlSocketGID == 0 {
		return errors.New("local_control_socket_gid must be nonzero")
	}
	for name, value := range map[string]string{
		"model_preset": config.ModelPreset,
		"model_id":     config.ModelID,
		"log_filter":   config.LogFilter,
	} {
		if len(value) > MaxOpaqueBytes || strings.ContainsAny(value, "\x00\r\n") {
			return fmt.Errorf("%s is not a valid activation value", name)
		}
	}
	return nil
}

func (request AbortRequest) Validate() error {
	if err := validateVersion(request.Version); err != nil {
		return err
	}
	return request.PreparedEpoch.Validate()
}

func (request InspectRequest) Validate() error {
	if err := validateVersion(request.Version); err != nil {
		return err
	}
	return ValidatePersonalityAgentID(request.PersonalityAgentID)
}

func (request StopRequest) Validate() error {
	if err := validateVersion(request.Version); err != nil {
		return err
	}
	return ValidatePersonalityAgentID(request.PersonalityAgentID)
}

func (request ReconcileRequest) Validate() error {
	if err := validateVersion(request.Version); err != nil {
		return err
	}
	return ValidatePersonalityAgentID(request.PersonalityAgentID)
}

func (inspection Inspection) Validate() error {
	if err := ValidatePersonalityAgentID(inspection.PersonalityAgentID); err != nil {
		return err
	}
	switch inspection.Phase {
	case PhaseUnknown:
		if inspection.Epoch != nil {
			return errors.New("unknown inspection must not carry an epoch")
		}
	case PhasePrepared, PhaseActive:
		if inspection.Epoch == nil {
			return errors.New("prepared or active inspection must carry an epoch")
		}
		if err := inspection.Epoch.Validate(); err != nil {
			return err
		}
		if inspection.Epoch.PersonalityAgentID != inspection.PersonalityAgentID {
			return errors.New("inspection epoch personality_agent_id mismatch")
		}
	default:
		return fmt.Errorf("unknown lifecycle phase %q", inspection.Phase)
	}
	return nil
}
