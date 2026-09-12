package runtimeprovision

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// RecoverLocalControlRequest recovers only the existing API credential of an
// exact live epoch. It neither starts compute nor changes runtime identity.
type RecoverLocalControlRequest struct {
	Version int `json:"version"`
	PreparedEpoch
}

type RecoveredLocalControl struct {
	PreparedEpoch
	Bearer               string `json:"bearer"`
	SelectionFingerprint string `json:"selection_fingerprint"`
}

// SelectionFingerprint names admitted model configuration without carrying any
// provider, wrapping, or local-control secret. Connection versions distinguish
// user-managed credentials whose public model configuration is unchanged.
func (c ActivationConfig) SelectionFingerprint() string {
	data, _ := json.Marshal([]any{
		c.APIConnectionID, c.APIConnectionVersion, c.APIConnectionHumanID,
		c.ChatGPTConnectionID, c.ModelPreset, c.ModelID, c.ModelBaseURL,
		c.ModelPublicEndpoint, c.ModelReasoningEffort, c.ModelAccountScope,
	})
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func (r RecoverLocalControlRequest) Validate() error {
	return (StopRequest{Version: r.Version, PreparedEpoch: r.PreparedEpoch}).Validate()
}

func (r RecoveredLocalControl) validate(expected PreparedEpoch) error {
	if r.PreparedEpoch != expected {
		return errors.New("recovered local control belongs to a different runtime epoch")
	}
	decoded, err := hex.DecodeString(r.SelectionFingerprint)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != r.SelectionFingerprint {
		return errors.New("runtime admitted configuration is unavailable")
	}
	if len(r.Bearer) < 32 || len(r.Bearer) > 1024 {
		return errors.New("recovered local control credential has invalid length")
	}
	for _, c := range r.Bearer {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return errors.New("recovered local control credential has invalid encoding")
		}
	}
	return nil
}

type localControlRecoveryBackend interface {
	RecoverLocalControl(context.Context, PreparedEpoch) (RecoveredLocalControl, error)
}

func (s *Service) RecoverLocalControl(ctx context.Context, request RecoverLocalControlRequest) (RecoveredLocalControl, error) {
	if err := request.Validate(); err != nil {
		return RecoveredLocalControl{}, err
	}
	entry := s.entry(request.PersonalityAgentID)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	inspection, err := s.backend.Inspect(ctx, request.PersonalityAgentID)
	if err != nil {
		return RecoveredLocalControl{}, errors.New("inspect runtime before local control recovery failed")
	}
	if inspection.Phase != PhaseActive || inspection.Epoch == nil || *inspection.Epoch != request.PreparedEpoch {
		return RecoveredLocalControl{}, fmt.Errorf("%w: local control recovery requires the exact active epoch", ErrConflict)
	}
	backend, ok := s.backend.(localControlRecoveryBackend)
	if !ok {
		return RecoveredLocalControl{}, errors.New("backend does not support local control recovery")
	}
	recovered, err := backend.RecoverLocalControl(ctx, request.PreparedEpoch)
	if err != nil {
		return RecoveredLocalControl{}, errors.New("recover runtime local control failed")
	}
	if err := recovered.validate(request.PreparedEpoch); err != nil {
		return RecoveredLocalControl{}, err
	}
	entry.setInspection(inspection)
	return recovered, nil
}

func (c *Client) RecoverLocalControl(ctx context.Context, request RecoverLocalControlRequest) (RecoveredLocalControl, error) {
	if err := request.Validate(); err != nil {
		return RecoveredLocalControl{}, err
	}
	var response RecoveredLocalControl
	if err := c.call(ctx, "/v1/recover-local-control", request, &response); err != nil {
		return RecoveredLocalControl{}, err
	}
	if err := response.validate(request.PreparedEpoch); err != nil {
		return RecoveredLocalControl{}, err
	}
	return response, nil
}

func (b *DockerBackend) RecoverLocalControl(ctx context.Context, epoch PreparedEpoch) (RecoveredLocalControl, error) {
	output, err := b.run(ctx, "recover-local-control", epoch.PersonalityAgentID, nil, map[string]string{
		"SUMI_EXPECTED_RPC_GENERATION": fmt.Sprint(epoch.Generation),
		"SUMI_EXPECTED_RPC_NONCE":      epoch.RPCBootNonce,
	})
	if err != nil {
		// Recovery output contains a credential. Never propagate subprocess
		// diagnostics or include its response in parse errors.
		return RecoveredLocalControl{}, errors.New("supervisor local control recovery failed")
	}
	defer clear(output)
	var wire struct {
		PersonalityAgentID   string `json:"personality_agent_id"`
		Generation           uint64 `json:"generation"`
		RPCBootNonce         string `json:"rpc_boot_nonce"`
		Bearer               string `json:"bearer"`
		SelectionFingerprint string `json:"selection_fingerprint"`
	}
	decoder := json.NewDecoder(bytes.NewReader(output))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return RecoveredLocalControl{}, errors.New("supervisor returned invalid local control recovery")
	}
	result := RecoveredLocalControl{PreparedEpoch: PreparedEpoch{
		PersonalityAgentID: wire.PersonalityAgentID, Generation: wire.Generation,
		RPCBootNonce:         wire.RPCBootNonce,
		OpaquePreparedHandle: dockerPreparedHandle(wire.PersonalityAgentID, wire.Generation, wire.RPCBootNonce),
	}, Bearer: wire.Bearer, SelectionFingerprint: wire.SelectionFingerprint}
	if err := result.validate(epoch); err != nil {
		return RecoveredLocalControl{}, err
	}
	return result, nil
}
