package runtimeprovision

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

func (client *Client) Prepare(ctx context.Context, request PrepareRequest) (PreparedEpoch, error) {
	var response PreparedEpoch
	if err := request.Validate(); err != nil {
		return PreparedEpoch{}, err
	}
	if err := client.call(ctx, "/v1/prepare", request, &response); err != nil {
		return PreparedEpoch{}, err
	}
	if err := response.Validate(); err != nil {
		return PreparedEpoch{}, fmt.Errorf("provisioner returned invalid prepared epoch: %w", err)
	}
	if response.PersonalityAgentID != request.PersonalityAgentID {
		return PreparedEpoch{}, fmt.Errorf("provisioner prepared a different personality agent")
	}
	return response, nil
}

func (client *Client) Activate(ctx context.Context, request ActivateRequest) (Inspection, error) {
	if err := request.Validate(); err != nil {
		return Inspection{}, err
	}
	return client.operation(ctx, "/v1/activate", request, request.PersonalityAgentID)
}

func (client *Client) Abort(ctx context.Context, request AbortRequest) (Inspection, error) {
	if err := request.Validate(); err != nil {
		return Inspection{}, err
	}
	return client.operation(ctx, "/v1/abort", request, request.PersonalityAgentID)
}

func (client *Client) Inspect(ctx context.Context, request InspectRequest) (Inspection, error) {
	if err := request.Validate(); err != nil {
		return Inspection{}, err
	}
	return client.operation(ctx, "/v1/inspect", request, request.PersonalityAgentID)
}

func (client *Client) Stop(ctx context.Context, request StopRequest) (Inspection, error) {
	if err := request.Validate(); err != nil {
		return Inspection{}, err
	}
	return client.operation(ctx, "/v1/stop", request, request.PersonalityAgentID)
}

func (client *Client) Reconcile(ctx context.Context, request ReconcileRequest) (Inspection, error) {
	if err := request.Validate(); err != nil {
		return Inspection{}, err
	}
	return client.operation(ctx, "/v1/reconcile", request, request.PersonalityAgentID)
}

func (client *Client) operation(ctx context.Context, path string, request any, expectedPersonalityAgentID string) (Inspection, error) {
	var response OperationResponse
	if err := client.call(ctx, path, request, &response); err != nil {
		return Inspection{}, err
	}
	if err := response.Inspection.Validate(); err != nil {
		return Inspection{}, fmt.Errorf("provisioner returned invalid inspection: %w", err)
	}
	if response.Inspection.PersonalityAgentID != expectedPersonalityAgentID {
		return Inspection{}, errors.New("provisioner returned a different personality agent")
	}
	return response.Inspection, nil
}

func (client *Client) call(ctx context.Context, path string, input, output any) error {
	body, err := json.Marshal(input)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://runtime-provisioner"+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		limited, _ := io.ReadAll(io.LimitReader(response.Body, 16<<10))
		var protocolError errorResponse
		if json.Unmarshal(limited, &protocolError) == nil && protocolError.Message != "" {
			return fmt.Errorf("provisioner %s: %s", protocolError.Code, protocolError.Message)
		}
		return fmt.Errorf("provisioner returned HTTP %d", response.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxRequestBytes))
	decoder.DisallowUnknownFields()
	return decoder.Decode(output)
}
