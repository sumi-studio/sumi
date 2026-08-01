package runtimeprovision

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

func (client *Client) Prepare(ctx context.Context, request PrepareRequest) (PreparedEpoch, error) {
	var response PreparedEpoch
	err := client.call(ctx, "/v1/prepare", request, &response)
	return response, err
}

func (client *Client) Activate(ctx context.Context, request ActivateRequest) (Inspection, error) {
	return client.operation(ctx, "/v1/activate", request)
}

func (client *Client) Abort(ctx context.Context, request AbortRequest) (Inspection, error) {
	return client.operation(ctx, "/v1/abort", request)
}

func (client *Client) Inspect(ctx context.Context, request InspectRequest) (Inspection, error) {
	return client.operation(ctx, "/v1/inspect", request)
}

func (client *Client) Stop(ctx context.Context, request StopRequest) (Inspection, error) {
	return client.operation(ctx, "/v1/stop", request)
}

func (client *Client) Reconcile(ctx context.Context, request ReconcileRequest) (Inspection, error) {
	return client.operation(ctx, "/v1/reconcile", request)
}

func (client *Client) operation(ctx context.Context, path string, request any) (Inspection, error) {
	var response OperationResponse
	err := client.call(ctx, path, request, &response)
	return response.Inspection, err
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
