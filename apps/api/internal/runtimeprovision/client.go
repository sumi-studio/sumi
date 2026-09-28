package runtimeprovision

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

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
			switch protocolError.Code {
			case "process_not_found":
				return fmt.Errorf("%w: %s", ErrProcessNotFound, protocolError.Message)
			case "process_busy":
				return fmt.Errorf("%w: %s", ErrProcessBusy, protocolError.Message)
			case "invalid_process_request":
				return fmt.Errorf("%w: %s", ErrInvalidProcessRequest, protocolError.Message)
			case "conflict":
				return fmt.Errorf("%w: %s", ErrConflict, protocolError.Message)
			case "workspace_unavailable":
				return fmt.Errorf("%w: %s", ErrProcessWorkspace, protocolError.Message)
			case "not_interactive":
				return fmt.Errorf("%w: %s", ErrProcessNotInteractive, protocolError.Message)
			case "resize_unsupported":
				return fmt.Errorf("%w: %s", ErrProcessResizeUnsupported, protocolError.Message)
			case "invalid_request", "method_not_allowed", "not_found":
				return fmt.Errorf("%w: %s", ErrInvalidProcessRequest, protocolError.Message)
			}
			return fmt.Errorf("provisioner %s: %s", protocolError.Code, protocolError.Message)
		}
		return fmt.Errorf("provisioner returned HTTP %d", response.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxRequestBytes))
	decoder.DisallowUnknownFields()
	return decoder.Decode(output)
}
