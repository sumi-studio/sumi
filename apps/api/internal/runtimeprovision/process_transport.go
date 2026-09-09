package runtimeprovision

import (
	"context"
	"errors"
	"net/http"
)

func (h *Handler) serveProcess(w http.ResponseWriter, r *http.Request) bool {
	var result any
	var err error
	switch r.URL.Path {
	case "/v1/process/start":
		var input ProcessStartRequest
		if !decodeRequest(w, r, &input) {
			return true
		}
		result, err = h.service.StartProcess(r.Context(), input)
	case "/v1/process/status":
		var input ProcessLookupRequest
		if !decodeRequest(w, r, &input) {
			return true
		}
		result, err = h.service.ProcessStatus(r.Context(), input)
	case "/v1/process/output":
		var input ProcessOutputRequest
		if !decodeRequest(w, r, &input) {
			return true
		}
		result, err = h.service.ReadProcessOutput(r.Context(), input)
	case "/v1/process/cancel":
		var input ProcessLookupRequest
		if !decodeRequest(w, r, &input) {
			return true
		}
		result, err = h.service.CancelProcess(r.Context(), input)
	case "/v1/process/completions":
		var input struct{}
		if !decodeRequest(w, r, &input) {
			return true
		}
		result, err = h.service.PendingProcessCompletions(r.Context())
	case "/v1/process/acknowledge":
		var input ProcessCompletionReceipt
		if !decodeRequest(w, r, &input) {
			return true
		}
		err = h.service.AcknowledgeProcessCompletion(r.Context(), input)
		result = struct{}{}
	default:
		return false
	}
	if err != nil {
		status, code := http.StatusBadGateway, "operation_failed"
		switch {
		case errors.Is(err, ErrInvalidProcessRequest):
			status, code = 400, "invalid_process_request"
		case errors.Is(err, ErrProcessNotFound):
			status, code = 404, "process_not_found"
		case errors.Is(err, ErrProcessBusy):
			status, code = 409, "process_busy"
		case errors.Is(err, ErrConflict):
			status, code = 409, "conflict"
		}
		writeError(w, status, code, err.Error())
	} else {
		writeJSON(w, 200, result)
	}
	return true
}
func (c *Client) StartProcess(ctx context.Context, r ProcessStartRequest) (o ProcessOperation, e error) {
	if e = r.Validate(); e != nil {
		return
	}
	e = c.call(ctx, "/v1/process/start", r, &o)
	return
}
func (c *Client) ProcessStatus(ctx context.Context, r ProcessLookupRequest) (o ProcessOperation, e error) {
	if e = r.Validate(); e != nil {
		return
	}
	e = c.call(ctx, "/v1/process/status", r, &o)
	return
}
func (c *Client) ReadProcessOutput(ctx context.Context, r ProcessOutputRequest) (o ProcessOutput, e error) {
	if e = r.Validate(); e != nil {
		return
	}
	e = c.call(ctx, "/v1/process/output", r, &o)
	return
}
func (c *Client) CancelProcess(ctx context.Context, r ProcessLookupRequest) (o ProcessOperation, e error) {
	if e = r.Validate(); e != nil {
		return
	}
	e = c.call(ctx, "/v1/process/cancel", r, &o)
	return
}
func (c *Client) PendingProcessCompletions(ctx context.Context) (o []ProcessOperation, e error) {
	e = c.call(ctx, "/v1/process/completions", struct{}{}, &o)
	return
}
func (c *Client) AcknowledgeProcessCompletion(ctx context.Context, r ProcessCompletionReceipt) error {
	return c.call(ctx, "/v1/process/acknowledge", r, &struct{}{})
}
