// Package processoperations connects reviewed PA operations to their isolated
// process owner. It exposes no host process or container control to callers.
package processoperations

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
)

type Backend interface {
	StartProcess(context.Context, runtimeprovision.ProcessStartRequest) (runtimeprovision.ProcessOperation, error)
	ProcessStatus(context.Context, runtimeprovision.ProcessLookupRequest) (runtimeprovision.ProcessOperation, error)
	ReadProcessOutput(context.Context, runtimeprovision.ProcessOutputRequest) (runtimeprovision.ProcessOutput, error)
	CancelProcess(context.Context, runtimeprovision.ProcessLookupRequest) (runtimeprovision.ProcessOperation, error)
	PendingProcessCompletions(context.Context) ([]runtimeprovision.ProcessOperation, error)
	AcknowledgeProcessCompletion(context.Context, runtimeprovision.ProcessCompletionReceipt) error
}

type Server struct {
	Backend  Backend
	Delivery Delivery
}

// Request bodies deliberately cannot supply a PA identity. Ownership comes
// from the authenticated local-control transport, including its current epoch.
type startBody struct {
	OriginatingToolCallID string   `json:"originating_tool_call_id"`
	Executable            string   `json:"executable"`
	Args                  []string `json:"args"`
	Cwd                   string   `json:"cwd"`
	TimeoutSeconds        int      `json:"timeout_seconds"`
}

type lookupBody struct {
	OperationID string `json:"operation_id"`
}
type outputBody struct {
	OperationID string `json:"operation_id"`
	Stream      string `json:"stream"`
	Offset      int64  `json:"offset"`
	Limit       int    `json:"limit"`
}

func (s *Server) RegisterLocalControlRoutes(control *agentevents.LocalControlServer) error {
	if s == nil || s.Backend == nil || control == nil {
		return errors.New("process operation backend is unavailable")
	}
	for _, action := range []string{"start", "status", "output", "cancel"} {
		if err := control.RegisterStagedAuthorizedRoute("POST /process-operations/"+action, s.handler(action)); err != nil {
			return err
		}
	}
	return nil
}

func decodeBody(w http.ResponseWriter, r *http.Request, value any) error {
	// A valid 32 KiB argv can expand sixfold when JSON escapes control bytes.
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256*1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("expected one JSON object")
	}
	return nil
}

func (s *Server) handler(action string) agentevents.LocalStagedAuthorizedHandler {
	return func(w http.ResponseWriter, r *http.Request, auth agentevents.LocalRuntimeAuthorization, release func(), admit agentevents.LocalAuthorizationAdmission) {
		release()
		var invoke func() (any, error)
		switch action {
		case "start":
			var body startBody
			if decodeBody(w, r, &body) != nil {
				processError(w, 400, "invalid_request")
				return
			}
			request := runtimeprovision.ProcessStartRequest{PersonalityAgentID: auth.PersonalityAgentID, OriginatingToolCallID: body.OriginatingToolCallID, Executable: body.Executable, Args: body.Args, Cwd: body.Cwd, TimeoutSeconds: body.TimeoutSeconds}
			if request.Validate() != nil {
				processError(w, 400, "invalid_request")
				return
			}
			invoke = func() (any, error) { return s.Backend.StartProcess(r.Context(), request) }
		case "output":
			var body outputBody
			if decodeBody(w, r, &body) != nil {
				processError(w, 400, "invalid_request")
				return
			}
			request := runtimeprovision.ProcessOutputRequest{ProcessLookupRequest: runtimeprovision.ProcessLookupRequest{PersonalityAgentID: auth.PersonalityAgentID, OperationID: body.OperationID}, Stream: body.Stream, Offset: body.Offset, Limit: body.Limit}
			if request.Validate() != nil {
				processError(w, 400, "invalid_request")
				return
			}
			invoke = func() (any, error) { return s.Backend.ReadProcessOutput(r.Context(), request) }
		default:
			var body lookupBody
			if decodeBody(w, r, &body) != nil {
				processError(w, 400, "invalid_request")
				return
			}
			request := runtimeprovision.ProcessLookupRequest{PersonalityAgentID: auth.PersonalityAgentID, OperationID: body.OperationID}
			if request.Validate() != nil {
				processError(w, 400, "invalid_request")
				return
			}
			invoke = func() (any, error) {
				if action == "cancel" {
					return s.Backend.CancelProcess(r.Context(), request)
				}
				return s.Backend.ProcessStatus(r.Context(), request)
			}
		}
		var result any
		admitted, err := admit(func() error { var err error; result, err = invoke(); return err })
		if !admitted {
			processError(w, 409, "runtime_epoch_changed")
			return
		}
		if err != nil {
			switch {
			case errors.Is(err, runtimeprovision.ErrConflict):
				processError(w, 409, "operation_conflict")
			case errors.Is(err, runtimeprovision.ErrProcessNotFound):
				processError(w, 404, "operation_not_found")
			case errors.Is(err, runtimeprovision.ErrProcessBusy):
				processError(w, 429, "operation_capacity")
			default:
				processError(w, 502, "operation_backend_unavailable")
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	}
}

func processError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": code})
}
