package publicweb

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

type Server struct{ Fetcher *Fetcher }

func (s *Server) RegisterLocalControlRoutes(control *agentevents.LocalControlServer) error {
	if s == nil || s.Fetcher == nil || control == nil {
		return errors.New("public URL reader unavailable")
	}
	return control.RegisterStagedAuthorizedRoute("POST /public-web/read", s.handle)
}
func (s *Server) handle(w http.ResponseWriter, r *http.Request, _ agentevents.LocalRuntimeAuthorization, release func(), admit agentevents.LocalAuthorizationAdmission) {
	release()
	var request Request
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeFailure(w, fail("invalid_request"))
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		writeFailure(w, fail("invalid_request"))
		return
	}
	check := func() bool { ok, err := admit(func() error { return nil }); return ok && err == nil }
	result, err := s.Fetcher.Read(r.Context(), request, check)
	if !check() {
		err = fail("runtime_epoch_changed")
	}
	if err != nil {
		var failure *Failure
		if !errors.As(err, &failure) {
			failure = fail("fetch_failed")
		}
		copy := *failure
		if len(request.URL) <= MaxURLBytes {
			copy.RequestedURL = request.URL
		}
		writeFailure(w, &copy)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}
func writeFailure(w http.ResponseWriter, failure *Failure) {
	status := http.StatusBadGateway
	switch failure.Code {
	case "invalid_url", "invalid_request":
		status = 400
	case "destination_not_public":
		status = 403
	case "runtime_epoch_changed":
		status = 409
	case "busy":
		status = 429
	case "timeout":
		status = 504
	case "cancelled":
		status = 408
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(failure)
}
