package workspace

import (
	"encoding/json"
	"net/http"
	"strconv"

	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
)

func (s *Server) serveAppCatalog(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.browserActor(w, r); !ok {
		return
	}
	descriptors, err := s.Apps.Catalog(r.Context())
	if err != nil {
		writeDomainError(w, err)
		return
	}
	wires := make([]appDescriptorWire, len(descriptors))
	for i, descriptor := range descriptors {
		wires[i] = descriptorToWire(descriptor)
	}
	writeJSON(w, http.StatusOK, struct {
		Apps []appDescriptorWire `json:"apps"`
	}{Apps: wires})
}

func (s *Server) serveAppInstallations(w http.ResponseWriter, r *http.Request) {
	actor, _, ok := s.browserActor(w, r)
	if !ok {
		return
	}
	owner, err := appOwnerFromQuery(r)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	installations, err := s.Apps.Installations(r.Context(), owner, actor)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeInstallationList(w, installations)
}

func (s *Server) serveInstallApp(w http.ResponseWriter, r *http.Request) {
	actor, claims, ok := s.browserActor(w, r)
	if !ok {
		return
	}
	var request struct {
		Owner       appOwnerWire    `json:"owner"`
		AppID       string          `json:"app_id"`
		OperationID json.RawMessage `json:"operation_id"`
	}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	owner, err := request.Owner.ref()
	if err != nil || request.AppID == "" {
		writeAPIError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	operationID, operationPresent, operationValid := optionalNonEmptyString(request.OperationID)
	if !operationValid ||
		(owner.Kind == applicationapps.OwnerParticipant && !operationPresent) ||
		(operationPresent && applicationapps.ValidateInstallOperationID(operationID) != nil) {
		writeAPIError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	var installation applicationapps.Installation
	done, err := s.browserMutation(w, r, claims, func() error {
		var installErr error
		if operationPresent {
			installation, installErr = s.Apps.InstallAtOperation(
				r.Context(), owner, actor, request.AppID, operationID,
			)
		} else {
			installation, installErr = s.Apps.Install(
				r.Context(), owner, actor, request.AppID,
			)
		}
		return installErr
	})
	if !done {
		return
	}
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, installationToWire(installation))
}

func (s *Server) serveSetAppEnabled(w http.ResponseWriter, r *http.Request) {
	actor, claims, ok := s.browserActor(w, r)
	if !ok {
		return
	}
	var request struct {
		State                  string          `json:"state"`
		ExpectedAuthorityEpoch json.RawMessage `json:"expected_authority_epoch"`
	}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	if request.State != string(applicationapps.StateEnabled) &&
		request.State != string(applicationapps.StateDisabled) {
		writeAPIError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	var installation applicationapps.Installation
	var expectedAuthorityEpoch *int64
	epoch, epochPresent, epochValid := optionalNonEmptyString(request.ExpectedAuthorityEpoch)
	if !epochValid {
		writeAPIError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	if epochPresent {
		parsed, parseErr := strconv.ParseInt(epoch, 10, 64)
		if parseErr != nil || parsed < 1 || strconv.FormatInt(parsed, 10) != epoch {
			writeAPIError(w, http.StatusBadRequest, "invalid_request")
			return
		}
		expectedAuthorityEpoch = &parsed
	}
	done, err := s.browserMutation(w, r, claims, func() error {
		var stateErr error
		if expectedAuthorityEpoch == nil {
			installation, stateErr = s.Apps.SetEnabledByID(r.Context(),
				r.PathValue("installation_id"), actor,
				request.State == string(applicationapps.StateEnabled))
		} else {
			installation, stateErr = s.Apps.SetEnabledByIDAtEpoch(r.Context(),
				r.PathValue("installation_id"), actor,
				request.State == string(applicationapps.StateEnabled), *expectedAuthorityEpoch)
		}
		return stateErr
	})
	if !done {
		return
	}
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, installationToWire(installation))
}

func (s *Server) serveUninstallApp(w http.ResponseWriter, r *http.Request) {
	actor, claims, ok := s.browserActor(w, r)
	if !ok {
		return
	}
	done, err := s.browserMutation(w, r, claims, func() error {
		return s.Apps.UninstallByID(r.Context(), r.PathValue("installation_id"), actor)
	})
	if !done {
		return
	}
	if err != nil {
		writeDomainError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func optionalNonEmptyString(raw json.RawMessage) (string, bool, bool) {
	if raw == nil {
		return "", false, true
	}
	var value *string
	if err := json.Unmarshal(raw, &value); err != nil || value == nil || *value == "" {
		return "", true, false
	}
	return *value, true, true
}

func writeInstallationList(w http.ResponseWriter, installations []applicationapps.Installation) {
	wires := make([]appInstallationWire, len(installations))
	for i, installation := range installations {
		wires[i] = installationToWire(installation)
	}
	writeJSON(w, http.StatusOK, struct {
		Installations []appInstallationWire `json:"installations"`
	}{Installations: wires})
}
