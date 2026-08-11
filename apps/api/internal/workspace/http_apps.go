package workspace

import (
	"net/http"

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
		Owner appOwnerWire `json:"owner"`
		AppID string       `json:"app_id"`
	}
	if !decodeStrictJSON(w, r, &request) {
		return
	}
	owner, err := request.Owner.ref()
	if err != nil || request.AppID == "" {
		writeAPIError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	var installation applicationapps.Installation
	done, err := s.browserMutation(w, r, claims, func() error {
		var installErr error
		installation, installErr = s.Apps.Install(r.Context(), owner, actor, request.AppID)
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
		State string `json:"state"`
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
	done, err := s.browserMutation(w, r, claims, func() error {
		var stateErr error
		installation, stateErr = s.Apps.SetEnabledByID(r.Context(),
			r.PathValue("installation_id"), actor,
			request.State == string(applicationapps.StateEnabled))
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

func writeInstallationList(w http.ResponseWriter, installations []applicationapps.Installation) {
	wires := make([]appInstallationWire, len(installations))
	for i, installation := range installations {
		wires[i] = installationToWire(installation)
	}
	writeJSON(w, http.StatusOK, struct {
		Installations []appInstallationWire `json:"installations"`
	}{Installations: wires})
}
