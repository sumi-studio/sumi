package runtimeprovision

import (
	"errors"
	"fmt"
)

var ErrConflict = errors.New("process state conflict")

type Service struct {
	processes *processStore
	files     *durableFilesBindings
	filesEnv  FilesEnvironment
}
type ServiceConfig struct {
	StateDirectory string
	Files          FilesEnvironment
}
type FilesEnvironment struct {
	Mountpoint       string
	VolumeUUID       string
	CheckPath        string
	CheckWaitSeconds int
}

func (e FilesEnvironment) configured() bool {
	return e.Mountpoint != "" && e.VolumeUUID != "" && e.CheckPath != ""
}
func NewService(backend ProcessBackend, config ServiceConfig) (*Service, error) {
	if backend == nil {
		return nil, errors.New("process service requires a backend")
	}
	files, err := newDurableFilesBindings(config.StateDirectory)
	if err != nil {
		return nil, err
	}
	processes, err := newProcessStore(config.StateDirectory, backend)
	if err != nil {
		return nil, err
	}
	s := &Service{processes: processes, files: files, filesEnv: config.Files}
	if s.filesEnv.configured() {
		s.processes.reverify = s.recheckProcessWorkspace
	}
	return s, nil
}
func (service *Service) checkFilesBinding(personalityAgentID string) error {
	binding, bound := service.files.lookup(personalityAgentID)
	if !bound {
		return nil
	}
	if !service.filesEnv.configured() {
		return fmt.Errorf(
			"%w: personality agent is bound to canonical files volume %s and SUMI_FILES_* configuration is absent or incomplete; refusing host-local workspace substitution",
			ErrConflict, binding.VolumeUUID,
		)
	}
	if service.filesEnv.VolumeUUID != binding.VolumeUUID {
		return fmt.Errorf(
			"%w: personality agent is bound to canonical files volume %s; refusing retarget to volume %s",
			ErrConflict, binding.VolumeUUID, service.filesEnv.VolumeUUID,
		)
	}
	return nil
}
