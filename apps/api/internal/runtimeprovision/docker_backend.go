package runtimeprovision

import (
	"sync"
	"time"
)

// DockerBackend runs isolated jobs and terminals. It does not launch secretary runtimes.
type DockerBackend struct {
	baseEnvironment    []string
	processLogTimeout  time.Duration
	journalRootMu      sync.Mutex
	journalRoot        string
	journalRootRetryAt time.Time
	liveOutputs        sync.Map
}
type DockerBackendConfig struct{ BaseEnvironment []string }

func NewDockerBackend(config DockerBackendConfig) (*DockerBackend, error) {
	return &DockerBackend{baseEnvironment: append([]string(nil), config.BaseEnvironment...)}, nil
}
