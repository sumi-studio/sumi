package main

import (
	"os"
	"testing"
)

// The interactive journal tailer reads the daemon's data root wherever
// the deployer mounted it; the override must reach DockerBackend
// through the real command's environment passthrough, not only through
// a direct backend construction.
func TestHostEnvironmentPassesJournalRoot(t *testing.T) {
	t.Setenv("SUMI_DOCKER_JOURNAL_ROOT", "/run/sumi/docker-root")
	found := false
	for _, v := range hostEnvironment() {
		if v == "SUMI_DOCKER_JOURNAL_ROOT=/run/sumi/docker-root" {
			found = true
		}
	}
	if !found {
		t.Fatal("SUMI_DOCKER_JOURNAL_ROOT must reach the backend environment")
	}
	if err := os.Unsetenv("SUMI_DOCKER_JOURNAL_ROOT"); err != nil {
		t.Fatal(err)
	}
	for _, v := range hostEnvironment() {
		if v == "SUMI_DOCKER_JOURNAL_ROOT=/run/sumi/docker-root" {
			t.Fatal("unset journal root must not be forwarded")
		}
	}
}
