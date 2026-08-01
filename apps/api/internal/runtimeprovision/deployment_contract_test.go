package runtimeprovision

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestDeploymentPrepareGraphCannotStartLongLivedRoles(t *testing.T) {
	prepare := readDeploymentFile(t, "compose.prepare.yaml")
	for _, service := range []string{"runtime", "executor", "broker"} {
		pattern := regexp.MustCompile(`(?m)^  ` + service + `:$`)
		if pattern.MatchString(prepare) {
			t.Fatalf("prepare graph contains long-lived %s service", service)
		}
	}
	if !regexp.MustCompile(`(?m)^  allocator:$`).MatchString(prepare) || !regexp.MustCompile(`(?m)^  prepare:$`).MatchString(prepare) {
		t.Fatal("prepare graph omits allocator or filesystem prepare role")
	}
}

func TestDeploymentActivateCannotRerunAllocatorOrExposeDockerSocket(t *testing.T) {
	supervisor := readDeploymentFile(t, "supervisor")
	activateStart := strings.Index(supervisor, "  activate)")
	abortStart := strings.Index(supervisor, "  abort)")
	if activateStart < 0 || abortStart <= activateStart {
		t.Fatal("supervisor has no explicit activate phase")
	}
	activate := supervisor[activateStart:abortStart]
	if !strings.Contains(activate, "--no-deps") || !strings.Contains(activate, "executor broker runtime") {
		t.Fatalf("activate phase can reach a one-shot allocator dependency:\n%s", activate)
	}
	for _, file := range []string{"compose.yaml", "compose.prepare.yaml", "compose.lifecycle.yaml"} {
		if strings.Contains(readDeploymentFile(t, file), "docker.sock") {
			t.Fatalf("%s exposes the Docker socket to a container", file)
		}
	}
	stopStart := strings.Index(supervisor, "  stop|down)")
	statusStart := strings.Index(supervisor, "  status|ps)")
	if stopStart < 0 || statusStart <= stopStart || strings.Contains(supervisor[stopStart:statusStart], "--volumes") {
		t.Fatal("ordinary stop can remove PAID-private volumes")
	}
}

func readDeploymentFile(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join("..", "..", "..", "..", "deploy", "agent", name)
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(contents)
}
