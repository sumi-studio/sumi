package runtimeprovision

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Explicit opt-in; creates only a synthetic workspace volume and labelled
// operation containers. Uses an already present pinned image, never pulls/builds.
func TestProcessDockerIntegration(t *testing.T) {
	if os.Getenv("SUMI_TEST_PROCESS_DOCKER") != "1" {
		t.Skip("requires explicit owned Docker integration opt-in")
	}
	revision := os.Getenv("SUMI_TEST_PROCESS_IMAGE_TAG")
	if !processImageTag.MatchString(revision) {
		t.Fatal("SUMI_TEST_PROCESS_IMAGE_TAG must explicitly name an existing full 40-character image revision")
	}
	ctx := context.Background()
	paid := uuid.NewString()
	project := "sumi-" + strings.ReplaceAll(paid, "-", "")
	volume := project + "_workspace"
	image := "ghcr.io/sumi-studio/sumi-agent:" + revision
	docker := func(args ...string) []byte {
		t.Helper()
		cmd := exec.Command("docker", args...)
		raw, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("docker %s: %v %s", args[0], err, raw)
		}
		return raw
	}
	docker("image", "inspect", image)
	docker("volume", "create", "--label", "com.docker.compose.project="+project, "--label", "com.docker.compose.volume=workspace", "--label", "sumi.process_test="+paid, volume)
	operations := []ProcessOperation{}
	t.Cleanup(func() {
		for _, o := range operations {
			_ = exec.Command("docker", "rm", "-f", processContainer(o)).Run()
		}
		if raw, err := exec.Command("docker", "volume", "rm", volume).CombinedOutput(); err != nil {
			t.Errorf("owned volume cleanup: %v %s", err, raw)
		}
	})
	docker("run", "--rm", "--network", "none", "--user", "0:0", "--mount", "type=volume,src="+volume+",dst=/workspace", "--entrypoint", "/bin/chown", image, "10002:10002", "/workspace")
	environment := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "SUMI_AGENT_IMAGE_TAG=" + revision}
	for _, key := range []string{"DOCKER_HOST", "DOCKER_CONFIG"} {
		if v := os.Getenv(key); v != "" {
			environment = append(environment, key+"="+v)
		}
	}
	backend := &DockerBackend{baseEnvironment: environment}
	directory := t.TempDir() + "/state"
	createService := func() *Service {
		t.Helper()
		s, err := NewService(backend, ServiceConfig{StateDirectory: directory})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	service := createService()
	start := func(call, script string) ProcessOperation {
		t.Helper()
		o, err := service.StartProcess(ctx, ProcessStartRequest{PersonalityAgentID: paid, OriginatingToolCallID: call, Executable: "/bin/sh", Args: []string{"-c", script}, TimeoutSeconds: 30})
		if err != nil {
			t.Fatal(err)
		}
		operations = append(operations, o)
		return o
	}
	wait := func(o ProcessOperation) ProcessOperation {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			service.observeProcesses(ctx)
			current, err := service.ProcessStatus(ctx, ProcessLookupRequest{paid, o.OperationID})
			if err != nil {
				t.Fatal(err)
			}
			if current.State.terminal() {
				return current
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatal("process did not finish")
		return ProcessOperation{}
	}
	secretPath := t.TempDir() + "/control-secret"
	if err := os.WriteFile(secretPath, []byte("synthetic provisioner-only secret"), 0600); err != nil {
		t.Fatal(err)
	}
	script := "set -e; test $(id -u) = 10002; test ! -S /var/run/docker.sock; test ! -S /run/sumi/runtime-provisioner/control.sock; test ! -S /ipc/executor.sock; test ! -r " + secretPath + "; if touch /sumi-process-root-write-check 2>/dev/null; then exit 90; fi; test $(id -u) = 10002 && printf x >> /workspace/count && printf artifact > /workspace/artifact && sleep 3 && printf finished"
	first := start("restart", script)
	service.observeProcesses(ctx)
	raw := docker("inspect", processContainer(first))
	var containers []struct {
		Config struct {
			User string
			Env  []string
		}
		HostConfig struct {
			ReadonlyRootfs       bool
			NetworkMode          string
			CapDrop, SecurityOpt []string
			Memory, NanoCpus     int64
			PidsLimit            int64
			Tmpfs                map[string]string
		}
		Mounts []struct{ Type, Name, Destination string }
	}
	if err := json.Unmarshal(raw, &containers); err != nil {
		t.Fatal(err)
	}
	c := containers[0]
	if c.Config.User != "10002:10002" || !c.HostConfig.ReadonlyRootfs || c.HostConfig.NetworkMode != "none" || c.HostConfig.Memory != 384<<20 || c.HostConfig.NanoCpus != 1e9 || c.HostConfig.PidsLimit != 128 || len(c.Mounts) != 1 || c.Mounts[0].Name != volume || c.Mounts[0].Destination != "/workspace" || !strings.Contains(strings.Join(c.HostConfig.CapDrop, ","), "ALL") || !strings.Contains(strings.Join(c.HostConfig.SecurityOpt, ","), "no-new-privileges") || !strings.Contains(c.HostConfig.Tmpfs["/tmp"], "33554432") {
		t.Fatalf("sandbox mismatch: %+v", c)
	}
	for _, v := range c.Config.Env {
		for _, forbidden := range []string{"PROVIDER", "SECRET", "TOKEN", "DOCKER_HOST", "WRAPPING"} {
			if strings.Contains(v, forbidden) {
				t.Fatalf("unexpected worker environment %q", v)
			}
		}
	}
	service = createService()
	done := wait(first)
	if done.State != ProcessSucceeded {
		t.Fatal(done)
	}
	// Removal runs only after durable terminal publication; restart then retry
	// must return the original operation even though its container no longer exists.
	service.observeProcesses(ctx)
	service = createService()
	again, err := service.StartProcess(ctx, ProcessStartRequest{PersonalityAgentID: paid, OriginatingToolCallID: "restart", Executable: "/bin/sh", Args: []string{"-c", script}, TimeoutSeconds: 30})
	if err != nil || again.OperationID != first.OperationID || again.State != ProcessSucceeded {
		t.Fatal(again, err)
	}
	output, err := service.ReadProcessOutput(ctx, ProcessOutputRequest{ProcessLookupRequest: ProcessLookupRequest{paid, first.OperationID}, Stream: "stdout"})
	if err != nil || output.Content != "finished" {
		t.Fatal(output, err)
	}
	check := start("verify-artifact", "test $(cat /workspace/count) = x && test $(cat /workspace/artifact) = artifact")
	if done = wait(check); done.State != ProcessSucceeded {
		t.Fatal(done)
	}
	cancel := start("cancel", "sleep 30")
	service.observeProcesses(ctx)
	if _, err = service.CancelProcess(ctx, ProcessLookupRequest{paid, cancel.OperationID}); err != nil {
		t.Fatal(err)
	}
	if done = wait(cancel); done.State != ProcessCancelled {
		t.Fatal(done)
	}
	flood := start("output-cap", `head -c 1500000 /dev/zero | tr '\000' x`)
	done = wait(flood)
	if done.State != ProcessSucceeded || !done.StdoutTruncated || done.StdoutBytes != processOutputLimit {
		t.Fatal(done)
	}
	t.Logf("verified synthetic PA %s: sandbox, artifact, restart/no replay, cancellation, bounded output", paid)
}
