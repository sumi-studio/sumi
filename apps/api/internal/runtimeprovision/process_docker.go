package runtimeprovision

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const processOutputLimit = 1 << 20

var processImageTag = regexp.MustCompile(`^[0-9a-f]{40}$`)

// Process Docker calls execute only in the root provisioner. Callers provide
// inert argv; all container authority and image selection remain server-owned.
func (b *DockerBackend) processDocker(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Env = b.baseEnvironment
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("process Docker operation failed: %w", err)
	}
	return out.Bytes(), nil
}
func processContainer(o ProcessOperation) string { return "sumi-process-" + o.OperationID }
func (b *DockerBackend) LaunchProcess(ctx context.Context, o ProcessOperation) error {
	tag := ""
	for _, v := range b.baseEnvironment {
		if strings.HasPrefix(v, "SUMI_AGENT_IMAGE_TAG=") {
			tag = strings.TrimPrefix(v, "SUMI_AGENT_IMAGE_TAG=")
		}
	}
	if !processImageTag.MatchString(tag) {
		return errors.New("process image requires a pinned full revision")
	}
	volume := "sumi-" + strings.ReplaceAll(o.PersonalityAgentID, "-", "") + "_workspace"
	raw, err := b.processDocker(ctx, "volume", "inspect", volume)
	if err != nil {
		return err
	}
	var volumes []struct {
		Name   string
		Labels map[string]string
	}
	if err = json.Unmarshal(raw, &volumes); err != nil {
		return err
	}
	project := strings.TrimSuffix(volume, "_workspace")
	if len(volumes) != 1 || volumes[0].Name != volume || volumes[0].Labels["com.docker.compose.project"] != project || volumes[0].Labels["com.docker.compose.volume"] != "workspace" {
		return errors.New("workspace volume ownership mismatch")
	}
	raw, err = b.processDocker(ctx, "image", "inspect", "--format", "{{.Id}}", "ghcr.io/sumi-studio/sumi-agent:"+tag)
	if err != nil {
		return err
	}
	image := strings.TrimSpace(string(raw))
	if !regexp.MustCompile(`^sha256:[0-9a-f]{64}$`).MatchString(image) {
		return errors.New("invalid pinned process image")
	}
	args := []string{"create", "--name", processContainer(o), "--label", "sumi.operation_id=" + o.OperationID, "--label", "sumi.personality_agent_id=" + o.PersonalityAgentID, "--read-only", "--network", "none", "--user", "10002:10002", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true", "--cpus", "1", "--memory", "384m", "--memory-swap", "384m", "--pids-limit", "128", "--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=33554432", "--workdir", path.Join("/workspace", o.Cwd), "--mount", "type=volume,src=" + volume + ",dst=/workspace,volume-nocopy", "--env", "PATH=/usr/local/bin:/usr/bin:/bin", "--env", "HOME=/workspace", "--env", "LANG=C.UTF-8", "--log-driver", "json-file", "--log-opt", "max-size=16m", "--log-opt", "max-file=1", "--entrypoint", "/bin/bash", image, "-c", `printf '%s\n' "$1"; shift; exec "$@"`, "sumi-process", processMarker(o), "/usr/bin/timeout", "--signal=TERM", "--kill-after=2", "--", strconv.Itoa(o.TimeoutSeconds), o.Executable}
	args = append(args, o.Args...)
	if _, err = b.processDocker(ctx, args...); err != nil {
		return err
	}
	_, err = b.processDocker(ctx, "start", processContainer(o))
	return err
}
func processMarker(o ProcessOperation) string { return "SUMI_PROCESS_START_" + o.OperationID }

type cappedProcessOutput struct {
	bytes.Buffer
	truncated bool
}

func (w *cappedProcessOutput) Write(p []byte) (int, error) {
	n := len(p)
	remaining := processOutputLimit + 256 - w.Len()
	if remaining < len(p) {
		w.truncated = true
		if remaining < 0 {
			remaining = 0
		}
		p = p[:remaining]
	}
	_, _ = w.Buffer.Write(p)
	return n, nil
}
func (b *DockerBackend) inspectProcessState(ctx context.Context, o ProcessOperation) (ProcessObservation, error) {
	raw, err := b.processDocker(ctx, "container", "ls", "-a", "--filter", "name=^/"+processContainer(o)+"$", "--format", "{{.ID}}")
	if err != nil {
		return ProcessObservation{}, err
	}
	if strings.TrimSpace(string(raw)) == "" {
		return ProcessObservation{}, nil
	}
	raw, err = b.processDocker(ctx, "inspect", processContainer(o))
	if err != nil {
		return ProcessObservation{}, err
	}
	var objects []struct {
		Config struct{ Labels map[string]string }
		State  struct {
			Running               bool
			ExitCode              int
			StartedAt, FinishedAt time.Time
		}
	}
	if err = json.Unmarshal(raw, &objects); err != nil {
		return ProcessObservation{}, err
	}
	if len(objects) != 1 || objects[0].Config.Labels["sumi.operation_id"] != o.OperationID || objects[0].Config.Labels["sumi.personality_agent_id"] != o.PersonalityAgentID {
		return ProcessObservation{}, errors.New("process container ownership mismatch")
	}
	state := objects[0].State
	return ProcessObservation{Exists: true, Running: state.Running, ExitCode: state.ExitCode, StartedAt: state.StartedAt, FinishedAt: state.FinishedAt}, nil
}
func (b *DockerBackend) InspectProcess(ctx context.Context, o ProcessOperation) (ProcessObservation, error) {
	observation, err := b.inspectProcessState(ctx, o)
	if err != nil || !observation.Exists {
		return observation, err
	}
	logTimeout := b.processLogTimeout
	if logTimeout <= 0 {
		logTimeout = 5 * time.Second
	}
	logContext, cancelLogs := context.WithTimeout(ctx, logTimeout)
	defer cancelLogs()
	cmd := exec.CommandContext(logContext, "docker", "logs", processContainer(o))
	cmd.WaitDelay = time.Second
	cmd.Env = b.baseEnvironment
	var stdout, stderr cappedProcessOutput
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err = cmd.Run(); err != nil {
		observation.OutputIncomplete = true
		observation.StdoutTruncated = true
		observation.StderrTruncated = true
		return observation, nil
	}
	out := stdout.Bytes()
	marker := []byte(processMarker(o) + "\n")
	missing := !bytes.HasPrefix(out, marker)
	if !missing {
		out = out[len(marker):]
	}
	outTruncated := stdout.truncated || missing
	errTruncated := stderr.truncated || missing
	if len(out) > processOutputLimit {
		out = out[:processOutputLimit]
		outTruncated = true
	}
	errout := stderr.Bytes()
	if len(errout) > processOutputLimit {
		errout = errout[:processOutputLimit]
		errTruncated = true
	}
	observation.OutputIncomplete = missing
	observation.Stdout = out
	observation.Stderr = errout
	observation.StdoutTruncated = outTruncated
	observation.StderrTruncated = errTruncated
	return observation, nil
}
func (b *DockerBackend) StopProcess(ctx context.Context, o ProcessOperation) error {
	observation, err := b.inspectProcessState(ctx, o)
	if err != nil {
		return err
	}
	if !observation.Exists || !observation.Running {
		return nil
	}
	_, err = b.processDocker(ctx, "kill", processContainer(o))
	return err
}

// Remove only the exact stopped operation container, after durable terminal
// output commit. No volume removal and no force: a running process is retained.
func (b *DockerBackend) RemoveProcess(ctx context.Context, o ProcessOperation) error {
	observation, err := b.inspectProcessState(ctx, o)
	if err != nil {
		return err
	}
	if !observation.Exists {
		return nil
	}
	if observation.Running {
		return errors.New("cannot remove a running process container")
	}
	_, err = b.processDocker(ctx, "rm", processContainer(o))
	return err
}
