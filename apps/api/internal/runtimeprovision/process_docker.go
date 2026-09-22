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
	"sort"
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

// LaunchProcess creates the detached container that observe() then polls.
// Identity is labelled, not named, so an interrupted launch leaves no
// colliding container name behind.
//
// An operation resolved onto a canonical files scope (WorkspaceBind set by
// the service after the mount/volume/binding checks) gets a bind mount of
// that verified path plus the volume-UUID label — never the legacy shared
// workspace volume. Operations without a resolved bind keep the established
// per-agent named-volume workspace with its ownership check.
func (b *DockerBackend) LaunchProcess(ctx context.Context, o ProcessOperation) error {
	repo, tagEnv := "ghcr.io/sumi-studio/sumi-agent", "SUMI_AGENT_IMAGE_TAG"
	if o.Image == "job" {
		repo, tagEnv = "ghcr.io/sumi-studio/sumi-job", "SUMI_JOB_IMAGE_TAG"
	}
	tag := ""
	for _, v := range b.baseEnvironment {
		if strings.HasPrefix(v, tagEnv+"=") {
			tag = strings.TrimPrefix(v, tagEnv+"=")
		}
	}
	if !processImageTag.MatchString(tag) {
		return errors.New("process image requires a pinned full revision")
	}
	labels := []string{"--label", "sumi.operation_id=" + o.OperationID, "--label", "sumi.personality_agent_id=" + o.PersonalityAgentID}
	mount := ""
	if o.WorkspaceBind != "" {
		labels = append(labels, "--label", "sumi.files_volume_uuid="+o.FilesVolumeUUID)
		mount = "type=bind,src=" + o.WorkspaceBind + ",dst=/workspace"
	} else {
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
		mount = "type=volume,src=" + volume + ",dst=/workspace,volume-nocopy"
	}
	raw, err := b.processDocker(ctx, "image", "inspect", "--format", "{{.Id}}", repo+":"+tag)
	if err != nil {
		return err
	}
	image := strings.TrimSpace(string(raw))
	if !regexp.MustCompile(`^sha256:[0-9a-f]{64}$`).MatchString(image) {
		return errors.New("invalid pinned process image")
	}
	// Public egress is opt-in per deployment: SUMI_JOB_EGRESS_DIR names the
	// host directory holding the egress proxy's unix socket. Job-image
	// containers get that directory bind-mounted (read-only rootfs, so the
	// mountpoint is pre-created in the image), a fixed loopback proxy
	// environment, and a bridge spawn in the launch wrapper. The container
	// itself still runs --network none: the socket is the entire path out,
	// and the proxy behind it dials public addresses only. Agent-image ops
	// and unconfigured deployments keep the exact no-network contract.
	egressDir := ""
	if o.Image == "job" {
		egressDir = b.jobEgressDir()
	}
	// pip --user installs console scripts into /workspace/.local/bin
	// (HOME=/workspace). Egress-enabled job ops put it first on PATH —
	// the normal user-site precedence — so an installed CLI is runnable
	// by name in the installing job and in later sessions on the same
	// workspace. Agent ops and no-egress launches keep the old PATH
	// byte-for-byte.
	pathEnv := "PATH=/usr/local/bin:/usr/bin:/bin"
	if egressDir != "" {
		pathEnv = "PATH=/workspace/.local/bin:/usr/local/bin:/usr/bin:/bin"
	}
	args := []string{"create", "--name", processContainer(o)}
	args = append(args, labels...)
	args = append(args, "--read-only", "--network", "none", "--user", "10002:10002", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true", "--cpus", "1", "--memory", "384m", "--memory-swap", "384m", "--pids-limit", "128", "--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=33554432", "--workdir", path.Join("/workspace", o.Cwd), "--mount", mount, "--env", pathEnv, "--env", "HOME=/workspace", "--env", "LANG=C.UTF-8")
	if egressDir != "" {
		args = append(args, "--mount", "type=bind,src="+egressDir+",dst=/run/sumi/egress")
	}
	if o.Interactive {
		// A held session keeps stdin open; a TTY session allocates the
		// pseudo-terminal so the shell/job control is real. TERM is a
		// fixed launch property like LANG — a request cannot pick it.
		args = append(args, "--interactive")
		if o.TTY {
			args = append(args, "--tty", "--env", "TERM=xterm-256color")
		}
	}
	envKeys := make([]string, 0, len(o.Env))
	for k := range o.Env {
		envKeys = append(envKeys, k)
	}
	sort.Strings(envKeys)
	for _, k := range envKeys {
		// When egress is configured the backend owns the proxy variables —
		// a request-supplied one could only name a destination the
		// container cannot reach anyway, so it is dropped for determinism.
		if egressDir != "" && jobEgressEnvNames[strings.ToUpper(k)] {
			continue
		}
		args = append(args, "--env", k+"="+o.Env[k])
	}
	if egressDir != "" {
		for _, kv := range jobEgressEnv {
			args = append(args, "--env", kv)
		}
	}
	args = append(args, "--log-driver", "json-file", "--log-opt", "max-size=16m", "--log-opt", "max-file=1")
	if o.Interactive {
		if egressDir != "" {
			args = append(args, "--entrypoint", "/bin/bash", image, "-c", jobEgressPrelude+`exec "$@"`, "sumi-egress", o.Executable)
		} else {
			// Interactive ops run the executable directly as PID 1: the
			// session's lifetime bound is enforced by the observer's
			// deadline kill, and the inner /usr/bin/timeout wrapper would
			// only duplicate it while hiding the real entrypoint signal
			// semantics (a TTY shell must be the session leader).
			args = append(args, image, o.Executable)
		}
	} else {
		payload := `printf '%s\n' "$1"; shift; `
		if egressDir != "" {
			payload += jobEgressPrelude
		}
		payload += `exec "$@"`
		args = append(args, "--entrypoint", "/bin/bash", image, "-c", payload, "sumi-process", processMarker(o), "/usr/bin/timeout", "--signal=TERM", "--kill-after=2", "--", strconv.Itoa(o.TimeoutSeconds), o.Executable)
	}
	args = append(args, o.Args...)
	if _, err = b.processDocker(ctx, args...); err != nil {
		return err
	}
	_, err = b.processDocker(ctx, "start", processContainer(o))
	return err
}
func processMarker(o ProcessOperation) string { return "SUMI_PROCESS_START_" + o.OperationID }

// jobEgressDir reads the deployment's opt-in egress socket directory out of
// the provisioner environment. Empty means no egress: the launch keeps the
// byte-for-byte no-network contract.
func (b *DockerBackend) jobEgressDir() string {
	for _, v := range b.baseEnvironment {
		if dir, ok := strings.CutPrefix(v, "SUMI_JOB_EGRESS_DIR="); ok {
			return dir
		}
	}
	return ""
}

// jobEgressEnv is the fixed proxy environment a job container receives when
// egress is configured. The loopback listener is spawned inside the
// container by the launch wrapper; NO_PROXY is pinned to a loopback-only
// bypass list — every other destination still goes through the proxy.
var jobEgressEnv = []string{
	"HTTP_PROXY=http://127.0.0.1:3128",
	"HTTPS_PROXY=http://127.0.0.1:3128",
	"ALL_PROXY=http://127.0.0.1:3128",
	// NO_PROXY pins a loopback-only bypass so a job-local server
	// (python3 -m http.server, a dev server, a local index) stays
	// reachable without custom flags. The bypass can never widen egress:
	// those destinations are refused by the proxy anyway, and a direct
	// loopback connection never leaves the container's netns.
	"NO_PROXY=localhost,127.0.0.1,::1",
	"http_proxy=http://127.0.0.1:3128",
	"https_proxy=http://127.0.0.1:3128",
	"all_proxy=http://127.0.0.1:3128",
	"no_proxy=localhost,127.0.0.1,::1",
}

var jobEgressEnvNames = map[string]bool{
	"HTTP_PROXY": true, "HTTPS_PROXY": true, "ALL_PROXY": true, "NO_PROXY": true,
}

// jobEgressPrelude starts the loopback->unix-socket bridge before the real
// argv. setsid detaches it into its own session/process group: the session
// signal path (Ctrl-C line-discipline INT, SignalProcess foreground-group
// delivery) must never kill it, while container teardown still reaps it
// with the PID namespace. A missing bridge is a loud warning, not a launch
// failure: the job still runs and its network calls fail honestly with
// connection refused. The /dev/tcp probe confirms the listener is bound
// before the workload starts, so a fast command cannot race the bridge;
// the probe subshell closes its fd so nothing leaks into the exec'd argv.
const jobEgressPrelude = `if [ -x /usr/local/bin/sumi-egress-bridge ]; then setsid /usr/local/bin/sumi-egress-bridge & sumi_egress_n=0; while [ "$sumi_egress_n" -lt 100 ]; do if (exec 3<>/dev/tcp/127.0.0.1/3128 && exec 3>&-) 2>/dev/null; then break; fi; sumi_egress_n=$((sumi_egress_n+1)); sleep 0.05; done; else echo 'sumi-egress: bridge unavailable; outbound network disabled' >&2; fi; `

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
	if o.Interactive {
		// Interactive output is streamed to the durable ttylog by the
		// supervised pump, not snapshotted here. A `docker logs` read
		// on every observe would do the same work twice for sessions
		// that can emit for hours.
		return observation, nil
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
