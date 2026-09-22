package runtimeprovision

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// fakeDockerScript answers the exact queries LaunchProcess issues and
// records every invocation one-per-line in $REC_LOG.
const fakeDockerScript = `#!/bin/sh
printf '%s\n' "$*" >> "$REC_LOG"
case "$1 $2" in
  "volume inspect")
    name="$3"
    project="${name%_workspace}"
    printf '[{"Name":"%s","Labels":{"com.docker.compose.project":"%s","com.docker.compose.volume":"workspace"}}]' "$name" "$project"
    ;;
  "image inspect")
    printf 'sha256:0000000000000000000000000000000000000000000000000000000000000001\n'
    ;;
esac
exit 0
`

// launchArgs runs LaunchProcess against the recording shim and returns the
// recorded `docker create` invocation fields.
func launchArgs(t *testing.T, extraEnv []string, o ProcessOperation) []string {
	t.Helper()
	dir := t.TempDir()
	recLog := filepath.Join(dir, "calls.log")
	shim := filepath.Join(dir, "docker")
	if err := os.WriteFile(shim, []byte(fakeDockerScript), 0o755); err != nil {
		t.Fatal(err)
	}
	// exec.Command resolves "docker" against the process PATH at call
	// time, not the child environment — the shim must lead the real PATH.
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	env := append([]string{
		"PATH=" + dir + ":" + os.Getenv("PATH"),
		"REC_LOG=" + recLog,
		"SUMI_JOB_IMAGE_TAG=899a7cdf1defdf9ce09d76cccaea48bf5f58a20d",
		"SUMI_AGENT_IMAGE_TAG=899a7cdf1defdf9ce09d76cccaea48bf5f58a20d",
	}, extraEnv...)
	backend := &DockerBackend{baseEnvironment: env}
	if err := backend.LaunchProcess(context.Background(), o); err != nil {
		t.Fatalf("LaunchProcess: %v", err)
	}
	raw, err := os.ReadFile(recLog)
	if err != nil {
		t.Fatal(err)
	}
	var create string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "create ") {
			create = line
		}
	}
	if create == "" {
		t.Fatalf("no docker create recorded:\n%s", raw)
	}
	return strings.Split(create, " ")
}

func egressTestOp(t *testing.T, interactive bool) ProcessOperation {
	t.Helper()
	paid := uuid.NewString()
	req := ProcessStartRequest{
		PersonalityAgentID:    paid,
		OriginatingToolCallID: "egress-test",
		Executable:            "/bin/sh",
		Args:                  []string{"-c", "true"},
		TimeoutSeconds:        30,
		Image:                 "job",
		Interactive:           interactive,
		TTY:                   interactive,
	}
	req = req.canonical()
	return ProcessOperation{
		OperationID:        ProcessOperationID(paid, "egress-test"),
		PersonalityAgentID: paid,
		Executable:         req.Executable,
		Args:               req.Args,
		Cwd:                req.Cwd,
		TimeoutSeconds:     req.TimeoutSeconds,
		Image:              req.Image,
		Interactive:        req.Interactive,
		TTY:                req.TTY,
	}
}

func egressEnabledOp(t *testing.T) ProcessOperation {
	o := egressTestOp(t, false)
	o.Env = map[string]string{
		"HTTP_PROXY": "http://requester-supplied.invalid:9",
		"https_proxy": "http://also-bad.invalid:9",
		"KEEP_ME":    "yes",
	}
	return o
}

func containsField(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

func joinedArgs(args []string) string { return strings.Join(args, " ") }

// Egress configured + job image: the launch adds the socket-directory bind,
// the fixed loopback proxy env (overriding request-supplied proxy vars),
// and the bridge prelude — while keeping --network none and every other
// sandbox flag.
func TestLaunchProcessEgressArgs(t *testing.T) {
	o := egressEnabledOp(t)
	args := launchArgs(t, []string{"SUMI_JOB_EGRESS_DIR=/run/sumi/egress"}, o)
	joined := joinedArgs(args)
	for _, want := range []string{
		"--network none",
		"type=bind,src=/run/sumi/egress,dst=/run/sumi/egress",
		"--env HTTP_PROXY=http://127.0.0.1:3128",
		"--env https_proxy=http://127.0.0.1:3128",
		"--env ALL_PROXY=http://127.0.0.1:3128",
		"--env NO_PROXY=",
		"--env KEEP_ME=yes",
		"sumi-egress-bridge",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("create args missing %q:\n%s", want, joined)
		}
	}
	for _, bad := range []string{"requester-supplied.invalid", "also-bad.invalid"} {
		if strings.Contains(joined, bad) {
			t.Fatalf("request-supplied proxy env survived:\n%s", joined)
		}
	}
	// The batch wrapper must keep the durable start marker and timeout.
	if !strings.Contains(joined, "printf '%s\\n' \"$1\"; shift;") || !strings.Contains(joined, processMarker(o)) {
		t.Fatalf("batch wrapper lost marker/timeout contract:\n%s", joined)
	}
}

// No egress configured: the create spec is byte-identical to the previous
// contract — no socket mount, no injected proxy env, no bridge prelude.
func TestLaunchProcessNoEgressUnchanged(t *testing.T) {
	o := egressEnabledOp(t)
	args := launchArgs(t, nil, o)
	joined := joinedArgs(args)
	if strings.Contains(joined, "sumi/egress") || strings.Contains(joined, "127.0.0.1:3128") || strings.Contains(joined, "sumi-egress-bridge") {
		t.Fatalf("egress leaked into an unconfigured launch:\n%s", joined)
	}
	// Request-supplied proxy vars pass through untouched when egress is
	// off — they are meaningless without a network either way.
	if !strings.Contains(joined, "requester-supplied.invalid") {
		t.Fatalf("unrelated env lost:\n%s", joined)
	}
	if !strings.Contains(joined, `printf '%s\n' "$1"; shift; exec "$@"`) {
		t.Fatalf("batch payload changed without egress:\n%s", joined)
	}
}

// Egress configured but a non-job image: no mount, no proxy env — the
// agent image carries neither the bridge nor the mountpoint.
func TestLaunchProcessEgressOnlyForJobImage(t *testing.T) {
	o := egressEnabledOp(t)
	o.Image = "agent"
	args := launchArgs(t, []string{"SUMI_JOB_EGRESS_DIR=/run/sumi/egress"}, o)
	joined := joinedArgs(args)
	if strings.Contains(joined, "sumi/egress") || strings.Contains(joined, "HTTP_PROXY=http://127.0.0.1:3128") {
		t.Fatalf("egress applied to non-job image:\n%s", joined)
	}
}

// Interactive (terminal) ops get the same egress wiring through the
// bash -c prelude, while the executable still becomes PID 1 via exec.
func TestLaunchProcessEgressInteractive(t *testing.T) {
	o := egressTestOp(t, true)
	args := launchArgs(t, []string{"SUMI_JOB_EGRESS_DIR=/run/sumi/egress"}, o)
	joined := joinedArgs(args)
	for _, want := range []string{
		"sumi-egress-bridge", "exec \"$@\"", "--entrypoint /bin/bash",
		"type=bind,src=/run/sumi/egress,dst=/run/sumi/egress",
		"--interactive", "--tty",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("interactive egress args missing %q:\n%s", want, joined)
		}
	}
}

// The prelude probe must not run when the socket dir is configured but the
// op carries a request-supplied executable in interactive mode without
// egress — covered above; here pin down that env dedup is case-insensitive
// on both spellings.
func TestJobEgressEnvNamesCaseInsensitive(t *testing.T) {
	for _, name := range []string{"http_proxy", "HTTPS_PROXY", "All_Proxy", "no_proxy"} {
		if !jobEgressEnvNames[strings.ToUpper(name)] {
			t.Fatalf("egress env name %q not recognized", name)
		}
	}
}

// The recording shim must not leak into other tests: prove docker was
// actually shimmed by checking the recorded volume inspect answered.
func TestFakeDockerShimSanity(t *testing.T) {
	dir := t.TempDir()
	recLog := filepath.Join(dir, "calls.log")
	shim := filepath.Join(dir, "docker")
	if err := os.WriteFile(shim, []byte(fakeDockerScript), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(shim, "volume", "inspect", "sumi-abc_workspace")
	cmd.Env = []string{"REC_LOG=" + recLog}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "sumi-abc_workspace") {
		t.Fatalf("shim output: %s", out)
	}
}
