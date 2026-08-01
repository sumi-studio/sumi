package spawn

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

// ExecSpawner starts the Rust PersonalityAgent binary as a subprocess for each
// agent. It is the production ProcessSpawner for the dev control plane.
type ExecSpawner struct {
	BinaryPath string
	// SharedEnv is the model/provider/approval configuration shared across all
	// agents (key=value entries).
	SharedEnv map[string]string
}

// NewExecSpawner returns an ExecSpawner rooted at binaryPath. The binary must
// exist and be executable.
func NewExecSpawner(binaryPath string, sharedEnv map[string]string) (*ExecSpawner, error) {
	if strings.TrimSpace(binaryPath) == "" {
		return nil, errors.New("exec spawner requires a binary path")
	}
	if info, err := os.Stat(binaryPath); err != nil || info.IsDir() {
		return nil, fmt.Errorf("agent binary %q is not usable: %w", binaryPath, err)
	}
	return &ExecSpawner{BinaryPath: binaryPath, SharedEnv: sharedEnv}, nil
}

func (e *ExecSpawner) Spawn(ctx context.Context, config AgentRuntimeConfig) (Process, error) {
	for _, dir := range []string{config.StateDir, config.WorkspaceDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("create dir %s: %w", dir, err)
		}
	}
	cmd := exec.CommandContext(ctx, e.BinaryPath)
	cmd.Dir = config.WorkspaceDir
	cmd.Env = buildAgentEnv(config, e.SharedEnv)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start agent %s: %w", config.AgentID, err)
	}
	log.Printf("spawn: started agent %s (pid %d, warmth %s)", config.AgentID, cmd.Process.Pid, config.Warmth)
	return &execProcess{cmd: cmd}, nil
}

func buildAgentEnv(config AgentRuntimeConfig, shared map[string]string) []string {
	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + config.StateDir,
		"LANG=C.UTF-8",
		"SUMI_PERSONALITY_AGENT_ID=" + config.AgentID,
		fmt.Sprintf("SUMI_RPC_GENERATION=%d", config.Generation),
		"SUMI_RPC_NONCE=" + config.Nonce,
		"SUMI_PROCESS_GENERATION_LEASE_ID=" + config.GenerationLeaseID,
		"SUMI_GENERATION_RECOVERY_FENCE_ID=" + config.GenerationRecoveryFenceID,
		"SUMI_STATE_DIR=" + config.StateDir,
		"SUMI_WORKSPACE=" + config.WorkspaceDir,
		"SUMI_GATEWAY_URL=" + config.GatewayURL,
		"SUMI_LOCAL_CONTROL_URL=" + config.LocalControlURL,
		"SUMI_LOCAL_CONTROL_BEARER=" + config.Bearer,
		fmt.Sprintf("SUMI_LOCAL_CONTROL_BEARER_EXPIRES_AT_UNIX=%d", config.BearerExpiresAtUnix),
		"SUMI_AGENT_WRAPPING_KEY_ID=local-ephemeral/v1",
		"SUMI_AGENT_WRAPPING_KEY=" + config.WrappingKey,
		"SUMI_LOG=sumi_agent=info",
	}
	if _, ok := shared["SUMI_ALLOW_INSECURE_LOOPBACK_GATEWAY"]; !ok && isInsecureLoopbackGateway(config.GatewayURL) {
		env = append(env, "SUMI_ALLOW_INSECURE_LOOPBACK_GATEWAY=true")
	}
	if config.ExecutorSocket != "" {
		env = append(env, "SUMI_EXECUTOR_SOCKET="+config.ExecutorSocket)
	}
	for k, v := range shared {
		env = append(env, k+"="+v)
	}
	return env
}

// isInsecureLoopbackGateway reports whether url is a ws:// scheme pointing at
// a loopback address, which requires SUMI_ALLOW_INSECURE_LOOPBACK_GATEWAY.
func isInsecureLoopbackGateway(url string) bool {
	url = strings.ToLower(url)
	if !strings.HasPrefix(url, "ws://") {
		return false
	}
	rest := strings.TrimPrefix(url, "ws://")
	// Strip any path before looking for host:port.
	if i := strings.Index(rest, "/"); i >= 0 {
		rest = rest[:i]
	}
	host, _, err := net.SplitHostPort(rest)
	if err != nil {
		// Maybe no port; treat as host.
		host = rest
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

type execProcess struct {
	cmd *exec.Cmd
}

func (p *execProcess) Wait() error {
	if p.cmd.Process == nil {
		return errors.New("process not started")
	}
	return p.cmd.Wait()
}

func (p *execProcess) Stop() error {
	if p.cmd.Process == nil {
		return nil
	}
	// Send SIGTERM to the process group so the supervisor and children exit.
	_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGTERM)
	_, _ = p.cmd.Process.Wait()
	return nil
}

// resolveBinaryPath finds the agent binary relative to the repository root or
// the SUMI_AGENT_BINARY env var.
func resolveBinaryPath() (string, error) {
	if p := os.Getenv("SUMI_AGENT_BINARY"); p != "" {
		return p, nil
	}
	return "", errors.New("SUMI_AGENT_BINARY not set")
}

// SharedAgentEnvFromOS collects the model/provider/approval env shared across
// all spawned agents.
func SharedAgentEnvFromOS() map[string]string {
	shared := map[string]string{}
	for _, name := range []string{
		"SUMI_EXECUTOR_SOCKET",
		"SUMI_ALLOW_INSECURE_LOOPBACK_GATEWAY",
		"SUMI_APPROVAL_SECRET_DIGEST_KEY",
		"SUMI_MODEL_PRESET",
		"SUMI_MODEL_API_KEY_ENV",
		"SUMI_MODEL_ID",
		"SUMI_MODEL_BASE_URL",
	} {
		if v := os.Getenv(name); v != "" {
			shared[name] = v
		}
	}
	// Propagate the provider API key env itself.
	if keyEnv := os.Getenv("SUMI_MODEL_API_KEY_ENV"); keyEnv != "" {
		if v := os.Getenv(keyEnv); v != "" {
			shared[keyEnv] = v
		}
	}
	return shared
}

// EnsureParentDirs is a small helper for callers that want to validate roots.
func EnsureParentDirs(paths ...string) error {
	for _, p := range paths {
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			return err
		}
	}
	return nil
}
