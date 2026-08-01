package main

import (
	"context"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/spawn"
)

type fakeAgentResolver struct {
	keys   map[string]string
	warmth map[string]string
}

type fakeProcess struct{}

func (p *fakeProcess) Wait() error { return nil }
func (p *fakeProcess) Stop() error { return nil }

type fakeSpawner struct{}

func (s *fakeSpawner) Spawn(_ context.Context, _ spawn.AgentRuntimeConfig) (spawn.Process, error) {
	return &fakeProcess{}, nil
}

func (r fakeAgentResolver) AgentWrappingKey(_ context.Context, agentID string) (string, error) {
	return r.keys[agentID], nil
}

func (r fakeAgentResolver) AgentWarmth(_ context.Context, agentID string) (string, error) {
	return r.warmth[agentID], nil
}

func TestSpawnManagerFromEnvDisabledWithoutProvisioner(t *testing.T) {
	t.Setenv("SUMI_RUNTIME_PROVISIONER_SOCKET", "")
	t.Setenv("SUMI_AGENT_BINARY", "")
	mgr, err := spawnManagerFromEnv(fakeAgentResolver{}, nil, nil)
	if err != nil || mgr != nil {
		t.Fatalf("disabled spawn manager: manager=%v err=%v", mgr, err)
	}
}

func TestSpawnManagerFromEnvRequiresResolver(t *testing.T) {
	t.Setenv("SUMI_RUNTIME_PROVISIONER_SOCKET", "/run/sumi/runtime-provisioner/control.sock")
	if _, err := spawnManagerFromEnv(nil, nil, nil); err == nil {
		t.Fatal("expected error for nil resolver")
	}
}

func TestSpawnManagerFromEnvRejectsHostExecSpawner(t *testing.T) {
	t.Setenv("SUMI_RUNTIME_PROVISIONER_SOCKET", "")
	t.Setenv("SUMI_AGENT_BINARY", "/usr/local/bin/sumi-agent")
	if _, err := spawnManagerFromEnv(fakeAgentResolver{}, nil, nil); err == nil {
		t.Fatal("expected host ExecSpawner configuration to be rejected")
	}
}

func TestSpawnGatewayURLFromEnv(t *testing.T) {
	// Explicit gateway URL wins.
	t.Setenv("SUMI_AGENT_GATEWAY_URL", "wss://explicit.test/agent/ws")
	if got, err := spawnGatewayURLFromEnv(); err != nil || got != "wss://explicit.test/agent/ws" {
		t.Fatalf("explicit gateway URL: got %q, err %v", got, err)
	}

	t.Setenv("SUMI_AGENT_GATEWAY_URL", "")
	t.Setenv("SUMI_PUBLIC_LOOPBACK_LISTEN", "127.0.0.1:9090")
	if got, err := spawnGatewayURLFromEnv(); err != nil || got != "ws://127.0.0.1:9090/agent/ws" {
		t.Fatalf("loopback gateway URL: got %q, err %v", got, err)
	}

	t.Setenv("SUMI_PUBLIC_LOOPBACK_LISTEN", "")
	t.Setenv("SUMI_PUBLIC_LISTEN", "0.0.0.0:8080")
	if got, err := spawnGatewayURLFromEnv(); err != nil || got != "ws://127.0.0.1:8080/agent/ws" {
		t.Fatalf("unspecified public listen: got %q, err %v", got, err)
	}

	t.Setenv("SUMI_PUBLIC_LISTEN", "192.168.1.1:8080")
	if _, err := spawnGatewayURLFromEnv(); err == nil {
		t.Fatal("expected error for non-loopback public listen without explicit gateway URL")
	}
}

func TestRequireDirFromEnv(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SUMI_TEST_REQUIRE_DIR", dir)
	got, err := requireDirFromEnv("SUMI_TEST_REQUIRE_DIR")
	if err != nil {
		t.Fatalf("requireDirFromEnv: %v", err)
	}
	if got != dir {
		t.Fatalf("got %q want %q", got, dir)
	}

	t.Setenv("SUMI_TEST_REQUIRE_DIR", "")
	if _, err := requireDirFromEnv("SUMI_TEST_REQUIRE_DIR"); err == nil {
		t.Fatal("expected error for empty env")
	}
}

func TestRequiredUintFromEnv(t *testing.T) {
	t.Setenv("SUMI_TEST_UINT", "42")
	if got, err := requiredUintFromEnv("SUMI_TEST_UINT"); err != nil || got != 42 {
		t.Fatalf("got %d, err %v", got, err)
	}

	t.Setenv("SUMI_TEST_UINT", "not-a-number")
	if _, err := requiredUintFromEnv("SUMI_TEST_UINT"); err == nil {
		t.Fatal("expected parse error")
	}

	t.Setenv("SUMI_TEST_UINT", "")
	if _, err := requiredUintFromEnv("SUMI_TEST_UINT"); err == nil {
		t.Fatal("expected error for empty env")
	}
}

func TestRunIdleReaperExitsOnContextDone(t *testing.T) {
	mgr, err := spawn.New(spawn.Config{
		Spawner:       &fakeSpawner{},
		Resolver:      fakeAgentResolver{},
		IdleTimeout:   time.Minute,
		SharedBearer:  "b",
		SharedNonce:   "n",
		StateRoot:     t.TempDir(),
		WorkspaceRoot: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runIdleReaper(ctx, mgr)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runIdleReaper did not exit after context cancellation")
	}
}
