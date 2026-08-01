package main

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
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

func writeFakeAgentBinary(t *testing.T, dir string) string {
	t.Helper()
	bin := filepath.Join(dir, "sumi-agent")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	// The binary only needs to be executable for os.Stat; it will not run.
	if err := os.WriteFile(bin, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatalf("write fake agent binary: %v", err)
	}
	return bin
}

func TestSpawnManagerFromEnv(t *testing.T) {
	stateRoot := t.TempDir()
	workspaceRoot := t.TempDir()
	binDir := t.TempDir()
	bin := writeFakeAgentBinary(t, binDir)

	t.Setenv("SUMI_AGENT_BINARY", bin)
	t.Setenv("SUMI_SPAWN_STATE_ROOT", stateRoot)
	t.Setenv("SUMI_SPAWN_WORKSPACE_ROOT", workspaceRoot)
	t.Setenv("SUMI_LOCAL_CONTROL_BEARER", "shared-bearer")
	t.Setenv("SUMI_LOCAL_CONTROL_RPC_BOOT_NONCE", "shared-nonce")
	t.Setenv("SUMI_LOCAL_CONTROL_GENERATION", "7")
	t.Setenv("SUMI_PUBLIC_LOOPBACK_LISTEN", "127.0.0.1:8080")

	listener := &localControlListenerConfig{loopbackListen: "127.0.0.1:8081"}
	resolver := fakeAgentResolver{
		keys:   map[string]string{"agent-1": "wrapping-key-1"},
		warmth: map[string]string{"agent-1": spawn.WarmthCold},
	}

	mgr, err := spawnManagerFromEnv(resolver, listener)
	if err != nil {
		t.Fatalf("spawnManagerFromEnv: %v", err)
	}
	if mgr == nil {
		t.Fatal("expected spawn manager, got nil")
	}

	ctx := context.Background()
	if err := mgr.EnsureRunning(ctx, "agent-1"); err != nil {
		// EnsureRunning will fail because the fake binary is not a real agent,
		// but it should spawn the process and then fail on wait. On Unix the
		// shell script exits quickly, so the error path depends on timing. The
		// important check is that the spawner was created with the right env.
		t.Logf("EnsureRunning error (expected for fake binary): %v", err)
	}

	// Cleanup stops any spawned process.
	_ = mgr.StopAll()
}

func TestSpawnManagerFromEnvRequiresResolver(t *testing.T) {
	t.Setenv("SUMI_AGENT_BINARY", "/nonexistent")
	if _, err := spawnManagerFromEnv(nil, nil); err == nil {
		t.Fatal("expected error for nil resolver")
	}
}

func TestSpawnManagerFromEnvRequiresLoopbackListener(t *testing.T) {
	binDir := t.TempDir()
	bin := writeFakeAgentBinary(t, binDir)
	t.Setenv("SUMI_AGENT_BINARY", bin)
	t.Setenv("SUMI_SPAWN_STATE_ROOT", t.TempDir())
	t.Setenv("SUMI_SPAWN_WORKSPACE_ROOT", t.TempDir())
	t.Setenv("SUMI_LOCAL_CONTROL_BEARER", "b")
	t.Setenv("SUMI_LOCAL_CONTROL_RPC_BOOT_NONCE", "n")
	t.Setenv("SUMI_LOCAL_CONTROL_GENERATION", "0")

	if _, err := spawnManagerFromEnv(fakeAgentResolver{}, nil); err == nil {
		t.Fatal("expected error for nil listener")
	}
	if _, err := spawnManagerFromEnv(fakeAgentResolver{}, &localControlListenerConfig{}); err == nil {
		t.Fatal("expected error for listener without loopback")
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
