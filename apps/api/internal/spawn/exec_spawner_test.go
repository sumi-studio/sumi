package spawn

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

func writeFakeBinary(t *testing.T, dir string) string {
	t.Helper()
	bin := filepath.Join(dir, "sumi-agent")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	if err := os.WriteFile(bin, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatalf("write fake binary: %v", err)
	}
	return bin
}

func TestExecSpawnerBuildsAgentEnv(t *testing.T) {
	bin := writeFakeBinary(t, t.TempDir())
	spawner, err := NewExecSpawner(bin, map[string]string{
		"SUMI_MODEL_PRESET": "kimi-k3",
		"SUMI_MODEL_ID":     "test-model",
	})
	if err != nil {
		t.Fatalf("NewExecSpawner: %v", err)
	}

	stateRoot := t.TempDir()
	workspaceRoot := t.TempDir()
	stateDir := filepath.Join(stateRoot, "agent-1")
	workspaceDir := filepath.Join(workspaceRoot, "agent-1")
	config := AgentRuntimeConfig{
		AgentID:                   "agent-1",
		StateDir:                  stateDir,
		WorkspaceDir:              workspaceDir,
		WrappingKey:               "wrapping-key",
		Bearer:                    "bearer-1",
		Nonce:                     "nonce-1",
		BearerExpiresAtUnix:       time.Now().Add(time.Hour).Unix(),
		Warmth:                    WarmthCold,
		Generation:                7,
		GenerationLeaseID:         "lease-1",
		GenerationRecoveryFenceID: "fence-1",
		GatewayURL:                "ws://127.0.0.1:8080/agent/ws",
		ExecutorSocket:            "/tmp/executor.sock",
		LocalControlURL:           "http://127.0.0.1:8081",
	}

	proc, err := spawner.Spawn(context.Background(), config)
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if proc == nil {
		t.Fatal("expected non-nil process")
	}
	_ = proc.Stop()

	// Directories are created and owner-only.
	if info, err := os.Stat(stateDir); err != nil || !info.IsDir() {
		t.Fatalf("state dir not created: %v", err)
	}
	if info, err := os.Stat(workspaceDir); err != nil || !info.IsDir() {
		t.Fatalf("workspace dir not created: %v", err)
	}
}

func TestExecSpawnerStartupContextDoesNotOwnRuntimeLifetime(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell process-group fixture is Unix-only")
	}
	dir := t.TempDir()
	marker := filepath.Join(dir, "runtime-alive")
	bin := filepath.Join(dir, "sumi-agent")
	script := fmt.Sprintf(
		"#!/bin/sh\nsleep 0.05\nprintf alive > %q\nwhile :; do sleep 1; done\n",
		marker,
	)
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatalf("write long-running fake binary: %v", err)
	}
	spawner, err := NewExecSpawner(bin, nil)
	if err != nil {
		t.Fatalf("NewExecSpawner: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	proc, err := spawner.Spawn(ctx, AgentRuntimeConfig{
		AgentID:      "agent-1",
		StateDir:     filepath.Join(dir, "state"),
		WorkspaceDir: filepath.Join(dir, "workspace"),
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	t.Cleanup(func() { _ = proc.Stop() })
	cancel()

	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		} else if !os.IsNotExist(err) {
			t.Fatalf("inspect runtime marker: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("successfully-started runtime died with its startup context")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestBuildAgentEnvIncludesRequiredValues(t *testing.T) {
	config := AgentRuntimeConfig{
		AgentID:                   "agent-1",
		StateDir:                  "/state/agent-1",
		WorkspaceDir:              "/workspace/agent-1",
		WrappingKey:               "wrapping-key",
		Bearer:                    "bearer-1",
		Nonce:                     "nonce-1",
		BearerExpiresAtUnix:       1900000000,
		Warmth:                    WarmthWarm,
		Generation:                7,
		GenerationLeaseID:         "lease-1",
		GenerationRecoveryFenceID: "fence-1",
		GatewayURL:                "ws://127.0.0.1:8080/agent/ws",
		ExecutorSocket:            "/tmp/executor.sock",
		LocalControlURL:           "http://127.0.0.1:8081",
	}

	env := buildAgentEnv(config, map[string]string{
		"SUMI_MODEL_PRESET": "kimi-k3",
	})

	byKey := map[string]string{}
	for _, e := range env {
		if i := strings.Index(e, "="); i >= 0 {
			byKey[e[:i]] = e[i+1:]
		}
	}

	mustHave := map[string]string{
		"SUMI_PERSONALITY_AGENT_ID":                 "agent-1",
		"SUMI_RPC_GENERATION":                       "7",
		"SUMI_RPC_NONCE":                            "nonce-1",
		"SUMI_PROCESS_GENERATION_LEASE_ID":          "lease-1",
		"SUMI_GENERATION_RECOVERY_FENCE_ID":         "fence-1",
		"SUMI_STATE_DIR":                            "/state/agent-1",
		"SUMI_WORKSPACE":                            "/workspace/agent-1",
		"SUMI_GATEWAY_URL":                          "ws://127.0.0.1:8080/agent/ws",
		"SUMI_LOCAL_CONTROL_URL":                    "http://127.0.0.1:8081",
		"SUMI_LOCAL_CONTROL_BEARER":                 "bearer-1",
		"SUMI_LOCAL_CONTROL_BEARER_EXPIRES_AT_UNIX": "1900000000",
		"SUMI_AGENT_WRAPPING_KEY_ID":                "local-ephemeral/v1",
		"SUMI_AGENT_WRAPPING_KEY":                   "wrapping-key",
		"SUMI_EXECUTOR_SOCKET":                      "/tmp/executor.sock",
		"SUMI_ALLOW_INSECURE_LOOPBACK_GATEWAY":      "true",
		"SUMI_MODEL_PRESET":                         "kimi-k3",
	}

	for k, want := range mustHave {
		if got := byKey[k]; got != want {
			t.Errorf("%s: got %q, want %q", k, got, want)
		}
		delete(byKey, k)
	}

	// PATH, HOME, LANG, SUMI_LOG are automatic.
	automatic := []string{"PATH", "HOME", "LANG", "SUMI_LOG"}
	for _, k := range automatic {
		if _, ok := byKey[k]; !ok {
			t.Errorf("expected automatic env key %s", k)
		}
		delete(byKey, k)
	}

	if len(byKey) > 0 {
		extra := make([]string, 0, len(byKey))
		for k := range byKey {
			extra = append(extra, k)
		}
		sort.Strings(extra)
		t.Errorf("unexpected extra env keys: %v", extra)
	}
}

func TestBuildAgentEnvAllowsSharedEnvToOverrideLoopback(t *testing.T) {
	config := AgentRuntimeConfig{
		AgentID:         "agent-1",
		StateDir:        "/state/agent-1",
		WorkspaceDir:    "/workspace/agent-1",
		WrappingKey:     "k",
		Bearer:          "b",
		Nonce:           "n",
		GatewayURL:      "ws://127.0.0.1:8080/agent/ws",
		LocalControlURL: "http://127.0.0.1:8081",
	}

	env := buildAgentEnv(config, map[string]string{
		"SUMI_ALLOW_INSECURE_LOOPBACK_GATEWAY": "false",
	})

	byKey := map[string]string{}
	for _, e := range env {
		if i := strings.Index(e, "="); i >= 0 {
			byKey[e[:i]] = e[i+1:]
		}
	}

	if byKey["SUMI_ALLOW_INSECURE_LOOPBACK_GATEWAY"] != "false" {
		t.Fatalf("shared loopback override not respected: got %q", byKey["SUMI_ALLOW_INSECURE_LOOPBACK_GATEWAY"])
	}
}

func TestIsInsecureLoopbackGateway(t *testing.T) {
	for _, tc := range []struct {
		url  string
		want bool
	}{
		{"ws://127.0.0.1:8080/agent/ws", true},
		{"WS://127.0.0.1:8080/agent/ws", true},
		{"ws://[::1]:8080/agent/ws", true},
		{"wss://127.0.0.1:8080/agent/ws", false},
		{"ws://192.168.1.1:8080/agent/ws", false},
		{"http://127.0.0.1:8080/agent/ws", false},
		{"ws://localhost:8080/agent/ws", false},
	} {
		if got := isInsecureLoopbackGateway(tc.url); got != tc.want {
			t.Errorf("isInsecureLoopbackGateway(%q) = %v, want %v", tc.url, got, tc.want)
		}
	}
}
