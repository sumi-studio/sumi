package agentevents

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestLocalControlListenerRegistryUsesIndependentPAIDBoundSockets(t *testing.T) {
	root, err := os.MkdirTemp("", "sumi-lc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if err := os.Chmod(root, localControlRegistryDirectoryMode); err != nil {
		t.Fatal(err)
	}

	_, gateway := openLocalControlTestGateway(t, privateRuntimeDir(t))
	firstAuthorization := localControlAuthorization(localControlTestBearer, localControlTestPAID, 7, "boot-a")
	secondAuthorization := localControlAuthorization(localControlOtherBearer, localControlOtherPAID, 9, "boot-other")
	control, err := NewLocalControlServer(
		gateway,
		localControlTestSigningSecret,
		[]LocalRuntimeAuthorization{firstAuthorization, secondAuthorization},
	)
	if err != nil {
		t.Fatal(err)
	}
	var openerMu sync.Mutex
	opened := make(map[string]int)
	opener := func(socketPath string, _ int, personalityAgentID string) (net.Listener, error) {
		expected, err := LocalControlSocketPath(root, personalityAgentID)
		if err != nil {
			return nil, err
		}
		if socketPath != expected {
			t.Fatalf("noncanonical socket path: got %q want %q", socketPath, expected)
		}
		openerMu.Lock()
		opened[personalityAgentID]++
		openerMu.Unlock()
		return net.Listen("unix", socketPath)
	}
	registry, err := NewLocalControlListenerRegistry(control, LocalControlListenerRegistryConfig{
		RootDir:      root,
		SocketGID:    os.Getegid(),
		OpenListener: opener,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	t.Cleanup(func() { _ = registry.Close(ctx) })

	if err := registry.ReconcileLocalRuntimes(
		ctx,
		[]string{localControlTestPAID, localControlOtherPAID},
	); err != nil {
		t.Fatal(err)
	}
	if err := registry.EnsureLocalRuntime(localControlTestPAID); err != nil {
		t.Fatalf("idempotent ensure: %v", err)
	}
	openerMu.Lock()
	if opened[localControlTestPAID] != 1 || opened[localControlOtherPAID] != 1 {
		t.Fatalf("listeners were not created exactly once: %+v", opened)
	}
	openerMu.Unlock()

	firstPath, err := LocalControlSocketPath(root, localControlTestPAID)
	if err != nil {
		t.Fatal(err)
	}
	secondPath, err := LocalControlSocketPath(root, localControlOtherPAID)
	if err != nil {
		t.Fatal(err)
	}
	if firstPath == secondPath || filepath.Dir(firstPath) == filepath.Dir(secondPath) {
		t.Fatalf("PAIDs shared a socket namespace: first=%q second=%q", firstPath, secondPath)
	}

	status, body := postLocalControlUnix(
		t,
		firstPath,
		localControlTestBearer,
		startupPublication("first-startup", localControlTestPAID, 7, "boot-a"),
	)
	if status != http.StatusOK {
		t.Fatalf("first bound socket rejected its PAID: status=%d body=%s", status, body)
	}
	status, _ = postLocalControlUnix(
		t,
		firstPath,
		localControlOtherBearer,
		startupPublication("cross-socket", localControlOtherPAID, 9, "boot-other"),
	)
	if status != http.StatusUnauthorized {
		t.Fatalf("A's socket accepted B's exact bearer and payload: got %d", status)
	}
	status, body = postLocalControlUnix(
		t,
		secondPath,
		localControlOtherBearer,
		startupPublication("second-startup", localControlOtherPAID, 9, "boot-other"),
	)
	if status != http.StatusOK {
		t.Fatalf("second bound socket rejected its PAID: status=%d body=%s", status, body)
	}

	if err := registry.ReconcileLocalRuntimes(ctx, []string{localControlOtherPAID}); err != nil {
		t.Fatalf("reconcile removal: %v", err)
	}
	if err := registry.CloseLocalRuntime(ctx, localControlTestPAID); err != nil {
		t.Fatalf("idempotent close: %v", err)
	}
	if _, err := net.DialTimeout("unix", firstPath, 50*time.Millisecond); err == nil {
		t.Fatal("removed PAID listener remained reachable")
	}
	if conn, err := net.DialTimeout("unix", secondPath, 50*time.Millisecond); err != nil {
		t.Fatalf("reconcile of A disturbed B: %v", err)
	} else {
		_ = conn.Close()
	}
}

func TestLocalControlListenerRegistryRejectsUntrustedOrNoncanonicalRoot(t *testing.T) {
	root, err := os.MkdirTemp("", "sumi-lc-root-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if err := os.Chmod(root, localControlRegistryDirectoryMode); err != nil {
		t.Fatal(err)
	}
	link := root + "-link"
	if err := os.Symlink(root, link); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(link) })

	_, gateway := openLocalControlTestGateway(t, privateRuntimeDir(t))
	control, err := NewLocalControlServer(gateway, localControlTestSigningSecret, nil)
	if err != nil {
		t.Fatal(err)
	}
	opener := func(string, int, string) (net.Listener, error) {
		t.Fatal("listener opener called for an untrusted root")
		return nil, nil
	}
	if _, err := NewLocalControlListenerRegistry(control, LocalControlListenerRegistryConfig{
		RootDir:      link,
		SocketGID:    os.Getegid(),
		OpenListener: opener,
	}); err == nil {
		t.Fatal("symlinked local-control root was accepted")
	}
	if _, err := LocalControlSocketPath(root, "../"+localControlTestPAID); err == nil {
		t.Fatal("noncanonical PAID-derived socket path was accepted")
	}
}

func postLocalControlUnix(
	t *testing.T,
	socketPath string,
	bearer string,
	publication LocalRuntimeStatePublication,
) (int, []byte) {
	t.Helper()
	raw, err := json.Marshal(publication)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		},
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
	request, err := http.NewRequest(
		http.MethodPost,
		"http://local-control.invalid"+LocalRuntimeStatePublishPath,
		bytes.NewReader(raw),
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+bearer)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, body
}
