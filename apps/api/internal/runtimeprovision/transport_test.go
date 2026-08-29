package runtimeprovision

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestUnixTransportRoundTrip(t *testing.T) {
	backend := newFakeBackend()
	service := newTestService(t, backend)
	handler, _ := NewHandler(service)
	socketDirectory := t.TempDir()
	if err := os.Chmod(socketDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(socketDirectory, "provisioner.sock")
	listener, err := ListenUnix(UnixListenerConfig{
		SocketPath:           socketPath,
		SocketGID:            os.Getegid(),
		SocketMode:           0o660,
		AllowNonRootForTests: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler}
	defer func() {
		_ = server.Close()
		_ = listener.Close()
	}()
	go func() { _ = server.Serve(listener) }()

	info, err := os.Lstat(socketPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o660 {
		t.Fatalf("unexpected socket metadata: %v", info.Mode())
	}
	client, err := NewUnixClient(socketPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	epoch, err := client.Prepare(ctx, PrepareRequest{
		Version: ProtocolVersion, PersonalityAgentID: testPAID, IdempotencyKey: "unix-round-trip",
	})
	if err != nil {
		t.Fatal(err)
	}
	active, err := client.Activate(ctx, ActivateRequest{Version: ProtocolVersion, PreparedEpoch: epoch, Activation: testActivationConfig()})
	if err != nil || active.Phase != PhaseActive {
		t.Fatalf("activate round trip: %#v %v", active, err)
	}
	observed, err := client.Inspect(ctx, InspectRequest{Version: ProtocolVersion, PersonalityAgentID: testPAID})
	if err != nil || observed.Phase != PhaseActive || observed.Epoch == nil || *observed.Epoch != epoch {
		t.Fatalf("inspect round trip: %#v %v", observed, err)
	}
	reconciled, err := client.Reconcile(ctx, ReconcileRequest{Version: ProtocolVersion, PersonalityAgentID: testPAID})
	if err != nil || reconciled.Phase != PhaseActive || reconciled.Epoch == nil || *reconciled.Epoch != epoch {
		t.Fatalf("reconcile round trip: %#v %v", reconciled, err)
	}
	aborted, err := client.Abort(ctx, AbortRequest{Version: ProtocolVersion, PreparedEpoch: epoch})
	if err != nil || aborted.Phase != PhaseUnknown {
		t.Fatalf("abort round trip: %#v %v", aborted, err)
	}
}

func TestListenUnixRefusesUntrustedExistingPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "provisioner.sock")
	if err := os.WriteFile(path, []byte("not a socket"), 0o600); err != nil {
		t.Fatal(err)
	}
	listener, err := ListenUnix(UnixListenerConfig{
		SocketPath:           path,
		SocketGID:            os.Getegid(),
		AllowNonRootForTests: true,
	})
	if listener != nil {
		_ = listener.Close()
	}
	if err == nil {
		t.Fatal("untrusted existing socket path was replaced")
	}
}
