package runtimeprovision

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestActivateAcceptsLegacyBearerExpiryWithoutRestoringIt(t *testing.T) {
	const legacyField = "local_control_bearer_expires_at_unix"
	currentConfig, err := json.Marshal(testActivationConfig())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(currentConfig, []byte(legacyField)) {
		t.Fatal("new activation producers must omit the obsolete expiry")
	}
	for _, test := range []struct {
		name       string
		mutate     func(map[string]any)
		wantStatus int
	}{
		{name: "old producer", wantStatus: http.StatusOK},
		{
			name:       "unrecognized authority field",
			mutate:     func(config map[string]any) { config["host_environment"] = "injected" },
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing bearer",
			mutate:     func(config map[string]any) { delete(config, "local_control_bearer") },
			wantStatus: http.StatusBadRequest,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := newFakeBackend()
			service := newTestService(t, backend)
			epoch, err := service.Prepare(context.Background(), PrepareRequest{
				Version: ProtocolVersion, PersonalityAgentID: testPAID, IdempotencyKey: "legacy-activation",
			})
			if err != nil {
				t.Fatal(err)
			}
			var activation map[string]any
			if err := json.Unmarshal(currentConfig, &activation); err != nil {
				t.Fatal(err)
			}
			// The old v1 producer always sent this field. An expired value must
			// neither prevent activation nor reach the runtime environment.
			activation[legacyField] = 1
			if test.mutate != nil {
				test.mutate(activation)
			}
			payload, err := json.Marshal(struct {
				Version int `json:"version"`
				PreparedEpoch
				Activation map[string]any `json:"activation"`
			}{Version: ProtocolVersion, PreparedEpoch: epoch, Activation: activation})
			if err != nil {
				t.Fatal(err)
			}
			handler, err := NewHandler(service)
			if err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/activate", bytes.NewReader(payload)))
			if response.Code != test.wantStatus {
				t.Fatalf("activation status = %d, want %d: %s", response.Code, test.wantStatus, response.Body.String())
			}
			if test.wantStatus != http.StatusOK {
				if backend.activateCalls[testPAID] != 0 {
					t.Fatal("invalid activation reached the backend")
				}
				return
			}
			if backend.activateCalls[testPAID] != 1 {
				t.Fatal("legacy activation did not reach the backend")
			}
			var decoded ActivateRequest
			if err := json.Unmarshal(payload, &decoded); err != nil {
				t.Fatal(err)
			}
			if _, exists := activationEnvironment(decoded.Activation)["SUMI_LOCAL_CONTROL_BEARER_EXPIRES_AT_UNIX"]; exists {
				t.Fatal("obsolete expiry was forwarded to the runtime")
			}
		})
	}
}

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
