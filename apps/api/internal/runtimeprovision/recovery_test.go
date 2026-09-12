package runtimeprovision

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

type recoveryBackend struct {
	*fakeBackend
	recovered RecoveredLocalControl
	calls     int
}

func (b *recoveryBackend) RecoverLocalControl(_ context.Context, epoch PreparedEpoch) (RecoveredLocalControl, error) {
	b.calls++
	if b.recovered.PreparedEpoch != epoch {
		return RecoveredLocalControl{}, ErrConflict
	}
	return b.recovered, nil
}

func TestUnixLocalControlRecoveryReturnsOnlyExactActiveCredential(t *testing.T) {
	ctx := context.Background()
	backend := &recoveryBackend{fakeBackend: newFakeBackend()}
	service := newTestService(t, backend)
	epoch, err := service.Prepare(ctx, PrepareRequest{Version: ProtocolVersion, PersonalityAgentID: testPAID, IdempotencyKey: "recovery"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Activate(ctx, ActivateRequest{Version: ProtocolVersion, PreparedEpoch: epoch, Activation: testActivationConfig()}); err != nil {
		t.Fatal(err)
	}
	backend.recovered = RecoveredLocalControl{PreparedEpoch: epoch, Bearer: strings.Repeat("b", 43), SelectionFingerprint: testActivationConfig().SelectionFingerprint()}
	handler, err := NewHandler(service)
	if err != nil {
		t.Fatal(err)
	}
	socketDir := t.TempDir()
	if err := os.Chmod(socketDir, 0o700); err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(socketDir, "p.sock")
	listener, err := ListenUnix(UnixListenerConfig{SocketPath: socketPath, SocketGID: os.Getegid(), SocketMode: 0o660, AllowNonRootForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler}
	defer server.Close()
	go func() { _ = server.Serve(listener) }()
	client, err := NewUnixClient(socketPath)
	if err != nil {
		t.Fatal(err)
	}
	request := RecoverLocalControlRequest{Version: ProtocolVersion, PreparedEpoch: epoch}
	recovered, err := client.RecoverLocalControl(ctx, request)
	if err != nil || recovered != backend.recovered {
		t.Fatalf("recovery round trip failed: %v", err)
	}
	for _, mutate := range []func(*RecoverLocalControlRequest){
		func(r *RecoverLocalControlRequest) { r.Generation++ },
		func(r *RecoverLocalControlRequest) { r.RPCBootNonce += "-wrong" },
		func(r *RecoverLocalControlRequest) { r.OpaquePreparedHandle += "-wrong" },
	} {
		wrong := request
		mutate(&wrong)
		if _, err := client.RecoverLocalControl(ctx, wrong); !errors.Is(err, ErrConflict) {
			t.Fatalf("wrong epoch recovery error=%v", err)
		}
	}
	if backend.calls != 1 {
		t.Fatal("wrong epoch reached secret recovery")
	}
	if backend.prepareCalls[testPAID] != 1 || backend.activateCalls[testPAID] != 1 || backend.stopCalls[testPAID] != 0 {
		t.Fatal("recovery performed a lifecycle mutation")
	}
	if _, err := service.Stop(ctx, StopRequest{Version: ProtocolVersion, PreparedEpoch: epoch}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.RecoverLocalControl(ctx, request); !errors.Is(err, ErrConflict) {
		t.Fatalf("retired epoch recovery error=%v", err)
	}
}

func TestDockerLocalControlRecoveryRejectsWrongEpochAndSecretDiagnostic(t *testing.T) {
	epoch := PreparedEpoch{PersonalityAgentID: testPAID, Generation: 7, RPCBootNonce: "boot", OpaquePreparedHandle: dockerPreparedHandle(testPAID, 7, "boot")}
	wire := map[string]any{"personality_agent_id": testPAID, "generation": 7, "rpc_boot_nonce": "boot", "bearer": strings.Repeat("b", 43), "selection_fingerprint": testActivationConfig().SelectionFingerprint()}
	data, _ := json.Marshal(wire)
	runner := &recordingRunner{outputs: map[string]string{"recover-local-control": string(data)}, failed: make(map[string]bool)}
	backend := &DockerBackend{runner: runner, supervisor: "/fake/supervisor"}
	if _, err := backend.RecoverLocalControl(context.Background(), epoch); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(runner.envs[0], "\n"), "SUMI_EXPECTED_RPC_NONCE=boot") {
		t.Fatal("recovery did not constrain root epoch")
	}
	wrong := epoch
	wrong.Generation++
	if _, err := backend.RecoverLocalControl(context.Background(), wrong); err == nil {
		t.Fatal("wrong response epoch accepted")
	}
	runner.failed["recover-local-control"] = true
	if _, err := backend.RecoverLocalControl(context.Background(), epoch); err == nil || strings.Contains(err.Error(), "secret backend detail") {
		t.Fatal("recovery leaked private diagnostics")
	}
}

// Run the real supervisor recovery function against native file metadata. Only
// Docker role/allocator observations and the root lock are substituted.
func TestSupervisorLocalControlRecoveryChecksEpochAndPrivateFileMetadata(t *testing.T) {
	source := readDeploymentFile(t, "supervisor")
	var functions strings.Builder
	for _, name := range []string{"validate_runtime_secret_anchor", "set_runtime_secret_host_dir", "require_expected_epoch", "recover_local_control"} {
		start := strings.Index(source, name+"() {")
		if start < 0 {
			t.Fatalf("missing supervisor function %s", name)
		}
		end := strings.Index(source[start:], "\n}\n")
		functions.WriteString(source[start : start+end+3])
		functions.WriteByte('\n')
	}
	for _, scenario := range []string{"valid", "valid-minimum", "valid-maximum", "too-short", "too-long", "invalid-character", "trailing-newline", "wrong-epoch", "wrong-mode", "symlink", "hardlink", "wrong-parent-mode", "missing-selection", "inactive"} {
		t.Run(scenario, func(t *testing.T) {
			root := t.TempDir()
			if err := os.Chmod(root, 0o700); err != nil {
				t.Fatal(err)
			}
			paidRoot := filepath.Join(root, "paid")
			generationRoot := filepath.Join(paidRoot, "7")
			if err := os.MkdirAll(generationRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			bearerPath := filepath.Join(generationRoot, "sumi_local_control_bearer")
			bearer := strings.Repeat("b", 43)
			switch scenario {
			case "valid-minimum":
				bearer = strings.Repeat("b", 32)
			case "valid-maximum":
				bearer = strings.Repeat("b", 1024)
			case "too-short":
				bearer = strings.Repeat("b", 31)
			case "too-long":
				bearer = strings.Repeat("b", 1025)
			case "invalid-character":
				bearer = strings.Repeat("b", 42) + "/"
			case "trailing-newline":
				bearer += "\n"
			}
			if err := os.WriteFile(bearerPath, []byte(bearer), 0o400); err != nil {
				t.Fatal(err)
			}
			selectionPath := filepath.Join(generationRoot, "selection-fingerprint")
			if err := os.WriteFile(selectionPath, []byte(testActivationConfig().SelectionFingerprint()), 0o400); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "wrong-mode":
				_ = os.Chmod(bearerPath, 0o600)
			case "symlink":
				_ = os.Rename(bearerPath, bearerPath+"-target")
				_ = os.Symlink(bearerPath+"-target", bearerPath)
			case "hardlink":
				_ = os.Link(bearerPath, bearerPath+"-link")
			case "wrong-parent-mode":
				_ = os.Chmod(paidRoot, 0o750)
			case "missing-selection":
				_ = os.Remove(selectionPath)
			}
			script := "set -eu\nfail() { printf '%s\\n' \"$1\" >&2; exit 23; }\nrun_external() { \"$@\"; }\nacquire_supervisor_lock() { :; }\nrequire_docker() { :; }\nepoch_identity() { EPOCH_GENERATION=7; EPOCH_NONCE=boot; }\nlong_lived_roles_match_expected_state() { test \"$SCENARIO\" != inactive; }\n" + functions.String() + "\nrecover_local_control\n"
			cmd := exec.Command("bash", "-c", script)
			nonce := "boot"
			if scenario == "wrong-epoch" {
				nonce = "wrong"
			}
			cmd.Env = append(os.Environ(), "SCENARIO="+scenario, "SUMI_TEST_ALLOW_NONROOT_SECRET_ROOT=true", "RUNTIME_SECRET_HOST_ROOT="+root, "PAID_COMPACT=paid", "SUMI_PERSONALITY_AGENT_ID="+testPAID, "SUMI_EXPECTED_RPC_GENERATION=7", "SUMI_EXPECTED_RPC_NONCE="+nonce, "RUNTIME_SECRET_UID=10001", "RUNTIME_SECRET_GID=10001")
			output, err := cmd.CombinedOutput()
			if scenario == "valid" || scenario == "valid-minimum" || scenario == "valid-maximum" {
				if err != nil {
					t.Fatalf("valid recovery failed: %v: %s", err, output)
				}
				var recovered map[string]any
				if err := json.Unmarshal(output, &recovered); err != nil {
					t.Fatal("invalid recovery JSON")
				}
				if recovered["bearer"] != bearer || len(recovered) != 5 {
					t.Fatal("recovery did not return the bounded control-only record")
				}
			} else if err == nil || strings.Contains(string(output), bearer) {
				t.Fatalf("invalid recovery was accepted or leaked credential: %s", scenario)
			}
		})
	}
}

func TestActivationSelectionFingerprintTracksConnectionVersionWithoutSecrets(t *testing.T) {
	first := testActivationConfig()
	next := first
	next.ProviderAPIKey = "different-private-key"
	next.AgentWrappingKey = strings.Repeat("c", 64)
	next.LocalControlBearer = "different-control"
	if first.SelectionFingerprint() != next.SelectionFingerprint() {
		t.Fatal("selection identity depends on a secret")
	}
	next.APIConnectionVersion = "new-version"
	if first.SelectionFingerprint() == next.SelectionFingerprint() {
		t.Fatal("connection version was omitted from selection identity")
	}
}
