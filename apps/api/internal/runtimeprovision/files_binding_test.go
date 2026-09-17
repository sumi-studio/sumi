package runtimeprovision

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

const testVolumeUUID = "6abc2f47-1111-2222-3333-444455556666"

func filesTestConfig(stateDirectory string) ServiceConfig {
	return ServiceConfig{
		StateDirectory: stateDirectory,
		Files: FilesEnvironment{
			Mountpoint: "/var/lib/sumi-files/mnt",
			VolumeUUID: testVolumeUUID,
			CheckPath:  "/usr/local/bin/sumi-files-check",
		},
	}
}

func filesTestPrepare(paid string) PrepareRequest {
	return PrepareRequest{
		Version:            ProtocolVersion,
		PersonalityAgentID: paid,
		IdempotencyKey:     "request-1",
	}
}

func filesTestActivate(epoch PreparedEpoch) ActivateRequest {
	return ActivateRequest{
		Version:       ProtocolVersion,
		PreparedEpoch: epoch,
		Activation:    testActivationConfig(),
	}
}

func bindingFileContent(t *testing.T, stateDirectory string) filesBindingsDocument {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(stateDirectory, filesBindingsFileName))
	if err != nil {
		t.Fatalf("read bindings: %v", err)
	}
	var document filesBindingsDocument
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("bindings file is not JSON: %v", err)
	}
	return document
}

func TestFilesBindingRecordedOnCanonicalPrepare(t *testing.T) {
	stateDirectory := filepath.Join(t.TempDir(), "state")
	service, err := NewService(newFakeBackend(), filesTestConfig(stateDirectory))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Prepare(context.Background(), filesTestPrepare(testPAID)); err != nil {
		t.Fatalf("canonical prepare failed: %v", err)
	}
	document := bindingFileContent(t, stateDirectory)
	if document.Bindings[testPAID].VolumeUUID != testVolumeUUID {
		t.Fatalf("binding not recorded: %+v", document.Bindings)
	}
	info, err := os.Stat(filepath.Join(stateDirectory, filesBindingsFileName))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("bindings file mode = %o, want 0600", info.Mode().Perm())
	}
}

func TestFilesBindingSurvivesProvisionerRestart(t *testing.T) {
	stateDirectory := filepath.Join(t.TempDir(), "state")
	backend := newFakeBackend()
	first, err := NewService(backend, filesTestConfig(stateDirectory))
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := first.Prepare(context.Background(), filesTestPrepare(testPAID))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Activate(context.Background(), filesTestActivate(epoch)); err != nil {
		t.Fatal(err)
	}
	// Stop remains ungated: a bound personality agent stays stoppable even
	// when the canonical configuration is about to disappear.
	if _, err := first.Stop(context.Background(), StopRequest{
		Version:       ProtocolVersion,
		PreparedEpoch: epoch,
	}); err != nil {
		t.Fatal(err)
	}
	// A recreated provisioner with the files overlay removed must refuse to
	// launch rather than silently substitute a host-local workspace.
	restarted, err := NewService(backend, ServiceConfig{StateDirectory: stateDirectory})
	if err != nil {
		t.Fatal(err)
	}
	before := backend.prepareCalls[testPAID]
	_, err = restarted.Prepare(context.Background(), filesTestPrepare(testPAID))
	if err == nil || !errors.Is(err, ErrConflict) {
		t.Fatalf("prepare without files configuration err = %v, want conflict", err)
	}
	if backend.prepareCalls[testPAID] != before {
		t.Fatal("refused launch still reached the backend")
	}
}

func TestFilesBindingRefusesRetarget(t *testing.T) {
	stateDirectory := filepath.Join(t.TempDir(), "state")
	backend := newFakeBackend()
	first, err := NewService(backend, filesTestConfig(stateDirectory))
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := first.Prepare(context.Background(), filesTestPrepare(testPAID))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Activate(context.Background(), filesTestActivate(epoch)); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Stop(context.Background(), StopRequest{
		Version:       ProtocolVersion,
		PreparedEpoch: epoch,
	}); err != nil {
		t.Fatal(err)
	}
	config := filesTestConfig(stateDirectory)
	config.Files.VolumeUUID = "ffffffff-0000-0000-0000-000000000000"
	restarted, err := NewService(backend, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Prepare(context.Background(), filesTestPrepare(testPAID)); err == nil || !errors.Is(err, ErrConflict) {
		t.Fatalf("retargeted prepare err = %v, want conflict", err)
	}
}

func TestFilesBindingPermitsSameVolumeRecovery(t *testing.T) {
	stateDirectory := filepath.Join(t.TempDir(), "state")
	backend := newFakeBackend()
	first, err := NewService(backend, filesTestConfig(stateDirectory))
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := first.Prepare(context.Background(), filesTestPrepare(testPAID))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Activate(context.Background(), filesTestActivate(prepared)); err != nil {
		t.Fatal(err)
	}
	// Stop the live project so the next prepare launches rather than reporting.
	if _, err := first.Stop(context.Background(), StopRequest{
		Version:       ProtocolVersion,
		PreparedEpoch: *backend.state[testPAID].Epoch,
	}); err != nil {
		t.Fatal(err)
	}
	// Mountpoint may legitimately change when the same volume is remounted;
	// only the volume identity must match.
	config := filesTestConfig(stateDirectory)
	config.Files.Mountpoint = "/mnt/relocated"
	restarted, err := NewService(backend, config)
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := restarted.Prepare(context.Background(), filesTestPrepare(testPAID))
	if err != nil {
		t.Fatalf("same-volume recovery refused: %v", err)
	}
	if epoch.Generation != 1 {
		t.Fatalf("recovery generation = %d, want 1", epoch.Generation)
	}
}

func TestFilesBindingActivateRefusesWithoutConfiguration(t *testing.T) {
	stateDirectory := filepath.Join(t.TempDir(), "state")
	backend := newFakeBackend()
	first, err := NewService(backend, filesTestConfig(stateDirectory))
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := first.Prepare(context.Background(), filesTestPrepare(testPAID))
	if err != nil {
		t.Fatal(err)
	}
	// The prepared epoch survives in the backend; a restarted provisioner with
	// no files configuration must refuse to activate it. Inspect rebuilds the
	// in-memory entry (observation stays ungated) — Prepare cannot be used to
	// hydrate here because adopting a bound epoch without configuration is
	// itself refused.
	restarted, err := NewService(backend, ServiceConfig{StateDirectory: stateDirectory})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Inspect(context.Background(), InspectRequest{
		Version:            ProtocolVersion,
		PersonalityAgentID: testPAID,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Activate(context.Background(), filesTestActivate(epoch)); err == nil || !errors.Is(err, ErrConflict) {
		t.Fatalf("activate without files configuration err = %v, want conflict", err)
	}
	if backend.activateCalls[testPAID] != 0 {
		t.Fatal("refused activate still reached the backend")
	}
}

func TestFilesBindingUnboundPlacementKeepsLocalContract(t *testing.T) {
	stateDirectory := filepath.Join(t.TempDir(), "state")
	service, err := NewService(newFakeBackend(), ServiceConfig{StateDirectory: stateDirectory})
	if err != nil {
		t.Fatal(err)
	}
	// A personality agent never launched in files scope mode has no binding
	// and keeps the established local-development contract.
	if _, err := service.Prepare(context.Background(), filesTestPrepare(testPAID2)); err != nil {
		t.Fatalf("unbound local prepare refused: %v", err)
	}
	if _, err := os.Stat(filepath.Join(stateDirectory, filesBindingsFileName)); err == nil {
		document := bindingFileContent(t, stateDirectory)
		if _, bound := document.Bindings[testPAID2]; bound {
			t.Fatal("local-mode prepare recorded a canonical binding")
		}
	}
}

func TestFilesBindingNotRecordedOnFailedPrepare(t *testing.T) {
	stateDirectory := filepath.Join(t.TempDir(), "state")
	backend := newFakeBackend()
	backend.failPrepare = true
	service, err := NewService(backend, filesTestConfig(stateDirectory))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Prepare(context.Background(), filesTestPrepare(testPAID)); err == nil {
		t.Fatal("expected prepare failure")
	}
	if _, bound := service.files.lookup(testPAID); bound {
		t.Fatal("failed canonical prepare recorded a binding")
	}
	// A subsequent launch without files configuration is unconstrained: no
	// canonical workspace was ever established for this personality agent.
	backend.failPrepare = false
	local, err := NewService(backend, ServiceConfig{StateDirectory: stateDirectory})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := local.Prepare(context.Background(), filesTestPrepare(testPAID)); err != nil {
		t.Fatalf("local prepare after failed canonical prepare refused: %v", err)
	}
}

// TestFilesBindingAdoptCannotOverwriteAuthority is the discriminating
// regression for configuration replacing the recorded canonical binding while
// an already-live epoch is adopted: launch and bind volume A, recreate the
// provisioner with configuration for volume B while the A-bound epoch is
// still active, then adopt via Prepare. The old ordering recorded current
// environment before the binding gate ran, so adopting the live epoch
// persisted B over A and a later stop/prepare could silently retarget. The
// adopt must now refuse, write nothing, and leave the A binding in force.
func TestFilesBindingAdoptCannotOverwriteAuthority(t *testing.T) {
	for _, inspectFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("inspectFirst=%t", inspectFirst), func(t *testing.T) {
			stateDirectory := filepath.Join(t.TempDir(), "state")
			backend := newFakeBackend()
			first, err := NewService(backend, filesTestConfig(stateDirectory))
			if err != nil {
				t.Fatal(err)
			}
			epoch, err := first.Prepare(context.Background(), filesTestPrepare(testPAID))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := first.Activate(context.Background(), filesTestActivate(epoch)); err != nil {
				t.Fatal(err)
			}
			// Recreate the provisioner with configuration for a different
			// volume while the A-bound epoch is still active. The physical
			// workspace bind is still volume A, so the supervisor cannot
			// report files_scope=bound under this environment.
			config := filesTestConfig(stateDirectory)
			config.Files.VolumeUUID = "ffffffff-0000-0000-0000-000000000000"
			restarted, err := NewService(backend, config)
			if err != nil {
				t.Fatal(err)
			}
			if inspectFirst {
				if _, err := restarted.Inspect(context.Background(), InspectRequest{
					Version:            ProtocolVersion,
					PersonalityAgentID: testPAID,
				}); err != nil {
					t.Fatal(err)
				}
			}
			_, err = restarted.Prepare(context.Background(), filesTestPrepare(testPAID))
			if err == nil || !errors.Is(err, ErrConflict) {
				t.Fatalf("adopting the live epoch under different configuration err = %v, want conflict", err)
			}
			if document := bindingFileContent(t, stateDirectory); document.Bindings[testPAID].VolumeUUID != testVolumeUUID {
				t.Fatalf("adopt overwrote the recorded binding: %+v", document.Bindings)
			}
			// Stop remains ungated, and the refused adopt must not have
			// silently retargeted the authority: the next launch attempt is
			// still constrained by the original volume.
			if _, err := restarted.Stop(context.Background(), StopRequest{
				Version:       ProtocolVersion,
				PreparedEpoch: epoch,
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := restarted.Prepare(context.Background(), filesTestPrepare(testPAID)); err == nil || !errors.Is(err, ErrConflict) {
				t.Fatalf("post-stop prepare under different configuration err = %v, want conflict", err)
			}
			if document := bindingFileContent(t, stateDirectory); document.Bindings[testPAID].VolumeUUID != testVolumeUUID {
				t.Fatalf("binding lost original authority: %+v", document.Bindings)
			}
			// Restoring the matching configuration permits ordinary recovery.
			recovered, err := NewService(backend, filesTestConfig(stateDirectory))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := recovered.Prepare(context.Background(), filesTestPrepare(testPAID)); err != nil {
				t.Fatalf("same-volume relaunch refused: %v", err)
			}
		})
	}
}

// An adopt of a bound live epoch under a provisioner that lost its files
// configuration must refuse rather than report success: without it, callers
// would see a prepared epoch and proceed to activate a workspace the process
// can no longer describe.
func TestFilesBindingAdoptRefusesWithoutConfiguration(t *testing.T) {
	stateDirectory := filepath.Join(t.TempDir(), "state")
	backend := newFakeBackend()
	first, err := NewService(backend, filesTestConfig(stateDirectory))
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := first.Prepare(context.Background(), filesTestPrepare(testPAID))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Activate(context.Background(), filesTestActivate(epoch)); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewService(backend, ServiceConfig{StateDirectory: stateDirectory})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Inspect(context.Background(), InspectRequest{
		Version:            ProtocolVersion,
		PersonalityAgentID: testPAID,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Prepare(context.Background(), filesTestPrepare(testPAID)); err == nil || !errors.Is(err, ErrConflict) {
		t.Fatalf("adopt without files configuration err = %v, want conflict", err)
	}
	if document := bindingFileContent(t, stateDirectory); document.Bindings[testPAID].VolumeUUID != testVolumeUUID {
		t.Fatalf("binding changed under missing configuration: %+v", document.Bindings)
	}
}

// A binding record lost to state-directory repair is healed only on verified
// physical evidence — the supervisor confirming the live workspace bind is
// the configured scope — never on environment alone.
func TestFilesBindingAdoptHealsOnlyOnVerifiedScope(t *testing.T) {
	for _, verified := range []bool{true, false} {
		t.Run(fmt.Sprintf("verified=%t", verified), func(t *testing.T) {
			stateDirectory := filepath.Join(t.TempDir(), "state")
			backend := newFakeBackend()
			first, err := NewService(backend, filesTestConfig(stateDirectory))
			if err != nil {
				t.Fatal(err)
			}
			epoch, err := first.Prepare(context.Background(), filesTestPrepare(testPAID))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := first.Activate(context.Background(), filesTestActivate(epoch)); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(stateDirectory, filesBindingsFileName)); err != nil {
				t.Fatal(err)
			}
			if verified {
				backend.filesScope = FilesScopeBound
			}
			restarted, err := NewService(backend, filesTestConfig(stateDirectory))
			if err != nil {
				t.Fatal(err)
			}
			adopted, err := restarted.Prepare(context.Background(), filesTestPrepare(testPAID))
			if err != nil {
				t.Fatalf("adopting the live epoch failed: %v", err)
			}
			if adopted != epoch {
				t.Fatalf("adopted epoch %+v, want %+v", adopted, epoch)
			}
			if _, err := os.Stat(filepath.Join(stateDirectory, filesBindingsFileName)); verified {
				if err != nil {
					t.Fatalf("verified adopt did not heal the binding record: %v", err)
				}
				if document := bindingFileContent(t, stateDirectory); document.Bindings[testPAID].VolumeUUID != testVolumeUUID {
					t.Fatalf("healed binding = %+v", document.Bindings)
				}
			} else if err == nil {
				if document := bindingFileContent(t, stateDirectory); len(document.Bindings) != 0 {
					t.Fatalf("unverified adopt recorded a binding from environment alone: %+v", document.Bindings)
				}
			}
		})
	}
}

// The durable store itself refuses overwrite: a record call carrying a
// different volume than the established binding fails instead of replacing
// it, so no caller can accidentally retarget canonical authority.
func TestFilesBindingStoreRefusesOverwrite(t *testing.T) {
	stateDirectory := filepath.Join(t.TempDir(), "state")
	service, err := NewService(newFakeBackend(), filesTestConfig(stateDirectory))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Prepare(context.Background(), filesTestPrepare(testPAID)); err != nil {
		t.Fatal(err)
	}
	err = service.files.record(testPAID, filesBinding{VolumeUUID: "ffffffff-0000-0000-0000-000000000000"})
	if err == nil || !errors.Is(err, ErrConflict) {
		t.Fatalf("overwriting record err = %v, want conflict", err)
	}
	if document := bindingFileContent(t, stateDirectory); document.Bindings[testPAID].VolumeUUID != testVolumeUUID {
		t.Fatalf("binding overwritten: %+v", document.Bindings)
	}
	if err := service.files.record(testPAID, filesBinding{VolumeUUID: testVolumeUUID}); err != nil {
		t.Fatalf("identical record must stay idempotent: %v", err)
	}
}

func TestFilesBindingRejectsUnsafeStateFile(t *testing.T) {
	stateDirectory := filepath.Join(t.TempDir(), "state")
	first, err := NewService(newFakeBackend(), filesTestConfig(stateDirectory))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Prepare(context.Background(), filesTestPrepare(testPAID)); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(stateDirectory, filesBindingsFileName)
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewService(newFakeBackend(), filesTestConfig(stateDirectory)); err == nil {
		t.Fatal("group-readable bindings file was accepted")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewService(newFakeBackend(), filesTestConfig(stateDirectory)); err == nil {
		t.Fatal("corrupt bindings file was accepted")
	}
}
