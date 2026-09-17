package runtimeprovision

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var ErrConflict = errors.New("runtime provision state conflict")

// Service serializes every lifecycle transition for one PAID and makes
// retries idempotent before delegating to the machine backend.
type Service struct {
	backend   Backend
	processes *processStore
	reaps     *durableReapState
	files     *durableFilesBindings
	filesEnv  FilesEnvironment
	mu        sync.Mutex
	entries   map[string]*serviceEntry
}

type ServiceConfig struct {
	StateDirectory string
	// Files is the daemon's canonical-files scope configuration (the
	// SUMI_FILES_* environment). When all three fields are set the supervisor
	// runs in files scope mode and launches bind the canonical volume.
	Files FilesEnvironment
}

// FilesEnvironment is the provisioner's own canonical-files configuration,
// observed once at process start. It is the values the supervisor receives,
// not a per-request input.
type FilesEnvironment struct {
	Mountpoint string
	VolumeUUID string
	CheckPath  string
}

func (environment FilesEnvironment) configured() bool {
	return environment.Mountpoint != "" && environment.VolumeUUID != "" &&
		environment.CheckPath != ""
}

type serviceEntry struct {
	mu             sync.Mutex
	known          bool
	phase          Phase
	epoch          PreparedEpoch
	idempotencyKey string
	stopped        bool
	reapedThrough  *uint64
}

func NewService(backend Backend, config ServiceConfig) (*Service, error) {
	if backend == nil {
		return nil, errors.New("runtime provision service requires a backend")
	}
	reaps, err := newDurableReapState(config.StateDirectory)
	if err != nil {
		return nil, fmt.Errorf("initialize durable reap state: %w", err)
	}
	files, err := newDurableFilesBindings(config.StateDirectory)
	if err != nil {
		return nil, fmt.Errorf("initialize durable files bindings: %w", err)
	}
	service := &Service{
		backend:  backend,
		reaps:    reaps,
		files:    files,
		filesEnv: config.Files,
		entries:  make(map[string]*serviceEntry),
	}
	if processBackend, ok := backend.(ProcessBackend); ok {
		service.processes, err = newProcessStore(config.StateDirectory, processBackend)
		if err != nil {
			return nil, err
		}
	}
	return service, nil
}

func (service *Service) entry(personalityAgentID string) *serviceEntry {
	service.mu.Lock()
	defer service.mu.Unlock()
	entry := service.entries[personalityAgentID]
	if entry == nil {
		entry = &serviceEntry{}
		service.entries[personalityAgentID] = entry
	}
	return entry
}

func (service *Service) Prepare(ctx context.Context, request PrepareRequest) (PreparedEpoch, error) {
	if err := request.Validate(); err != nil {
		return PreparedEpoch{}, err
	}
	entry := service.entry(request.PersonalityAgentID)
	entry.mu.Lock()
	defer entry.mu.Unlock()

	if entry.known && (entry.phase == PhasePrepared || entry.phase == PhaseActive) {
		if entry.idempotencyKey != "" && entry.idempotencyKey != request.IdempotencyKey {
			return PreparedEpoch{}, fmt.Errorf("%w: personality agent already has a live prepared epoch", ErrConflict)
		}
		if err := service.recordFilesBinding(request.PersonalityAgentID); err != nil {
			return PreparedEpoch{}, fmt.Errorf("persist canonical files binding: %w", err)
		}
		return entry.epoch, nil
	}

	// Recover a prepare that committed in the backend before a daemon response
	// was delivered. This check is what prevents a daemon restart or cancelled
	// client from allocating the next generation on retry.
	inspection, err := service.backend.Inspect(ctx, request.PersonalityAgentID)
	if err != nil {
		return PreparedEpoch{}, fmt.Errorf("inspect before prepare: %w", err)
	}
	if err := inspection.Validate(); err != nil {
		return PreparedEpoch{}, fmt.Errorf("backend returned invalid inspection: %w", err)
	}
	if err := service.attachDurableReap(&inspection); err != nil {
		return PreparedEpoch{}, err
	}
	// A verified teardown removes containers but deliberately keeps the
	// allocator's named volume, so the epoch identity written there outlives
	// every stop, abort, and reconcile. The supervisor reports any project that
	// still answers with an identity but is not in a reusable container shape as
	// PhaseRecovery, which would otherwise make a fully reaped personality agent
	// look like it needs recovery on its next spawn and fail every spawn after
	// the first. The durable reap receipt is this daemon's own physical record of
	// an observed-empty project and is the single authority on "already cleaned
	// up"; the surviving identity is not evidence of live processes.
	//
	// Falling through is safe for the covered epoch: the receipt exists only
	// because a fenced teardown of that exact generation already completed, and
	// the backend prepare joins a synchronous `down` before the allocator issues
	// a replacement generation.
	if inspection.Phase == PhaseRecovery &&
		!service.reapReceiptCovers(request.PersonalityAgentID, inspection.Epoch.Generation) {
		return PreparedEpoch{}, fmt.Errorf("%w: runtime requires fenced reconciliation before prepare", ErrConflict)
	}
	if inspection.Phase == PhasePrepared || inspection.Phase == PhaseActive {
		entry.known = true
		entry.phase = inspection.Phase
		entry.epoch = *inspection.Epoch
		entry.idempotencyKey = request.IdempotencyKey
		entry.stopped = false
		if err := service.recordFilesBinding(request.PersonalityAgentID); err != nil {
			return PreparedEpoch{}, fmt.Errorf("persist canonical files binding: %w", err)
		}
		return entry.epoch, nil
	}

	if err := service.checkFilesBinding(request.PersonalityAgentID); err != nil {
		return PreparedEpoch{}, err
	}
	epoch, err := service.backend.Prepare(ctx, request)
	if err != nil {
		return PreparedEpoch{}, err
	}
	if err := epoch.Validate(); err != nil {
		return PreparedEpoch{}, fmt.Errorf("backend returned invalid prepared epoch: %w", err)
	}
	if epoch.PersonalityAgentID != request.PersonalityAgentID {
		return PreparedEpoch{}, errors.New("backend prepared a different personality agent")
	}
	if err := service.recordFilesBinding(request.PersonalityAgentID); err != nil {
		return PreparedEpoch{}, fmt.Errorf("persist canonical files binding: %w", err)
	}
	entry.known = true
	entry.phase = PhasePrepared
	entry.epoch = epoch
	entry.idempotencyKey = request.IdempotencyKey
	entry.stopped = false
	return epoch, nil
}

func (service *Service) Activate(ctx context.Context, request ActivateRequest) (Inspection, error) {
	if err := request.Validate(); err != nil {
		return Inspection{}, err
	}
	if err := service.verifyReapAttestation(request); err != nil {
		return Inspection{}, err
	}
	entry := service.entry(request.PersonalityAgentID)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if err := service.hydrateEntry(ctx, request.PersonalityAgentID, entry); err != nil {
		return Inspection{}, err
	}
	if !entry.known || (entry.phase != PhasePrepared && entry.phase != PhaseActive) || entry.epoch != request.PreparedEpoch {
		return Inspection{}, fmt.Errorf("%w: activate does not match the prepared epoch", ErrConflict)
	}
	if entry.phase == PhaseActive {
		return inspectionOf(entry), nil
	}
	if err := service.checkFilesBinding(request.PersonalityAgentID); err != nil {
		return Inspection{}, err
	}
	if err := service.backend.Activate(ctx, request); err != nil {
		return Inspection{}, err
	}
	entry.phase = PhaseActive
	entry.stopped = false
	return inspectionOf(entry), nil
}

func (service *Service) Abort(ctx context.Context, request AbortRequest) (Inspection, error) {
	if err := request.Validate(); err != nil {
		return Inspection{}, err
	}
	entry := service.entry(request.PersonalityAgentID)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.known && entry.phase == PhaseUnknown && entry.epoch == request.PreparedEpoch {
		return entry.unknownInspection(request.PersonalityAgentID), nil
	}
	if err := service.hydrateEntry(ctx, request.PersonalityAgentID, entry); err != nil {
		return Inspection{}, err
	}
	service.settleReapedRecovery(entry)
	if entry.known && entry.phase == PhaseUnknown {
		return entry.unknownInspection(request.PersonalityAgentID), nil
	}
	if entry.known && entry.phase == PhaseRecovery {
		return Inspection{}, fmt.Errorf("%w: runtime requires fenced reconciliation before abort", ErrConflict)
	}
	if !entry.known || entry.phase == PhaseUnknown || entry.epoch != request.PreparedEpoch {
		return Inspection{}, fmt.Errorf("%w: abort does not match the prepared epoch", ErrConflict)
	}
	inspection, err := service.backend.Abort(ctx, entry.epoch)
	if err != nil {
		return Inspection{}, err
	}
	if err := validateExactReapInspection(inspection, entry.epoch); err != nil {
		return Inspection{}, err
	}
	if err := service.reaps.record(inspection.PersonalityAgentID, *inspection.ReapedThroughGeneration); err != nil {
		return Inspection{}, fmt.Errorf("persist verified abort reap: %w", err)
	}
	entry.phase = PhaseUnknown
	entry.stopped = false
	entry.recordReap(*inspection.ReapedThroughGeneration)
	return inspection, nil
}

func (service *Service) Inspect(ctx context.Context, request InspectRequest) (Inspection, error) {
	if err := request.Validate(); err != nil {
		return Inspection{}, err
	}
	entry := service.entry(request.PersonalityAgentID)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Inspection{}, err
	}
	inspection, err := service.backend.Inspect(ctx, request.PersonalityAgentID)
	if err != nil {
		return Inspection{}, err
	}
	if err := inspection.Validate(); err != nil {
		return Inspection{}, fmt.Errorf("backend returned invalid inspection: %w", err)
	}
	if err := service.attachDurableReap(&inspection); err != nil {
		return Inspection{}, err
	}
	entry.setInspection(inspection)
	return inspection, nil
}

func (service *Service) Stop(ctx context.Context, request StopRequest) (Inspection, error) {
	if err := request.Validate(); err != nil {
		return Inspection{}, err
	}
	entry := service.entry(request.PersonalityAgentID)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.known && entry.phase == PhaseUnknown && entry.epoch == request.PreparedEpoch {
		return entry.unknownInspection(request.PersonalityAgentID), nil
	}
	if err := service.hydrateEntry(ctx, request.PersonalityAgentID, entry); err != nil {
		return Inspection{}, err
	}
	service.settleReapedRecovery(entry)
	if entry.known && entry.phase == PhaseUnknown {
		return entry.unknownInspection(request.PersonalityAgentID), nil
	}
	if !entry.known || entry.phase != PhaseActive || entry.epoch != request.PreparedEpoch {
		return Inspection{}, fmt.Errorf("%w: stop does not match the active epoch", ErrConflict)
	}
	inspection, err := service.backend.Stop(ctx, entry.epoch)
	if err != nil {
		return Inspection{}, err
	}
	if err := validateExactReapInspection(inspection, entry.epoch); err != nil {
		return Inspection{}, err
	}
	if err := service.reaps.record(inspection.PersonalityAgentID, *inspection.ReapedThroughGeneration); err != nil {
		return Inspection{}, fmt.Errorf("persist verified stop reap: %w", err)
	}
	entry.phase = PhaseUnknown
	entry.stopped = true
	entry.recordReap(*inspection.ReapedThroughGeneration)
	return inspection, nil
}

func (service *Service) Reconcile(ctx context.Context, request ReconcileRequest) (Inspection, error) {
	if err := request.Validate(); err != nil {
		return Inspection{}, err
	}
	entry := service.entry(request.PersonalityAgentID)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.known && entry.phase == PhaseRecovery &&
		(request.FencedEpoch == nil || *request.FencedEpoch != entry.epoch) {
		return Inspection{}, fmt.Errorf("%w: recovery reconcile requires its exact fenced epoch", ErrConflict)
	}
	inspection, err := service.backend.Reconcile(ctx, request)
	if err != nil {
		return Inspection{}, err
	}
	if err := inspection.Validate(); err != nil {
		return Inspection{}, fmt.Errorf("backend returned invalid reconciliation: %w", err)
	}
	if err := service.attachDurableReap(&inspection); err != nil {
		return Inspection{}, err
	}
	entry.setInspection(inspection)
	return inspection, nil
}

func (service *Service) hydrateEntry(ctx context.Context, personalityAgentID string, entry *serviceEntry) error {
	if entry.known {
		return nil
	}
	inspection, err := service.backend.Inspect(ctx, personalityAgentID)
	if err != nil {
		return err
	}
	if err := inspection.Validate(); err != nil {
		return fmt.Errorf("backend returned invalid inspection: %w", err)
	}
	if err := service.attachDurableReap(&inspection); err != nil {
		return err
	}
	entry.setInspection(inspection)
	return nil
}

// reapReceiptCovers reports whether this daemon's own durable state already
// holds an observed-empty teardown receipt for every generation through
// generation. It is the single answer to "is this personality agent already
// cleaned up", shared by Prepare's phase classification and by the Activate
// attestation check, so neither one re-derives that judgement from weaker
// evidence such as a surviving allocator identity or a caller's claim.
func (service *Service) reapReceiptCovers(personalityAgentID string, generation uint64) bool {
	reaped, ok := service.reaps.lookup(personalityAgentID)
	return ok && reaped >= generation
}

// settleReapedRecovery collapses a cached recovery classification whose
// generation the durable receipt already covers into the observed-empty result
// it really is.
//
// The allocator volume outlives a verified teardown, so the host keeps
// answering with the retired epoch identity and the supervisor keeps reporting
// the empty project as recovery. A provisioner that restarts between persisting
// the receipt and answering its caller would otherwise reject that caller's
// retry of the teardown it had already completed, and the API would never close
// the personality agent's local-control listener. Teardown must be idempotent
// for the epoch a receipt already covers.
//
// Only the teardown paths use this. Inspect and Reconcile keep reporting
// recovery with its epoch, because the API still needs that epoch to fence
// local control before it asks for a destructive reconciliation.
func (service *Service) settleReapedRecovery(entry *serviceEntry) {
	if !entry.known || entry.phase != PhaseRecovery {
		return
	}
	if !service.reapReceiptCovers(entry.epoch.PersonalityAgentID, entry.epoch.Generation) {
		return
	}
	reaped, _ := service.reaps.lookup(entry.epoch.PersonalityAgentID)
	entry.phase = PhaseUnknown
	entry.stopped = true
	entry.recordReap(reaped)
}

// checkFilesBinding refuses a launch-shaped transition that would silently
// substitute a host-local workspace for a personality agent whose workspace
// was already bound to the canonical files volume. The binding is recorded
// when a files-mode prepare commits and lives in the durable state directory,
// so losing the SUMI_FILES_* environment (recreated provisioner, removed
// overlay) cannot turn the next launch into an unrelated local directory, and
// pointing the configuration at a different mountpoint or volume cannot
// retarget the established binding. Stop/abort/reconcile are deliberately
// ungated: a bound personality agent must remain stoppable and fenced, and
// only launches need the canonical configuration restored.
func (service *Service) checkFilesBinding(personalityAgentID string) error {
	binding, bound := service.files.lookup(personalityAgentID)
	if !bound {
		return nil
	}
	if !service.filesEnv.configured() {
		return fmt.Errorf(
			"%w: personality agent is bound to canonical files volume %s and SUMI_FILES_* configuration is absent or incomplete; refusing host-local workspace substitution",
			ErrConflict, binding.VolumeUUID,
		)
	}
	if service.filesEnv.VolumeUUID != binding.VolumeUUID {
		return fmt.Errorf(
			"%w: personality agent is bound to canonical files volume %s; refusing retarget to volume %s",
			ErrConflict, binding.VolumeUUID, service.filesEnv.VolumeUUID,
		)
	}
	return nil
}

// recordFilesBinding persists the canonical binding once a files-mode prepare
// has committed (including the already-prepared inspection fall-through that
// heals a crash between backend prepare and the binding write). Personality
// agents never launched in files scope mode have no record and keep the
// established local-workspace contract.
func (service *Service) recordFilesBinding(personalityAgentID string) error {
	if !service.filesEnv.configured() {
		return nil
	}
	return service.files.record(personalityAgentID, filesBinding{
		VolumeUUID: service.filesEnv.VolumeUUID,
	})
}

// verifyReapAttestation recomputes a caller's claimed reap receipt against the
// durable record this daemon wrote when it observed the empty project. ADR 0007
// assigns kill/reap and its physical proof to the control plane, so the control
// plane may forward a receipt the provisioner already holds, but it may not
// issue one: the runtime consumes this value as a host observation, not as an
// API assertion. ActivateRequest.Validate only checks that the attestation is
// self-consistent and bound to the prepared epoch; this is the check against
// physical evidence.
func (service *Service) verifyReapAttestation(request ActivateRequest) error {
	attestation := request.Activation.ReapAttestation
	if attestation == nil {
		return nil
	}
	if !service.reapReceiptCovers(request.PersonalityAgentID, attestation.ReapedThroughGeneration) {
		return fmt.Errorf(
			"%w: reap attestation claims a generation no durable teardown receipt covers",
			ErrConflict,
		)
	}
	return nil
}

func (service *Service) attachDurableReap(inspection *Inspection) error {
	if inspection.Phase != PhaseUnknown {
		return nil
	}
	if inspection.ReapedThroughGeneration != nil {
		if err := service.reaps.record(inspection.PersonalityAgentID, *inspection.ReapedThroughGeneration); err != nil {
			return fmt.Errorf("persist reconciled reap: %w", err)
		}
		return nil
	}
	if reaped, ok := service.reaps.lookup(inspection.PersonalityAgentID); ok {
		inspection.ReapedThroughGeneration = &reaped
	}
	return nil
}

func (entry *serviceEntry) setInspection(inspection Inspection) {
	entry.known = true
	entry.phase = inspection.Phase
	entry.stopped = inspection.Phase == PhaseUnknown
	if inspection.Epoch != nil {
		entry.epoch = *inspection.Epoch
	}
	if inspection.ReapedThroughGeneration != nil {
		entry.recordReap(*inspection.ReapedThroughGeneration)
	}
}

func (entry *serviceEntry) recordReap(generation uint64) {
	if entry.reapedThrough == nil || generation > *entry.reapedThrough {
		reaped := generation
		entry.reapedThrough = &reaped
	}
}

func (entry *serviceEntry) unknownInspection(personalityAgentID string) Inspection {
	inspection := unknownInspection(personalityAgentID)
	if entry.reapedThrough != nil {
		reaped := *entry.reapedThrough
		inspection.ReapedThroughGeneration = &reaped
	}
	return inspection
}

func inspectionOf(entry *serviceEntry) Inspection {
	epoch := entry.epoch
	return Inspection{PersonalityAgentID: epoch.PersonalityAgentID, Phase: entry.phase, Epoch: &epoch}
}

func unknownInspection(personalityAgentID string) Inspection {
	return Inspection{PersonalityAgentID: personalityAgentID, Phase: PhaseUnknown}
}

func validateExactReapInspection(inspection Inspection, epoch PreparedEpoch) error {
	if err := inspection.Validate(); err != nil {
		return fmt.Errorf("backend returned invalid reap inspection: %w", err)
	}
	if inspection.PersonalityAgentID != epoch.PersonalityAgentID ||
		inspection.Phase != PhaseUnknown || inspection.ReapedThroughGeneration == nil ||
		*inspection.ReapedThroughGeneration != epoch.Generation {
		return errors.New("backend teardown did not attest the exact retired generation")
	}
	return nil
}
