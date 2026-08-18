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
	backend Backend
	reaps   *durableReapState
	mu      sync.Mutex
	entries map[string]*serviceEntry
}

type ServiceConfig struct {
	StateDirectory string
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
	return &Service{backend: backend, reaps: reaps, entries: make(map[string]*serviceEntry)}, nil
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
		return entry.epoch, nil
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
