package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
	"github.com/sumi-studio/sumi/apps/api/internal/spawn"
)

const defaultProvisionedLifecycleTimeout = 20 * time.Minute
const defaultProvisionedTeardownTimeout = 90 * time.Second
const defaultProvisionedStartupReadyTimeout = 30 * time.Second

type runtimeProvisioner interface {
	Prepare(context.Context, runtimeprovision.PrepareRequest) (runtimeprovision.PreparedEpoch, error)
	Activate(context.Context, runtimeprovision.ActivateRequest) (runtimeprovision.Inspection, error)
	Abort(context.Context, runtimeprovision.AbortRequest) (runtimeprovision.Inspection, error)
	Inspect(context.Context, runtimeprovision.InspectRequest) (runtimeprovision.Inspection, error)
	Stop(context.Context, runtimeprovision.StopRequest) (runtimeprovision.Inspection, error)
	Reconcile(context.Context, runtimeprovision.ReconcileRequest) (runtimeprovision.Inspection, error)
	RecoverLocalControl(context.Context, runtimeprovision.RecoverLocalControlRequest) (runtimeprovision.RecoveredLocalControl, error)
}

type localRuntimeAuthorizationController interface {
	InstallLocalRuntimeAuthorization(context.Context, agentevents.LocalRuntimeAuthorization) error
	FenceLocalRuntimeAuthorization(context.Context, string, uint64, string) error
	RecoverLocalRuntimeAuthorization(context.Context, agentevents.LocalRuntimeAuthorization) error
}

type localRuntimeListenerController interface {
	EnsureLocalRuntime(string) error
	CloseLocalRuntime(context.Context, string) error
}

type runtimeReadinessController interface {
	Observe(context.Context, agentevents.TokenClaims, uint64) (agentevents.HydrationObservation, error)
}

type provisionedRuntimeSpawnerConfig struct {
	Provisioner         runtimeProvisioner
	Authorizations      localRuntimeAuthorizationController
	Listeners           localRuntimeListenerController
	Readiness           runtimeReadinessController
	TenantID            string
	Audience            string
	Delivery            agentevents.LocalDeliveryAuthorization
	LifecycleTimeout    time.Duration
	TeardownTimeout     time.Duration
	StartupReadyTimeout time.Duration
	Activation          runtimeprovision.ActivationConfig
	ResolveActivation   runtimeActivationResolver
	OnRecovered         func(context.Context, string) error
}

// provisionedRuntimeSpawner is the only production lazy-spawn implementation.
// It cannot execute host processes or reach Docker: its sole host capability is
// the typed root-provisioner Unix protocol.
type provisionedRuntimeSpawner struct {
	config      provisionedRuntimeSpawnerConfig
	selectionMu sync.Mutex
	selections  map[string]string
}

func newProvisionedRuntimeSpawner(config provisionedRuntimeSpawnerConfig) (*provisionedRuntimeSpawner, error) {
	if config.Provisioner == nil || config.Authorizations == nil || config.Listeners == nil || config.Readiness == nil {
		return nil, errors.New("provisioned runtime spawner requires provisioner, authorization, listener, and readiness controllers")
	}
	if config.TenantID == "" || config.Audience == "" {
		return nil, errors.New("provisioned runtime spawner requires tenant and audience")
	}
	if config.Delivery != agentevents.LocalDeliveryRaw &&
		config.Delivery != agentevents.LocalDeliveryRedactionOnly {
		return nil, errors.New("provisioned runtime spawner requires a valid delivery authorization")
	}
	if config.LifecycleTimeout <= 0 {
		config.LifecycleTimeout = defaultProvisionedLifecycleTimeout
	}
	if config.TeardownTimeout <= 0 {
		config.TeardownTimeout = defaultProvisionedTeardownTimeout
	}
	if config.StartupReadyTimeout <= 0 {
		config.StartupReadyTimeout = defaultProvisionedStartupReadyTimeout
	}
	return &provisionedRuntimeSpawner{config: config, selections: make(map[string]string)}, nil
}

func (s *provisionedRuntimeSpawner) Spawn(
	ctx context.Context,
	config spawn.AgentRuntimeConfig,
) (spawn.Process, error) {
	if err := runtimeprovision.ValidatePersonalityAgentID(config.AgentID); err != nil {
		return nil, err
	}
	if process, err := s.Restore(ctx, config); err != nil || process != nil {
		return process, err
	}
	if err := runtimeprovision.ValidateAgentWrappingKey(config.WrappingKey.Bytes); err != nil {
		return nil, err
	}
	if err := runtimeprovision.ValidateAgentWrappingKeyID(config.WrappingKey.ID); err != nil {
		return nil, err
	}
	reapedThroughGeneration, err := s.reconcilePreviousRuntime(ctx, config.AgentID)
	if err != nil {
		return nil, err
	}
	idempotencyKey, err := randomProvisioningSecret()
	if err != nil {
		return nil, fmt.Errorf("generate prepare idempotency key: %w", err)
	}
	epoch, err := s.config.Provisioner.Prepare(ctx, runtimeprovision.PrepareRequest{
		Version:            runtimeprovision.ProtocolVersion,
		PersonalityAgentID: config.AgentID,
		IdempotencyKey:     idempotencyKey,
	})
	if err != nil {
		return nil, err
	}

	cleanup := func(cause error) error {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), s.config.TeardownTimeout)
		defer cancel()
		fenceErr := s.config.Authorizations.FenceLocalRuntimeAuthorization(
			cleanupCtx,
			epoch.PersonalityAgentID,
			epoch.Generation,
			epoch.RPCBootNonce,
		)
		_, abortErr := s.config.Provisioner.Abort(cleanupCtx, runtimeprovision.AbortRequest{
			Version:       runtimeprovision.ProtocolVersion,
			PreparedEpoch: epoch,
		})
		var listenerErr error
		if abortErr == nil {
			listenerErr = s.config.Listeners.CloseLocalRuntime(cleanupCtx, epoch.PersonalityAgentID)
		}
		return errors.Join(cause, fenceErr, listenerErr, abortErr)
	}
	retireActive := func(cause error) error {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), s.config.TeardownTimeout)
		defer cancel()
		fenceErr := s.config.Authorizations.FenceLocalRuntimeAuthorization(
			cleanupCtx,
			epoch.PersonalityAgentID,
			epoch.Generation,
			epoch.RPCBootNonce,
		)
		_, stopErr := s.config.Provisioner.Stop(cleanupCtx, runtimeprovision.StopRequest{
			Version:       runtimeprovision.ProtocolVersion,
			PreparedEpoch: epoch,
		})
		listenerErr := s.config.Listeners.CloseLocalRuntime(cleanupCtx, epoch.PersonalityAgentID)
		if stopErr != nil {
			inspection, inspectErr := s.config.Provisioner.Inspect(cleanupCtx, runtimeprovision.InspectRequest{
				Version:            runtimeprovision.ProtocolVersion,
				PersonalityAgentID: epoch.PersonalityAgentID,
			})
			if inspectErr == nil &&
				inspection.Phase == runtimeprovision.PhaseRecovery &&
				inspection.Epoch != nil && *inspection.Epoch == epoch {
				reconcileErr := fenceAndReconcileRecovery(
					cleanupCtx,
					s.config.Provisioner,
					s.config.Authorizations,
					s.config.Listeners,
					epoch,
				)
				// Stop deliberately rejects a recovery-shaped project. Once the
				// exact fenced reconcile has observed it empty, that rejection is
				// no longer a teardown failure; Spawn still returns its original
				// pre-Ready failure.
				return errors.Join(cause, fenceErr, listenerErr, reconcileErr)
			}
			return errors.Join(cause, fenceErr, listenerErr, stopErr, inspectErr)
		}
		return errors.Join(cause, fenceErr, listenerErr, stopErr)
	}

	bearer, err := randomProvisioningSecret()
	if err != nil {
		return nil, cleanup(fmt.Errorf("generate local-control bearer: %w", err))
	}
	authorization := agentevents.LocalRuntimeAuthorization{
		BearerToken:           bearer,
		TenantID:              s.config.TenantID,
		PersonalityAgentID:    epoch.PersonalityAgentID,
		Generation:            epoch.Generation,
		RPCBootNonce:          epoch.RPCBootNonce,
		Audience:              s.config.Audience,
		DeliveryAuthorization: s.config.Delivery,
	}
	if err := s.config.Authorizations.InstallLocalRuntimeAuthorization(ctx, authorization); err != nil {
		return nil, cleanup(fmt.Errorf("install prepared runtime authorization: %w", err))
	}
	if err := s.config.Listeners.EnsureLocalRuntime(epoch.PersonalityAgentID); err != nil {
		return nil, cleanup(fmt.Errorf("ensure PAID-bound local-control listener: %w", err))
	}

	activation := s.config.Activation
	if s.config.ResolveActivation != nil {
		activation, err = s.config.ResolveActivation(ctx, config.AgentID, activation)
		if err != nil {
			return nil, cleanup(errors.New("resolve PA model connection failed"))
		}
	}
	activation.GatewayURL = config.GatewayURL
	activation.LocalControlBearer = bearer
	activation.AgentWrappingKey = config.WrappingKey.Bytes
	activation.AgentWrappingKeyID = config.WrappingKey.ID
	if reapedThroughGeneration != nil {
		activation.ReapAttestation = &runtimeprovision.ReapAttestation{
			PersonalityAgentID:      epoch.PersonalityAgentID,
			EpochGeneration:         epoch.Generation,
			RPCBootNonce:            epoch.RPCBootNonce,
			ReapedThroughGeneration: *reapedThroughGeneration,
		}
	} else {
		activation.ReapAttestation = nil
	}
	inspection, err := s.config.Provisioner.Activate(ctx, runtimeprovision.ActivateRequest{
		Version:       runtimeprovision.ProtocolVersion,
		PreparedEpoch: epoch,
		Activation:    activation,
	})
	if err != nil {
		return nil, cleanup(fmt.Errorf("activate prepared runtime: %w", err))
	}
	if inspection.Phase != runtimeprovision.PhaseActive ||
		inspection.Epoch == nil || *inspection.Epoch != epoch {
		return nil, cleanup(errors.New("provisioner activation did not confirm the exact prepared epoch"))
	}
	if err := s.awaitRuntimeReady(ctx, epoch); err != nil {
		return nil, retireActive(fmt.Errorf("runtime failed startup readiness: %w", err))
	}
	s.selectionMu.Lock()
	s.selections[epoch.PersonalityAgentID] = activation.SelectionFingerprint()
	s.selectionMu.Unlock()

	return s.process(epoch), nil
}

func (s *provisionedRuntimeSpawner) process(epoch runtimeprovision.PreparedEpoch) *provisionedProcess {
	return &provisionedProcess{
		provisioner:     s.config.Provisioner,
		authorizations:  s.config.Authorizations,
		listeners:       s.config.Listeners,
		epoch:           epoch,
		timeout:         s.config.LifecycleTimeout,
		teardownTimeout: s.config.TeardownTimeout,
		monitorInterval: 5 * time.Second,
		done:            make(chan struct{}),
	}
}

// Restore never fences or retires a live process on recovery failure. The root
// service remains its physical owner while the next API attempt can retry.
func (s *provisionedRuntimeSpawner) Restore(ctx context.Context, config spawn.AgentRuntimeConfig) (spawn.Process, error) {
	inspection, err := s.config.Provisioner.Inspect(ctx, runtimeprovision.InspectRequest{
		Version: runtimeprovision.ProtocolVersion, PersonalityAgentID: config.AgentID,
	})
	if err != nil {
		return nil, fmt.Errorf("inspect runtime for API attachment: %w", err)
	}
	if inspection.Phase != runtimeprovision.PhaseActive {
		return nil, nil
	}
	if inspection.Epoch == nil || inspection.PersonalityAgentID != config.AgentID {
		return nil, errors.New("runtime attachment inspection has no coherent epoch")
	}
	epoch := *inspection.Epoch
	recovered, err := s.config.Provisioner.RecoverLocalControl(ctx, runtimeprovision.RecoverLocalControlRequest{
		Version: runtimeprovision.ProtocolVersion, PreparedEpoch: epoch,
	})
	if err != nil {
		return nil, fmt.Errorf("recover active runtime attachment: %w", err)
	}
	if recovered.PreparedEpoch != epoch || recovered.SelectionFingerprint == "" {
		return nil, errors.New("runtime attachment has no exact admitted configuration")
	}
	if err := s.config.Authorizations.RecoverLocalRuntimeAuthorization(ctx, agentevents.LocalRuntimeAuthorization{
		BearerToken: recovered.Bearer, TenantID: s.config.TenantID,
		PersonalityAgentID: epoch.PersonalityAgentID, Generation: epoch.Generation,
		RPCBootNonce: epoch.RPCBootNonce, Audience: s.config.Audience,
		DeliveryAuthorization: s.config.Delivery,
	}); err != nil {
		if errors.Is(err, agentevents.ErrLocalRuntimeEpochTerminal) {
			// The previous API already committed an exact terminal stop fence.
			// Finish only that physical cleanup; never reinterpret an arbitrary
			// recovery failure as authority to stop established work.
			if stopErr := s.process(epoch).Stop(); stopErr != nil {
				return nil, fmt.Errorf("finish durably requested runtime stop: %w", stopErr)
			}
			return nil, nil
		}
		return nil, fmt.Errorf("restore runtime authority: %w", err)
	}
	// The bearer and durable generation are installed before the socket is
	// reachable; no reconnect sees an empty authorization registry.
	if err := s.config.Listeners.EnsureLocalRuntime(config.AgentID); err != nil {
		return nil, fmt.Errorf("restore runtime listener: %w", err)
	}
	if err := s.requireExactActiveEpoch(ctx, epoch); err != nil {
		return nil, err
	}
	s.selectionMu.Lock()
	s.selections[config.AgentID] = recovered.SelectionFingerprint
	s.selectionMu.Unlock()
	if s.config.OnRecovered != nil {
		if err := s.config.OnRecovered(ctx, config.AgentID); err != nil {
			return nil, err
		}
	}
	return s.process(epoch), nil
}

func (s *provisionedRuntimeSpawner) ConfigurationCurrent(ctx context.Context, agentID string) (bool, error) {
	s.selectionMu.Lock()
	fingerprint, known := s.selections[agentID]
	s.selectionMu.Unlock()
	if !known {
		return false, nil
	}
	activation := s.config.Activation
	if s.config.ResolveActivation != nil {
		var err error
		activation, err = s.config.ResolveActivation(ctx, agentID, activation)
		if err != nil {
			return false, err
		}
	}
	return activation.SelectionFingerprint() == fingerprint, nil
}

func (s *provisionedRuntimeSpawner) awaitRuntimeReady(
	ctx context.Context,
	epoch runtimeprovision.PreparedEpoch,
) error {
	readyCtx, cancel := context.WithTimeout(ctx, s.config.StartupReadyTimeout)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := s.requireExactActiveEpoch(readyCtx, epoch); err != nil {
			return err
		}
		observation, err := s.config.Readiness.Observe(
			readyCtx,
			agentevents.TokenClaims{
				PersonalityAgentID: epoch.PersonalityAgentID,
				Generation:         epoch.Generation,
			},
			epoch.Generation,
		)
		if err != nil {
			return fmt.Errorf("observe exact runtime readiness: %w", err)
		}
		if observation.TerminalNotReady {
			return errors.New("runtime entered terminal NotReady before Ready")
		}
		// Ready and process liveness are separate authorities. Reconcile again
		// after observing Ready so a runtime that exited concurrently cannot be
		// returned as a healthy process.
		if err := s.requireExactActiveEpoch(readyCtx, epoch); err != nil {
			return err
		}
		if observation.Ready {
			return nil
		}
		select {
		case <-readyCtx.Done():
			return readyCtx.Err()
		case <-ticker.C:
		}
	}
}

func (s *provisionedRuntimeSpawner) requireExactActiveEpoch(
	ctx context.Context,
	epoch runtimeprovision.PreparedEpoch,
) error {
	inspection, err := s.config.Provisioner.Inspect(ctx, runtimeprovision.InspectRequest{
		Version:            runtimeprovision.ProtocolVersion,
		PersonalityAgentID: epoch.PersonalityAgentID,
	})
	if err != nil {
		return fmt.Errorf("inspect active runtime before Ready: %w", err)
	}
	if inspection.Phase != runtimeprovision.PhaseActive ||
		inspection.Epoch == nil || *inspection.Epoch != epoch {
		return errors.New("runtime left its exact active epoch before Ready")
	}
	return nil
}

func (s *provisionedRuntimeSpawner) reconcilePreviousRuntime(ctx context.Context, personalityAgentID string) (*uint64, error) {
	inspection, err := s.config.Provisioner.Inspect(ctx, runtimeprovision.InspectRequest{
		Version:            runtimeprovision.ProtocolVersion,
		PersonalityAgentID: personalityAgentID,
	})
	if err != nil {
		return nil, fmt.Errorf("inspect previous runtime before reconcile: %w", err)
	}
	if inspection.Phase == runtimeprovision.PhaseActive {
		return nil, errors.New("active runtime appeared during startup; retry API attachment")
	}
	var fencedEpoch *runtimeprovision.PreparedEpoch
	if inspection.Epoch != nil {
		epoch := *inspection.Epoch
		if err := s.fenceLocalRuntimeBeforeReap(epoch); err != nil {
			return nil, err
		}
		fencedEpoch = &epoch
	}
	inspection, err = s.config.Provisioner.Reconcile(ctx, runtimeprovision.ReconcileRequest{
		Version:            runtimeprovision.ProtocolVersion,
		PersonalityAgentID: personalityAgentID,
		FencedEpoch:        fencedEpoch,
	})
	if err != nil {
		return nil, fmt.Errorf("reconcile previous runtime: %w", err)
	}
	if inspection.Phase == runtimeprovision.PhaseUnknown {
		if inspection.ReapedThroughGeneration == nil {
			if fencedEpoch != nil {
				return nil, errors.New("reconciled runtime did not return an observed-empty reap receipt")
			}
			return nil, nil
		}
		reaped := *inspection.ReapedThroughGeneration
		if fencedEpoch != nil && reaped < fencedEpoch.Generation {
			return nil, errors.New("reconcile reaped a generation older than the fenced runtime")
		}
		return &reaped, nil
	}
	if inspection.Epoch == nil {
		return nil, errors.New("reconcile returned a live phase without an epoch")
	}
	epoch := *inspection.Epoch
	if fencedEpoch == nil || *fencedEpoch != epoch {
		if err := s.fenceLocalRuntimeBeforeReap(epoch); err != nil {
			return nil, err
		}
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), s.config.TeardownTimeout)
	defer cancel()
	var teardown runtimeprovision.Inspection
	var lifecycleErr error
	switch inspection.Phase {
	case runtimeprovision.PhasePrepared:
		teardown, lifecycleErr = s.config.Provisioner.Abort(cleanupCtx, runtimeprovision.AbortRequest{
			Version:       runtimeprovision.ProtocolVersion,
			PreparedEpoch: epoch,
		})
	case runtimeprovision.PhaseActive:
		teardown, lifecycleErr = s.config.Provisioner.Stop(cleanupCtx, runtimeprovision.StopRequest{
			Version:       runtimeprovision.ProtocolVersion,
			PreparedEpoch: epoch,
		})
	default:
		lifecycleErr = fmt.Errorf("reconcile returned unsupported phase %q", inspection.Phase)
	}
	if lifecycleErr != nil {
		return nil, fmt.Errorf("retire reconciled runtime: %w", lifecycleErr)
	}
	if teardown.Phase != runtimeprovision.PhaseUnknown ||
		teardown.PersonalityAgentID != epoch.PersonalityAgentID ||
		teardown.ReapedThroughGeneration == nil ||
		*teardown.ReapedThroughGeneration != epoch.Generation {
		return nil, errors.New("retired runtime did not return an exact observed-empty reap receipt")
	}
	reaped := *teardown.ReapedThroughGeneration
	return &reaped, nil
}

// fenceLocalRuntimeBeforeReap revokes both the bearer/Ready authority and its
// listener before any call that can destroy this epoch's Compose project.
func (s *provisionedRuntimeSpawner) fenceLocalRuntimeBeforeReap(epoch runtimeprovision.PreparedEpoch) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), s.config.TeardownTimeout)
	defer cancel()
	fenceErr := s.config.Authorizations.FenceLocalRuntimeAuthorization(
		cleanupCtx,
		epoch.PersonalityAgentID,
		epoch.Generation,
		epoch.RPCBootNonce,
	)
	if fenceErr != nil {
		return fmt.Errorf("fence local runtime before reap: %w", fenceErr)
	}
	if err := s.config.Listeners.CloseLocalRuntime(cleanupCtx, epoch.PersonalityAgentID); err != nil {
		return fmt.Errorf("close local runtime before reap: %w", err)
	}
	return nil
}

// fenceAndReconcileRecovery removes a partial project only after revoking the
// exact epoch's local authority. Reconcile's observed-empty receipt is the
// proof that no runtime containers were stranded by a failed lifecycle call.
func fenceAndReconcileRecovery(
	ctx context.Context,
	provisioner runtimeProvisioner,
	authorizations localRuntimeAuthorizationController,
	listeners localRuntimeListenerController,
	epoch runtimeprovision.PreparedEpoch,
) error {
	if err := authorizations.FenceLocalRuntimeAuthorization(
		ctx,
		epoch.PersonalityAgentID,
		epoch.Generation,
		epoch.RPCBootNonce,
	); err != nil {
		return fmt.Errorf("fence recovering runtime before reconcile: %w", err)
	}
	if err := listeners.CloseLocalRuntime(ctx, epoch.PersonalityAgentID); err != nil {
		return fmt.Errorf("close recovering runtime before reconcile: %w", err)
	}
	inspection, err := provisioner.Reconcile(ctx, runtimeprovision.ReconcileRequest{
		Version:            runtimeprovision.ProtocolVersion,
		PersonalityAgentID: epoch.PersonalityAgentID,
		FencedEpoch:        &epoch,
	})
	if err != nil {
		return fmt.Errorf("reconcile recovering runtime: %w", err)
	}
	if inspection.Phase != runtimeprovision.PhaseUnknown ||
		inspection.PersonalityAgentID != epoch.PersonalityAgentID ||
		inspection.ReapedThroughGeneration == nil ||
		*inspection.ReapedThroughGeneration != epoch.Generation {
		return errors.New("recovering runtime reconcile did not return an exact observed-empty reap receipt")
	}
	return nil
}

type provisionedProcess struct {
	provisioner     runtimeProvisioner
	authorizations  localRuntimeAuthorizationController
	listeners       localRuntimeListenerController
	epoch           runtimeprovision.PreparedEpoch
	timeout         time.Duration
	teardownTimeout time.Duration
	monitorInterval time.Duration
	// done wakes Wait after a teardown attempt; stopErr distinguishes incomplete
	// cleanup from physical completion. Failures can be retried under stopMu.
	done          chan struct{}
	stopMu        sync.Mutex
	monitorCancel context.CancelFunc
	doneOnce      sync.Once
	stopped       bool
	detached      bool
	retiring      bool
	stopErr       error
}

func (p *provisionedProcess) Wait() error {
	interval := p.monitorInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	// Inspection availability is independent of runtime liveness. Keep lifecycle
	// ownership while observation is unavailable; only a positive epoch loss or
	// an explicit Stop retires the process. Returning from Wait would release
	// manager ownership and allow an overlapping replacement.
	observationLost := false
	monitorCtx, cancelMonitor := context.WithCancel(context.Background())
	defer cancelMonitor()
	p.stopMu.Lock()
	p.monitorCancel = cancelMonitor
	p.stopMu.Unlock()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			p.stopMu.Lock()
			err := p.stopErr
			detached := p.detached
			p.stopMu.Unlock()
			if err != nil {
				return errors.Join(spawn.ErrCleanupIncomplete, err)
			}
			if detached {
				return spawn.ErrRuntimeDetached
			}
			return nil
		case <-ticker.C:
			if monitorCtx.Err() != nil {
				<-p.done
				continue
			}
			select {
			case <-p.done:
				continue
			default:
			}
			ctx, cancel := context.WithTimeout(monitorCtx, p.timeout)
			inspection, err := p.provisioner.Inspect(ctx, runtimeprovision.InspectRequest{
				Version:            runtimeprovision.ProtocolVersion,
				PersonalityAgentID: p.epoch.PersonalityAgentID,
			})
			cancel()
			if monitorCtx.Err() != nil {
				<-p.done
				continue
			}
			select {
			case <-p.done:
				continue // Stop owns teardown; report its result through p.done.
			default:
			}
			if err != nil {
				if !observationLost {
					log.Printf("spawn: runtime observation unavailable: agent=%q; retaining runtime and retrying", p.epoch.PersonalityAgentID)
					observationLost = true
				}
				continue
			}
			if observationLost {
				log.Printf("spawn: runtime observation restored: agent=%q", p.epoch.PersonalityAgentID)
				observationLost = false
			}
			if inspection.Phase != runtimeprovision.PhaseActive ||
				inspection.Epoch == nil || *inspection.Epoch != p.epoch {
				return p.retireAfterMonitorFailure(spawn.ErrRuntimeEpochLost)
			}
		}
	}
}

// retireAfterMonitorFailure owns an observed-bad runtime until it has fenced
// its exact epoch and reconciled it to an observed-empty receipt.  Wait must
// not simply return here: Manager removes a completed process from its map and
// does not call Stop after Wait, so returning would strand a surviving local
// runtime outside lifecycle ownership.
func (p *provisionedProcess) retireAfterMonitorFailure(cause error) error {
	p.stopMu.Lock()
	defer p.stopMu.Unlock()
	if p.detached {
		return nil
	}
	if p.stopped {
		return cause
	}
	p.retiring = true
	p.stopErr = nil
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), p.teardownTimeout)
		defer cancel()

		if err := p.authorizations.FenceLocalRuntimeAuthorization(
			ctx,
			p.epoch.PersonalityAgentID,
			p.epoch.Generation,
			p.epoch.RPCBootNonce,
		); err != nil {
			p.stopErr = fmt.Errorf("fence monitored runtime before reconcile: %w", err)
			return
		}
		if err := p.listeners.CloseLocalRuntime(ctx, p.epoch.PersonalityAgentID); err != nil {
			p.stopErr = fmt.Errorf("close monitored runtime before reconcile: %w", err)
			return
		}

		inspection, err := p.provisioner.Reconcile(ctx, runtimeprovision.ReconcileRequest{
			Version:            runtimeprovision.ProtocolVersion,
			PersonalityAgentID: p.epoch.PersonalityAgentID,
			FencedEpoch:        &p.epoch,
		})
		if err != nil {
			p.stopErr = fmt.Errorf("reconcile monitored runtime: %w", err)
			return
		}
		if inspection.Phase == runtimeprovision.PhaseUnknown {
			if inspection.ReapedThroughGeneration == nil ||
				*inspection.ReapedThroughGeneration != p.epoch.Generation {
				p.stopErr = errors.New("monitored runtime reconcile did not return an exact observed-empty reap receipt")
			}
			return
		}
		if inspection.Epoch == nil || *inspection.Epoch != p.epoch {
			p.stopErr = errors.New("monitored runtime reconcile returned a different live epoch")
			return
		}

		var teardown runtimeprovision.Inspection
		switch inspection.Phase {
		case runtimeprovision.PhasePrepared:
			teardown, err = p.provisioner.Abort(ctx, runtimeprovision.AbortRequest{
				Version: runtimeprovision.ProtocolVersion, PreparedEpoch: p.epoch,
			})
		case runtimeprovision.PhaseActive:
			teardown, err = p.provisioner.Stop(ctx, runtimeprovision.StopRequest{
				Version: runtimeprovision.ProtocolVersion, PreparedEpoch: p.epoch,
			})
		default:
			p.stopErr = fmt.Errorf("reconcile monitored runtime returned unsupported phase %q", inspection.Phase)
			return
		}
		if err != nil {
			p.stopErr = fmt.Errorf("retire reconciled monitored runtime: %w", err)
			return
		}
		if teardown.Phase != runtimeprovision.PhaseUnknown ||
			teardown.PersonalityAgentID != p.epoch.PersonalityAgentID ||
			teardown.ReapedThroughGeneration == nil ||
			*teardown.ReapedThroughGeneration != p.epoch.Generation {
			p.stopErr = errors.New("monitored runtime teardown did not return an exact observed-empty reap receipt")
		}
	}()
	p.stopped = p.stopErr == nil
	p.doneOnce.Do(func() { close(p.done) })
	if p.stopErr != nil {
		return errors.Join(spawn.ErrCleanupIncomplete, cause, p.stopErr)
	}
	return cause
}

func (p *provisionedProcess) Detach() error {
	p.stopMu.Lock()
	defer p.stopMu.Unlock()
	p.detached = true
	if p.monitorCancel != nil {
		p.monitorCancel()
	}
	p.doneOnce.Do(func() { close(p.done) })
	return nil
}

func (p *provisionedProcess) Stop() error {
	p.stopMu.Lock()
	if p.detached {
		p.stopMu.Unlock()
		return errors.New("runtime observation is detached")
	}
	// Release any read-only inspection before Stop asks the provisioner for
	// the same per-PA lifecycle lock. Waiting for teardown to finish first
	// would leave Stop blocked behind the observation it needs to cancel.
	if p.monitorCancel != nil {
		p.monitorCancel()
	}
	if p.retiring {
		p.stopMu.Unlock()
		return p.retireAfterMonitorFailure(nil)
	}
	defer p.stopMu.Unlock()
	if p.stopped {
		return nil
	}
	p.stopErr = nil
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), p.teardownTimeout)
		defer cancel()
		// Fence credentials and Ready before asking the privileged service to
		// stop compute. A stale process cannot publish after this returns.
		fenceErr := p.authorizations.FenceLocalRuntimeAuthorization(
			ctx,
			p.epoch.PersonalityAgentID,
			p.epoch.Generation,
			p.epoch.RPCBootNonce,
		)
		_, stopErr := p.provisioner.Stop(ctx, runtimeprovision.StopRequest{
			Version:       runtimeprovision.ProtocolVersion,
			PreparedEpoch: p.epoch,
		})
		if stopErr != nil {
			inspection, inspectErr := p.provisioner.Inspect(ctx, runtimeprovision.InspectRequest{
				Version:            runtimeprovision.ProtocolVersion,
				PersonalityAgentID: p.epoch.PersonalityAgentID,
			})
			if inspectErr == nil &&
				inspection.Phase == runtimeprovision.PhaseRecovery &&
				inspection.Epoch != nil && *inspection.Epoch == p.epoch {
				reconcileErr := fenceAndReconcileRecovery(ctx, p.provisioner, p.authorizations, p.listeners, p.epoch)
				p.stopErr = errors.Join(fenceErr, reconcileErr)
				return
			}
			p.stopErr = errors.Join(fenceErr, stopErr, inspectErr)
			return
		}
		var listenerErr error
		listenerErr = p.listeners.CloseLocalRuntime(ctx, p.epoch.PersonalityAgentID)
		p.stopErr = errors.Join(fenceErr, stopErr, listenerErr)
	}()
	p.stopped = p.stopErr == nil
	p.doneOnce.Do(func() { close(p.done) })
	return p.stopErr
}

func randomProvisioningSecret() (string, error) {
	value := make([]byte, 32)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}
