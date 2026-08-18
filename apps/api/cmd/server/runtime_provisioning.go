package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
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
}

type localRuntimeAuthorizationController interface {
	InstallLocalRuntimeAuthorization(context.Context, agentevents.LocalRuntimeAuthorization) error
	FenceLocalRuntimeAuthorization(context.Context, string, uint64, string) error
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
	BearerTTL           time.Duration
	LifecycleTimeout    time.Duration
	TeardownTimeout     time.Duration
	StartupReadyTimeout time.Duration
	Activation          runtimeprovision.ActivationConfig
}

// provisionedRuntimeSpawner is the only production lazy-spawn implementation.
// It cannot execute host processes or reach Docker: its sole host capability is
// the typed root-provisioner Unix protocol.
type provisionedRuntimeSpawner struct {
	config provisionedRuntimeSpawnerConfig
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
	if config.BearerTTL <= 0 {
		config.BearerTTL = 8 * time.Hour
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
	return &provisionedRuntimeSpawner{config: config}, nil
}

func (s *provisionedRuntimeSpawner) Spawn(
	ctx context.Context,
	config spawn.AgentRuntimeConfig,
) (spawn.Process, error) {
	if err := runtimeprovision.ValidatePersonalityAgentID(config.AgentID); err != nil {
		return nil, err
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
	activation.GatewayURL = config.GatewayURL
	activation.LocalControlBearer = bearer
	activation.LocalControlBearerExpiresAtUnix = time.Now().Add(s.config.BearerTTL).Unix()
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

	return &provisionedProcess{
		provisioner:     s.config.Provisioner,
		authorizations:  s.config.Authorizations,
		listeners:       s.config.Listeners,
		epoch:           epoch,
		timeout:         s.config.LifecycleTimeout,
		teardownTimeout: s.config.TeardownTimeout,
		monitorInterval: 5 * time.Second,
		done:            make(chan struct{}),
	}, nil
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
	done            chan struct{}
	stopOnce        sync.Once
	stopErr         error
}

func (p *provisionedProcess) Wait() error {
	interval := p.monitorInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			return p.stopErr
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
			inspection, err := p.provisioner.Inspect(ctx, runtimeprovision.InspectRequest{
				Version:            runtimeprovision.ProtocolVersion,
				PersonalityAgentID: p.epoch.PersonalityAgentID,
			})
			cancel()
			if err != nil {
				return p.retireAfterMonitorFailure(fmt.Errorf("monitor provisioned runtime: %w", err))
			}
			if inspection.Phase != runtimeprovision.PhaseActive ||
				inspection.Epoch == nil || *inspection.Epoch != p.epoch {
				return p.retireAfterMonitorFailure(errors.New("provisioned runtime left its active epoch"))
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
	p.stopOnce.Do(func() {
		defer close(p.done)
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
	})
	return errors.Join(cause, p.stopErr)
}

func (p *provisionedProcess) Stop() error {
	p.stopOnce.Do(func() {
		defer close(p.done)
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
	})
	return p.stopErr
}

func randomProvisioningSecret() (string, error) {
	value := make([]byte, 32)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}
