package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/fileaccess"
	"github.com/sumi-studio/sumi/apps/api/internal/jobexec"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
)

// jobexecFromEnv wires the Cloud Linux job runner. It is deliberately
// opt-in (SUMI_JOBEXEC_ENABLED): a deployment without the driver must not
// pretend subprocess jobs have somewhere to go, and a deployment with it
// must fail loudly at boot when the pieces it needs are absent — the
// failure mode this wiring refuses is a permanently queued job that looks
// admitted.
//
// Requirements when enabled:
//   - the core state service (job admission and the claim ledger);
//   - SUMI_RUNTIME_PROVISIONER_SOCKET (the root daemon that owns Docker);
//   - the canonical file service (every job binds a verified JuiceFS scope;
//     without filesvc there is no verified creation path and no launch).
func jobexecFromEnv(store *agentstate.Store, filesClient *fileaccess.Client) (*jobexec.Driver, error) {
	if !truthy(os.Getenv("SUMI_JOBEXEC_ENABLED")) {
		return nil, nil
	}
	if store == nil {
		return nil, errors.New("SUMI_JOBEXEC_ENABLED requires the core state service (SUMI_CORE_STATE_TOKEN and a database)")
	}
	socket := strings.TrimSpace(os.Getenv("SUMI_RUNTIME_PROVISIONER_SOCKET"))
	if socket == "" {
		return nil, errors.New("SUMI_JOBEXEC_ENABLED requires SUMI_RUNTIME_PROVISIONER_SOCKET")
	}
	if filesClient == nil {
		return nil, errors.New("SUMI_JOBEXEC_ENABLED requires the canonical file service (SUMI_FILESVC_URL plus a token); every job mounts a verified files scope")
	}
	client, err := runtimeprovision.NewUnixClient(socket)
	if err != nil {
		return nil, err
	}
	// Readiness probe: the socket file existing says nothing about the
	// service behind it. One bounded status call for an operation that can
	// never exist must answer ErrProcessNotFound — anything else (refused,
	// timeout, transport error) means the backend cannot actually serve
	// work, and boot fails honestly rather than advertising a Cloud runner
	// that queues jobs it can never start.
	probeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, probeErr := client.ProcessStatus(probeCtx, runtimeprovision.ProcessLookupRequest{
		PersonalityAgentID: probePersonaID,
		OperationID:        probeOperationID,
	})
	cancel()
	switch {
	case errors.Is(probeErr, runtimeprovision.ErrProcessNotFound):
		// Expected answer — the service and its journal are alive.
	case probeErr == nil:
		return nil, errors.New("jobexec readiness probe returned an operation that cannot exist")
	default:
		return nil, fmt.Errorf("jobexec readiness probe: process service unreachable or not answering: %w", probeErr)
	}
	// Routing is deterministic only if new work is stamped: jobs that name
	// no backend default to "cloud" once this driver is proven live, so the
	// unrestricted local runner can never claim them (the claim predicate
	// gives unstamped/'local' requests only to local claimants). An
	// explicit request.backend is always honored.
	if def := envOr("SUMI_JOBS_DEFAULT_BACKEND", "cloud"); def == "cloud" || def == "local" {
		store.SetDefaultJobBackend(def)
	}
	// The probed driver is the availability proof: submissions naming its
	// backend are servable from this point on.
	backend := envOr("SUMI_JOBEXEC_BACKEND", "cloud")
	store.SetJobBackendAvailable(backend)
	return jobexec.New(store, client, &jobexec.FileScopeEnsurer{Client: filesClient}, jobexec.Config{
		RunnerID:    envOr("SUMI_JOBEXEC_RUNNER_ID", "jobexec-docker"),
		Backend:     backend,
		Lease:       envDuration("SUMI_JOBEXEC_LEASE_SECONDS", 120*time.Second),
		Interval:    envDuration("SUMI_JOBEXEC_INTERVAL_SECONDS", 2*time.Second),
		ClaimLimit:  envIntOr("SUMI_JOBEXEC_CLAIM_LIMIT", 2),
		BackendWait: envDuration("SUMI_JOBEXEC_BACKEND_WAIT_SECONDS", 10*time.Minute),
		UnknownWait: envDuration("SUMI_JOBEXEC_UNKNOWN_WAIT_SECONDS", 10*time.Minute),
		OutputWait:  envDuration("SUMI_JOBEXEC_OUTPUT_WAIT_SECONDS", 2*time.Minute),
		Logf:        log.Printf,
	}), nil
}

// probePersonaID/operationID are well-formed identities that cannot match a
// real operation — the readiness probe expects the service's journal to
// answer 'not found', which proves transport, journal and validation all
// work.
const (
	probePersonaID   = "01999999-9999-7fff-bfff-ffffffffffff"
	probeOperationID = "0000000000000000000000000000000000000000000000000000000000000000"
)

// startJobExec launches the single in-process runner. Exactly one driver
// may run per runner id and store — the claim identity is the fence, not a
// PID.
func (a *application) startJobExec() {
	if a.jobExec == nil {
		return
	}
	a.attentionWorkers.Add(1)
	go func() {
		defer a.attentionWorkers.Done()
		a.jobExec.Run(a.backgroundCtx)
	}()
}

func truthy(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

func envOr(name, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return fallback
}

func envIntOr(name string, fallback int) int {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func envDuration(name string, fallback time.Duration) time.Duration {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil && n > 0 {
			return time.Duration(n * float64(time.Second))
		}
	}
	return fallback
}
