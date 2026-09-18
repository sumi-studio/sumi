package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/fileaccess"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
	"github.com/sumi-studio/sumi/apps/api/internal/termexec"
)

// termexecFromEnv wires the Cloud interactive terminal driver. It is
// opt-in like jobexec and shares its prerequisites — a terminal
// session on a deployment without the driver must be refused at
// admission, never queued forever.
//
// Enabled by SUMI_TERMEXEC_ENABLED, or by SUMI_JOBEXEC_ENABLED (the
// terminal is part of the same Cloud execution environment — a
// deployment that runs Cloud jobs can host sessions without a second
// flag). SUMI_TERMEXEC_ENABLED=0 explicitly disables it.
func termexecFromEnv(store *agentstate.Store, filesClient *fileaccess.Client) (*termexec.Driver, error) {
	enabled := truthy(os.Getenv("SUMI_TERMEXEC_ENABLED"))
	if v := strings.TrimSpace(os.Getenv("SUMI_TERMEXEC_ENABLED")); !enabled && v == "" {
		enabled = truthy(os.Getenv("SUMI_JOBEXEC_ENABLED"))
	}
	if !enabled {
		return nil, nil
	}
	if store == nil {
		return nil, errors.New("terminal sessions require the core state service (SUMI_CORE_STATE_TOKEN and a database)")
	}
	socket := strings.TrimSpace(os.Getenv("SUMI_RUNTIME_PROVISIONER_SOCKET"))
	if socket == "" {
		return nil, errors.New("terminal sessions require SUMI_RUNTIME_PROVISIONER_SOCKET")
	}
	if filesClient == nil {
		return nil, errors.New("terminal sessions require the canonical file service (SUMI_FILESVC_URL plus a token); every session mounts a verified files scope")
	}
	client, err := runtimeprovision.NewUnixClient(socket)
	if err != nil {
		return nil, err
	}
	// Same readiness contract as jobexec: one bounded status call for
	// an operation that can never exist must answer ErrProcessNotFound.
	probeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, probeErr := client.ProcessStatus(probeCtx, runtimeprovision.ProcessLookupRequest{
		PersonalityAgentID: probePersonaID,
		OperationID:        probeOperationID,
	})
	cancel()
	switch {
	case errors.Is(probeErr, runtimeprovision.ErrProcessNotFound):
	case probeErr == nil:
		return nil, errors.New("termexec readiness probe returned an operation that cannot exist")
	default:
		return nil, fmt.Errorf("termexec readiness probe: process service unreachable or not answering: %w", probeErr)
	}
	backend := envOr("SUMI_TERMEXEC_BACKEND", "cloud")
	store.SetTerminalBackendAvailable(backend)
	if def := envOr("SUMI_TERMINALS_DEFAULT_BACKEND", "cloud"); def == "cloud" || def == "local" {
		store.SetDefaultTerminalBackend(def)
	}
	return termexec.New(store, client, &termexec.FileScopeEnsurer{Client: filesClient}, termexec.Config{
		RunnerID:           envOr("SUMI_TERMEXEC_RUNNER_ID", "termexec-docker"),
		Backend:            backend,
		Lease:              envDuration("SUMI_TERMEXEC_LEASE_SECONDS", 30*time.Second),
		Interval:           envDuration("SUMI_TERMEXEC_INTERVAL_SECONDS", 2*time.Second),
		PollInterval:       envDuration("SUMI_TERMEXEC_POLL_SECONDS", 350*time.Millisecond),
		ClaimLimit:         envIntOr("SUMI_TERMEXEC_CLAIM_LIMIT", 2),
		MaxLifetimeSeconds: envIntOr("SUMI_TERMEXEC_MAX_LIFETIME_SECONDS", 8*3600),
		UnknownWait:        envDuration("SUMI_TERMEXEC_UNKNOWN_WAIT_SECONDS", 5*time.Minute),
		Logf:               log.Printf,
	}), nil
}

// startTermExec launches the single in-process terminal driver — one
// claim identity, one owner, same rule as the job driver.
func (a *application) startTermExec() {
	if a.termExec == nil {
		return
	}
	a.attentionWorkers.Add(1)
	go func() {
		defer a.attentionWorkers.Done()
		a.termExec.Run(a.backgroundCtx)
	}()
}
