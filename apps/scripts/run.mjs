#!/usr/bin/env node
/**
 * Shipped entrypoint for the lightweight-script job supervisor.
 *
 *   node run.mjs            (Node >= 22.18 — TypeScript types are
 *                            stripped on import automatically)
 *   pnpm run run
 *
 * Startup is all-or-nothing: a bad environment fails fatally with a
 * named reason and a non-zero exit — the process never half-starts
 * under an identity or config it cannot sustain.
 *
 * Required env:
 *   SUMI_STATE_API        state API origin (http://host:port)
 *   SUMI_STATE_TOKEN      internal runtime credential (>=16 chars)
 *   SUMI_WORKERD_BIN      path to the workerd binary
 *
 * Optional env:
 *   SUMI_CGROUP_MODE      "systemd" | "prlimit" (default: autodetect;
 *                         an explicit systemd request fails honestly
 *                         when scopes cannot launch)
 *   SUMI_RUNNER_ID        explicit durable runner identity (else a
 *                         random id is minted and persisted under
 *                         SUMI_WORK_DIR — claims never begin under an
 *                         identity that cannot be recovered)
 *   SUMI_WORK_DIR         journal state dir (default ~/.local/state/sumi-scripts)
 *   SUMI_PERSONAS         comma-separated persona filter — local/dev
 *                         only; production discovers via the shared
 *                         /internal/core/jobs/runnable route
 *   SUMI_LEASE_MS         claim lease (default 30000)
 *   SUMI_HEARTBEAT_MS     heartbeat cadence (default 5000)
 *   SUMI_CLAIM_LIMIT      max claims per persona per pass (default 4)
 *   SUMI_MAX_CONCURRENT   max concurrent workerd executions (default 4)
 *   SUMI_POLL_MS          discovery poll interval (default 1000)
 *   SUMI_DISCOVERY_PAGE   runnable-route page size (default 64)
 *   SUMI_DISPATCHER_PATH  workerd dispatcher module path
 *   SUMI_RUNLIMITED_BIN   runlimited helper path
 *   SUMI_SHUTDOWN_GRACE_MS   cancel+drain window (default 20000)
 *   SUMI_SHUTDOWN_SETTLE_MS  post-kill report window (default 3000)
 *
 * Shutdown contract (SIGTERM/SIGINT) — a REAL wall-clock bound of
 * about grace + settle + a sub-second local kill pass:
 *   1. Discovery/claims stop; the pre-spawn guard refuses launches.
 *      A claim already in flight that resolves is reported
 *      'cancelled'/shutdown_before_start — provably never executed —
 *      not stranded for lease expiry.
 *   2. Every in-flight job gets cancel_requested CONCURRENTLY; its
 *      drive loop observes it via heartbeat and reports 'cancelled'
 *      with whatever usage was measured.
 *   3. At the grace deadline, surviving children are killed LOCALLY —
 *      the detached spawn's owned process group (atomic across the
 *      fork→exec window a descendant walk cannot see) plus verified
 *      journal identities, never a broad kill. Children are NOT left
 *      to "their own limits": wall_ms lives in this process's drive
 *      loop and RLIMIT_CPU does not bound an idle child's elapsed
 *      lifetime.
 *   4. Up to SUMI_SHUTDOWN_SETTLE_MS more for killed jobs' drive
 *      loops to land honest reports; then the process EXITS. What
 *      could not be reported stays in the durable journal — claims
 *      expire to 'lost' and the next supervisor's startup reconcile
 *      attaches the honest indeterminate outcome, idempotently.
 *   5. Second signal = emergency: synchronous kill of every owned
 *      child, then immediate exit(2).
 *   6. A fatal start() error after children launched runs the same
 *      bounded shutdown before exiting 1 — no orphan leak.
 */
import { supervisorFromEnv } from "./src/main.ts";

let sup;
try {
  sup = supervisorFromEnv();
} catch (e) {
  // Fatal startup: print the reason (variable names only — never secret
  // values) and exit non-zero so the service manager sees the failure.
  console.error(`[scripts] startup failed: ${e instanceof Error ? e.message : e}`);
  process.exit(1);
}

let stopping = false;
const onSignal = (sig) => {
  if (stopping) {
    // Emergency: still synchronous local kill of owned children —
    // journals hold whatever evidence was written; claims resolve via
    // lease expiry + next-supervisor reconcile. Then exit immediately.
    console.error(`[scripts] ${sig} again — emergency exit, killing owned children`);
    try { sup.runner.killAllOwned(); } catch { /* best effort */ }
    process.exit(2);
  }
  stopping = true;
  console.log(`[scripts] ${sig} — bounded shutdown: stop claims, cancel in-flight, drain, local-kill deadline`);
  void sup.shutdown();
};
process.on("SIGTERM", () => onSignal("SIGTERM"));
process.on("SIGINT", () => onSignal("SIGINT"));

try {
  await sup.start();
  // start() returned — the stop flag was set by a shutdown in flight.
  // Wait out its bounded work, then exit explicitly: leftover request/
  // timer handles must never linger past the contract.
  await sup.shutdown();
  process.exit(0);
} catch (e) {
  console.error(`[scripts] fatal: ${e instanceof Error ? e.message : e}`);
  // Children may already be launched — bound them before exiting.
  try { await sup.shutdown(5_000); } catch { /* best effort */ }
  process.exit(1);
}
