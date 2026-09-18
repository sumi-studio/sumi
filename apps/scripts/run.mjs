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
 *
 *      The shutdown PROMISE owns process completion — never the
 *      start() await: discovery/startup-recovery/periodic-recovery
 *      can be blocked inside a held HTTP response and start() would
 *      not return for it. When shutdown resolves the process exits
 *      even though start() is still pending; its settled/rejected
 *      result is already handled, so nothing surfaces as an
 *      unhandled rejection. A watchdog at grace+settle+2s is the
 *      hard backstop — if shutdown() itself ever regresses past the
 *      bound, owned children are killed and the process exits 2.
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

// start()'s settle/reject is ALWAYS handled here — including when the
// process exits on the shutdown path while start() is still pending,
// so an abandoned startup/discovery/recovery await can never surface
// as an unhandled rejection.
const startResult = sup.start().then(
  () => 0,
  async (e) => {
    console.error(`[scripts] fatal: ${e instanceof Error ? e.message : e}`);
    // Children may already be launched — bound them before exiting.
    try { await sup.shutdown(5_000); } catch { /* best effort */ }
    return 1;
  },
);

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
  // The SHUTDOWN promise — not start() — owns process completion from
  // here. start() may still be blocked inside a held HTTP request
  // (shared discovery, the startup reconcile, periodic recovery) and
  // will not return for it; the process must not wait on that. The
  // watchdog is the hard backstop: if shutdown() itself ever regresses
  // past its documented bound (grace + settle + kill pass), the
  // process still cannot outlive the contract.
  const bound = (sup.cfg.shutdownGraceMs ?? 20_000) + (sup.cfg.shutdownSettleMs ?? 3_000) + 2_000;
  const watchdog = setTimeout(() => {
    console.error(`[scripts] shutdown exceeded its bound — killing owned children, forcing exit`);
    try { sup.runner.killAllOwned(); } catch { /* best effort */ }
    process.exit(2);
  }, bound);
  watchdog.unref();
  void sup.shutdown().then(
    () => { clearTimeout(watchdog); process.exit(0); },
    () => { clearTimeout(watchdog); process.exit(1); },
  );
};
process.on("SIGTERM", () => onSignal("SIGTERM"));
process.on("SIGINT", () => onSignal("SIGINT"));

// Normal completion: start() returns only once the stop flag is set
// (i.e. a shutdown is already in flight) — wait out its bounded work,
// then exit explicitly. Leftover request/timer handles must never
// linger past the contract.
const code = await startResult;
await sup.shutdown();
process.exit(code);
