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
 *   SUMI_SHUTDOWN_GRACE_MS  shutdown drain bound (default 20000)
 *
 * Shutdown contract (SIGTERM/SIGINT):
 *   1. Discovery and claims stop immediately — no new reservations.
 *   2. Every in-flight job receives an honest cancel_requested through
 *      the API; its drive loop observes it within ~heartbeat and
 *      reports 'cancelled' with whatever usage was measured.
 *   3. The process waits up to SUMI_SHUTDOWN_GRACE_MS for the in-flight
 *      set to drain, then exits.
 *   4. Anything still running past the grace window is left bounded by
 *      its own rlimits/wall_ms — never silently killed mid-report and
 *      never reported on absent evidence. Its claim expires to 'lost';
 *      the next supervisor's startup reconcile reaps the orphan by
 *      verified identity and attaches the honest indeterminate outcome.
 *   5. A second signal forces immediate exit.
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
    console.error(`[scripts] ${sig} again — forcing exit`);
    process.exit(2);
  }
  stopping = true;
  console.log(`[scripts] ${sig} — stopping claims, cancelling in-flight jobs`);
  void sup.shutdown();
};
process.on("SIGTERM", () => onSignal("SIGTERM"));
process.on("SIGINT", () => onSignal("SIGINT"));

try {
  await sup.start();
} catch (e) {
  console.error(`[scripts] fatal: ${e instanceof Error ? e.message : e}`);
  process.exitCode = 1;
}
