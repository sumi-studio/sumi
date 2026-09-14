/**
 * Local host: runs one secretary life on Node against the Go state service.
 *
 *   SUMI_STATE_URL=http://127.0.0.1:PORT  (required)
 *   SUMI_PERSONA_ID, SUMI_PERSONA_TOKEN   (required — persona-scoped token)
 *   SUMI_HOLDER_ID                        (default: local-<pid>)
 *   SUMI_MODEL_PROVIDER=mock|openai       (default: mock)
 *   SUMI_MODEL_BASE_URL / _API_KEY / _MODEL  (openai only)
 *   SUMI_MODEL_HEADERS_JSON / _EXTRA_JSON / _TIMEOUT_MS  (openai only;
 *                                          see host/provider-env.ts)
 *   SUMI_PROVIDER_RETRY_BUDGET_MS  wall-clock budget for transient provider
 *                                  retries, measured from input submission
 *                                  (default 30 min)
 *   SUMI_MEMORY_PREPARATION_TIMEOUT_MS  wall-clock bound on one memory
 *                                  preparation branch (default 10 min)
 *   --once  drain pending work then exit (used by e2e + dev scripts)
 *
 * Kill -9 safe at any point: nothing canonical lives in this process.
 */

import { providerForPersona } from "./provider-env.ts";
import { Secretary } from "../secretary.ts";
import { HttpStateClient } from "../state-client.ts";

function env(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`missing env ${name}`);
  return v;
}

async function main() {
  const once = process.argv.includes("--once");
  const state = new HttpStateClient(
    env("SUMI_STATE_URL"),
    env("SUMI_PERSONA_TOKEN"),
  );
  const personaId = env("SUMI_PERSONA_ID");
  const leaseTtl = Number(process.env.SUMI_LEASE_TTL_MS ?? 30_000);
  const secretary = new Secretary({
    personaId,
    holderId: process.env.SUMI_HOLDER_ID ?? `local-${process.pid}`,
    state,
    // The selected model connection is authoritative: re-resolved through
    // the state service for every model call; env only applies when the
    // persona has no selection at all.
    provider: providerForPersona(
      state,
      personaId,
      (n) => process.env[n],
      (msg, fields) =>
        console.log(`[core] ${msg}`, fields ? JSON.stringify(fields) : ""),
    ),
    leaseTtlMs: leaseTtl,
    renewEveryMs: Math.max(250, Math.floor(leaseTtl / 3)),
    // Row bound only; the state service bounds raw context by capacity.
    contextLimit: 5_000,
    pollIntervalMs: 500,
    scheduleEveryMs: 1_000,
    providerRetryBudgetMs: process.env.SUMI_PROVIDER_RETRY_BUDGET_MS
      ? Number(process.env.SUMI_PROVIDER_RETRY_BUDGET_MS)
      : undefined,
    memoryPreparationTimeoutMs: process.env.SUMI_MEMORY_PREPARATION_TIMEOUT_MS
      ? Number(process.env.SUMI_MEMORY_PREPARATION_TIMEOUT_MS)
      : undefined,
    memoryUnavailablePauseMs: process.env.SUMI_MEMORY_UNAVAILABLE_PAUSE_MS
      ? Number(process.env.SUMI_MEMORY_UNAVAILABLE_PAUSE_MS)
      : undefined,
    idgen: () => crypto.randomUUID(),
    log: (msg, fields) =>
      console.log(`[core] ${msg}`, fields ? JSON.stringify(fields) : ""),
  });

  if (once) {
    await secretary.start();
    // Drain until no work has arrived for idleGraceMs — near-future
    // scheduled wakes still get fired — bounded so a stuck loop can't
    // hang CI forever.
    const deadline = Date.now() + 60_000;
    const idleGraceMs = Number(process.env.SUMI_ONCE_IDLE_MS ?? 2_000);
    let lastWork = Date.now();
    while (Date.now() < deadline && Date.now() - lastWork < idleGraceMs) {
      const r = await secretary.step();
      // A memory preparation branch in flight is work too: stopping would
      // interrupt it and leave the chunk to be prepared again next run.
      if (r === "turn" || secretary.memoryBusy) lastWork = Date.now();
      if (r !== "turn") await new Promise((res) => setTimeout(res, 100));
    }
    // The deadline bounds new work, not a preparation already running: the
    // branch keeps its lease renewed and ends within its own timeout, by
    // recording a result or a retryable failure. No new branch starts here.
    if (secretary.memoryBusy) {
      console.log("[core] --once: waiting for the running memory preparation");
      await secretary.settleMemory();
    }
    await secretary.stop();
    return;
  }

  const ac = new AbortController();
  process.on("SIGINT", () => ac.abort());
  process.on("SIGTERM", () => ac.abort());
  await secretary.run(ac.signal);
}

main().catch((e) => {
  console.error("[core] fatal:", e);
  process.exit(1);
});
