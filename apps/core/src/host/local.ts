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
 *   --once  drain pending work then exit (used by e2e + dev scripts)
 *
 * Kill -9 safe at any point: nothing canonical lives in this process.
 */

import { providerFromEnv } from "./provider-env.ts";
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
  const leaseTtl = Number(process.env.SUMI_LEASE_TTL_MS ?? 30_000);
  const secretary = new Secretary({
    personaId: env("SUMI_PERSONA_ID"),
    holderId: process.env.SUMI_HOLDER_ID ?? `local-${process.pid}`,
    state,
    provider: providerFromEnv((n) => process.env[n]),
    leaseTtlMs: leaseTtl,
    renewEveryMs: Math.max(250, Math.floor(leaseTtl / 3)),
    contextLimit: 60,
    pollIntervalMs: 500,
    scheduleEveryMs: 1_000,
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
      if (r === "turn") {
        lastWork = Date.now();
      } else {
        await new Promise((res) => setTimeout(res, 100));
      }
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
