/**
 * Local call-bridge host: a Node process that owns LiveKit media sessions for
 * one persona, independent of the secretary writer process. RTC media
 * lifetime deliberately lives outside the short-lived core writer — the
 * claim/heartbeat protocol, not the writer lease, authorizes media work.
 *
 *   SUMI_STATE_URL=http://127.0.0.1:PORT  (required)
 *   SUMI_PERSONA_ID, SUMI_PERSONA_TOKEN   (required — persona-scoped token)
 *   SUMI_CALL_RUNNER_ID                   (default: call-runner-<pid>)
 *   SUMI_CALL_LEASE_MS                    (default: 30000; server clamps <=90s)
 *   SUMI_CALL_POLL_MS                     (default: 1500)
 *   SUMI_CALL_STT_FIXTURE                 (path to {"default":[lines]} JSON —
 *                                          fixture STT, no real engine yet)
 *   SUMI_CALL_TTS_WAV                     (optional prerecorded PCM16 WAV used
 *                                          as fixture speech output)
 *   SUMI_CALL_VAD_MAX_MS                  (default 30000 — bound one detected
 *                                          speech segment)
 *   SUMI_CALL_VAD_HANGOVER_MS             (default 500 — silence that closes
 *                                          a segment)
 *   --once  run claim passes until no live sessions remain, then exit
 *           (used by the e2e harness)
 *
 * Kill -9 safe at any point: sessions it held are not replayed — their
 * claims expire, the next claim sweeps them to 'interrupted', and pending
 * utterances are recorded 'unknown' rather than replayed.
 */

import { CallRunner } from "../call/runner.ts";
import { CallBridgeClient } from "../call/bridge-client.ts";
import { FixtureSTT, FixtureTTS } from "../call/adapters.ts";

function env(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`missing env ${name}`);
  return v;
}

async function main() {
  const once = process.argv.includes("--once");
  const client = new CallBridgeClient(
    env("SUMI_STATE_URL"),
    env("SUMI_PERSONA_TOKEN"),
  );
  const sttScript = process.env.SUMI_CALL_STT_FIXTURE;
  const stt = sttScript
    ? await FixtureSTT.fromFile(sttScript)
    : new FixtureSTT(new Map());
  const tts = await FixtureTTS.create(process.env.SUMI_CALL_TTS_WAV);
  const runner = new CallRunner({
    client,
    personaId: env("SUMI_PERSONA_ID"),
    runnerId: process.env.SUMI_CALL_RUNNER_ID ?? `call-runner-${process.pid}`,
    leaseMs: Number(process.env.SUMI_CALL_LEASE_MS ?? 30_000),
    pollMs: Number(process.env.SUMI_CALL_POLL_MS ?? 1_500),
    stt,
    tts,
    vadMaxMs: Number(process.env.SUMI_CALL_VAD_MAX_MS ?? 30_000),
    vadHangoverMs: Number(process.env.SUMI_CALL_VAD_HANGOVER_MS ?? 500),
    log: (msg, fields) =>
      console.log(`[call-runner] ${msg}`, fields ? JSON.stringify(fields) : ""),
  });

  if (once) {
    const deadline = Date.now() + 120_000;
    const idleGraceMs = Number(process.env.SUMI_ONCE_IDLE_MS ?? 3_000);
    let lastActivity = Date.now();
    while (Date.now() < deadline && Date.now() - lastActivity < idleGraceMs) {
      const before = runner.activeCount;
      await runner.step();
      if (runner.activeCount > 0 || runner.activeCount !== before) {
        lastActivity = Date.now();
      }
      await new Promise((res) => setTimeout(res, 200));
    }
    await runner.stop();
    return;
  }

  const ac = new AbortController();
  process.on("SIGINT", () => ac.abort());
  process.on("SIGTERM", () => ac.abort());
  await runner.run(ac.signal);
}

main().catch((e) => {
  console.error("[call-runner] fatal:", e);
  process.exit(1);
});
