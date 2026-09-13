#!/usr/bin/env node
/**
 * E2E for the F1 durable-plan contract: REAL Go state service + REAL
 * PostgreSQL + real Node core child processes.
 *
 * Covers:
 *   1. Scripted decision A persisted → hard exit after an effect commits →
 *      a replacement provider that would choose plan B is never consulted;
 *      plan A continues with exactly one effect per position.
 *   2. Lost savePlan/claim responses: identical resends replay the stored
 *      decision and stored receipts — one effect, one commit.
 *   3. Zero-call plan: crash before commitTurn → retry replays the recorded
 *      text-only decision without consulting the model.
 *   4. Server-derived effect identity: a spoofed legacy idempotency_key and
 *      a fresh operation_id cannot mint a second effect.
 *
 * Requires SUMI_TEST_DB_URL pointing at a database migrated to 0047+
 * (core_turn_plans lives in 0047_core_state). Example:
 *   SUMI_TEST_DB_URL=postgres://sumi:sumi-dev@127.0.0.1:55432/sumi_core_f1?sslmode=disable \
 *     node scripts/e2e-plan.mjs
 *
 * Child mode (--child) runs one secretary attempt with a scripted provider;
 * the parent orchestrates crashes and assertions.
 */
import { spawn, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const CHILD = process.argv.includes("--child");

if (CHILD) {
  await childMain();
} else {
  await main();
}

// ---------------------------------------------------------------- child ---
// One attempt of a secretary with a deterministic scripted provider. The
// plan it would emit comes from SUMI_SCRIPT; crash/replay knobs simulate
// lost responses and a hard exit mid-turn.
async function childMain() {
  const { Secretary } = await import("../src/secretary.ts");
  const { HttpStateClient } = await import("../src/state-client.ts");

  const env = (n) => {
    const v = process.env[n];
    if (!v) throw new Error(`missing env ${n}`);
    return v;
  };
  const script = JSON.parse(env("SUMI_SCRIPT")); // {rounds:[{text,calls:[{tool,request}]}]}
  const dieAfterClaims = Number(process.env.SUMI_DIE_AFTER_CLAIMS ?? 0);
  const dieBeforeCommit = process.env.SUMI_DIE_BEFORE_COMMIT === "1";
  const resendPlan = process.env.SUMI_RESEND_PLAN === "1";
  const resendClaim = process.env.SUMI_RESEND_CLAIM === "1";
  const spoofClaim = process.env.SUMI_SPOOF_CLAIM === "1";

  class ScriptedProvider {
    name = "scripted";
    async *stream(req) {
      // The round being consulted is explicit in the request — a retry that
      // replays recorded rounds must never re-consult them.
      const round = req.round ?? 0;
      console.log(`[child] MODEL CONSULTED round=${round}`);
      const decision = script.rounds?.[round] ?? { text: "", calls: [] };
      yield { type: "text", delta: decision.text };
      for (const [i, c] of (decision.calls ?? []).entries()) {
        yield {
          type: "tool_call",
          call: { id: `call-${round}-${i}`, name: c.tool, arguments: c.request },
        };
      }
      yield { type: "done", usage: { scripted: true, round } };
    }
  }

  const inner = new HttpStateClient(
    env("SUMI_STATE_URL"),
    env("SUMI_PERSONA_TOKEN"),
  );
  const baseUrl = env("SUMI_STATE_URL");
  const token = env("SUMI_PERSONA_TOKEN");
  let claims = 0;
  // Wrap the state client to simulate transport-level realities the
  // protocol must absorb — identical to a lost HTTP response.
  const state = Object.create(inner, {
    savePlan: {
      value: async (p, g, req) => {
        const r = await inner.savePlan(p, g, req);
        if (resendPlan) {
          const again = await inner.savePlan(p, g, req);
          console.log(`[child] plan resend -> created=${again.created}`);
          return again;
        }
        return r;
      },
    },
    claimOperation: {
      value: async (p, g, op) => {
        const r = await inner.claimOperation(p, g, op);
        claims++;
        if (resendClaim) {
          const again = await inner.claimOperation(p, g, op);
          console.log(
            `[child] claim resend -> fresh=${again.fresh} op=${again.operation.operation_id}`,
          );
        }
        if (spoofClaim && claims === 1) {
          // Legacy-protocol spoof mid-turn: a caller-supplied
          // idempotency_key and a fresh operation_id on the same plan
          // position. The agreed boundary rejects the old field outright
          // (400, DisallowUnknownFields) — it can never mint a new effect.
          const res = await fetch(
            `${baseUrl}/internal/core/personas/${p}/operations/claim`,
            {
              method: "POST",
              headers: {
                Authorization: `Bearer ${token}`,
                "Content-Type": "application/json",
              },
              body: JSON.stringify({
                generation: g,
                operation_id: "spoof-attempt",
                turn_id: op.turnId,
                tool: op.tool,
                call_index: op.callIndex,
                request: op.request,
                idempotency_key: "attacker-chosen-key",
              }),
            },
          );
          const body = await res.json().catch(() => ({}));
          console.log(
            `[child] spoofed claim -> status=${res.status} op=${body?.operation?.operation_id ?? "none"} fresh=${body?.fresh ?? "n/a"}`,
          );
        }
        if (claims === dieAfterClaims) {
          // Hard exit: the effect committed; the caller never came back.
          console.log(`[child] dying after claim ${claims}`);
          process.exit(9);
        }
        return r;
      },
    },
    commitTurn: {
      value: async (p, t, g, req) => {
        if (dieBeforeCommit) {
          console.log("[child] dying before commitTurn");
          process.exit(9);
        }
        return inner.commitTurn(p, t, g, req);
      },
    },
  });

  const secretary = new Secretary({
    personaId: env("SUMI_PERSONA_ID"),
    holderId: process.env.SUMI_HOLDER_ID ?? `plan-e2e-${process.pid}`,
    state,
    provider: new ScriptedProvider(),
    leaseTtlMs: 30_000,
    renewEveryMs: 5_000,
    contextLimit: 60,
    pollIntervalMs: 100,
    scheduleEveryMs: 0,
    idgen: () => crypto.randomUUID(),
    log: (msg, fields) =>
      console.log(`[child] ${msg}`, fields ? JSON.stringify(fields) : ""),
  });
  await secretary.start();
  const deadline = Date.now() + 30_000;
  let lastWork = Date.now();
  while (Date.now() < deadline && Date.now() - lastWork < 1_500) {
    const r = await secretary.step();
    if (r === "turn") lastWork = Date.now();
    else await new Promise((res) => setTimeout(res, 100));
  }

  await secretary.stop();
}

// --------------------------------------------------------------- parent ---
async function main() {
  const DB_URL = process.env.SUMI_TEST_DB_URL ?? process.env.SUMI_DB_URL;
  if (!DB_URL) {
    console.error("e2e-plan: SUMI_TEST_DB_URL required — real PostgreSQL");
    process.exit(2);
  }
  const API_DIR = resolve(import.meta.dirname, "../../api");
  const SELF = resolve(import.meta.dirname, "e2e-plan.mjs");
  const PORT = 9390 + (process.pid % 500);
  const BASE = `http://127.0.0.1:${PORT}`;
  const ADMIN = `e2e-admin-${randomUUID().replaceAll("-", "")}`;

  const log = (...a) => console.log("[e2e-plan]", ...a);
  const fail = (msg) => {
    console.error("[e2e-plan] FAIL:", msg);
    process.exit(1);
  };
  const assert = (cond, msg, evidence) => {
    if (cond) return;
    if (evidence) console.error(evidence);
    fail(msg);
  };

  function uuidv7() {
    const now = Date.now().toString(16).padStart(12, "0");
    const r = randomUUID().replaceAll("-", "");
    return `${now.slice(0, 8)}-${now.slice(8, 12)}-7${r.slice(13, 16)}-${((parseInt(r.slice(16, 18), 16) & 0x3f) | 0x80).toString(16).padStart(2, "0")}${r.slice(18, 20)}-${r.slice(20, 32)}`;
  }
  const personaId = uuidv7();

  async function req(method, path, token, body) {
    const res = await fetch(BASE + path, {
      method,
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const text = await res.text();
    let json = null;
    try {
      json = JSON.parse(text);
    } catch {
      /* non-JSON */
    }
    return { status: res.status, json, text };
  }

  const binDir = mkdtempSync(join(tmpdir(), "sumi-e2e-plan-"));
  const bin = join(binDir, "state-dev");
  log("building state-dev…");
  const build = spawnSync(
    "go",
    ["build", "-buildvcs=false", "-o", bin, "./cmd/state-dev"],
    { cwd: API_DIR, stdio: "inherit" },
  );
  if (build.status !== 0) fail("go build failed");

  log("starting state-dev on", BASE);
  const svc = spawn(bin, [], {
    env: {
      ...process.env,
      SUMI_DB_URL: DB_URL,
      SUMI_CORE_STATE_TOKEN: ADMIN,
      SUMI_STATE_LISTEN: `127.0.0.1:${PORT}`,
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  svc.stderr.on("data", (d) => process.stderr.write(`[state-dev] ${d}`));
  process.on("exit", () => svc.kill("SIGKILL"));
  const deadline = Date.now() + 15_000;
  for (;;) {
    try {
      const r = await fetch(`${BASE}/health`);
      if (r.ok) break;
    } catch {
      /* not up */
    }
    if (Date.now() > deadline) fail("state-dev did not become healthy");
    await new Promise((r) => setTimeout(r, 200));
  }

  const created = await req("POST", "/internal/core/personas", ADMIN, {
    persona_id: personaId,
    display_name: "e2e plan secretary",
  });
  assert(created.status === 201, `createPersona ${created.status}`);
  const ptoken = created.json.persona_token;

  const childEnv = (extra) => ({
    ...process.env,
    SUMI_STATE_URL: BASE,
    SUMI_PERSONA_ID: personaId,
    SUMI_PERSONA_TOKEN: ptoken,
    SUMI_HOLDER_ID: "e2e-plan-holder", // same holder: deterministic re-acquire
    ...extra,
  });
  const runChild = (extra, expectExit = 0) => {
    const r = spawnSync("node", [SELF, "--child"], {
      env: childEnv(extra),
      encoding: "utf8",
      timeout: 90_000,
    });
    if (r.status !== expectExit) {
      console.error(r.stdout, r.stderr);
      fail(`child exited ${r.status}, expected ${expectExit}`);
    }
    return r.stdout ?? "";
  };
  const submit = (text) =>
    req("POST", `/internal/core/personas/${personaId}/inputs`, ptoken, {
      input_id: `in-${randomUUID()}`,
      kind: "message",
      payload: { text },
      actor_kind: "human",
      actor_id: "e2e",
      source_surface: "e2e",
    });
  const events = () =>
    req(
      "GET",
      `/internal/core/personas/${personaId}/events?after_seq=0`,
      ptoken,
    ).then((r) => r.json.events);
  const outboxFor = async (inputId) =>
    (
      await req(
        "GET",
        `/internal/core/personas/${personaId}/outbox?after_seq=0`,
        ptoken,
      )
    ).json.outbox.filter((o) => o.payload.input_id === inputId);
  const noteTexts = async () =>
    (await events())
      .filter((e) => e.kind === "note")
      .map((e) => e.payload.text);

  // --- scenario 1: decision A persisted → kill after effect → recorded round
  //     replays verbatim; the model is consulted only for the round that was
  //     never recorded (the final reply, fed by committed receipts) ---------
  log("scenario 1: kill after first effect; recorded round must replay");
  const in1 = (await submit("input-one")).json.input.input_id;
  const scriptA = JSON.stringify({
    rounds: [
      {
        text: "reply-A",
        calls: [
          { tool: "journal.note", request: { text: "note-A0" } },
          { tool: "journal.note", request: { text: "note-A1" } },
        ],
      },
      { text: "reply-A-final", calls: [] },
    ],
  });
  runChild({ SUMI_SCRIPT: scriptA, SUMI_DIE_AFTER_CLAIMS: "1" }, 9);
  assert(
    (await noteTexts()).filter((t) => t === "note-A0").length === 1,
    "position-0 effect must have committed before the kill",
  );

  const out2 = runChild({
    SUMI_SCRIPT: JSON.stringify({
      rounds: [
        {
          text: "reply-B",
          calls: [{ tool: "journal.note", request: { text: "note-B0" } }],
        },
        { text: "reply-B-final", calls: [] },
      ],
    }),
    SUMI_SPOOF_CLAIM: "1",
  });
  assert(
    !out2.includes("MODEL CONSULTED round=0"),
    "retry re-consulted a recorded round",
    out2,
  );
  assert(
    out2.includes("MODEL CONSULTED round=1"),
    "retry must consult the model for the unrecorded final round",
    out2,
  );
  // The agreed boundary explicitly rejects a supplied legacy
  // idempotency_key (400 via DisallowUnknownFields): one effect or nothing.
  assert(
    out2.includes("spoofed claim -> status=400"),
    "legacy idempotency_key spoof was not explicitly rejected",
    out2,
  );
  const notes1 = await noteTexts();
  assert(
    notes1.filter((t) => t === "note-A0").length === 1 &&
      notes1.filter((t) => t === "note-A1").length === 1,
    `plan A must continue exactly once; got notes ${JSON.stringify(notes1)}`,
  );
  assert(
    !notes1.includes("note-B0"),
    "plan B executed — the recorded plan was bypassed",
  );
  const replies1 = await outboxFor(in1);
  assert(replies1.length === 1, `expected 1 reply, got ${replies1.length}`);
  assert(
    replies1[0].payload.output.text === "reply-B-final",
    `committed ${replies1[0].payload.output.text}, expected reply-B-final — ` +
      "the final round was unrecorded, so the retry's own consultation answers it",
  );
  log("  plan A round 0 replayed; effects exactly once; reply post-dates effects");

  // --- scenario 2: lost savePlan/claim responses resend identically ---------
  log("scenario 2: lost plan-save and claim responses replay identically");
  const in2 = (await submit("input-two")).json.input.input_id;
  const out3 = runChild({
    SUMI_SCRIPT: JSON.stringify({
      rounds: [
        {
          text: "reply-C",
          calls: [{ tool: "journal.note", request: { text: "note-C" } }],
        },
        { text: "reply-C-final", calls: [] },
      ],
    }),
    SUMI_RESEND_PLAN: "1",
    SUMI_RESEND_CLAIM: "1",
  });
  assert(
    out3.includes("plan resend -> created=false"),
    "savePlan resend did not return the stored plan",
  );
  assert(
    out3.includes("claim resend -> fresh=false"),
    "claim resend did not replay the stored receipt",
  );
  assert(
    (await noteTexts()).filter((t) => t === "note-C").length === 1,
    "lost responses must not duplicate the effect",
  );
  assert(
    (await outboxFor(in2)).length === 1,
    "expected exactly one committed turn",
  );
  log("  plan-save + claim resends replayed; one effect, one commit");

  // --- scenario 3: zero-call plan survives a crash before commit ------------
  log("scenario 3: zero-call plan replayed without the model after kill");
  const in3 = (await submit("input-three")).json.input.input_id;
  runChild(
    {
      SUMI_SCRIPT: JSON.stringify({
        rounds: [{ text: "reply-D", calls: [] }],
      }),
      SUMI_DIE_BEFORE_COMMIT: "1",
    },
    9,
  );
  const out4 = runChild({
    SUMI_SCRIPT: JSON.stringify({
      rounds: [
        {
          text: "reply-E",
          calls: [{ tool: "journal.note", request: { text: "note-E" } }],
        },
      ],
    }),
  });
  assert(
    !out4.includes("MODEL CONSULTED"),
    "zero-call plan retry consulted the model",
  );
  const replies3 = await outboxFor(in3);
  assert(
    replies3.length === 1 && replies3[0].payload.output.text === "reply-D",
    "recorded zero-call decision must commit its own text",
  );
  assert(
    !(await noteTexts()).includes("note-E"),
    "replacement plan must not execute",
  );
  log("  zero-call decision replayed verbatim");

  // --- scenario 4: deterministic bad tool data resolves honestly ----------
  // CR3-B1 (ported from 0cd5410, adapted to multi-round plans): a decision
  // that can never persist must fail the input non-retryable — recorded,
  // observable in the outbox, and never blocking later inputs.
  log("scenario 4: deterministic bad tool data resolves; queue unblocked");
  const in4 = (await submit("input-four")).json.input.input_id;
  const out5 = runChild({
    SUMI_SCRIPT: JSON.stringify({
      rounds: [
        {
          text: "reply",
          calls: [{ tool: "journal.note", request: { text: "a\0b" } }],
        },
        { text: "unreachable", calls: [] },
      ],
    }),
  });
  assert(
    out5.includes("decision could not be recorded"),
    "NUL decision was not recorded as a non-retryable failure",
    out5,
  );
  const in4State = await req(
    "GET",
    `/internal/core/personas/${personaId}/inputs/${in4}`,
    ptoken,
  );
  assert(
    in4State.json?.input?.status === "done",
    `poisoned input must resolve (done), got ${in4State.text}`,
  );
  const failed4 = await outboxFor(in4);
  assert(
    failed4.length === 1 && failed4[0].kind === "turn_failed",
    `a terminal failure must reach the outbox, got ${JSON.stringify(failed4)}`,
  );
  // The queue is unblocked: a later normal input completes on the same
  // persona with no residue from the failed decision.
  const in5 = (await submit("input-five")).json.input.input_id;
  runChild({
    SUMI_SCRIPT: JSON.stringify({
      rounds: [
        {
          text: "reply-F",
          calls: [{ tool: "journal.note", request: { text: "note-F" } }],
        },
        { text: "reply-F-final", calls: [] },
      ],
    }),
  });
  const replies5 = await outboxFor(in5);
  assert(
    replies5.length === 1 &&
      replies5[0].payload.output.text === "reply-F-final" &&
      (await noteTexts()).includes("note-F"),
    "the input after a poisoned decision must complete normally",
  );
  log("  NUL decision failed non-retryable; next input completed");

  // CR3-B1 (claim side): a plan-valid but semantically invalid argument is
  // a recorded tool error, fed back to the model, and the turn commits —
  // not a retried 500.
  const in6 = (await submit("input-six")).json.input.input_id;
  runChild({
    SUMI_SCRIPT: JSON.stringify({
      rounds: [
        {
          text: "scheduling",
          calls: [
            {
              tool: "schedule.set",
              request: {
                wake_at: "2030-01-01T00:00:00Z",
                miss_policy: "bogus",
              },
            },
          ],
        },
        { text: "I could not set that reminder", calls: [] },
      ],
    }),
  });
  const evs6 = await events();
  const toolErr = evs6.find(
    (e) =>
      e.kind === "tool_result" && /miss_policy/.test(e.payload.error ?? ""),
  );
  assert(toolErr, "invalid miss_policy must surface as a tool_result error");
  const replies6 = await outboxFor(in6);
  assert(
    replies6.length === 1 &&
      replies6[0].payload.output.text === "I could not set that reminder",
    "the tool-error-informed reply must commit",
  );
  const in6State = await req(
    "GET",
    `/internal/core/personas/${personaId}/inputs/${in6}`,
    ptoken,
  );
  assert(
    in6State.json?.input?.status === "done",
    `bad-policy input must resolve (done), got ${in6State.text}`,
  );
  log("  invalid miss_policy recorded as a tool error; input resolved");

  // CR3-B2: a second schedule.set reusing an id must not report a stale
  // success — different contents are an explicit tool error.
  const in7 = (await submit("input-seven")).json.input.input_id;
  runChild({
    SUMI_SCRIPT: JSON.stringify({
      rounds: [
        {
          text: "reminder set",
          calls: [
            {
              tool: "schedule.set",
              request: {
                schedule_id: "rem-e2e",
                wake_at: "2030-01-01T00:00:00Z",
                payload: { text: "first" },
                miss_policy: "coalesce",
              },
            },
          ],
        },
        { text: "reminder set", calls: [] },
      ],
    }),
  });
  const evs7 = await events();
  assert(
    evs7.some(
      (e) =>
        e.kind === "tool_result" &&
        e.payload.response?.schedule?.schedule_id === "rem-e2e",
    ),
    "initial schedule.set did not commit",
  );
  const in8 = (await submit("input-eight")).json.input.input_id;
  runChild({
    SUMI_SCRIPT: JSON.stringify({
      rounds: [
        {
          text: "reminder again",
          calls: [
            {
              tool: "schedule.set",
              request: {
                schedule_id: "rem-e2e",
                wake_at: "2031-06-01T00:00:00Z",
                payload: { text: "different" },
                miss_policy: "coalesce",
              },
            },
          ],
        },
        { text: "that reminder id is taken", calls: [] },
      ],
    }),
  });
  const evs8 = await events();
  assert(
    evs8.some(
      (e) =>
        e.kind === "tool_result" &&
        /already exists with different contents/.test(e.payload.error ?? ""),
    ),
    "conflicting schedule_id reuse must be an explicit tool error",
  );
  const in8State = await req(
    "GET",
    `/internal/core/personas/${personaId}/inputs/${in8}`,
    ptoken,
  );
  assert(
    in8State.json?.input?.status === "done",
    `conflicting-reuse input must resolve (done), got ${in8State.text}`,
  );
  log("  schedule_id reuse with different contents is an honest error");

  svc.kill("SIGKILL");
  log("PASS — durable-plan scenarios green on real PG + real Go + real Node");
}
