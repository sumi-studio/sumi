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
import { createServer } from "node:http";
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
  // SUMI_SCRIPT drives the in-process provider; the real OpenAI adapter
  // (SUMI_PROVIDER=openai) needs no script at all.
  const script = JSON.parse(process.env.SUMI_SCRIPT ?? "{}");
  const dieAfterClaims = Number(process.env.SUMI_DIE_AFTER_CLAIMS ?? 0);
  const dieBeforeCommit = process.env.SUMI_DIE_BEFORE_COMMIT === "1";
  const resendPlan = process.env.SUMI_RESEND_PLAN === "1";
  const resendClaim = process.env.SUMI_RESEND_CLAIM === "1";
  const spoofClaim = process.env.SUMI_SPOOF_CLAIM === "1";

  class ScriptedProvider {
    name = "scripted";
    consults = 0;
    async *stream(req) {
      // The round being consulted is explicit in the request — a retry that
      // replays recorded rounds must never re-consult them.
      const round = req.round ?? 0;
      this.consults++;
      console.log(`[child] MODEL CONSULTED round=${round}`);
      // throwSize/textSize build oversized values in-process — a 1.5 MB
      // env var would flirt with execve limits. The multibyte pattern
      // makes sure truncation stays on code-point boundaries.
      if (script.throwSize)
        throw new Error("provider exploded " + "ø😀".repeat(script.throwSize));
      // throwCount fails only the first N consults — a scripted provider
      // outage that heals mid-run.
      if (script.throwCount && this.consults <= script.throwCount)
        throw new Error(script.throw ?? "provider exploded");
      if (script.throw) throw new Error(script.throw);
      const decision = script.rounds?.[round] ?? { text: "", calls: [] };
      // pad_kb/textSize inflate the reply in-process — a 700KiB script
      // cannot travel through an env var (MAX_ARG_STRLEN).
      const text = script.textSize
        ? "ø😀".repeat(script.textSize)
        : decision.pad_kb
          ? decision.text + "x".repeat(decision.pad_kb * 1024)
          : decision.text;
      yield { type: "text", delta: text };
      for (const [i, c] of (decision.calls ?? []).entries()) {
        yield {
          type: "tool_call",
          call: {
            id: `call-${round}-${i}`,
            name: c.tool,
            route: c.route ?? "normal",
            arguments: c.request,
          },
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

  // The default provider is deterministic and in-process; SUMI_PROVIDER=
  // openai swaps in the real OpenAI adapter against whatever SSE endpoint
  // SUMI_MODEL_BASE_URL points at — same secretary, same state service.
  const provider =
    process.env.SUMI_PROVIDER === "openai"
      ? new (await import("../src/providers/openai.ts")).OpenAIProvider({
          baseUrl: env("SUMI_MODEL_BASE_URL"),
          apiKey: process.env.SUMI_MODEL_API_KEY ?? "e2e",
          model: process.env.SUMI_MODEL ?? "e2e-model",
          timeoutMs: Number(process.env.SUMI_MODEL_TIMEOUT_MS ?? 15_000),
        })
      : new ScriptedProvider();

  const secretary = new Secretary({
    personaId: env("SUMI_PERSONA_ID"),
    holderId: process.env.SUMI_HOLDER_ID ?? `plan-e2e-${process.pid}`,
    state,
    provider,
    leaseTtlMs: 30_000,
    renewEveryMs: 5_000,
    maxAttempts: Number(process.env.SUMI_MAX_ATTEMPTS ?? 5),
    providerRetryBudgetMs: process.env.SUMI_PROVIDER_RETRY_BUDGET_MS
      ? Number(process.env.SUMI_PROVIDER_RETRY_BUDGET_MS)
      : undefined,
    contextLimit: 60,
    pollIntervalMs: 100,
    scheduleEveryMs: 0,
    idgen: () => crypto.randomUUID(),
    log: (msg, fields) =>
      console.log(`[child] ${msg}`, fields ? JSON.stringify(fields) : ""),
  });
  await secretary.start();
  const deadline = Date.now() + 30_000;
  // Idle grace before exit; a short value makes each boot one consult —
  // a parked not_before outlives the grace and the run exits.
  const idleGrace = Number(process.env.SUMI_ONCE_IDLE_MS ?? 1_500);
  let lastWork = Date.now();
  while (Date.now() < deadline && Date.now() - lastWork < idleGrace) {
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
  // SUMI_E2E_PORT pins the port when parallel worktrees own port ranges.
  const PORT = Number(process.env.SUMI_E2E_PORT ?? 9390 + (process.pid % 500));
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
      maxBuffer: 64 * 1024 * 1024, // oversized-error scenarios log big lines
    });
    if (r.status !== expectExit) {
      console.error(r.stdout, r.stderr);
      fail(`child exited ${r.status}, expected ${expectExit}`);
    }
    return r.stdout ?? "";
  };
  // Async variant: lets the parent observe queue state while the child
  // drains (e.g. an input parked behind not_before mid-boot).
  const runChildAsync = (extra) => {
    const proc = spawn("node", [SELF, "--child"], { env: childEnv(extra) });
    let out = "";
    proc.stdout.on("data", (d) => (out += d));
    proc.stderr.on("data", (d) => (out += d));
    const done = new Promise((res, rej) => {
      proc.on("exit", (code) =>
        code === 0 ? res(out) : rej(new Error(`child exited ${code}\n${out}`)),
      );
      proc.on("error", rej);
    });
    return { proc, done, out: () => out };
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
  // CR3-B1: a decision that can never persist must fail the input
  // non-retryable — recorded, observable, and never blocking later inputs.
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

  // --- scenario 5: failure disposition -------------------------------------
  // Review-B F1 / fresh reviews: a provider that always throws (with a NUL
  // in its message — un-storable bytes in diagnostic text) must fail each
  // attempt retryably, paced by the durable not_before backoff — not hot-
  // loop or crash the process — and each input resolves honestly once the
  // retry budget elapses, with a turn_failed record. A later queued input
  // is served during the backoff, not starved behind the failing one.
  // The children carry a short retry budget (2.5s of input age) so the
  // terminal boundary is observable inside the scenario — the default
  // 30-minute budget is exercised separately in scenario 7.
  log("scenario 5: failing provider resolves honestly under bounded backoff");
  const in9 = (await submit("input-nine")).json.input.input_id;
  const in10 = (await submit("input-ten")).json.input.input_id;
  const getInput = async (id) =>
    (
      await req(
        "GET",
        `/internal/core/personas/${personaId}/inputs/${id}`,
        ptoken,
      )
    ).json?.input;
  let consults = 0;
  let parkedSeen = false;
  const stormStart = Date.now();
  // Each child drains for its idle grace and exits; a parked input simply
  // survives to the next run — the durable backoff carries across boots.
  for (let boot = 0; boot < 6; boot++) {
    // The child runs while the parent polls — a parked not_before is
    // observable mid-boot, between the child's own retries.
    const child = runChildAsync({
      SUMI_SCRIPT: JSON.stringify({ throw: "provider exploded \u0000" }),
      SUMI_PROVIDER_RETRY_BUDGET_MS: "2500",
    });
    let exited = false;
    child.done.then(() => (exited = true), () => (exited = true));
    while (!exited) {
      const i9 = await getInput(in9);
      const i10 = await getInput(in10);
      if (i9?.not_before && i9?.status === "queued") parkedSeen = true;
      if (i10?.not_before && i10?.status === "queued") parkedSeen = true;
      await new Promise((r) => setTimeout(r, 120));
    }
    await child.done; // propagate a non-zero child exit
    consults += (child.out().match(/MODEL CONSULTED/g) ?? []).length;
    const i9 = await getInput(in9);
    const i10 = await getInput(in10);
    if (i9?.status === "done" && i10?.status === "done") break;
    // Give the durable backoff room before the next boot.
    await new Promise((r) => setTimeout(r, 700));
  }
  const stormMs = Date.now() - stormStart;
  assert(
    consults >= 4 && consults <= 20,
    `failing provider must be bounded by not_before + retry budget, got ${consults} consults`,
  );
  assert(
    stormMs >= 1500,
    `retries must be paced by durable backoff, storm took only ${stormMs}ms`,
  );
  assert(
    parkedSeen,
    "durable not_before backoff must be observable while inputs retry",
  );
  for (const id of [in9, in10]) {
    const st = await getInput(id);
    assert(
      st?.status === "done",
      `input ${id} must resolve honestly, got ${JSON.stringify(st)}`,
    );
    const fails = await outboxFor(id);
    assert(
      fails.length === 1 &&
        fails[0].kind === "turn_failed" &&
        /provider exploded/.test(fails[0].payload.error ?? ""),
      `input ${id} needs one honest turn_failed record, got ${JSON.stringify(fails)}`,
    );
  }
  log("  bounded paced retries; both inputs resolved with turn_failed");

  // The queue is healthy after the storm: a fresh input completes normally.
  const in11 = (await submit("input-eleven")).json.input.input_id;
  runChild({
    SUMI_SCRIPT: JSON.stringify({ rounds: [{ text: "reply-G", calls: [] }] }),
  });
  const replies11 = await outboxFor(in11);
  assert(
    replies11.length === 1 && replies11[0].payload.output.text === "reply-G",
    "post-storm input must complete",
  );
  log("  queue healthy after the failure storm");

  // --- scenario 6: >body-limit commit still finalizes (F-B1) ----------
  // A commit payload over the server's 1 MiB body limit used to defeat
  // every commitTurnFinal tier when `error` itself was huge: three 400s,
  // child exit 1, input claimed forever, each recovery cycle re-claiming
  // the same oldest input before any later queued work — head-of-line
  // starvation at process cadence. The recorded failure must be bounded.
  //
  // 6a — oversized *complete* commit: a ~3.36 MB multibyte reply keeps
  // savePlan under the current 4 MiB limit, but the commit body
  // (assistant_message event + output) doubles past it → 400 "read body"
  // → tiers land with a bounded honest error. The turn is observably
  // failed — not fabricated — and the input resolves.
  log("scenario 6: >4MiB commit resolves bounded; queue proceeds");
  const in12 = (await submit("input-twelve")).json.input.input_id;
  const big = runChild({
    SUMI_SCRIPT: JSON.stringify({ textSize: 560_000 }), // ~3.36 MB reply
  });
  assert(
    !big.includes("fatal"),
    `oversize commit must not kill the process\n${big.slice(0, 400)}`,
  );
  const in12State = await getInput(in12);
  assert(
    in12State?.status === "done",
    `oversized complete must resolve the input, got ${JSON.stringify(in12State)}`,
  );
  const fails12 = await outboxFor(in12);
  const recErr = fails12[0]?.payload?.error ?? "";
  assert(
    fails12.length === 1 &&
      fails12[0].kind === "turn_failed" &&
      recErr.includes("commit rejected deterministically") &&
      recErr.includes("read body") &&
      new TextEncoder().encode(recErr).length < 20_000,
    `recorded failure must be bounded and honest, got: ${recErr.slice(0, 200)}`,
  );
  log("  oversized complete resolved via fallback; honest bounded error");

  // 6b — oversized provider *error* (retryable): bounded recorded
  // failure, backoff-bounded retries, and a later queued input still
  // progresses. SUMI_MAX_ATTEMPTS keeps both honestly queued through the
  // storm; not_before set on each proves the later input was claimed,
  // failed, and reparked — not starved behind the first.
  const in13 = (await submit("input-thirteen")).json.input.input_id;
  const in14 = (await submit("input-fourteen")).json.input.input_id;
  const giant = runChild({
    SUMI_SCRIPT: JSON.stringify({ throwSize: 1_040_000 }), // ~6.24 MB error
    SUMI_MAX_ATTEMPTS: "50",
  });
  // The error is bounded at the source (8 KiB), so the first commit
  // already fits — no scrubbed/minimal tier is needed for it to land.
  assert(
    giant.includes('"retryable":true') &&
      giant.includes("turn failed at model"),
    "the oversized provider error must commit as a bounded retryable failure",
    giant.slice(0, 400),
  );
  for (const id of [in13, in14]) {
    const st = await getInput(id);
    assert(
      st?.status === "queued" && st?.not_before !== null,
      `input ${id} stays honestly queued for retry (claimed + reparked), got ${JSON.stringify(st)}`,
    );
  }
  // A parked input is not claimable yet, and a --once child exits after 2 s
  // without work: wait out both reparks so the heal child claims them.
  const waitClaimable = async (id) => {
    const t0 = Date.now();
    for (;;) {
      const st = await getInput(id);
      const nb = st?.not_before ? Date.parse(st.not_before) : 0;
      if (st?.status === "queued" && nb <= Date.now()) return st;
      assert(
        Date.now() - t0 < 30_000,
        `input ${id} never became claimable: ${JSON.stringify(st)}`,
      );
      await new Promise((r) => setTimeout(r, 200));
    }
  };
  await waitClaimable(in13);
  await waitClaimable(in14);
  // The provider healing completes both, exactly once — the recorded
  // failure was honest retry, not fabrication or a strand.
  runChild({
    SUMI_SCRIPT: JSON.stringify({ rounds: [{ text: "reply-H", calls: [] }] }),
  });
  for (const id of [in13, in14]) {
    const replies = await outboxFor(id);
    assert(
      replies.length === 1 && replies[0].payload.output.text === "reply-H",
      `input ${id} must complete once after the provider recovers`,
    );
  }
  log("  oversized error recorded bounded; later input progressed; both recovered");

  // --- scenario 7: real OpenAIProvider failure modes on the real stack ---
  // The actual OpenAI adapter (not ScriptedProvider) against a scripted
  // OpenAI-compatible SSE endpoint, real Go + PG + Node, default retry
  // budget (no override). Every ordinary provider failure mode must leave
  // the input honestly queued — a short outage, a 429 honoring
  // Retry-After, an in-band stream error, and a truncated stream — and a
  // healed provider completes the SAME input exactly once.
  log("scenario 7: real OpenAIProvider survives ordinary provider failures");
  let sseReqs = 0;
  const sseServer = createServer((sreq, res) => {
    sseReqs++;
    const n = sseReqs;
    if (n === 1) {
      sreq.socket.destroy(); // hard network outage mid-request
      return;
    }
    if (n === 2) {
      res.writeHead(429, { "retry-after": "8" });
      res.end("rate limited");
      return;
    }
    if (n === 3) {
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.end(
        `data: {"choices":[{"delta":{"content":"partial "}}]}\n\n` +
          `data: {"error":{"message":"router blew up","code":502}}\n\n`,
      );
      return;
    }
    if (n === 4) {
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.end(`data: {"choices":[{"delta":{"content":"partial "}}]}\n\n`); // EOF, no DONE
      return;
    }
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end(
      `data: {"choices":[{"delta":{"content":"real adapter recovered"}}]}\n\n` +
        `data: {"choices":[{"delta":{},"finish_reason":"stop"}]}\n\n` +
        `data: [DONE]\n\n`,
    );
  });
  await new Promise((r) => sseServer.listen(0, "127.0.0.1", r));
  const sseBase = `http://127.0.0.1:${sseServer.address().port}`;
  const openaiEnv = {
    SUMI_PROVIDER: "openai",
    SUMI_MODEL_BASE_URL: sseBase,
    // One consult per boot: any retryable park (>=200ms) outlives the
    // 60ms idle grace, so each scripted failure mode maps to one boot.
    SUMI_ONCE_IDLE_MS: "60",
  };
  const in15 = (await submit("input-fifteen")).json.input.input_id;

  // The scripted SSE endpoint lives in this parent process, so the
  // openai children MUST be spawned asynchronously — a spawnSync child
  // would block the event loop and the server could never answer.
  const runOpenAI = () => runChildAsync(openaiEnv).done;

  // Phase 1 — network outage: retryable failure, input parked not lost.
  await runOpenAI();
  let st15 = await getInput(in15);
  assert(
    st15?.status === "queued" && st15?.not_before !== null,
    `network outage must leave the input queued for retry, got ${JSON.stringify(st15)}`,
  );
  assert(
    (await outboxFor(in15)).length === 0,
    "a transient outage must not resolve the request",
  );

  // Phase 2 — 429 with Retry-After:8s: the durable requeue honors provider
  // pacing — the parked delay far exceeds the attempt backoff (~400ms).
  await waitClaimable(in15);
  await runOpenAI();
  st15 = await getInput(in15);
  const raDelay = Date.parse(st15?.not_before ?? 0) - Date.now();
  assert(
    st15?.status === "queued" && raDelay > 3_000 && raDelay < 60_000,
    `Retry-After must pace the requeue (~8s), got not_before=${st15?.not_before} (${raDelay}ms)`,
  );

  // Phase 3 — in-band SSE error chunk on a 200 stream: a failure, never a reply.
  await waitClaimable(in15);
  await runOpenAI();
  st15 = await getInput(in15);
  assert(
    st15?.status === "queued",
    `in-band stream error must stay retryable, got ${JSON.stringify(st15)}`,
  );

  // Phase 4 — truncated stream (EOF, no [DONE]/finish_reason): not a reply.
  await waitClaimable(in15);
  await runOpenAI();
  st15 = await getInput(in15);
  assert(
    st15?.status === "queued",
    `incomplete stream must stay retryable, got ${JSON.stringify(st15)}`,
  );
  const failedTurns15 = (
    await req(
      "GET",
      `/internal/core/personas/${personaId}/events?after_seq=0`,
      ptoken,
    )
  ).json.events.filter(
    (e) => e.kind === "assistant_message" && /partial/.test(e.payload.text ?? ""),
  );
  assert(
    failedTurns15.length === 0,
    "partial stream text must never be journaled as a reply",
  );

  // Phase 5 — provider heals: the SAME input completes exactly once.
  await waitClaimable(in15);
  await runOpenAI();
  st15 = await getInput(in15);
  assert(
    st15?.status === "done",
    `input must complete after the provider heals, got ${JSON.stringify(st15)}`,
  );
  const replies15 = await outboxFor(in15);
  assert(
    replies15.length === 1 &&
      replies15[0].kind === "turn_completed" &&
      replies15[0].payload.output.text === "real adapter recovered",
    `exactly one real reply expected, got ${JSON.stringify(replies15)}`,
  );
  assert(
    sseReqs >= 5,
    `each failure mode must reach the real adapter, saw ${sseReqs} requests`,
  );
  sseServer.close();
  log("  outage, Retry-After, in-band error, truncated stream all survived; heal completed once");
  // --- scenario 8: near-limit input + transient error keeps retryable --
  // Opus F1/F2: when the input_received event alone (~1.04 MB) pushes
  // every event-carrying commit tier over the body limit, only the
  // minimal commit can land. That tier must still preserve a retryable
  // disposition — a transient provider error on a near-limit message is
  // not a terminal failure — and the record keeps both server rejection
  // reasons ahead of the bounded detail.
  log("scenario 8: near-limit input + transient error still retries");
  const bigText = "x".repeat(1_042_000); // ~1.02 MiB — accepted at submit
  const sub16 = await req(
    "POST",
    `/internal/core/personas/${personaId}/inputs`,
    ptoken,
    {
      input_id: `in-${randomUUID()}`,
      kind: "message",
      payload: { text: bigText },
      actor_kind: "human",
      actor_id: "e2e",
      source_surface: "e2e",
    },
  );
  assert(sub16.status === 201, `near-limit submit ${sub16.status}`);
  const in16 = sub16.json.input.input_id;
  const in17 = (await submit("input-fifteen")).json.input.input_id;
  const near = runChild({
    SUMI_SCRIPT: JSON.stringify({ throwSize: 4_000 }), // ~24 KB error
  });
  assert(
    near.includes("turn failed at model"),
    "transient error should be recorded",
    near,
  );
  // in16 is honestly queued for retry — on this branch a retryable
  // failure commits events:[] so it lands on the first tier, and the
  // retryable disposition flows through directly. If it had been
  // dropped the input would read `done` + failed.
  const st16 = await req(
    "GET",
    `/internal/core/personas/${personaId}/inputs/${in16}`,
    ptoken,
  );
  assert(
    st16.json?.input?.status === "queued",
    `near-limit input must stay queued for retry, got ${st16.text}`,
  );
  // On this branch a retrying input commits no per-attempt journal —
  // claimed + reparked is observable via a queued status + not_before.
  const st17 = await getInput(in17);
  assert(
    st17?.status === "queued" && st17?.not_before !== null,
    `later input must progress during the near-limit input's backoff, got ${JSON.stringify(st17)}`,
  );
  // Provider heals: wait for both reparks to expire so a single heal
  // child claims them — a parked input is not claimable yet.
  await waitClaimable(in16);
  await waitClaimable(in17);
  runChild({
    SUMI_SCRIPT: JSON.stringify({ rounds: [{ text: "reply-I", calls: [] }] }),
  });
  for (const id of [in16, in17]) {
    const replies = await outboxFor(id);
    assert(
      replies.length === 1 && replies[0].payload.output.text === "reply-I",
      `input ${id} must complete once after the provider recovers`,
    );
  }
  log("  minimal-tier commit kept retryable; near-limit input recovered");

  svc.kill("SIGKILL");
  log("PASS — durable-plan scenarios green on real PG + real Go + real Node");
}
