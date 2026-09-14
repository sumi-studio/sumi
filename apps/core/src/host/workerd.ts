/**
 * Cloud/workerd host: a Durable Object per persona that wakes the shared
 * core. Canonical state lives ONLY in PostgreSQL via the state service;
 * `ctx.storage` holds routing/wakeup metadata only (the persona id and the
 * DO's alarm) so an evicted instance can resume its own life — durable
 * secretary state is never written here.
 *
 * Liveness: activating a persona arms a periodic heartbeat alarm
 * (SUMI_HEARTBEAT_MS, default 30s). Each alarm drains pending work —
 * including dispatching due schedules — and re-arms. A wake via fetch drains
 * immediately and ensures the alarm exists. Honest scope: this is a fixed
 * heartbeat per active persona, not next-due-wake scheduling; the state
 * contract does not yet expose pending wake times. Memory preparation that
 * waits is the exception: the alarm is armed for when it can start.
 *
 * Concurrency: drains are serialized per DO instance. A wake arriving while
 * a drain runs marks a re-drain instead of starting a parallel drain — two
 * drains sharing one Secretary sabotaged each other (generation bump fenced
 * the in-flight turn; the loser's stop() aborted the winner's stream).
 *
 * Lifetime: a drain started by fetch serves turns for the turn budget
 * (default 25s) and never starts memory preparation — a preparation branch
 * can take minutes of model time (~104s observed), and a fetch-started drain
 * has no platform wall-clock guarantee to hold it. Preparation runs only in
 * alarm-invoked drains: Cloudflare documents a 15-minute wall time for an
 * alarm handler, and a Durable Object stays active while the handler has
 * pending I/O (ctx.waitUntil does not extend a DO's lifetime). The drain
 * keeps a margin under that limit: it starts a branch only when the branch's
 * own timeout still fits, keeps the branch alive until it records its
 * result, and stops at the lifetime end otherwise — a stop records nothing,
 * so the state service counts an interruption rather than a failed attempt.
 * After a drain that left preparation waiting, the alarm is armed for when
 * it can start (at least 1s ahead). When the model layer reports itself
 * unavailable the secretary shelves memory work for a pause interval, so
 * this wake rests on the shelf/heartbeat cadence — an unbound persona does
 * not spin claim/probe/release at the 1s floor.
 *
 * Env bindings (worker config):
 *   SUMI_STATE_URL    — base URL of the Go state service
 *   SUMI_MODEL_*      — provider config (same as local host)
 *   SUMI_HEARTBEAT_MS — alarm interval override (default 30000)
 *   SUMI_DORMANT_REARM_MS — re-arm interval while the persona token
 *                       binding is missing (default 30min)
 *   SUMI_DRAIN_TURN_BUDGET_MS — how long a fetch-started drain takes new
 *                       turns (default 25000)
 *   SUMI_ALARM_DRAIN_LIFETIME_MS — how long an alarm drain may run
 *                       (default and maximum 14min, under the 15min limit)
 *   SUMI_MEMORY_PREPARATION_TIMEOUT_MS — bound on one preparation branch
 *                       (default 10min; clamped to fit the alarm lifetime)
 *   SUMI_MEMORY_UNAVAILABLE_PAUSE_MS — shelf for pending memory while the
 *                       model layer is unusable (default 30s)
 *   SECRETARY         — Durable Object namespace binding
 * Persona capability tokens are provisioned per-persona via the admin
 * surface (POST /internal/core/personas) — the worker stores them in its
 * own secret store, never in DO storage.
 */

import { providerForPersona } from "./provider-env.ts";
import { DEFAULT_MEMORY_PREPARATION_TIMEOUT_MS } from "../memory.ts";
import { Secretary } from "../secretary.ts";
import { HttpStateClient } from "../state-client.ts";

/** Minimal structural types — avoids a workers-types hard dependency. */
interface DOStorage {
  get(key: string): Promise<unknown>;
  put(key: string, value: unknown): Promise<void>;
  setAlarm(when: number): Promise<void>;
  getAlarm(): Promise<number | null>;
}

interface AlarmState {
  waitUntil(promise: Promise<unknown>): void;
  storage: DOStorage;
}

interface EnvLike {
  SUMI_STATE_URL: string;
  SUMI_MODEL_PROVIDER?: string;
  SUMI_HEARTBEAT_MS?: string;
  SECRETARY: {
    idFromName(name: string): unknown;
    get(id: unknown): { fetch(req: Request): Promise<Response> };
  };
  [key: string]: unknown;
}

/** DO-storage key for the persona this object serves (routing metadata). */
const PERSONA_KEY = "sumi/persona_id";
const DEFAULT_HEARTBEAT_MS = 30_000;
// While a persona's token binding is missing, a heartbeat-paced retry
// only loops the same error — but a permanently disarmed DO never
// recovers once the binding lands (fresh-review F5). The dormant cadence
// is long: provisioning is a rare operator action, and a fetch wake or a
// new activation still drains immediately.
const DEFAULT_DORMANT_REARM_MS = 30 * 60_000;
/** New turns a fetch-started drain takes before yielding to the alarm. */
const DEFAULT_DRAIN_TURN_BUDGET_MS = 25_000;
/**
 * An alarm drain's lifetime: under Cloudflare's 15-minute alarm handler
 * wall time, with a minute for stopping and re-arming.
 */
const MAX_ALARM_DRAIN_LIFETIME_MS = 14 * 60_000;
/** Soonest re-arm when memory preparation waits for an alarm drain. */
const MEMORY_WAKE_MIN_MS = 1_000;
/** Poll while a preparation branch runs and no turn is waiting. */
const MEMORY_POLL_MS = 500;

/**
 * Thrown when no SUMI_PERSONA_TOKEN_* binding exists for a persona — a
 * provisioning gap no amount of retrying fixes. alarm() logs it once and
 * re-arms on the long dormant cadence instead of a per-heartbeat error
 * loop or a permanent disarm.
 */
export class MissingPersonaTokenError extends Error {
  constructor(persona: string) {
    super(`missing persona token binding for ${persona}`);
    this.name = "MissingPersonaTokenError";
  }
}

function envToken(env: EnvLike, persona: string): string {
  const v = env[`SUMI_PERSONA_TOKEN_${persona.replace(/-/g, "_")}`];
  if (typeof v !== "string" || !v) throw new MissingPersonaTokenError(persona);
  return v;
}

export class SecretaryObject {
  private secretary: Secretary | null = null;
  private personaId = "";
  private drainPromise: Promise<void> | null = null;
  private wakeAgain = false;
  private missingTokenLogged = false;
  /** Set while alarm() runs: the drain may use the alarm's lifetime. */
  private alarmStartedAt: number | null = null;
  private readonly ctx: AlarmState;
  private readonly env: EnvLike;

  constructor(ctx: AlarmState, env: EnvLike) {
    this.ctx = ctx;
    this.env = env;
  }

  private envMs(name: string, min: number, fallback: number): number {
    const v = Number(this.env[name]);
    return Number.isFinite(v) && v >= min ? v : fallback;
  }

  private heartbeatMs(): number {
    return this.envMs("SUMI_HEARTBEAT_MS", 250, DEFAULT_HEARTBEAT_MS);
  }

  private dormantRearmMs(): number {
    return this.envMs("SUMI_DORMANT_REARM_MS", 1_000, DEFAULT_DORMANT_REARM_MS);
  }

  private turnBudgetMs(): number {
    return this.envMs(
      "SUMI_DRAIN_TURN_BUDGET_MS",
      100,
      DEFAULT_DRAIN_TURN_BUDGET_MS,
    );
  }

  private alarmLifetimeMs(): number {
    return Math.min(
      this.envMs(
        "SUMI_ALARM_DRAIN_LIFETIME_MS",
        1_000,
        MAX_ALARM_DRAIN_LIFETIME_MS,
      ),
      MAX_ALARM_DRAIN_LIFETIME_MS,
    );
  }

  /** Time kept free at the end of an alarm lifetime for recording a result. */
  private lifetimeMarginMs(): number {
    return Math.min(60_000, Math.floor(this.alarmLifetimeMs() / 10));
  }

  /** The preparation bound, clamped so one branch fits an alarm lifetime. */
  protected memoryPreparationTimeoutMs(): number {
    return Math.min(
      this.envMs(
        "SUMI_MEMORY_PREPARATION_TIMEOUT_MS",
        1_000,
        DEFAULT_MEMORY_PREPARATION_TIMEOUT_MS,
      ),
      this.alarmLifetimeMs() - this.lifetimeMarginMs(),
    );
  }

  /** Build the per-persona secretary; overridable for tests. */
  protected newSecretary(personaId: string): Secretary {
    const state = new HttpStateClient(
      this.env.SUMI_STATE_URL,
      envToken(this.env, personaId),
    );
    // The persona's selected model connection is authoritative — resolved
    // through the state service for every model call, with env config only
    // for an unselected persona (same contract as the local host).
    const provider = providerForPersona(
      state,
      personaId,
      (n) =>
        typeof this.env[n] === "string" ? (this.env[n] as string) : undefined,
      (msg, fields) => console.log(`[core] ${msg}`, fields ?? {}),
    );
    return new Secretary({
      personaId,
      holderId: `workerd-${personaId}`,
      state,
      provider,
      leaseTtlMs: 30_000,
      renewEveryMs: 10_000,
      // Row bound only; the state service bounds raw context by capacity.
      contextLimit: 5_000,
      pollIntervalMs: 0,
      scheduleEveryMs: 1_000,
      memoryPreparationTimeoutMs: this.memoryPreparationTimeoutMs(),
      memoryUnavailablePauseMs: this.envMs(
        "SUMI_MEMORY_UNAVAILABLE_PAUSE_MS",
        1_000,
        30_000,
      ),
      idgen: () => crypto.randomUUID(),
    });
  }

  private async build(personaId: string): Promise<Secretary> {
    if (this.personaId && this.personaId !== personaId) {
      throw new Error(
        `persona mismatch: DO serves ${this.personaId}, got ${personaId}`,
      );
    }
    if (!this.personaId) {
      // Construct before persisting: an unprovisioned persona (no token
      // binding, bad provider config) must not arm a heartbeat that can
      // never succeed — the persona id lands in storage only once the
      // secretary could actually be built. (Review-A F2 / B F3.)
      const s = this.newSecretary(personaId);
      this.personaId = personaId;
      await this.ctx.storage.put(PERSONA_KEY, personaId);
      this.secretary = s;
    }
    this.secretary ??= this.newSecretary(personaId);
    return this.secretary;
  }

  /** Wake via fetch: acquire, recover, drain pending work once. */
  async fetch(req: Request): Promise<Response> {
    const persona = new URL(req.url).pathname.split("/")[2];
    if (!persona)
      return Response.json({ error: "persona required" }, { status: 400 });
    let s: Secretary;
    try {
      s = await this.build(persona);
    } catch (e) {
      return Response.json(
        { error: e instanceof Error ? e.message : String(e) },
        { status: 500 },
      );
    }
    const coalesced = this.drainPromise !== null;
    this.ctx.waitUntil(this.requestDrain(s));
    await this.ensureAlarm();
    return Response.json({ ok: true, persona, coalesced });
  }

  /**
   * Alarm-driven wake: drains pending work (due schedules become wake inputs)
   * and re-arms the heartbeat. After an eviction the persona id is recovered
   * from DO storage, so the heartbeat survives restarts.
   */
  async alarm(): Promise<void> {
    const persona =
      this.personaId ||
      (await this.ctx.storage.get(PERSONA_KEY))?.toString() ||
      "";
    if (!persona) return; // never activated — nothing to re-arm either
    const heartbeat = this.heartbeatMs();
    let rearmIn = heartbeat;
    let dormant = false;
    this.alarmStartedAt = Date.now();
    try {
      const s = await this.build(persona);
      this.missingTokenLogged = false;
      await this.requestDrain(s);
    } catch (e) {
      if (e instanceof MissingPersonaTokenError) {
        // The binding is absent until provisioned — re-arming at
        // heartbeat pace only loops the same uncaught error, but
        // disarming entirely leaves an activated persona asleep forever
        // (fresh-review F5). Log once and re-arm on the long dormant
        // cadence: no model/state calls while the token is absent, yet
        // the persona recovers on its own once the binding exists.
        dormant = true;
        rearmIn = this.dormantRearmMs();
        if (!this.missingTokenLogged) {
          this.missingTokenLogged = true;
          console.log(
            `[core] no persona token binding for ${persona}; dormant re-arm in ${rearmIn}ms`,
          );
        }
      } else {
        console.log(
          `[core] alarm error: ${e instanceof Error ? e.message : e}`,
        );
      }
    } finally {
      this.alarmStartedAt = null;
      // Memory preparation that is waiting (a retry backoff, an
      // interrupted branch, one that could not fit this lifetime) starts
      // at the next alarm, armed for when it can start.
      const memoryAt = dormant ? null : this.secretary?.memoryWakeAt();
      if (memoryAt != null) {
        rearmIn = Math.min(
          Math.max(memoryAt - Date.now(), MEMORY_WAKE_MIN_MS),
          heartbeat,
        );
      }
      // Re-arm on every outcome — nothing must permanently disarm an
      // activated writer.
      await this.ctx.storage.setAlarm(Date.now() + rearmIn);
    }
  }

  /**
   * Serialize drains: one runs at a time; a trigger during a drain marks a
   * re-drain so new work is still picked up without a parallel writer.
   */
  private requestDrain(s: Secretary): Promise<void> {
    if (this.drainPromise) {
      this.wakeAgain = true;
      return this.drainPromise;
    }
    const p = this.drainLoop(s).finally(() => {
      if (this.drainPromise === p) this.drainPromise = null;
    });
    this.drainPromise = p;
    return p;
  }

  private async drainLoop(s: Secretary): Promise<void> {
    do {
      this.wakeAgain = false;
      await this.drain(s);
    } while (this.wakeAgain);
  }

  private async ensureAlarm(): Promise<void> {
    if ((await this.ctx.storage.getAlarm()) === null) {
      await this.ctx.storage.setAlarm(Date.now() + this.heartbeatMs());
    }
  }

  /** Arm the alarm no later than `at` (at least MEMORY_WAKE_MIN_MS ahead). */
  private async armAlarmBy(at: number): Promise<void> {
    const when = Math.max(at, Date.now() + MEMORY_WAKE_MIN_MS);
    const current = await this.ctx.storage.getAlarm();
    if (current === null || current > when) {
      await this.ctx.storage.setAlarm(when);
    }
  }

  private async drain(s: Secretary): Promise<void> {
    try {
      await s.start();
    } catch (e) {
      // Lease held by another live writer — it owns the life; back off.
      console.log(
        `[core] drain start failed: ${e instanceof Error ? e.message : e}`,
      );
      return;
    }
    try {
      const turnDeadline = Date.now() + this.turnBudgetMs();
      const margin = this.lifetimeMarginMs();
      for (;;) {
        const now = Date.now();
        // Re-read each round: an alarm firing during a fetch-started drain
        // joins this drain and lends it the alarm's lifetime.
        const lifetimeEnd =
          this.alarmStartedAt === null
            ? null
            : this.alarmStartedAt + this.alarmLifetimeMs();
        const end = lifetimeEnd ?? turnDeadline;
        if (now >= end) break;
        // A turn started near the lifetime end could be cut off mid-call;
        // leave the last stretch to a running branch and the stop.
        const takeTurns =
          lifetimeEnd === null ||
          now < lifetimeEnd - Math.min(this.turnBudgetMs(), margin * 2);
        if (takeTurns) {
          const startMemory =
            lifetimeEnd !== null &&
            now + s.memoryTimeoutMs + margin <= lifetimeEnd;
          const r = await s.step({ startMemory });
          if (r === "stopped") break;
          if (r === "turn") continue;
        }
        // Idle. A preparation branch in flight keeps the drain — and so the
        // alarm handler's pending I/O — alive until it records its result.
        if (!s.memoryBusy) break;
        await new Promise((res) =>
          setTimeout(res, Math.min(MEMORY_POLL_MS, Math.max(end - now, 1))),
        );
      }
      // At the lifetime end this aborts a still-running branch: it records
      // nothing and the next claim counts an interruption.
      await s.stop();
      if (this.alarmStartedAt === null) {
        const memoryAt = s.memoryWakeAt();
        if (memoryAt !== null) await this.armAlarmBy(memoryAt);
      }
    } catch (e) {
      // A mid-drain error (state outage, transient step failure) leaves
      // the Secretary running and the lease held — deliberately: the next
      // alarm's start() renews the same generation and continues the
      // in-flight turn instead of fencing it into a new attempt
      // (fresh-review F6). A real fence loss still surfaces via
      // FencedError on the next step.
      console.log(
        `[core] drain error: ${e instanceof Error ? (e.stack ?? e.message) : e}`,
      );
    }
  }
}

/** Worker entry: POST /personas/:id/wake triggers the DO for that persona. */
export default {
  async fetch(request: Request, env: EnvLike): Promise<Response> {
    const url = new URL(request.url);
    const m = /^\/personas\/([^/]+)\/wake$/.exec(url.pathname);
    if (request.method === "POST" && m) {
      const persona = m[1];
      if (!persona)
        return Response.json({ error: "persona required" }, { status: 400 });
      const id = env.SECRETARY.idFromName(persona);
      return env.SECRETARY.get(id).fetch(request);
    }
    if (url.pathname === "/health") return Response.json({ ok: true });
    return Response.json({ error: "not found" }, { status: 404 });
  },
};
