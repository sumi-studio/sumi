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
 * contract does not yet expose pending wake times.
 *
 * Concurrency: drains are serialized per DO instance. A wake arriving while
 * a drain runs marks a re-drain instead of starting a parallel drain — two
 * drains sharing one Secretary sabotaged each other (generation bump fenced
 * the in-flight turn; the loser's stop() aborted the winner's stream).
 *
 * Env bindings (worker config):
 *   SUMI_STATE_URL    — base URL of the Go state service
 *   SUMI_MODEL_*      — provider config (same as local host)
 *   SUMI_HEARTBEAT_MS — alarm interval override (default 30000)
 *   SUMI_DORMANT_REARM_MS — re-arm interval while the persona token
 *                       binding is missing (default 30min)
 *   SECRETARY         — Durable Object namespace binding
 * Persona capability tokens are provisioned per-persona via the admin
 * surface (POST /internal/core/personas) — the worker stores them in its
 * own secret store, never in DO storage.
 */

import { providerFromEnv } from "./provider-env.ts";
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
  private readonly ctx: AlarmState;
  private readonly env: EnvLike;

  constructor(ctx: AlarmState, env: EnvLike) {
    this.ctx = ctx;
    this.env = env;
  }

  private heartbeatMs(): number {
    const v = Number(this.env.SUMI_HEARTBEAT_MS);
    return Number.isFinite(v) && v >= 250 ? v : DEFAULT_HEARTBEAT_MS;
  }

  private dormantRearmMs(): number {
    const v = Number(this.env.SUMI_DORMANT_REARM_MS);
    return Number.isFinite(v) && v >= 1_000 ? v : DEFAULT_DORMANT_REARM_MS;
  }

  /** Build the per-persona secretary; overridable for tests. */
  protected newSecretary(personaId: string): Secretary {
    // Same SUMI_MODEL_* contract as the local host — a persona's secretary
    // runs the identical provider config under workerd and Node.
    const provider = providerFromEnv((n) =>
      typeof this.env[n] === "string" ? (this.env[n] as string) : undefined,
    );
    return new Secretary({
      personaId,
      holderId: `workerd-${personaId}`,
      state: new HttpStateClient(
        this.env.SUMI_STATE_URL,
        envToken(this.env, personaId),
      ),
      provider,
      leaseTtlMs: 30_000,
      renewEveryMs: 10_000,
      // Row bound only; the state service bounds raw context by capacity.
      contextLimit: 5_000,
      pollIntervalMs: 0,
      scheduleEveryMs: 1_000,
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
    let rearmIn = this.heartbeatMs();
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
      const deadline = Date.now() + 25_000; // DO wall-clock budget
      while (Date.now() < deadline) {
        const r = await s.step();
        if (r === "turn") continue;
        // Idle, but a memory preparation branch is still in flight: keep
        // serving inputs while it finishes rather than aborting it at stop.
        if (r === "idle" && s.memoryBusy) {
          await new Promise((res) => setTimeout(res, 200));
          continue;
        }
        break;
      }
      await s.stop();
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
