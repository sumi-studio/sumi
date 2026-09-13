/**
 * Cloud/workerd host: a Durable Object per persona that wakes the shared
 * core. Canonical state lives ONLY in PostgreSQL via the state service —
 * `ctx.storage` is deliberately unused (cache-only rule from progress.md).
 * Recovery after isolate loss = next alarm/fetch re-acquires the writer
 * lease and calls recover(); PG is the sole source of truth.
 *
 * Env bindings (worker config):
 *   SUMI_STATE_URL    — base URL of the Go state service
 *   SUMI_MODEL_*      — provider config (same as local host)
 *   SECRETARY         — Durable Object namespace binding
 * Persona capability tokens are provisioned per-persona via the admin
 * surface (POST /internal/core/personas) — the worker stores them in its
 * own secret store, never in DO storage.
 */

import type { ModelProvider } from "../provider.ts";
import { MockProvider } from "../providers/mock.ts";
import { Secretary } from "../secretary.ts";
import { HttpStateClient } from "../state-client.ts";

/** Minimal structural types — avoids a workers-types hard dependency. */
interface AlarmState {
  waitUntil(promise: Promise<unknown>): void;
  storage: {
    setAlarm(when: number): Promise<void>;
    getAlarm(): Promise<number | null>;
  };
}

interface EnvLike {
  SUMI_STATE_URL: string;
  SUMI_MODEL_PROVIDER?: string;
  SECRETARY: {
    idFromName(name: string): unknown;
    get(id: unknown): { fetch(req: Request): Promise<Response> };
  };
  [key: string]: unknown;
}

function envToken(env: EnvLike, persona: string): string {
  const v = env[`SUMI_PERSONA_TOKEN_${persona.replace(/-/g, "_")}`];
  if (typeof v !== "string" || !v)
    throw new Error(`missing persona token binding for ${persona}`);
  return v;
}

export class SecretaryObject {
  private secretary: Secretary | null = null;
  private personaId = "";
  private readonly ctx: AlarmState;
  private readonly env: EnvLike;

  constructor(ctx: AlarmState, env: EnvLike) {
    this.ctx = ctx;
    this.env = env;
  }

  private build(personaId: string): Secretary {
    if (this.secretary) return this.secretary;
    this.personaId = personaId;
    const provider: ModelProvider = new MockProvider(); // real provider wiring lands with secrets
    this.secretary = new Secretary({
      personaId,
      holderId: `workerd-${personaId}`,
      state: new HttpStateClient(
        this.env.SUMI_STATE_URL,
        envToken(this.env, personaId),
      ),
      provider,
      leaseTtlMs: 30_000,
      renewEveryMs: 10_000,
      contextLimit: 60,
      pollIntervalMs: 0,
      scheduleEveryMs: 1_000,
      idgen: () => crypto.randomUUID(),
    });
    return this.secretary;
  }

  /** Wake via fetch: acquire, recover, drain pending work once. */
  async fetch(req: Request): Promise<Response> {
    const persona = new URL(req.url).pathname.split("/")[2];
    if (!persona)
      return Response.json({ error: "persona required" }, { status: 400 });
    const s = this.build(persona);
    this.ctx.waitUntil(this.drain(s));
    return Response.json({ ok: true, persona });
  }

  /** Alarm-driven wake for schedules and deferred work. */
  async alarm(): Promise<void> {
    if (!this.personaId) return; // alarm before any fetch — nothing to do
    await this.drain(this.build(this.personaId));
    await this.ctx.storage.setAlarm(Date.now() + 30_000); // next heartbeat
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
        if (r !== "turn") break;
      }
      await s.stop();
    } catch (e) {
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
