import { CallBridgeClient } from "./bridge-client.ts";
import { CallSessionActor, type SessionActorDeps } from "./session.ts";
import type { CallSTT, CallTTS } from "./adapters.ts";

/**
 * The per-placement call media runner: one long-lived process per placement
 * (not per secretary) that claims durable call sessions and runs one media
 * actor per claimed session. Multiple sessions — even across multiple
 * personas, when the runner is started with several persona credentials —
 * are served by the same process; claim authority, not process identity, is
 * what fences media.
 *
 * Kill -9 safe at any point: held claims lapse, their sessions become
 * 'interrupted' and reclaimable, and pending utterances are recorded
 * 'unknown' — never replayed — on the next claim.
 */

export interface CallRunnerDeps {
  client: CallBridgeClient;
  personaId: string;
  runnerId: string;
  leaseMs: number;
  pollMs: number;
  stt: CallSTT;
  tts: CallTTS;
  vadMaxMs?: number;
  vadHangoverMs?: number;
  log: (msg: string, fields?: Record<string, unknown>) => void;
}

export class CallRunner {
  private readonly deps: CallRunnerDeps;
  private readonly actors = new Map<string, CallSessionActor>();
  private stopped = false;

  constructor(deps: CallRunnerDeps) {
    this.deps = deps;
  }

  get activeCount(): number {
    return this.actors.size;
  }

  /** One claim pass; used by --once harnesses and the run loop. */
  async step(): Promise<void> {
    const sessions = await this.deps.client.claimSessions(this.deps.personaId, {
      runnerId: this.deps.runnerId,
      leaseMs: this.deps.leaseMs,
    });
    for (const session of sessions) {
      if (this.actors.has(session.session_id)) continue;
      const actor = new CallSessionActor(session, {
        client: this.deps.client,
        persona: this.deps.personaId,
        runnerId: this.deps.runnerId,
        leaseMs: this.deps.leaseMs,
        stt: this.deps.stt,
        tts: this.deps.tts,
        vadMaxMs: this.deps.vadMaxMs,
        vadHangoverMs: this.deps.vadHangoverMs,
        submitInput: (input) =>
          this.deps.client.submitInput(this.deps.personaId, input),
        log: this.deps.log,
      } satisfies SessionActorDeps);
      this.actors.set(session.session_id, actor);
      void actor.run().finally(() => {
        this.actors.delete(session.session_id);
      });
      this.deps.log("claimed call session", {
        session: session.session_id,
        place: session.place_id,
        epoch: session.epoch,
      });
    }
    for (const [id, actor] of this.actors) {
      if (!actor.live) this.actors.delete(id);
    }
  }

  async run(signal: AbortSignal): Promise<void> {
    const onAbort = () => {
      this.stopped = true;
    };
    signal.addEventListener("abort", onAbort, { once: true });
    while (!this.stopped) {
      try {
        await this.step();
      } catch (e) {
        this.deps.log("claim pass failed", { error: String(e) });
      }
      await new Promise((res) => setTimeout(res, this.deps.pollMs));
    }
    await this.stop();
  }

  async stop(): Promise<void> {
    this.stopped = true;
    await Promise.all(
      [...this.actors.values()].map((actor) =>
        actor.shutdown().catch((e) =>
          this.deps.log("actor shutdown failed", { error: String(e) }),
        ),
      ),
    );
  }
}
