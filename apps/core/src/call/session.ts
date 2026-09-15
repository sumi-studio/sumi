import {
  AudioFrame,
  AudioSource,
  AudioStream,
  DisconnectReason,
  LocalAudioTrack,
  Room,
  RoomEvent,
  TrackKind,
  TrackPublishOptions,
  TrackSource,
} from "@livekit/rtc-node";
import type { RemoteTrack } from "@livekit/rtc-node";
import type { CallSession, CallUtterance } from "../types.ts";
import {
  CallBridgeClient,
  CallBridgeError,
  CallClaimLostError,
} from "./bridge-client.ts";
import type { CallSTT, CallTTS } from "./adapters.ts";
import { VadSegmenter } from "./adapters.ts";

/**
 * One claimed call session's media actor. It owns exactly what its claim
 * authorizes: join under the epoch-tagged identity, hear speech into durable
 * inputs, emit committed utterances, and report truthful dispositions.
 *
 * Stop rules (never rejoin without the claim):
 *  - heartbeat 409 → the claim is gone → disconnect and exit;
 *  - DUPLICATE_IDENTITY / PARTICIPANT_REMOVED disconnect → the server evicted
 *    this actor → disconnect and exit (a successor or an authority decision
 *    owns the room now; rejoining would fight them);
 *  - session status 'ending' → disconnect, report 'ended', exit;
 *  - network loss without eviction → bounded reconnect: each attempt re-mints
 *    a ticket, which itself re-verifies the live claim.
 */

export interface SessionActorDeps {
  client: CallBridgeClient;
  persona: string;
  runnerId: string;
  leaseMs: number;
  stt: CallSTT;
  tts: CallTTS;
  /** VAD tuning: bound on one detected speech segment. */
  vadMaxMs?: number;
  vadHangoverMs?: number;
  /** Deliver one transcript into the secretary's durable input stream. */
  submitInput: (input: {
    inputId: string;
    kind: string;
    attention: "reply" | "observe" | "defer";
    actorKind: string;
    actorId: string;
    sourceSurface: string;
    threadId: string;
    occurredAt?: string;
    payload: Record<string, unknown>;
  }) => Promise<void>;
  log: (msg: string, fields?: Record<string, unknown>) => void;
  /** Test hook: replace room construction. */
  makeRoom?: () => Room;
}

const HEARTBEAT_DIVISOR = 3;
const UTTERANCE_POLL_MS = 400;
const MAX_RECONNECTS = 8;



function speakerRef(identity: string): { kind: string; id: string } {
  const cut = identity.indexOf(":");
  if (cut < 0) return { kind: "unknown", id: identity };
  let id = identity.slice(cut + 1);
  const tag = id.indexOf("#");
  if (tag >= 0) id = id.slice(0, tag);
  return { kind: identity.slice(0, cut), id };
}

export class CallSessionActor {
  private readonly deps: SessionActorDeps;
  private session: CallSession;
  private room: Room | null = null;
  private stopped = false;
  private segmentSeq = 0;
  private emitting: {
    utterance: CallUtterance;
    startedAt: number;
    cancel: () => void;
  } | null = null;
  private trackLastVoiced = new Map<string, number>();
  private trackSpeechOnset = new Map<string, number>();

  constructor(session: CallSession, deps: SessionActorDeps) {
    this.deps = deps;
    this.session = session;
  }

  /** True while this actor still holds its claim and runs media. */
  get live(): boolean {
    return !this.stopped;
  }

  /**
   * Orderly runner shutdown: disconnect media and report 'ended' under the
   * claim if it is still ours — a claim already lost means the record is no
   * longer ours to move and the lapse path owns it.
   */
  async shutdown(): Promise<void> {
    if (this.stopped) return;
    await this.gracefulEnd("runner_shutdown");
  }

  get sessionId(): string {
    return this.session.session_id;
  }

  async run(): Promise<void> {
    try {
      await this.connectWithClaim();
      if (this.stopped) return;
      await this.reportStatus("active", "connected");
      await Promise.all([
        this.heartbeatLoop(),
        this.utteranceLoop(),
      ]);
    } catch (e) {
      if (e instanceof CallClaimLostError) {
        this.deps.log("call session claim lost; stopping media", {
          session: this.session.session_id,
        });
        return;
      }
      if (!this.stopped) {
        await this.reportStatus("failed", String(e)).catch(() => {});
        this.deps.log("call session failed", {
          session: this.session.session_id,
          error: String(e),
        });
      }
    } finally {
      await this.teardown();
    }
  }

  // --- lifecycle -----------------------------------------------------------

  private async connectWithClaim(): Promise<void> {
    const ticket = await this.deps.client.sessionTicket(
      this.deps.persona,
      this.session.session_id,
      { runnerId: this.deps.runnerId, epoch: this.session.epoch },
    );
    const room = (this.deps.makeRoom ?? (() => new Room()))();
    this.room = room;
    this.wireRoomEvents(room);
    await room.connect(ticket.url, ticket.token, {
      autoSubscribe: true,
      dynacast: false,
    });
  }

  private wireRoomEvents(room: Room): void {
    room.on(RoomEvent.TrackSubscribed, (track, _pub, participant) => {
      if (track.kind !== TrackKind.KIND_AUDIO) return;
      this.deps.log("subscribed audio track", {
        session: this.session.session_id,
        speaker: participant.identity,
      });
      this.hearTrack(track, participant.identity).catch((e) =>
        this.deps.log("audio hear error", { error: String(e) }),
      );
    });
    room.on(RoomEvent.Disconnected, (reason) => {
      this.onRoomDisconnected(reason);
    });
  }

  private onRoomDisconnected(reason?: DisconnectReason): void {
    if (this.stopped) return;
    this.deps.log("room disconnected", {
      session: this.session.session_id,
      reason,
    });
    if (
      reason === DisconnectReason.DUPLICATE_IDENTITY ||
      reason === DisconnectReason.PARTICIPANT_REMOVED ||
      reason === DisconnectReason.ROOM_DELETED
    ) {
      // Evicted — a successor or an authority decision owns this room
      // presence. Do not fight it: stop and let the claim lapse report.
      this.stopped = true;
      this.reportStatus("failed", `evicted:${reason}`).catch(() => {});
      return;
    }
    // Transport loss: bounded reconnect — every attempt re-mints a ticket,
    // which re-verifies the claim before any media resumes.
    void this.reconnectLoop();
  }

  private async reconnectLoop(): Promise<void> {
    for (let attempt = 1; attempt <= MAX_RECONNECTS && !this.stopped; attempt++) {
      await sleep(Math.min(500 * attempt, 3000));
      try {
        await this.connectWithClaim();
        this.deps.log("call session reconnected", {
          session: this.session.session_id,
          attempt,
        });
        return;
      } catch (e) {
        if (e instanceof CallClaimLostError) {
          this.stopped = true;
          return;
        }
        this.deps.log("reconnect attempt failed", {
          session: this.session.session_id,
          attempt,
          error: String(e),
        });
      }
    }
    if (!this.stopped) {
      this.stopped = true;
      await this.reportStatus("failed", "reconnect exhausted").catch(() => {});
    }
  }

  private async heartbeatLoop(): Promise<void> {
    while (!this.stopped) {
      await sleep(Math.max(500, this.deps.leaseMs / HEARTBEAT_DIVISOR));
      try {
        this.session = await this.deps.client.heartbeatSession(
          this.deps.persona,
          this.session.session_id,
          {
            runnerId: this.deps.runnerId,
            epoch: this.session.epoch,
            leaseMs: this.deps.leaseMs,
          },
        );
      } catch (e) {
        if (e instanceof CallClaimLostError || e instanceof CallBridgeError) {
          this.deps.log("heartbeat ended session actor", {
            session: this.session.session_id,
            error: String(e),
          });
          this.stopped = true;
          return;
        }
        throw e;
      }
      if (this.session.status === "ending") {
        await this.gracefulEnd("call.leave");
        return;
      }
    }
  }

  private async gracefulEnd(reason: string): Promise<void> {
    this.stopped = true;
    await this.teardown();
    await this.deps.client
      .reportSessionStatus(
        this.deps.persona,
        this.session.session_id,
        {
          runnerId: this.deps.runnerId,
          epoch: this.session.epoch,
          status: "ended",
          reason,
        },
      )
      .catch(() => {});
  }

  private async reportStatus(
    status: "active" | "ending" | "ended" | "failed",
    reason: string,
  ): Promise<void> {
    this.session = await this.deps.client.reportSessionStatus(
      this.deps.persona,
      this.session.session_id,
      {
        runnerId: this.deps.runnerId,
        epoch: this.session.epoch,
        status,
        reason,
      },
    );
  }

  private async teardown(): Promise<void> {
    this.stopped = true;
    if (this.emitting) this.emitting.cancel();
    const room = this.room;
    this.room = null;
    if (room) {
      try {
        await room.disconnect();
      } catch {
        /* already gone */
      }
    }
  }

  // --- inbound speech ------------------------------------------------------

  private async hearTrack(track: RemoteTrack, identity: string): Promise<void> {
    const ref = speakerRef(identity);
    const vad = new VadSegmenter({
      maxMs: this.deps.vadMaxMs,
      hangoverMs: this.deps.vadHangoverMs,
    });
    const stream = new AudioStream(track, 16000, 1);
    let lastFrameAt = new Date();
    for await (const frame of stream) {
      lastFrameAt = new Date();
      const data = frame.data as Int16Array;
      let peak = 0;
      for (const v of data) {
        const a = Math.abs(v);
        if (a > peak) peak = a;
      }
      const now = Date.now();
      const voiced = peak > 500;
      if (voiced) {
        const lastVoiced = this.trackLastVoiced.get(identity) ?? 0;
        if (now - lastVoiced > (this.deps.vadHangoverMs ?? 500)) {
          // Silence -> speech transition: a new onset.
          this.trackSpeechOnset.set(identity, now);
        }
        this.trackLastVoiced.set(identity, now);
      }
      const segment = vad.push({ data }, lastFrameAt);
      if (segment) {
        await this.transcribeAndSubmit(segment, ref, identity, vad, lastFrameAt);
      }
      // Barge-in: speech that BEGAN after we started emitting interrupts
      // us. Speech that was already in progress when we began does not —
      // otherwise continuous background talk would make the secretary
      // permanently mute. (Documented slice policy: we do not also hold
      // our start until the remote side is silent.)
      if (this.emitting) {
        const onset = this.trackSpeechOnset.get(identity) ?? 0;
        if (onset > this.emitting.startedAt) this.emitting.cancel();
      }
    }
    const tail = vad.close();
    if (tail) {
      await this.transcribeAndSubmit(tail, ref, identity, vad, lastFrameAt);
    }
  }

  private async transcribeAndSubmit(
    samples: Int16Array,
    ref: { kind: string; id: string },
    identity: string,
    _vad: VadSegmenter,
    endedAt: Date,
  ): Promise<void> {
    const transcript = await this.deps.stt.transcribe({
      speakerIdentity: identity,
      speakerRef: `${ref.kind}:${ref.id}`,
      samples,
      sampleRate: 16000,
      startedAt: new Date(endedAt.getTime() - samples.length / 16),
      endedAt,
    });
    if (!transcript) return;
    this.segmentSeq++;
    this.deps.log("speech segment transcribed", {
      session: this.session.session_id,
      speaker: identity,
      samples: samples.length,
      text: transcript.text.slice(0, 80),
    });
    await this.deps.submitInput({
      inputId: `call_utterance:${this.session.session_id}:e${this.session.epoch}:${this.segmentSeq}`,
      kind: "call_utterance",
      attention: "observe",
      actorKind: ref.kind,
      actorId: ref.id,
      sourceSurface: "call",
      threadId: this.session.place_id,
      occurredAt: endedAt.toISOString(),
      payload: {
        kind: "call_utterance",
        session_id: this.session.session_id,
        place_id: this.session.place_id,
        workspace_id: this.session.workspace_id,
        speaker: { kind: ref.kind, id: ref.id, identity },
        // 'transcript', not 'text': assemble() renders payload.text verbatim
        // as the user message, which would hide kind/session_id/provenance
        // from the model. Serializing the whole payload keeps them visible.
        transcript: transcript.text,
        engine: transcript.engine,
        segment_seq: this.segmentSeq,
        duration_ms: Math.round(samples.length / 16),
        ended_at: endedAt.toISOString(),
      },
    });
  }

  // --- outbound speech -----------------------------------------------------

  private async utteranceLoop(): Promise<void> {
    while (!this.stopped) {
      await sleep(UTTERANCE_POLL_MS);
      let pending: CallUtterance[];
      try {
        pending = await this.deps.client.pendingUtterances(
          this.deps.persona,
          this.session.session_id,
          { runnerId: this.deps.runnerId, epoch: this.session.epoch },
        );
      } catch (e) {
        if (e instanceof CallClaimLostError || e instanceof CallBridgeError) {
          this.stopped = true;
          return;
        }
        continue;
      }
      for (const utterance of pending) {
        if (this.stopped) return;
        await this.emit(utterance);
      }
    }
  }

  private async emit(utterance: CallUtterance): Promise<void> {
    const { client, persona, runnerId } = this.deps;
    const epoch = this.session.epoch;
    const report = (status: CallUtterance["status"], detail: Record<string, unknown>) => {
      this.deps.log("utterance disposition", {
        utterance_id: utterance.utterance_id,
        status,
        ...detail,
      });
      return client
        .reportUtterance(persona, this.session.session_id, utterance.utterance_id, {
          runnerId, epoch, status, detail,
        })
        .catch(() => {});
    };
    try {
      await client.reportUtterance(
        persona, this.session.session_id, utterance.utterance_id,
        { runnerId, epoch, status: "dequeued" },
      );
    } catch (e) {
      if (e instanceof CallClaimLostError || e instanceof CallBridgeError) {
        this.stopped = true;
        return;
      }
      throw e;
    }
    const room = this.room;
    const local = room?.localParticipant;
    if (!room || !local) {
      await report("unknown", { reason: "no room connection" });
      return;
    }
    const rendered = await this.deps.tts.render(utterance.text);
    await report("emitting", { started_at: new Date().toISOString() });
    const source = new AudioSource(16000, 1);
    const track = LocalAudioTrack.createAudioTrack("secretary-voice", source);
    const opts = new TrackPublishOptions();
    opts.source = TrackSource.SOURCE_MICROPHONE;
    let cancelled = false;
    const total = rendered.samples.length;
    let emitted = 0;
    this.emitting = {
      utterance,
      startedAt: Date.now(),
      cancel: () => {
        cancelled = true;
      },
    };
    try {
      await local.publishTrack(track, opts);
      const frameSamples = 160; // 10ms at 16kHz
      while (emitted < total) {
        if (cancelled || this.stopped) {
          await report("interrupted", {
            fraction: emitted / total,
            reason: "barge_in",
          });
          return;
        }
        const chunk = rendered.samples.subarray(
          emitted,
          Math.min(emitted + frameSamples, total),
        );
        await source.captureFrame(
          new AudioFrame(chunk, 16000, 1, chunk.length),
        );
        emitted += chunk.length;
        await sleep((chunk.length / 16000) * 1000);
      }
      await report("emitted", {
        emitted_at: new Date().toISOString(),
        duration_ms: Math.round(total / 16),
      });
    } catch (e) {
      await report("failed", { error: String(e), fraction: emitted / total });
    } finally {
      this.emitting = null;
      const sid = track.sid;
      if (sid) {
        try {
          await local.unpublishTrack(sid);
        } catch {
          /* room may be gone */
        }
      }
    }
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
