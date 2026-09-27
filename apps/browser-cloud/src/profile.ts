import { navigationURL, type TabRef } from "@sumi/desktop/browser-contract";
import { BrowserHostAgent, type GoalActivity, type HostCredential } from "@sumi/desktop/browser-host";
import { JevClient } from "@sumi/desktop/browser-jev";
import { type BrowserRunBinding, localPool, MAX_KEEP_ALIVE_MS, sessionAlive } from "./browser-run.ts";
import { takesControl, toCdp, type ViewerInput } from "./input.ts";
import { type Frame, MAX_SNAPSHOT_BYTES, RemoteBrowser, type Snapshot, VIEWPORT } from "./remote-browser.ts";
import { type HostSession, StateClient, StateError, type FetcherLike } from "./state.ts";
import { type AgentActivity, CloudTabPort, type Control } from "./tab-port.ts";
import { type TicketClaims, verifyTicket } from "./ticket.ts";

/** Minimal structural Workers types (no workers-types dependency). */
interface DOStorage {
  get<T>(key: string): Promise<T | undefined>;
  put(key: string, value: unknown): Promise<void>;
  delete(key: string): Promise<boolean>;
  setAlarm(when: number): Promise<void>;
}
export interface DOState {
  storage: DOStorage;
}
interface ServerSocket {
  accept(): void;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: "message", listener: (event: { data: unknown }) => void): void;
  addEventListener(type: "close" | "error", listener: () => void): void;
}
declare const WebSocketPair: { new (): { 0: ServerSocket; 1: ServerSocket } };

export interface Env {
  BROWSER?: BrowserRunBinding;
  /** `.dev.vars` only: local stand-in pool (scripts/local-pool.mjs). */
  LOCAL_POOL?: string;
  SUMI_STATE_URL: string;
  SUMI_STATE?: FetcherLike;
  SUMI_BROWSER_CLOUD_TOKEN?: string;
  /** How long an idle browser (no viewer, no job) stays up. Default 60 s. */
  SUMI_BROWSER_IDLE_GRACE_MS?: string;
  /** Browser Run keepAlive: how long the remote browser survives without
   * this host. Default 60 s; maximum 1,200,000. */
  SUMI_BROWSER_KEEPALIVE_MS?: string;
  /** Test/dev only: loopback Jev fixture. Unset in product. */
  SUMI_JEV_ENDPOINT?: string;
  /** Test configuration only: synthetic fixture sites reached through
   * Browser Run's outboundByHost (comma-separated host names). */
  FIXTURE?: unknown;
  FIXTURE_HOSTS?: string;
}

type Phase = "sleeping" | "starting" | "restoring" | "live" | "saving" | "closing" | "unavailable";

interface Meta {
  profile: string;
  session?: string;
  incarnation: number;
  /** targetId -> stable tab slot id, for reconnecting after a host restart. */
  slots: Record<string, string>;
  /** The session finished starting (restore included) for `incarnation`.
   * A session that is not ready is closed, never reconnected to: it may
   * hold a half-restored profile. */
  ready?: boolean;
}

interface Intent {
  tab: string;
  kind: string;
  label?: string;
  incarnation: number;
  at: number;
}

/** What the person is told after the browser was reconnected or rebuilt. */
interface Recovery {
  kind: "reconnected" | "restored" | "fresh";
  at: number;
  checkpointAt?: string;
  /** An action the secretary started whose result was never observed. */
  uncertain?: { kind: string; label?: string };
  skippedOrigins?: string[];
  /** IndexedDB records the new browser refused while restoring. */
  failedRecords?: number;
}

type Returned = { reason: "viewer_absent" | "person_release"; at: number };

/** The person's control, persisted at each transition (takeover, return,
 * hold start/clear) so that a DO restart keeps it. Never written per input,
 * frame or poll. */
interface SavedControl {
  mode: "agent" | "human";
  epoch: number;
  /** Set while the person has control and no viewer is connected. */
  holdUntil?: number;
  returned?: Returned;
}

interface Viewer {
  human: string;
  authUntil: number;
}

interface AgentEntry {
  agent: BrowserHostAgent;
  credential: HostCredential;
  ticking?: Promise<void>;
  tickStarted: number;
  /** The Jev key this agent was built with; replaced when it differs. */
  jevKey?: string;
}

const VIEWER_SESSION_MS = 10 * 60_000;
const HEARTBEAT_MS = 20_000;
const PERIODIC_CHECKPOINT_MS = 60_000;
const LOOP_LIFETIME_MS = 14 * 60_000;
const MAX_VIEWERS = 8;
/** A refused or failed start is retried by queued work no sooner than this,
 * doubling up to START_RETRY_MAX_MS (the person's 再開 always retries). */
const START_RETRY_MS = 30_000;
const START_RETRY_MAX_MS = 15 * 60_000;
/** A failed checkpoint save is retried after this, doubling up to the max. */
const CHECKPOINT_RETRY_MS = 5_000;
const CHECKPOINT_RETRY_MAX_MS = 5 * 60_000;
/** The person keeps control this long after their last viewer disconnects
 * (reload, brief network loss); then control returns to the secretary. */
export const HUMAN_HOLD_MS = 2 * 60_000;

function number(value: string | undefined, fallback: number, max: number): number {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? Math.min(n, max) : fallback;
}
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** One Durable Object per Cloud browser profile: the only owner of its
 * remote browser. It is the desktop host's counterpart in Cloud — the same
 * BrowserHostAgent drives the existing grant/job/progress contract — and it
 * relays the shared screen and the person's input. */
export class ProfileBrowser {
  private meta?: Meta;
  private remote?: RemoteBrowser;
  private readonly port: CloudTabPort;
  private readonly agents = new Map<string, AgentEntry>();
  private jevKey?: string;
  private jevKeyVersion?: number;
  private jevReported = false;
  private persona?: string;
  private control: Control = { mode: "agent", epoch: 0 };
  private humanAt = 0;
  /** Set while the person has control but no viewer is connected. */
  private humanHoldUntil?: number;
  /** Why control last went back to the secretary (shown to the person). */
  private controlReturned?: Returned;
  private controlLoaded = false;
  private controlWrite: Promise<void> = Promise.resolve();
  private controlUnsaved = false;
  private controlRetryAt = 0;
  private readonly viewers = new Map<ServerSocket, Viewer>();
  private readonly nonces = new Map<string, number>();
  /** The person's screen operations apply in arrival order (a new tab before
   * the address typed into it, key down before key up). */
  private viewerOps: Promise<void> = Promise.resolve();
  private phase: Phase = "sleeping";
  private phaseDetail?: string;
  private starting?: Promise<void>;
  private loopRunning = false;
  private startFailed = false;
  private startFailures = 0;
  private startRetryAt = 0;
  private wantLive = false;
  private closingIntentionally = false;
  private lastActivity = Date.now();
  private lastHeartbeat = 0;
  private checkpointSeq = 0;
  private lastCheckpoint?: Snapshot;
  private lastCheckpointAt?: string;
  private liveSince = 0;
  private checkpointFailures = 0;
  private checkpointRetryAt = 0;
  private dirty = false;
  private checkpointTimer?: ReturnType<typeof setTimeout>;
  private checkpointing?: Promise<void>;
  private goal?: GoalActivity;
  private recovery?: Recovery;
  private dialog?: { tab: string; dialogType: string; message: string };
  private readonly state: StateClient;

  private readonly ctx: DOState;
  private readonly env: Env;

  constructor(ctx: DOState, env: Env) {
    this.ctx = ctx;
    this.env = env;
    this.state = new StateClient(env.SUMI_STATE_URL, env.SUMI_BROWSER_CLOUD_TOKEN ?? "", env.SUMI_STATE);
    this.port = new CloudTabPort({
      browser: () => this.remote,
      profileId: () => this.meta?.profile ?? "",
      control: () => this.control,
      humanInputAt: () => this.humanAt,
      recordIntent: (intent) => this.recordIntent(intent),
      activity: (event) => this.onAgentActivity(event),
    });
  }

  private binding(): BrowserRunBinding {
    if (this.env.LOCAL_POOL) return localPool(this.env.LOCAL_POOL);
    if (!this.env.BROWSER) throw new Error("Browser Run binding is not configured");
    return this.env.BROWSER;
  }

  private async loadMeta(profile?: string): Promise<Meta | undefined> {
    this.meta ??= await this.ctx.storage.get<Meta>("meta");
    if (!this.meta && profile) this.meta = { profile, incarnation: 0, slots: {} };
    if (profile && this.meta && this.meta.profile !== profile) throw new Error("profile mismatch");
    if (this.meta && !this.controlLoaded) {
      this.controlLoaded = true;
      await this.restoreControl();
    }
    return this.meta;
  }

  private async saveMeta(): Promise<void> {
    if (this.meta) await this.ctx.storage.put("meta", this.meta);
  }

  // ---------- requests from the Worker ----------

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/wake" && request.method === "POST") {
      const body = (await request.json().catch(() => ({}))) as { profile?: string; work?: boolean };
      if (!body.profile) return new Response("bad request", { status: 400 });
      await this.loadMeta(body.profile);
      if (this.live()) {
        void this.refresh().catch(() => {});
        return Response.json({ accepted: true, phase: this.phase });
      }
      // Not live: grant/key changes wait for the next start, which receives
      // everything; only queued work starts the browser, and not while a
      // refused start is backing off.
      if (!body.work) return Response.json({ accepted: false, phase: this.phase });
      const wait = this.startRetryAt - Date.now();
      if (wait > 0) return Response.json({ accepted: false, phase: this.phase, detail: this.phaseDetail, retry_after_ms: wait });
      this.wantLive = true;
      await this.kick();
      return Response.json({ accepted: true, phase: this.phase });
    }
    if (url.pathname === "/viewer") {
      const claims = JSON.parse(request.headers.get("x-sumi-viewer") ?? "null") as TicketClaims | null;
      if (!claims) return new Response("forbidden", { status: 403 });
      return this.acceptViewer(claims);
    }
    return new Response("not found", { status: 404 });
  }

  async alarm(): Promise<void> {
    if (!(await this.loadMeta())) return;
    await this.runLoop();
  }

  private live(): boolean {
    return !!this.remote && !this.remote.closed && this.phase === "live";
  }

  /** The loop runs inside an alarm invocation, which keeps this object
   * active; a sleeping profile has no alarm and no loop. */
  private async kick(): Promise<void> {
    if (!this.loopRunning) await this.ctx.storage.setAlarm(Date.now());
  }

  // ---------- lifecycle ----------

  private setPhase(phase: Phase, detail?: string): void {
    this.phase = phase;
    this.phaseDetail = detail;
    this.broadcast({ type: "status", phase, detail, checkpointAt: this.lastCheckpointAt });
  }

  private async ensureLive(): Promise<boolean> {
    if (this.live()) return true;
    this.starting ??= this.start().finally(() => {
      this.starting = undefined;
    });
    await this.starting;
    return this.live();
  }

  private async start(): Promise<void> {
    const meta = await this.loadMeta();
    if (!meta) return;
    const binding = this.binding();
    this.setPhase("starting");
    const intent = await this.ctx.storage.get<Intent>("intent");
    let acquired: string | undefined;
    let begun: number | undefined;
    try {
      if (meta.session && meta.ready && (await sessionAlive(binding, meta.session).catch(() => false))) {
        try {
          const session = await this.state.begin(meta.profile, false, meta.incarnation);
          if (!session.enabled) return await this.retire(meta);
          await this.connect(meta.session, meta.slots);
          // Continue the stored checkpoint sequence and its carried origins.
          this.adoptCheckpoint(session);
          this.adopt(session, false);
          this.recovery = { kind: "reconnected", at: Date.now(), uncertain: intent ? { kind: intent.kind, label: intent.label } : undefined };
          if (intent) await this.ctx.storage.delete("intent");
          this.setPhase("live");
          this.started();
          this.announce();
          return;
        } catch (error) {
          // A stale incarnation or broken connection: never reuse it.
          this.closingIntentionally = true;
          this.remote?.close();
          this.remote = undefined;
          if (!(error instanceof StateError) || error.status !== 409) throw error;
        }
      }
      if (meta.session) {
        await binding.closeSession(meta.session).catch(() => {});
        meta.session = undefined;
        meta.ready = false;
        await this.saveMeta();
      }
      // Browser Run first: a refused start leaves the profile's incarnation,
      // tokens and state untouched.
      const outbound: Record<string, unknown> = {};
      if (this.env.FIXTURE && this.env.FIXTURE_HOSTS)
        for (const host of this.env.FIXTURE_HOSTS.split(",")) outbound[host.trim()] = this.env.FIXTURE;
      acquired = (
        await binding.acquire({
          keepAlive: number(this.env.SUMI_BROWSER_KEEPALIVE_MS, 60_000, MAX_KEEP_ALIVE_MS),
          ...(Object.keys(outbound).length ? { outboundByHost: outbound } : {}),
        })
      ).sessionId;
      meta.session = acquired;
      meta.ready = false;
      meta.slots = {};
      await this.saveMeta();
      const session = await this.state.begin(meta.profile, true);
      if (!session.enabled) return await this.retire(meta);
      begun = session.incarnation;
      meta.incarnation = session.incarnation;
      await this.saveMeta();
      await this.connect(acquired, {});
      this.adoptCheckpoint(session);
      let restored: Awaited<ReturnType<RemoteBrowser["restore"]>> | undefined;
      if (session.snapshot) {
        this.setPhase("restoring", session.snapshot_at);
        restored = await this.remote?.restore(session.snapshot);
      }
      const notSaved = session.snapshot?.notSaved;
      const skippedOrigins = [...new Set([...(notSaved?.skippedOrigins ?? []), ...(notSaved?.oversizedOrigins ?? []), ...(restored?.failedOrigins ?? [])])];
      this.recovery =
        session.snapshot || intent || session.incarnation > 1
          ? {
              kind: session.snapshot ? "restored" : "fresh",
              at: Date.now(),
              checkpointAt: session.snapshot_at,
              uncertain: intent ? { kind: intent.kind, label: intent.label } : undefined,
              skippedOrigins: skippedOrigins.length ? skippedOrigins : undefined,
              failedRecords: restored?.failedRecords || undefined,
            }
          : undefined;
      if (intent) await this.ctx.storage.delete("intent");
      this.adopt(session, true);
      meta.ready = true;
      await this.saveMeta();
      this.setPhase("live");
      this.started();
      this.announce();
      this.scheduleSlotSave();
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.closingIntentionally = true;
      this.remote?.close();
      this.remote = undefined;
      // Never leave a half-started browser behind: it would be reconnected
      // to and checkpointed over the good state.
      if (acquired) await binding.closeSession(acquired).catch(() => {});
      if (acquired && meta.session === acquired) {
        meta.session = undefined;
        meta.ready = false;
        await this.saveMeta().catch(() => {});
      }
      // The API marked the new incarnation live; say it is not.
      if (begun !== undefined) await this.state.setState(meta.profile, begun, "sleeping").catch(() => {});
      this.startFailures++;
      this.startRetryAt = Date.now() + Math.min(START_RETRY_MAX_MS, START_RETRY_MS * 2 ** (this.startFailures - 1));
      // Quota, concurrency and plan limits come from Browser Run; say so.
      const limited = /limit|quota|too many|429|concurren/i.test(message);
      this.setPhase("unavailable", limited ? "browser_limit" : "start_failed");
      console.error("cloud browser start failed", limited ? "limit" : message.slice(0, 200));
    }
  }

  private started(): void {
    this.startFailures = 0;
    this.startRetryAt = 0;
    this.liveSince = Date.now();
  }

  /** The stored checkpoint this browser continues from. */
  private adoptCheckpoint(session: HostSession): void {
    this.checkpointSeq = session.snapshot_seq;
    this.lastCheckpoint = session.snapshot ?? undefined;
    this.lastCheckpointAt = session.snapshot_at;
    this.checkpointFailures = 0;
    this.checkpointRetryAt = 0;
  }

  private async connect(sessionId: string, known: Record<string, string>): Promise<void> {
    this.closingIntentionally = false;
    this.port.invalidate();
    const remote = await RemoteBrowser.connect(this.binding(), sessionId, {
      onTabs: () => {
        this.broadcastTabs();
        this.scheduleSlotSave();
      },
      onFrame: (frame) => this.sendFrame(frame),
      onLoaded: () => {
        this.dirty = true;
        this.scheduleCheckpoint(3_000);
      },
      onNotice: (code, detail) => this.broadcast({ type: "notice", code, ...detail }),
      onDialog: (slot, dialog) => this.onDialog(slot.id, dialog),
      onClosed: (why) => this.onRemoteClosed(remote, why),
    });
    this.remote = remote;
    await remote.init(known);
    if (this.viewers.size) await remote.startScreencast();
  }

  /** Install the host session: persona, Jev key and one BrowserHostAgent
   * per standing grant (each with its freshly rotated host token). */
  private adopt(session: HostSession, fresh: boolean): void {
    this.persona = session.persona_id;
    if (fresh) {
      for (const entry of this.agents.values()) entry.agent.stop();
      this.agents.clear();
    }
    const key = session.jev_key || undefined;
    if (key !== this.jevKey || session.jev_key_version !== this.jevKeyVersion) this.jevReported = false;
    this.jevKey = key;
    this.jevKeyVersion = session.jev_key_version;
    // An agent in the middle of a tick is replaced when that tick ends.
    for (const [id, entry] of this.agents)
      if (entry.jevKey !== this.jevKey && !entry.ticking) {
        entry.agent.stop();
        this.agents.set(id, this.agent(entry.credential));
      }
    for (const credential of session.attachments) this.agents.set(credential.attachment.attachment_id, this.agent(credential));
    for (const viewer of [...this.viewers.keys()])
      if (this.persona && this.viewerPersona.get(viewer) !== this.persona) viewer.close(4403, "not_authorized");
    this.broadcastTabs();
  }

  private agent(credential: HostCredential): AgentEntry {
    let jev: JevClient | undefined;
    if (this.jevKey) {
      try {
        jev = new JevClient({ apiKey: this.jevKey, endpoint: this.env.SUMI_JEV_ENDPOINT || undefined });
      } catch {
        jev = undefined;
      }
    }
    const agent = new BrowserHostAgent({
      apiOrigin: `${new URL(this.env.SUMI_STATE_URL).origin}/`,
      credential,
      browser: this.port,
      tab: credential.attachment.tab as TabRef,
      jev,
      transport: this.state.hostTransport,
      onGoal: (activity) => {
        this.goal = activity;
        this.broadcast({ type: "goal", activity });
        if (activity.state === "ended") this.scheduleCheckpoint(2_000);
      },
    });
    return { agent, credential, tickStarted: 0, jevKey: this.jevKey };
  }

  private async refresh(): Promise<void> {
    const meta = this.meta;
    if (!meta || !this.live()) return;
    try {
      const session = await this.state.refresh(meta.profile, meta.incarnation, [...this.agents.keys()]);
      if (!session.enabled) return await this.retire(meta);
      this.adopt(session, false);
    } catch (error) {
      if (error instanceof StateError && error.status === 409) await this.sleepNow("stale");
    }
  }

  /** The profile was reset or its secretary retired: close without saving. */
  private async retire(meta: Meta): Promise<void> {
    this.closingIntentionally = true;
    for (const entry of this.agents.values()) entry.agent.stop();
    this.agents.clear();
    this.remote?.close();
    this.remote = undefined;
    if (meta.session) await this.binding().closeSession(meta.session).catch(() => {});
    meta.session = undefined;
    meta.ready = false;
    this.lastCheckpoint = undefined;
    await this.saveMeta();
    this.control = { mode: "agent", epoch: this.control.epoch + 1 };
    this.humanHoldUntil = undefined;
    this.controlReturned = undefined;
    this.controlUnsaved = false;
    // After any queued write, so an older put cannot bring the takeover back.
    this.controlWrite = this.controlWrite.then(() => this.ctx.storage.delete("control")).then(
      () => {},
      () => {},
    );
    await this.controlWrite;
    this.setPhase("sleeping", "reset");
    for (const viewer of [...this.viewers.keys()]) viewer.close(4410, "profile_reset");
  }

  /** Save, then release the remote browser. */
  private async sleepNow(reason: string): Promise<void> {
    const meta = this.meta;
    if (!meta) return;
    if (this.live() && reason !== "stale") {
      this.setPhase("saving");
      await this.checkpoint("before_close").catch(() => {});
    }
    this.setPhase("closing");
    this.closingIntentionally = true;
    for (const entry of this.agents.values()) entry.agent.stop();
    this.agents.clear();
    this.remote?.close();
    this.remote = undefined;
    if (meta.session) await this.binding().closeSession(meta.session).catch(() => {});
    const incarnation = meta.incarnation;
    meta.session = undefined;
    meta.ready = false;
    await this.saveMeta();
    await this.state.setState(meta.profile, incarnation, "sleeping").catch(() => {});
    // Observations of the closed browser are void; who has control does not
    // change by closing it (idle sleep never happens while the person holds it).
    this.control = { mode: this.control.mode, epoch: this.control.epoch + 1 };
    this.setPhase("sleeping", reason);
  }

  private onRemoteClosed(remote: RemoteBrowser, why: string): void {
    if (remote !== this.remote || this.closingIntentionally) return;
    this.remote = undefined;
    this.port.invalidate();
    // Admitted work fails as unknown through its own receipt; nothing is replayed.
    const meta = this.meta;
    if (meta) void this.state.setState(meta.profile, meta.incarnation, "lost").catch(() => {});
    this.setPhase("unavailable", "browser_lost");
    console.warn("cloud browser connection closed", why.slice(0, 80));
  }

  // ---------- main loop ----------

  private async runLoop(): Promise<void> {
    if (this.loopRunning) return;
    this.loopRunning = true;
    const until = Date.now() + LOOP_LIFETIME_MS;
    const grace = number(this.env.SUMI_BROWSER_IDLE_GRACE_MS, 60_000, 30 * 60_000);
    try {
      while (Date.now() < until) {
        const wanted = this.wantLive || this.viewers.size > 0;
        if (!this.live()) {
          if (!wanted) break;
          this.wantLive = false;
          if (!(await this.ensureLive())) {
            // Unavailable: viewers see why and may press 再開; queued work
            // retries after the start backoff. Nothing re-arms on its own.
            this.startFailed = true;
            break;
          }
          this.startFailed = false;
          this.lastActivity = Date.now();
        }
        this.wantLive = false;
        const now = Date.now();
        // Expire the hold before the secretary's agents run.
        const holding = this.control.mode === "human" && !this.viewers.size && this.humanHoldUntil !== undefined;
        if (holding && now >= (this.humanHoldUntil ?? 0)) this.release("viewer_absent");
        if (this.controlUnsaved && now >= this.controlRetryAt) {
          this.controlRetryAt = now + 5_000;
          void this.saveControl();
        }
        let working = false;
        for (const [id, entry] of this.agents) {
          if (entry.ticking) {
            if (now - entry.tickStarted > 2_000) working = true;
            continue;
          }
          entry.tickStarted = now;
          entry.ticking = entry.agent
            .tick()
            .catch((error: { status?: number }) => {
              // 403: the grant was revoked or the secretary retired.
              if (error?.status === 403) {
                entry.agent.stop();
                this.agents.delete(id);
                this.broadcastTabs();
              }
            })
            .finally(() => this.afterTick(id, entry));
        }
        if (working) this.lastActivity = now;
        if (now - this.lastHeartbeat > HEARTBEAT_MS) {
          this.lastHeartbeat = now;
          await this.remote?.heartbeat().catch(() => {});
        }
        const savedAt = Date.parse(this.lastCheckpointAt ?? "") || this.liveSince;
        if (this.dirty && now >= this.checkpointRetryAt && now - savedAt > PERIODIC_CHECKPOINT_MS) this.scheduleCheckpoint(0);
        for (const [viewer, info] of this.viewers)
          if (info.authUntil < now) viewer.close(4401, "reauth_required");
        for (const [nonce, exp] of this.nonces) if (exp < now) this.nonces.delete(nonce);
        const held = this.control.mode === "human" && this.humanHoldUntil !== undefined;
        if (!this.viewers.size && !working && !held && now - this.lastActivity > grace) {
          await this.sleepNow("idle");
          break;
        }
        await sleep(400);
      }
    } finally {
      this.loopRunning = false;
    }
    // Still in use at the end of this invocation's lifetime: continue in a
    // new alarm (one storage write per 14 minutes of live browser).
    if (this.live() || (this.viewers.size && !this.startFailed)) await this.ctx.storage.setAlarm(Date.now() + 1_000);
  }

  private afterTick(id: string, entry: AgentEntry): void {
    entry.ticking = undefined;
    if (this.agents.get(id) !== entry) return;
    // The key changed during the tick: this agent still holds the old one,
    // so its outcome says nothing about the new key.
    if (entry.jevKey !== this.jevKey) {
      entry.agent.stop();
      this.agents.set(id, this.agent(entry.credential));
      return;
    }
    // Jev refused the key (401): withdraw that key for every tab until the
    // person saves a new one.
    if (this.jevKey && this.jevKeyVersion !== undefined && !entry.agent.jevAvailable && !this.jevReported && this.meta) {
      this.jevReported = true;
      void this.state.jevRejected(this.meta.profile, this.jevKeyVersion).catch(() => {});
    }
  }

  // ---------- checkpoint ----------

  private scheduleCheckpoint(delay: number): void {
    if (this.checkpointTimer) return;
    this.checkpointTimer = setTimeout(() => {
      this.checkpointTimer = undefined;
      void this.checkpoint("scheduled").catch(() => {});
    }, delay);
  }

  private async checkpoint(reason: string): Promise<void> {
    if (this.checkpointing) return this.checkpointing;
    // After a failed save, wait (backing off) instead of re-collecting every
    // storage on each loop pass; closing still tries once more.
    if (reason !== "before_close" && Date.now() < this.checkpointRetryAt) return;
    const run = async () => {
      const meta = this.meta;
      const remote = this.remote;
      if (!meta || !remote || remote.closed) return;
      try {
        const snapshot = await remote.collect(this.lastCheckpoint, MAX_SNAPSHOT_BYTES);
        const size = new TextEncoder().encode(JSON.stringify(snapshot)).length;
        if (size > MAX_SNAPSHOT_BYTES) {
          // Cookies and tabs alone exceed the limit.
          this.broadcast({ type: "notice", code: "checkpoint_too_large", bytes: size });
          throw new Error("checkpoint too large");
        }
        const saved = await this.state.saveSnapshot(meta.profile, meta.incarnation, this.checkpointSeq + 1, snapshot);
        this.checkpointSeq++;
        this.lastCheckpoint = snapshot;
        this.lastCheckpointAt = saved.saved_at;
        this.checkpointFailures = 0;
        this.checkpointRetryAt = 0;
        this.dirty = false;
        this.broadcast({ type: "checkpoint", at: saved.saved_at, notSaved: snapshot.notSaved });
      } catch (error) {
        if (error instanceof StateError && error.status === 409) {
          // Another incarnation owns the profile now; this browser is stale.
          await this.sleepNow("stale");
          return;
        }
        this.checkpointFailures++;
        this.checkpointRetryAt = Date.now() + Math.min(CHECKPOINT_RETRY_MAX_MS, CHECKPOINT_RETRY_MS * 2 ** (this.checkpointFailures - 1));
      }
    };
    this.checkpointing = run().finally(() => {
      this.checkpointing = undefined;
    });
    return this.checkpointing;
  }

  private slotTimer?: ReturnType<typeof setTimeout>;
  private scheduleSlotSave(): void {
    if (this.slotTimer) return;
    this.slotTimer = setTimeout(() => {
      this.slotTimer = undefined;
      const meta = this.meta;
      const remote = this.remote;
      if (!meta || !remote || remote.closed) return;
      const slots = remote.targetsBySlot();
      if (JSON.stringify(slots) === JSON.stringify(meta.slots)) return;
      meta.slots = slots;
      void this.saveMeta();
      this.dirty = true;
      this.scheduleCheckpoint(2_000);
    }, 2_000);
  }

  private async recordIntent(intent: { tab: string; kind: string; label?: string } | null): Promise<void> {
    this.lastActivity = Date.now();
    if (!intent) {
      await this.ctx.storage.delete("intent");
      return;
    }
    await this.ctx.storage.put("intent", { ...intent, incarnation: this.meta?.incarnation ?? 0, at: Date.now() } satisfies Intent);
  }

  // ---------- control ----------

  /** The person's input or Take over: control moves to the person before
   * the input is dispatched (in memory at once, so the secretary's next
   * action is refused; durably before the person's input reaches the page).
   * The running goal stops before its next action (an action already handed
   * to the page completes and is recorded). Repeated input writes nothing. */
  private async takeControl(reason: string): Promise<void> {
    this.humanAt = Date.now();
    if (this.control.mode === "human") return;
    this.control = { mode: "human", epoch: this.control.epoch + 1 };
    this.controlReturned = undefined;
    let stopped = 0;
    for (const entry of this.agents.values()) if (entry.agent.stopGoal()) stopped++;
    this.broadcast({ type: "control", mode: "human", reason, goalStopped: stopped > 0, inFlight: this.inFlight });
    await this.saveControl();
  }

  /** Control returns to the secretary: 「秘書に戻す」, or the person stayed
   * disconnected past HUMAN_HOLD_MS. A goal stopped by the takeover stays
   * stopped; nothing is restarted here. */
  private release(reason: Returned["reason"] = "person_release"): void {
    const held = this.humanHoldUntil !== undefined;
    this.humanHoldUntil = undefined;
    if (this.control.mode === "agent") {
      if (held) void this.saveControl();
      return;
    }
    this.control = { mode: "agent", epoch: this.control.epoch + 1 };
    this.controlReturned = { reason, at: Date.now() };
    this.broadcast({ type: "control", mode: "agent", reason, at: this.controlReturned.at });
    void this.saveControl();
    this.dirty = true;
    this.scheduleCheckpoint(500);
  }

  /** Writes are serialized in transition order. A failed write keeps the
   * in-memory state (control stays where the person put it), tells the
   * viewers, and is retried by the loop every 5 s until it lands. */
  private saveControl(): Promise<void> {
    const record: SavedControl = {
      mode: this.control.mode,
      epoch: this.control.epoch,
      holdUntil: this.humanHoldUntil,
      returned: this.controlReturned,
    };
    this.controlWrite = this.controlWrite
      .then(() => this.ctx.storage.put("control", record))
      .then(
        () => {
          this.controlUnsaved = false;
        },
        (error: unknown) => {
          if (!this.controlUnsaved) this.broadcast({ type: "notice", code: "control_not_saved" });
          this.controlUnsaved = true;
          console.error("cloud browser control not saved", error instanceof Error ? error.message.slice(0, 120) : "");
        },
      );
    return this.controlWrite;
  }

  /** A new instance (deploy, DO restart) takes over the saved control. The
   * person's sockets closed with the old instance, so a held control with no
   * hold running starts its hold now; an expired hold returns control to the
   * secretary with its reason. This applies to the same live browser and to
   * a fresh one alike: the takeover belongs to the profile, not to a
   * browser session. Goals the takeover stopped stay stopped. */
  private async restoreControl(): Promise<void> {
    const saved = await this.ctx.storage.get<SavedControl>("control").catch(() => undefined);
    if (!saved) return;
    this.control = { mode: saved.mode === "human" ? "human" : "agent", epoch: Number(saved.epoch) || 0 };
    this.controlReturned = saved.returned;
    if (this.control.mode !== "human") return;
    this.humanAt = Date.now();
    const now = Date.now();
    if (saved.holdUntil !== undefined && saved.holdUntil <= now) {
      this.humanHoldUntil = saved.holdUntil;
      this.release("viewer_absent");
      return;
    }
    this.humanHoldUntil = saved.holdUntil ?? now + HUMAN_HOLD_MS;
    if (saved.holdUntil === undefined) await this.saveControl();
  }

  private inFlight?: { kind: string; label?: string };

  private onAgentActivity(event: AgentActivity): void {
    this.lastActivity = Date.now();
    this.inFlight = event.phase === "start" ? { kind: event.kind, label: event.label } : undefined;
    this.broadcast({ type: "agent", ...event });
    if (event.phase === "end" && event.outcome === "dispatched") {
      this.dirty = true;
      this.scheduleCheckpoint(2_000);
    }
  }

  private onDialog(tab: string, dialog: { type: string; message: string }): void {
    this.dialog = { tab, dialogType: dialog.type, message: dialog.message };
    if (!this.viewers.size) {
      // Nobody can answer: dismiss so the page does not stay blocked.
      void this.remote?.answerDialog(tab, false).catch(() => {});
      this.dialog = undefined;
      return;
    }
    this.broadcast({ type: "dialog", ...this.dialog });
  }

  // ---------- viewers ----------

  private readonly viewerPersona = new Map<ServerSocket, string>();

  private async acceptViewer(claims: TicketClaims): Promise<Response> {
    const meta = await this.loadMeta(claims.b);
    if (!meta || this.nonces.has(claims.n) || this.viewers.size >= MAX_VIEWERS || (this.persona && claims.p !== this.persona))
      return new Response("forbidden", { status: 403 });
    this.nonces.set(claims.n, claims.e);
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    server.accept();
    this.viewers.set(server, { human: claims.h, authUntil: Date.now() + VIEWER_SESSION_MS });
    if (this.humanHoldUntil !== undefined) {
      // The person is back within the hold: control stays theirs.
      this.humanHoldUntil = undefined;
      await this.saveControl();
    }
    this.viewerPersona.set(server, claims.p);
    server.addEventListener("message", (event) => {
      const fail = (error: { code?: string } | undefined) => this.send(server, { type: "error", code: error?.code ?? "failed" });
      // Control changes do not wait behind a slow navigation.
      if (typeof event.data === "string" && event.data.length < 64 && /"type":"(?:takeover|stop-goal)"/.test(event.data)) {
        void this.onViewerMessage(server, event.data).catch(fail);
        return;
      }
      this.viewerOps = this.viewerOps.then(() => this.onViewerMessage(server, event.data).catch(fail));
    });
    const gone = () => {
      if (!this.viewers.delete(server)) return;
      this.viewerPersona.delete(server);
      this.lastActivity = Date.now();
      if (!this.viewers.size) {
        void this.remote?.stopScreencast();
        // A reload or brief disconnect keeps the person's control; the loop
        // returns it to the secretary if nobody is back in HUMAN_HOLD_MS.
        // Save what the person did either way.
        if (this.control.mode === "human") {
          this.humanHoldUntil = Date.now() + HUMAN_HOLD_MS;
          void this.saveControl();
          this.dirty = true;
          this.scheduleCheckpoint(500);
        }
      }
    };
    server.addEventListener("close", gone);
    server.addEventListener("error", gone);
    this.send(server, this.hello());
    if (this.remote?.lastFrame) this.sendFrame(this.remote.lastFrame, server);
    this.wantLive = true;
    this.lastActivity = Date.now();
    if (this.live()) await this.remote?.startScreencast().catch(() => {});
    await this.kick();
    return new Response(null, { status: 101, webSocket: client } as ResponseInit);
  }

  private hello(): Record<string, unknown> {
    return {
      type: "hello",
      viewport: VIEWPORT,
      phase: this.phase,
      detail: this.phaseDetail,
      control: { mode: this.control.mode, holdMs: HUMAN_HOLD_MS, returned: this.controlReturned },
      tabs: this.remote?.tabs() ?? [],
      shared: this.sharedTabs(),
      goal: this.goal,
      recovery: this.recovery,
      checkpointAt: this.lastCheckpointAt,
      notSaved: this.lastCheckpoint?.notSaved,
      dialog: this.dialog,
      unsupported: ["file_upload", "file_drag_drop", "download", "clipboard_copy_out", "audio", "extensions"],
    };
  }

  private sharedTabs(): { tab: string; allowActions: boolean }[] {
    return [...this.agents.values()].map((entry) => ({
      tab: (entry.credential.attachment.tab as TabRef).tabId,
      allowActions: entry.credential.attachment.allow_actions,
    }));
  }

  private announce(): void {
    this.broadcast(this.hello());
  }

  private broadcastTabs(): void {
    this.broadcast({ type: "tabs", tabs: this.remote?.tabs() ?? [], shared: this.sharedTabs() });
  }

  private send(viewer: ServerSocket, message: Record<string, unknown>): void {
    try {
      viewer.send(JSON.stringify(message));
    } catch {
      this.viewers.delete(viewer);
    }
  }

  private broadcast(message: Record<string, unknown>): void {
    if (!this.viewers.size) return;
    const text = JSON.stringify(message);
    for (const viewer of [...this.viewers.keys()]) {
      try {
        viewer.send(text);
      } catch {
        this.viewers.delete(viewer);
      }
    }
  }

  private sendFrame(frame: Frame, only?: ServerSocket): void {
    const text = JSON.stringify({ type: "frame", ...frame });
    for (const viewer of only ? [only] : [...this.viewers.keys()]) {
      try {
        viewer.send(text);
      } catch {
        this.viewers.delete(viewer);
      }
    }
  }

  private async onViewerMessage(viewer: ServerSocket, data: unknown): Promise<void> {
    if (typeof data !== "string" || data.length > 16_384) return;
    const message = JSON.parse(data) as { type: string } & Record<string, unknown>;
    const info = this.viewers.get(viewer);
    if (!info) return;
    this.lastActivity = Date.now();
    switch (message.type) {
      case "reauth": {
        const claims = await verifyTicket(String(message.ticket), this.env.SUMI_BROWSER_CLOUD_TOKEN ?? "");
        if (!claims || claims.b !== this.meta?.profile || claims.h !== info.human || this.nonces.has(claims.n)) {
          viewer.close(4401, "reauth_failed");
          return;
        }
        this.nonces.set(claims.n, claims.e);
        info.authUntil = Date.now() + VIEWER_SESSION_MS;
        return;
      }
      case "start":
        // The person's 再開 retries at once, whatever the backoff.
        this.startRetryAt = 0;
        this.wantLive = true;
        await this.kick();
        return;
      case "takeover":
        await this.takeControl("person_takeover");
        return;
      case "release":
        this.release();
        return;
      case "stop-goal":
        for (const entry of this.agents.values()) entry.agent.stopGoal();
        return;
    }
    if (!this.live() || !this.remote) {
      this.send(viewer, { type: "error", code: "not_live" });
      return;
    }
    const remote = this.remote;
    switch (message.type) {
      case "input": {
        const input = message as unknown as ViewerInput & { tab?: unknown };
        const commands = toCdp(input);
        if (!commands) return;
        // Input applies to the tab whose frame the person saw; if the active
        // tab changed meanwhile (the secretary switched it), refuse it.
        if (input.tab !== remote.active) {
          if (takesControl(input)) this.send(viewer, { type: "notice", code: "tab_changed", tab: remote.active });
          return;
        }
        if (takesControl(input)) await this.takeControl("person_input");
        else if (this.control.mode !== "human") return;
        this.dirty = true;
        for (const command of commands) await remote.input(command.method, command.params);
        return;
      }
      case "tab": {
        await this.takeControl("person_tab");
        const op = String(message.op);
        const id = typeof message.id === "string" ? message.id : remote.active;
        if (op === "new") {
          const slot = await remote.newTab(typeof message.url === "string" ? navigationURL(message.url) : undefined);
          await remote.activate(slot.id);
        } else if (!id) return;
        else if (op === "activate") await remote.activate(id);
        else if (op === "close") await remote.closeTab(id);
        else if (op === "navigate") await remote.navigate(id, navigationURL(message.url));
        else if (op === "back") await remote.history(id, -1);
        else if (op === "forward") await remote.history(id, 1);
        else if (op === "reload") await remote.reload(id);
        this.dirty = true;
        return;
      }
      case "dialog": {
        if (!this.dialog) return;
        const { tab } = this.dialog;
        this.dialog = undefined;
        await this.takeControl("person_dialog");
        await remote.answerDialog(tab, message.accept === true, typeof message.text === "string" ? message.text.slice(0, 2000) : undefined);
        this.broadcast({ type: "dialog", closed: true });
        return;
      }
    }
  }
}
