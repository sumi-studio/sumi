import { LIMITS } from "@sumi/desktop/browser-contract";
import type { BrowserRunBinding } from "./browser-run.ts";
import { openCdp } from "./browser-run.ts";
import type { Cdp, CdpEvent } from "./cdp.ts";
import { COLLECT, type Collected, RESTORE, type OriginData } from "./storage-scripts.ts";

/** Every tab uses this CSS viewport. The person's viewer scales frames of
 * exactly this size, and the secretary's observations report bounds in it,
 * so both refer to the same pixels. */
export const VIEWPORT = { width: 1280, height: 800 } as const;

/** Isolated world for Sumi's page operations: page scripts cannot see or
 * tamper with the operation state. */
const WORLD = "sumi-cloud-browser";

export interface Slot {
  /** Stable Sumi tab id: survives reconnects and checkpoint restoration. */
  id: string;
  targetId: string;
  sessionId: string;
  frameId: string;
  url: string;
  title: string;
  /** Increments on every main-frame navigation (including same-document). */
  revision: number;
  loading: boolean;
  world?: number;
  busy: boolean;
  crashed: boolean;
}

export interface TabSummary {
  id: string;
  url: string;
  title: string;
  active: boolean;
  loading: boolean;
}

export interface Frame {
  /** Slot of the tab this frame shows. */
  tab: string;
  data: string;
  width: number;
  height: number;
  scrollX: number;
  scrollY: number;
}

export interface RemoteHooks {
  onTabs(): void;
  onFrame(frame: Frame): void;
  /** Main-frame document finished loading (checkpoint trigger). */
  onLoaded(slot: Slot): void;
  onNotice(code: string, detail?: Record<string, unknown>): void;
  onDialog(slot: Slot, dialog: { type: string; message: string; defaultPrompt?: string }): void;
  onClosed(why: string): void;
}

export interface Snapshot {
  v: 1;
  at: number;
  cookies: Record<string, unknown>[];
  origins: Record<string, OriginData & { carried?: boolean }>;
  tabs: { slot: string; url: string; title: string; scroll: { x: number; y: number }; active: boolean }[];
  notSaved: NotSaved;
}

export interface NotSaved {
  /** IndexedDB records whose key or value is not plain JSON data. */
  nonJsonIdbValues: number;
  sessionStorageKeys: number;
  /** Origins whose storage could not be read (page error, timeout). */
  skippedOrigins: string[];
  /** Origins whose storage did not fit the checkpoint budget. */
  oversizedOrigins?: string[];
}

/** The API's sealed checkpoint limit (plaintext JSON bytes). */
export const MAX_SNAPSHOT_BYTES = 2 << 20;
/** Room kept for the not-saved lists and the envelope. */
const SNAPSHOT_MARGIN_BYTES = 32 << 10;

const jsonBytes = (value: unknown) => new TextEncoder().encode(JSON.stringify(value)).length;

export class RemoteError extends Error {
  readonly code: string;
  constructor(code: string, message: string) {
    super(message);
    this.code = code;
  }
}

const HTTP = /^https?:/;
const FRAME_INTERVAL_MS = 66;

export class RemoteBrowser {
  readonly slots = new Map<string, Slot>();
  private readonly byTarget = new Map<string, string>();
  /** In-flight attaches: `Target.createTarget` also emits targetCreated, and
   * both callers must get the same slot. */
  private readonly attaching = new Map<string, Promise<Slot | undefined>>();
  private readonly bySession = new Map<string, string>();
  active?: string;
  private screencastSession?: string;
  private streaming = false;
  private lastAck = 0;
  lastFrame?: Frame;
  /** Slot ids to assign to pages opened during restoration, in order. */
  private pendingSlots: string[] = [];
  /** targetId -> slot id kept by a reconnecting host; discovery events for
   * existing pages arrive before init's own loop and must reuse them too. */
  private known: Record<string, string> = {};
  private restoring = false;

  private readonly cdp: Cdp;
  private readonly hooks: RemoteHooks;
  private readonly newId: () => string;

  private constructor(cdp: Cdp, hooks: RemoteHooks, newId: () => string) {
    this.cdp = cdp;
    this.hooks = hooks;
    this.newId = newId;
  }

  get closed(): boolean {
    return this.cdp.closed;
  }

  static async connect(
    binding: BrowserRunBinding,
    sessionId: string,
    hooks: RemoteHooks,
    newId: () => string = () => crypto.randomUUID(),
  ): Promise<RemoteBrowser> {
    let remote: RemoteBrowser | undefined;
    const cdp = await openCdp(binding, sessionId, {
      onEvent: (event) => remote?.onEvent(event),
      onClose: (why) => hooks.onClosed(why),
    });
    remote = new RemoteBrowser(cdp, hooks, newId);
    return remote;
  }

  /** Attach every open page. `known` maps targetIds to slot ids that a
   * reconnecting host kept (a fresh browser has none). */
  async init(known: Record<string, string> = {}): Promise<void> {
    this.known = known;
    await this.cdp.send("Browser.setDownloadBehavior", {
      behavior: "deny",
      eventsEnabled: true,
    }).catch(() => {});
    await this.cdp.send("Target.setDiscoverTargets", { discover: true });
    const { targetInfos } = await this.cdp.send<{
      targetInfos: { targetId: string; type: string; url: string; title: string }[];
    }>("Target.getTargets");
    for (const info of targetInfos.filter((t) => t.type === "page"))
      await this.attach(info.targetId, known[info.targetId], info);
    if (!this.active || !this.slots.has(this.active))
      this.active = this.slots.keys().next().value;
  }

  close(): void {
    this.cdp.close();
  }

  heartbeat(): Promise<unknown> {
    return this.cdp.send("Browser.getVersion", {}, undefined, 10_000);
  }

  targetsBySlot(): Record<string, string> {
    return Object.fromEntries(
      [...this.slots.values()].map((slot) => [slot.targetId, slot.id]),
    );
  }

  tabs(): TabSummary[] {
    return [...this.slots.values()].map((slot) => ({
      id: slot.id,
      url: slot.url,
      title: slot.title,
      active: slot.id === this.active,
      loading: slot.loading,
    }));
  }

  slot(id: string): Slot | undefined {
    return this.slots.get(id);
  }

  private attach(
    targetId: string,
    slotId: string | undefined,
    info?: { url: string; title: string },
  ): Promise<Slot | undefined> {
    const pending = this.attaching.get(targetId);
    if (pending) return pending;
    const attached = this.attachNow(targetId, slotId, info)
      .catch((error) => {
        // A failed attach releases its reservation; a later event may retry.
        const id = this.byTarget.get(targetId);
        if (id && !this.slots.has(id)) this.byTarget.delete(targetId);
        throw error;
      })
      .finally(() => this.attaching.delete(targetId));
    this.attaching.set(targetId, attached);
    return attached;
  }

  private async attachNow(
    targetId: string,
    slotId: string | undefined,
    info?: { url: string; title: string },
  ): Promise<Slot | undefined> {
    const existing = this.byTarget.get(targetId);
    if (existing) return this.slots.get(existing);
    // Reserved (in-flight) tabs count toward the limit.
    if (this.byTarget.size >= LIMITS.tabs) {
      await this.cdp.send("Target.closeTarget", { targetId }).catch(() => {});
      this.hooks.onNotice("tab_limit", { limit: LIMITS.tabs });
      return undefined;
    }
    const id = slotId ?? this.known[targetId] ?? this.pendingSlots.shift() ?? this.newId();
    // Reserve before any await so concurrent targetCreated events agree.
    this.byTarget.set(targetId, id);
    const { sessionId } = await this.cdp.send<{ sessionId: string }>(
      "Target.attachToTarget",
      { targetId, flatten: true },
    );
    const slot: Slot = {
      id,
      targetId,
      sessionId,
      frameId: targetId,
      url: info?.url ?? "about:blank",
      title: info?.title ?? "",
      revision: 0,
      loading: false,
      busy: false,
      crashed: false,
    };
    this.slots.set(id, slot);
    this.bySession.set(sessionId, id);
    await this.cdp.send("Page.enable", {}, sessionId);
    const tree = await this.cdp.send<{ frameTree: { frame: { id: string; url: string; urlFragment?: string } } }>(
      "Page.getFrameTree",
      {},
      sessionId,
    );
    slot.frameId = tree.frameTree.frame.id;
    slot.url = tree.frameTree.frame.url + (tree.frameTree.frame.urlFragment ?? "");
    await this.cdp.send(
      "Emulation.setDeviceMetricsOverride",
      { ...VIEWPORT, deviceScaleFactor: 1, mobile: false },
      sessionId,
    );
    await this.cdp.send("Page.setInterceptFileChooserDialog", { enabled: true }, sessionId).catch(() => {});
    this.hooks.onTabs();
    return slot;
  }

  private interceptor?: (event: CdpEvent) => void;

  private onEvent(event: CdpEvent): void {
    this.interceptor?.(event);
    // biome-ignore lint/suspicious/noExplicitAny: CDP event payloads are protocol JSON.
    const params = event.params as Record<string, any>;
    const slotId = event.sessionId ? this.bySession.get(event.sessionId) : undefined;
    const slot = slotId ? this.slots.get(slotId) : undefined;
    switch (event.method) {
      case "Target.targetCreated": {
        const info = params.targetInfo as { targetId: string; type: string; url: string; title: string };
        // Pages the site opens (target=_blank, window.open) become new tabs.
        if (info.type === "page" && !this.byTarget.has(info.targetId) && !this.restoring)
          void this.attach(info.targetId, undefined, info).catch(() => {});
        return;
      }
      case "Target.targetDestroyed": {
        const id = this.byTarget.get(params.targetId as string);
        if (!id) return;
        const gone = this.slots.get(id);
        this.byTarget.delete(params.targetId as string);
        this.slots.delete(id);
        if (gone) this.bySession.delete(gone.sessionId);
        if (this.active === id) {
          this.active = this.slots.keys().next().value;
          if (this.active) void this.activate(this.active).catch(() => {});
        }
        this.hooks.onTabs();
        return;
      }
      case "Target.targetInfoChanged": {
        const info = params.targetInfo as { targetId: string; url: string; title: string };
        const id = this.byTarget.get(info.targetId);
        const changed = id ? this.slots.get(id) : undefined;
        if (changed && changed.title !== info.title) {
          changed.title = info.title;
          this.hooks.onTabs();
        }
        return;
      }
      case "Browser.downloadWillBegin":
        this.hooks.onNotice("download_unsupported");
        return;
      case "Inspector.targetCrashed":
        if (slot) {
          slot.crashed = true;
          this.hooks.onNotice("tab_crashed", { tab: slot.id });
        }
        return;
    }
    if (!slot) return;
    switch (event.method) {
      case "Page.frameStartedLoading":
        if (params.frameId === slot.frameId) {
          slot.loading = true;
          slot.revision++;
          this.hooks.onTabs();
        }
        return;
      case "Page.frameStoppedLoading":
        if (params.frameId === slot.frameId) {
          slot.loading = false;
          this.hooks.onTabs();
          this.hooks.onLoaded(slot);
          // Chrome does not always report the final title on its own.
          void this.refreshInfo(slot);
        }
        return;
      case "Page.frameNavigated": {
        const frame = params.frame as { id: string; parentId?: string; url: string; urlFragment?: string };
        if (frame.parentId || frame.id !== slot.frameId) return;
        slot.url = frame.url + (frame.urlFragment ?? "");
        slot.revision++;
        slot.world = undefined;
        this.hooks.onTabs();
        return;
      }
      case "Page.navigatedWithinDocument":
        if (params.frameId === slot.frameId) {
          slot.url = params.url as string;
          slot.revision++;
          this.hooks.onTabs();
        }
        return;
      case "Page.screencastFrame": {
        // Acknowledge no faster than ~15 frames/s: Chrome sends the next
        // frame only after the ack, which bounds the viewer's bandwidth.
        const wait = Math.max(0, this.lastAck + FRAME_INTERVAL_MS - Date.now());
        this.lastAck = Date.now() + wait;
        setTimeout(() => {
          void this.cdp
            .send("Page.screencastFrameAck", { sessionId: params.sessionId }, event.sessionId)
            .catch(() => {});
        }, wait);
        if (event.sessionId !== this.screencastSession) return;
        const meta = params.metadata as { deviceWidth: number; deviceHeight: number; scrollOffsetX: number; scrollOffsetY: number };
        this.lastFrame = {
          tab: slot.id,
          data: params.data as string,
          width: meta.deviceWidth,
          height: meta.deviceHeight,
          scrollX: meta.scrollOffsetX,
          scrollY: meta.scrollOffsetY,
        };
        this.hooks.onFrame(this.lastFrame);
        return;
      }
      case "Page.javascriptDialogOpening":
        this.hooks.onDialog(slot, {
          type: params.type as string,
          message: String(params.message ?? "").slice(0, 2000),
          defaultPrompt: params.defaultPrompt as string | undefined,
        });
        return;
      case "Page.fileChooserOpened":
        this.hooks.onNotice("upload_unsupported", { tab: slot.id });
        return;
    }
  }

  async answerDialog(slotId: string, accept: boolean, promptText?: string): Promise<void> {
    const slot = this.need(slotId);
    await this.cdp.send("Page.handleJavaScriptDialog", { accept, promptText }, slot.sessionId);
  }

  need(id: string): Slot {
    const slot = this.slots.get(id);
    if (!slot) throw new RemoteError("tab_closed", "The tab is closed.");
    return slot;
  }

  async newTab(url?: string): Promise<Slot> {
    const { targetId } = await this.cdp.send<{ targetId: string }>(
      "Target.createTarget",
      { url: "about:blank" },
    );
    const slot = await this.attach(targetId, undefined);
    if (!slot) throw new RemoteError("tab_limit", `At most ${LIMITS.tabs} tabs.`);
    if (url) await this.navigate(slot.id, url);
    return slot;
  }

  async closeTab(id: string): Promise<void> {
    const slot = this.need(id);
    await this.cdp.send("Target.closeTarget", { targetId: slot.targetId });
  }

  /** Bring a tab to the front. Background tabs do not paint, so the shared
   * screen always streams the active tab. */
  async activate(id: string): Promise<void> {
    const slot = this.need(id);
    this.active = id;
    await this.cdp.send("Page.bringToFront", {}, slot.sessionId);
    if (this.streaming) await this.restartScreencast();
    this.hooks.onTabs();
  }

  async navigate(id: string, url: string): Promise<void> {
    const slot = this.need(id);
    const result = await this.cdp.send<{ errorText?: string }>(
      "Page.navigate",
      { url },
      slot.sessionId,
      LIMITS.operationMs,
    );
    if (result.errorText)
      throw new RemoteError("navigation_failed", `Navigation failed: ${result.errorText}`);
  }

  async history(id: string, delta: -1 | 1): Promise<void> {
    const slot = this.need(id);
    const { currentIndex, entries } = await this.cdp.send<{ currentIndex: number; entries: { id: number }[] }>(
      "Page.getNavigationHistory",
      {},
      slot.sessionId,
    );
    const entry = entries[currentIndex + delta];
    if (entry)
      await this.cdp.send("Page.navigateToHistoryEntry", { entryId: entry.id }, slot.sessionId);
  }

  async reload(id: string): Promise<void> {
    await this.cdp.send("Page.reload", {}, this.need(id).sessionId);
  }

  /** Evaluate in the page's main world (checkpoint/restore scripts need the
   * site's own storage) or in Sumi's isolated world (page operations). */
  async evaluate<T>(id: string, expression: string, isolated: boolean, timeoutMs: number = LIMITS.operationMs): Promise<T> {
    const slot = this.need(id);
    for (let attempt = 0; ; attempt++) {
      const params: Record<string, unknown> = {
        expression,
        awaitPromise: true,
        returnByValue: true,
      };
      if (isolated) params.contextId = await this.world(slot);
      try {
        const result = await this.cdp.send<{
          result: { value?: T };
          exceptionDetails?: { text?: string; exception?: { description?: string } };
        }>("Runtime.evaluate", params, slot.sessionId, timeoutMs);
        if (result.exceptionDetails)
          throw new RemoteError(
            "page_script_failed",
            result.exceptionDetails.exception?.description ?? result.exceptionDetails.text ?? "page script failed",
          );
        return result.result.value as T;
      } catch (error) {
        // The context vanished with a navigation before the script ran.
        if (isolated && attempt === 0 && /Cannot find context/i.test(String(error))) {
          slot.world = undefined;
          continue;
        }
        throw error;
      }
    }
  }

  private async world(slot: Slot): Promise<number> {
    if (slot.world !== undefined) return slot.world;
    const { executionContextId } = await this.cdp.send<{ executionContextId: number }>(
      "Page.createIsolatedWorld",
      { frameId: slot.frameId, worldName: WORLD, grantUniveralAccess: false },
      slot.sessionId,
    );
    slot.world = executionContextId;
    return executionContextId;
  }

  // ---------- shared screen ----------

  async startScreencast(): Promise<void> {
    this.streaming = true;
    await this.restartScreencast();
  }

  private async refreshInfo(slot: Slot): Promise<void> {
    try {
      const { targetInfo } = (await this.cdp.send("Target.getTargetInfo", { targetId: slot.targetId })) as {
        targetInfo: { title: string };
      };
      if (targetInfo.title !== slot.title) {
        slot.title = targetInfo.title;
        this.hooks.onTabs();
      }
    } catch {
      /* the tab may be gone */
    }
  }

  async stopScreencast(): Promise<void> {
    this.streaming = false;
    const session = this.screencastSession;
    this.screencastSession = undefined;
    if (session) await this.cdp.send("Page.stopScreencast", {}, session).catch(() => {});
  }

  private async restartScreencast(): Promise<void> {
    const slot = this.active ? this.slots.get(this.active) : undefined;
    if (!slot) return;
    if (this.screencastSession === slot.sessionId) return;
    const previous = this.screencastSession;
    this.screencastSession = slot.sessionId;
    this.lastFrame = undefined;
    if (previous) await this.cdp.send("Page.stopScreencast", {}, previous).catch(() => {});
    await this.cdp.send("Page.bringToFront", {}, slot.sessionId);
    await this.cdp.send(
      "Page.startScreencast",
      { format: "jpeg", quality: 70, maxWidth: VIEWPORT.width, maxHeight: VIEWPORT.height, everyNthFrame: 1 },
      slot.sessionId,
    );
  }

  /** The person's input, already validated and bounded by the caller. */
  async input(command: string, params: Record<string, unknown>): Promise<void> {
    const slot = this.active ? this.slots.get(this.active) : undefined;
    if (!slot) throw new RemoteError("tab_closed", "No open tab.");
    await this.cdp.send(command, params, slot.sessionId, 5_000);
  }

  // ---------- semantic checkpoint ----------

  /** Cookies and tabs are always kept; each origin's storage is added while
   * it fits the checkpoint budget (open tabs first, then origins carried
   * from the previous checkpoint). An origin that does not fit is reported
   * in notSaved.oversizedOrigins instead of dropping the whole checkpoint. */
  async collect(previous: Snapshot | undefined, maxBytes = MAX_SNAPSHOT_BYTES): Promise<Snapshot> {
    const tabs: Snapshot["tabs"] = [];
    const origins: Snapshot["origins"] = {};
    const notSaved: NotSaved = { nonJsonIdbValues: 0, sessionStorageKeys: 0, skippedOrigins: [], oversizedOrigins: [] };
    const readable: { slot: Slot; origin: string }[] = [];
    for (const slot of this.slots.values()) {
      let scroll = { x: 0, y: 0 };
      if (HTTP.test(slot.url) && !slot.loading && !slot.crashed) {
        const origin = new URL(slot.url).origin;
        try {
          scroll = await this.evaluate(slot.id, "({x: scrollX, y: scrollY})", false);
          if (!readable.some((r) => r.origin === origin)) readable.push({ slot, origin });
        } catch {
          if (!notSaved.skippedOrigins.includes(origin)) notSaved.skippedOrigins.push(origin);
        }
      }
      tabs.push({ slot: slot.id, url: slot.url, title: slot.title, scroll, active: slot.id === this.active });
    }
    const { cookies } = await this.cdp.send<{ cookies: Record<string, unknown>[] }>("Storage.getCookies", {});
    let budget = maxBytes - SNAPSHOT_MARGIN_BYTES - jsonBytes({ v: 1, at: Date.now(), cookies, tabs });
    const oversized = (origin: string) => notSaved.oversizedOrigins?.push(origin);
    readable.sort((a, b) => Number(b.slot.id === this.active) - Number(a.slot.id === this.active));
    for (const { slot, origin } of readable) {
      if (budget <= 0) {
        oversized(origin);
        continue;
      }
      try {
        const data = await this.evaluate<Collected>(slot.id, COLLECT(budget), false, 10_000);
        if (data.tooLarge) {
          oversized(origin);
          continue;
        }
        const entry: OriginData = { localStorage: data.localStorage, indexedDB: data.indexedDB };
        const bytes = jsonBytes(entry) + jsonBytes(origin) + 2;
        if (bytes > budget) {
          oversized(origin);
          continue;
        }
        budget -= bytes;
        notSaved.nonJsonIdbValues += data.nonJsonValues;
        notSaved.sessionStorageKeys += data.sessionStorageKeys;
        origins[origin] = entry;
      } catch {
        notSaved.skippedOrigins.push(origin);
      }
    }
    // Origins without an open tab cannot be read now: keep their last data
    // (never data that could not be read or did not fit just now).
    for (const [origin, data] of Object.entries(previous?.origins ?? {})) {
      if (origins[origin] || readable.some((r) => r.origin === origin) || notSaved.skippedOrigins.includes(origin)) continue;
      const entry = { ...data, carried: true };
      const bytes = jsonBytes(entry) + jsonBytes(origin) + 2;
      if (bytes > budget) {
        oversized(origin);
        continue;
      }
      budget -= bytes;
      origins[origin] = entry;
    }
    return { v: 1, at: Date.now(), cookies, origins, tabs, notSaved };
  }

  /** Rebuild a checkpoint in this (fresh) browser: cookies, then each
   * origin's storage seeded on an intercepted stub document before any of
   * the origin's own scripts run, then the tabs in their stable slots. */
  async restore(snapshot: Snapshot): Promise<{ origins: number; tabs: number; failedOrigins: string[]; failedRecords: number }> {
    this.restoring = true;
    try {
      const first = this.slots.values().next().value;
      if (!first) throw new RemoteError("tab_closed", "The new browser has no page.");
      const cookies = snapshot.cookies.map((c) => {
        const p: Record<string, unknown> = {
          name: c.name, value: c.value, domain: c.domain, path: c.path, secure: c.secure, httpOnly: c.httpOnly,
        };
        if (c.sameSite) p.sameSite = c.sameSite;
        if (!c.session && typeof c.expires === "number" && c.expires > 0) p.expires = c.expires;
        if (c.partitionKey) p.partitionKey = c.partitionKey;
        return p;
      });
      if (cookies.length) await this.cdp.send("Storage.setCookies", { cookies });
      let origins = 0;
      let failedRecords = 0;
      const failedOrigins: string[] = [];
      const stub = btoa("<!doctype html><title>Sumi restore</title>");
      const onPaused = (event: CdpEvent) => {
        if (event.method === "Fetch.requestPaused" && event.sessionId === first.sessionId)
          void this.cdp.send("Fetch.fulfillRequest", {
            requestId: event.params.requestId,
            responseCode: 200,
            responseHeaders: [{ name: "content-type", value: "text/html" }],
            body: stub,
          }, first.sessionId).catch(() => {});
      };
      this.interceptor = onPaused;
      try {
        for (const [origin, data] of Object.entries(snapshot.origins)) {
          if (!HTTP.test(origin)) continue;
          // One origin's storage failing to restore (blocked database,
          // refused record, timeout) skips that origin only.
          try {
            await this.cdp.send("Fetch.enable", { patterns: [{ urlPattern: `${origin}/*` }] }, first.sessionId);
            await this.navigate(first.id, `${origin}/__sumi_restore__`);
            await this.waitLoaded(first);
            const result = await this.evaluate<{ failedRecords?: number }>(first.id, RESTORE(data), false, 15_000);
            failedRecords += Number(result?.failedRecords) || 0;
            origins++;
          } catch {
            failedOrigins.push(origin);
          } finally {
            await this.cdp.send("Fetch.disable", {}, first.sessionId).catch(() => {});
          }
        }
      } finally {
        this.interceptor = undefined;
      }
      // Tabs return to their stable slots, so standing grants still apply.
      const tabs = snapshot.tabs.slice(0, LIMITS.tabs);
      for (const [index, tab] of tabs.entries()) {
        let slot: Slot;
        if (index === 0) {
          this.slots.delete(first.id);
          first.id = tab.slot;
          this.slots.set(tab.slot, first);
          this.byTarget.set(first.targetId, tab.slot);
          this.bySession.set(first.sessionId, tab.slot);
          slot = first;
        } else {
          this.pendingSlots.push(tab.slot);
          slot = await this.newTab();
        }
        if (HTTP.test(tab.url)) {
          await this.navigate(slot.id, tab.url).catch(() => {});
          await this.waitLoaded(slot);
          if (tab.scroll.x || tab.scroll.y)
            await this.evaluate(slot.id, `scrollTo(${Number(tab.scroll.x) || 0}, ${Number(tab.scroll.y) || 0})`, false).catch(() => {});
        } else if (index === 0) await this.navigate(slot.id, "about:blank").catch(() => {});
        if (tab.active) this.active = slot.id;
      }
      if (!this.active || !this.slots.has(this.active)) this.active = this.slots.keys().next().value;
      if (this.active) await this.activate(this.active);
      return { origins, tabs: tabs.length, failedOrigins, failedRecords };
    } finally {
      this.restoring = false;
      this.pendingSlots = [];
    }
  }

  async waitLoaded(slot: Slot, timeoutMs = 15_000): Promise<void> {
    const until = Date.now() + timeoutMs;
    // frameStartedLoading may not have arrived yet right after navigate.
    await new Promise((resolve) => setTimeout(resolve, 150));
    while (slot.loading && Date.now() < until)
      await new Promise((resolve) => setTimeout(resolve, 100));
  }
}
