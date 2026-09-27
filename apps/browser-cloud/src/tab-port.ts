import {
  type ActionReceipt,
  type ActOptions,
  type BrowserAction,
  type BrowserErrorCode,
  BrowserRuntimeError,
  type BrowserTabPort,
  LIMITS,
  navigationURL,
  type PageBinding,
  type PageObservation,
  type TabRef,
  validateAction,
} from "@sumi/desktop/browser-contract";
import { pageOperation } from "@sumi/desktop/browser-page";
import { RemoteError, type RemoteBrowser, type Slot } from "./remote-browser.ts";

/** Who may drive the shared screen now. The person's input takes control
 * (epoch+1) before it is dispatched; the secretary's actions check the mode
 * and the epoch of their observation immediately before dispatch. */
export interface Control {
  mode: "agent" | "human";
  epoch: number;
}

export interface AgentActivity {
  tab: string;
  kind: BrowserAction["kind"] | "observe";
  bounds?: { x: number; y: number; width: number; height: number };
  label?: string;
  phase: "start" | "end";
  outcome?: "dispatched" | "refused" | "failed";
}

export interface PortHost {
  browser(): RemoteBrowser | undefined;
  profileId(): string;
  control(): Control;
  humanInputAt(): number;
  /** Durable intent written before an action is handed to the page, so a
   * later restore can say its outcome is unknown instead of repeating it. */
  recordIntent(intent: { tab: string; kind: string; label?: string } | null): Promise<void>;
  activity(event: AgentActivity): void;
}

/** The page operation as one self-contained expression. The Worker bundle
 * (esbuild keepNames) may call `__name` inside functions; the page has no
 * such helper, so a no-op one is scoped around the operation. */
export function pageScript(request: unknown): string {
  return `(() => { const __name = (target) => target; return (${pageOperation.toString()})(${JSON.stringify(request)}); })()`;
}

interface Observed {
  binding: PageBinding;
  expires: number;
  observedAt: number;
  epoch: number;
  targets: Map<string, { bounds: AgentActivity["bounds"]; name: string }>;
}

/** The secretary's view of the Cloud browser: the same observe/act contract
 * as the desktop host, on the exact tab the person watches. */
export class CloudTabPort implements BrowserTabPort {
  private readonly observed = new Map<string, Observed>();
  private readonly host: PortHost;
  constructor(host: PortHost) {
    this.host = host;
  }

  /** Forget every observation (new incarnation, reconnect, takeover). */
  invalidate(): void {
    this.observed.clear();
  }

  private slot(ref: TabRef): Slot {
    const browser = this.host.browser();
    if (!browser || browser.closed)
      throw new BrowserRuntimeError("runtime_unavailable", "The Cloud browser is not running.");
    if (!ref || ref.runtimeId !== this.host.profileId() || ref.profileId !== "cloud")
      throw new BrowserRuntimeError("wrong_runtime", "The reference belongs to a different browser profile.");
    const slot = browser.slot(ref.tabId);
    if (!slot) throw new BrowserRuntimeError("tab_closed", "The original browser tab is closed.");
    if (slot.crashed)
      throw new BrowserRuntimeError("tab_unavailable", "The tab renderer is unavailable; close it and open a new tab.");
    return slot;
  }

  private async exclusive<T>(ref: TabRef, operation: (slot: Slot, browser: RemoteBrowser) => Promise<T>): Promise<T> {
    const slot = this.slot(ref);
    const browser = this.host.browser() as RemoteBrowser;
    if (slot.busy) throw new BrowserRuntimeError("tab_busy", "Another browser operation is still in flight.");
    slot.busy = true;
    try {
      return await operation(slot, browser);
    } catch (error) {
      if (error instanceof BrowserRuntimeError) throw error;
      if (error instanceof RemoteError)
        throw new BrowserRuntimeError(error.code as BrowserErrorCode, error.message);
      throw new BrowserRuntimeError(
        browser.closed ? "runtime_unavailable" : "tab_unavailable",
        error instanceof Error ? error.message : "Browser operation failed.",
      );
    } finally {
      slot.busy = false;
    }
  }

  private ready(slot: Slot): void {
    if (slot.loading)
      throw new BrowserRuntimeError("tab_navigating", "Wait for the current navigation, then observe again.");
  }

  private async page(
    browser: RemoteBrowser,
    slot: Slot,
    request: { kind: "observe" | "act" | "check"; observationId: string; url: string; action?: BrowserAction; guard?: boolean },
  ): Promise<ReturnType<typeof pageOperation>> {
    const full = { ...request, deadline: Date.now() + LIMITS.operationMs, limits: LIMITS };
    let value: ReturnType<typeof pageOperation>;
    try {
      value = await browser.evaluate(slot.id, pageScript(full), true);
    } catch (error) {
      if (/timed out/i.test(String(error)))
        throw new BrowserRuntimeError(
          "operation_timed_out",
          "Operation timed out; its effect may be unknown. Do not retry automatically.",
        );
      throw error;
    }
    if (value?.error)
      throw new BrowserRuntimeError(value.error as BrowserErrorCode, `Browser operation failed: ${value.error}.`);
    return value;
  }

  private async currentURL(browser: RemoteBrowser, slot: Slot): Promise<string> {
    return browser.evaluate<string>(slot.id, "location.href", true);
  }

  async observe(ref: TabRef): Promise<PageObservation> {
    return this.exclusive(ref, async (slot, browser) => {
      this.ready(slot);
      const observedAt = Date.now();
      const epoch = this.host.control().epoch;
      const revision = slot.revision;
      const binding = { revision, observationId: crypto.randomUUID(), url: await this.currentURL(browser, slot) };
      const value = await this.page(browser, slot, { kind: "observe", observationId: binding.observationId, url: binding.url });
      if (slot.revision !== revision)
        throw new BrowserRuntimeError("stale_observation", "The tab navigated during the observation.");
      const targets = value.targets ?? [];
      this.observed.set(slot.id, {
        binding,
        expires: Date.now() + LIMITS.observationMs,
        observedAt,
        epoch,
        targets: new Map(targets.map((t) => [t.id, { bounds: t.bounds, name: t.name }])),
      });
      return {
        tab: { ...ref },
        binding: { ...binding },
        title: value.title ?? "",
        text: value.text ?? "",
        targets,
        truncated: value.truncated ?? false,
      };
    });
  }

  async act(ref: TabRef, binding: PageBinding, action: BrowserAction, options: ActOptions = {}): Promise<ActionReceipt> {
    validateAction(action);
    const guard = options.guard === true;
    return this.exclusive(ref, async (slot, browser) => {
      this.ready(slot);
      const observed = this.observed.get(slot.id);
      if (
        !binding ||
        !observed ||
        observed.binding.observationId !== binding.observationId ||
        observed.expires < Date.now() ||
        binding.revision !== slot.revision
      )
        throw new BrowserRuntimeError("stale_observation", "Observe the tab again before acting.");
      // Single use: a refused or dispatched action consumes the observation.
      this.observed.delete(slot.id);
      if ((await this.currentURL(browser, slot)) !== binding.url || binding.revision !== slot.revision)
        throw new BrowserRuntimeError("stale_observation", "The tab navigated since this observation.");
      const target = "target" in action ? observed.targets.get(action.target) : undefined;
      const describe: AgentActivity = {
        tab: slot.id,
        kind: action.kind,
        bounds: target?.bounds,
        label: target?.name?.slice(0, 80) || (action.kind === "navigate" ? action.url.slice(0, 200) : undefined),
        phase: "start",
      };
      const admitted = () => {
        const control = this.host.control();
        if (control.mode !== "agent" || control.epoch !== observed.epoch)
          throw new BrowserRuntimeError(
            "page_changed",
            "The person took control of this browser after the observation. Wait until they hand it back, then observe again.",
          );
        if (guard && this.host.humanInputAt() >= observed.observedAt)
          throw new BrowserRuntimeError("page_changed", "The person used this tab after the observation. Observe again.");
        if (binding.revision !== slot.revision)
          throw new BrowserRuntimeError("stale_observation", "The tab navigated since this observation.");
      };
      admitted();
      // The person sees the tab the secretary acts on.
      if (browser.active !== slot.id) await browser.activate(slot.id);
      admitted();
      if (guard && action.kind === "navigate")
        await this.page(browser, slot, { kind: "check", observationId: binding.observationId, url: binding.url });
      await this.host.recordIntent({ tab: slot.id, kind: action.kind, label: describe.label });
      try {
        // Last check before the input is handed to the page; nothing awaits
        // between it and the dispatch below.
        admitted();
        this.host.activity(describe);
        if (action.kind === "navigate") {
          const url = navigationURL(action.url);
          await browser.navigate(slot.id, url);
        } else {
          await this.page(browser, slot, {
            kind: "act",
            observationId: binding.observationId,
            url: binding.url,
            action,
            guard,
          });
        }
        this.host.activity({ ...describe, phase: "end", outcome: "dispatched" });
      } catch (error) {
        const refused =
          error instanceof BrowserRuntimeError &&
          ["page_changed", "stale_observation", "target_unavailable", "tab_navigating"].includes(error.code);
        this.host.activity({ ...describe, phase: "end", outcome: refused ? "refused" : "failed" });
        // Only a refusal proves nothing reached the page; any other failure
        // keeps the intent so its outcome stays "unknown" after a restart.
        if (refused) await this.host.recordIntent(null);
        throw error;
      }
      await this.host.recordIntent(null);
      return { tab: { ...ref }, status: "dispatched", revision: slot.revision };
    });
  }
}
