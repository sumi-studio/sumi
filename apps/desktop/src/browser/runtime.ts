import { randomUUID } from "node:crypto";
import type { Session } from "electron";
import { type BaseWindow, session, WebContentsView } from "electron";
import type {
  ActionReceipt,
  BrowserAction,
  BrowserErrorCode,
  BrowserTabPort,
  PageBinding,
  PageObservation,
  TabRef,
} from "./contract.js";
import {
  BrowserRuntimeError,
  LIMITS,
  navigationURL,
  validateAction,
} from "./contract.js";
import { pageOperation } from "./page.js";
import { filterBrowserRequest } from "./request-policy.js";

const activeProfiles = new Set<string>();
const WORLD = 1001;

interface Tab {
  ref: TabRef;
  view: WebContentsView;
  window: BaseWindow;
  revision: number;
  busy: boolean;
  unavailable: boolean;
  snapshot?: { binding: PageBinding; expires: number };
  detach: () => void;
}

/** Electron main-process implementation; not an IPC endpoint or an authorization
 * service. Each open tab is attached to the supplied human-facing window. */
export class SharedBrowserRuntime implements BrowserTabPort {
  readonly runtimeId = randomUUID();
  readonly profileId: string;
  private readonly profile: Session;
  private readonly tabs = new Map<string, Tab>();
  private disposed = false;

  constructor(options: { profileId: string }) {
    if (
      typeof options.profileId !== "string" ||
      !/^[a-zA-Z0-9_-]{1,80}$/.test(options.profileId)
    ) {
      throw new BrowserRuntimeError(
        "invalid_request",
        "Expected a stable profile identifier (1–80 letters, digits, '_' or '-').",
      );
    }
    if (activeProfiles.has(options.profileId)) {
      throw new BrowserRuntimeError(
        "invalid_request",
        "This browser profile already has a runtime owner.",
      );
    }
    this.profileId = options.profileId;
    this.profile = session.fromPartition(
      `persist:sumi-browser-${options.profileId}`,
    );
    activeProfiles.add(options.profileId);
    this.profile.setPermissionRequestHandler(
      (_contents, _permission, callback) => callback(false),
    );
    this.profile.setPermissionCheckHandler(() => false);
    this.profile.setDevicePermissionHandler(() => false);
    this.profile.on("will-download", this.denyDownload);
    // Chromium still enforces normal origin/CORS/session boundaries. These
    // checks additionally keep local files/custom host protocols out of pages.
    this.profile.webRequest.onBeforeRequest(filterBrowserRequest);
  }

  private readonly denyDownload = (event: Electron.Event) =>
    event.preventDefault();

  /** Host attaches a tab to its real window. One tab per window in this first
   * slice; chrome/layout belongs to the host, not this runtime. */
  async openTab(window: BaseWindow, url: string): Promise<TabRef> {
    this.assertLive();
    const destination = navigationURL(url);
    if (
      window.isDestroyed() ||
      [...this.tabs.values()].some((tab) => tab.window === window) ||
      this.tabs.size >= LIMITS.tabs
    ) {
      throw new BrowserRuntimeError(
        "invalid_request",
        "Expected a live, unused browser window within the tab limit.",
      );
    }
    const view = new WebContentsView({
      webPreferences: {
        session: this.profile,
        nodeIntegration: false,
        nodeIntegrationInWorker: false,
        nodeIntegrationInSubFrames: false,
        contextIsolation: true,
        sandbox: true,
        webSecurity: true,
        allowRunningInsecureContent: false,
        webviewTag: false,
        navigateOnDragDrop: false,
        safeDialogs: true,
        disableDialogs: true,
      },
    });
    const ref = Object.freeze({
      runtimeId: this.runtimeId,
      profileId: this.profileId,
      tabId: randomUUID(),
    });
    const contents = view.webContents;
    const resize = () => {
      if (window.isDestroyed()) return;
      const [width = 0, height = 0] = window.getContentSize();
      view.setBounds({ x: 0, y: 0, width, height });
    };
    const close = () => this.closeTab(ref);
    const tab: Tab = {
      ref,
      view,
      window,
      revision: 0,
      busy: false,
      unavailable: false,
      detach: () => {
        window.removeListener("resize", resize);
        window.removeListener("closed", close);
        if (!window.isDestroyed()) window.contentView.removeChildView(view);
      },
    };
    this.tabs.set(ref.tabId, tab);
    window.contentView.addChildView(view);
    resize();
    window.on("resize", resize);
    window.once("closed", close);
    contents.setWindowOpenHandler(() => ({ action: "deny" }));
    const allowNavigation = (event: Electron.Event, destination: string) => {
      try {
        navigationURL(destination);
      } catch {
        event.preventDefault();
      }
    };
    contents.on("will-navigate", (event, destination) =>
      allowNavigation(event, destination),
    );
    contents.on("will-frame-navigate", (event) => {
      if (event.url !== "about:blank" && event.url !== "about:srcdoc")
        allowNavigation(event, event.url);
    });
    contents.on("will-redirect", (event, destination) =>
      allowNavigation(event, destination),
    );
    contents.on("will-attach-webview", (event) => event.preventDefault());
    contents.on(
      "did-start-navigation",
      (_event, _url, _inPlace, isMainFrame) => {
        if (isMainFrame) {
          tab.revision++;
          tab.snapshot = undefined;
        }
      },
    );
    contents.on("render-process-gone", () => {
      tab.unavailable = true;
      tab.snapshot = undefined;
    });
    contents.once("destroyed", () => {
      tab.detach();
      this.tabs.delete(ref.tabId);
    });
    try {
      await this.bounded(tab, contents.loadURL(destination));
      this.getTab(ref);
      return { ...ref };
    } catch (error) {
      if (!this.disposed) this.closeTab(ref);
      throw this.asError(error, "navigation_failed");
    }
  }

  listTabs(): {
    tab: TabRef;
    revision: number;
    url: string;
    status: "ready" | "loading" | "unavailable";
  }[] {
    this.assertLive();
    return [...this.tabs.values()].map((tab) => ({
      tab: { ...tab.ref },
      revision: tab.revision,
      url: tab.view.webContents.getURL(),
      status: tab.unavailable
        ? "unavailable"
        : tab.view.webContents.isLoadingMainFrame()
          ? "loading"
          : "ready",
    }));
  }

  async observe(ref: TabRef): Promise<PageObservation> {
    return this.exclusive(ref, async (tab) => {
      this.assertReady(tab);
      const binding = {
        revision: tab.revision,
        observationId: randomUUID(),
        url: tab.view.webContents.getURL(),
      };
      const value = await this.page(tab, {
        kind: "observe",
        observationId: binding.observationId,
        url: binding.url,
      });
      this.assertCurrent(tab, binding);
      tab.snapshot = { binding, expires: Date.now() + LIMITS.observationMs };
      return {
        tab: { ...tab.ref },
        binding: { ...binding },
        title: value.title ?? "",
        text: value.text ?? "",
        targets: value.targets ?? [],
        truncated: value.truncated ?? false,
      };
    });
  }

  async act(
    ref: TabRef,
    binding: PageBinding,
    action: BrowserAction,
  ): Promise<ActionReceipt> {
    validateAction(action);
    return this.exclusive(ref, async (tab) => {
      this.assertReady(tab);
      this.assertCurrent(tab, binding);
      const snapshot = tab.snapshot;
      if (
        !snapshot ||
        snapshot.binding.observationId !== binding.observationId ||
        snapshot.expires < Date.now()
      ) {
        throw new BrowserRuntimeError(
          "stale_observation",
          "Observe the tab again before acting.",
        );
      }
      tab.snapshot = undefined;
      if (action.kind === "navigate") {
        // No asynchronous gap between checking the host-owned binding and
        // requesting this navigation on the exact WebContents.
        try {
          await this.bounded(
            tab,
            tab.view.webContents.loadURL(navigationURL(action.url)),
          );
        } catch (error) {
          if (error instanceof BrowserRuntimeError) throw error;
          // Preserve lifecycle errors if the host closed the tab while loading.
          this.getTab(ref);
          throw this.asError(error, "navigation_failed");
        }
      } else {
        await this.page(tab, {
          kind: "act",
          observationId: binding.observationId,
          url: binding.url,
          action,
        });
      }
      this.getTab(ref);
      return {
        tab: { ...tab.ref },
        status: "dispatched",
        revision: tab.revision,
      };
    });
  }

  closeTab(ref: TabRef): void {
    this.assertLive();
    if (
      !ref ||
      ref.runtimeId !== this.runtimeId ||
      ref.profileId !== this.profileId
    ) {
      throw new BrowserRuntimeError(
        "wrong_runtime",
        "The reference belongs to a different browser runtime/profile.",
      );
    }
    const tab = this.tabs.get(ref.tabId);
    if (!tab) return;
    this.tabs.delete(ref.tabId);
    tab.detach();
    if (!tab.view.webContents.isDestroyed())
      tab.view.webContents.close({ waitForBeforeUnload: false });
  }

  dispose(): void {
    if (this.disposed) return;
    for (const tab of [...this.tabs.values()]) this.closeTab(tab.ref);
    this.disposed = true;
    this.profile.removeListener("will-download", this.denyDownload);
    // Keep the profile's deny handlers until its next runtime takes ownership.
    activeProfiles.delete(this.profileId);
  }

  private assertLive() {
    if (this.disposed)
      throw new BrowserRuntimeError(
        "runtime_unavailable",
        "The browser runtime has stopped.",
      );
  }

  private getTab(ref: TabRef): Tab {
    this.assertLive();
    if (
      !ref ||
      ref.runtimeId !== this.runtimeId ||
      ref.profileId !== this.profileId
    ) {
      throw new BrowserRuntimeError(
        "wrong_runtime",
        "The reference belongs to a different browser runtime/profile.",
      );
    }
    const tab = this.tabs.get(ref.tabId);
    if (!tab || tab.view.webContents.isDestroyed())
      throw new BrowserRuntimeError(
        "tab_closed",
        "The original browser tab is closed.",
      );
    if (tab.unavailable)
      throw new BrowserRuntimeError(
        "tab_unavailable",
        "The tab renderer is unavailable; close it and open a new tab.",
      );
    return tab;
  }

  private assertReady(tab: Tab) {
    if (tab.view.webContents.isLoadingMainFrame())
      throw new BrowserRuntimeError(
        "tab_navigating",
        "Wait for the current navigation, then observe again.",
      );
  }

  private assertCurrent(tab: Tab, binding: PageBinding) {
    this.getTab(tab.ref);
    if (
      !binding ||
      binding.revision !== tab.revision ||
      binding.url !== tab.view.webContents.getURL()
    ) {
      throw new BrowserRuntimeError(
        "stale_observation",
        "The tab navigated since this observation.",
      );
    }
  }

  private async exclusive<T>(
    ref: TabRef,
    operation: (tab: Tab) => Promise<T>,
  ): Promise<T> {
    const tab = this.getTab(ref);
    if (tab.busy)
      throw new BrowserRuntimeError(
        "tab_busy",
        "Another browser operation is still in flight.",
      );
    tab.busy = true;
    try {
      return await operation(tab);
    } catch (error) {
      throw this.asError(
        error,
        this.disposed
          ? "runtime_unavailable"
          : this.tabs.has(ref.tabId)
            ? "tab_unavailable"
            : "tab_closed",
      );
    } finally {
      tab.busy = false;
    }
  }

  private async page(
    tab: Tab,
    request: {
      kind: "observe" | "act";
      observationId: string;
      url: string;
      action?: BrowserAction;
    },
  ) {
    const full = {
      ...request,
      deadline: Date.now() + LIMITS.operationMs,
      limits: LIMITS,
    };
    const value: ReturnType<typeof pageOperation> = await this.bounded(
      tab,
      tab.view.webContents.executeJavaScriptInIsolatedWorld(WORLD, [
        { code: `(${pageOperation.toString()})(${JSON.stringify(full)})` },
      ]),
    );
    if (value.error)
      throw new BrowserRuntimeError(
        value.error as BrowserErrorCode,
        `Browser operation failed: ${value.error}.`,
      );
    return value;
  }

  private async bounded<T>(tab: Tab, promise: Promise<T>): Promise<T> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([
        promise,
        new Promise<never>((_resolve, reject) => {
          timer = setTimeout(() => {
            tab.unavailable = true;
            tab.snapshot = undefined;
            reject(
              new BrowserRuntimeError(
                "operation_timed_out",
                "Operation timed out; its effect may be unknown. Do not retry automatically. Close this tab and open a new one.",
              ),
            );
          }, LIMITS.operationMs);
        }),
      ]);
    } finally {
      clearTimeout(timer);
    }
  }

  private asError(
    error: unknown,
    fallback: BrowserErrorCode,
  ): BrowserRuntimeError {
    if (error instanceof BrowserRuntimeError) return error;
    return new BrowserRuntimeError(
      fallback,
      error instanceof Error ? error.message : "Browser operation failed.",
    );
  }
}
