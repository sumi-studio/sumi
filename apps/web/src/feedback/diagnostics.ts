import { useConversation } from "../agent/store";
import { useMessaging } from "../messaging/store";
import { useWorkspaceControl } from "../workspace/store";

export interface DiagnosticSource {
  path: string;
  workspace_id?: string;
  captured_at: string;
  states: Record<string, string>;
}
export interface DiagnosticClientEvent {
  at: string;
  kind: "navigation" | "click" | "error" | "unhandledrejection";
  path: string;
  summary: string;
}
export interface DiagnosticSelection {
  tag: string;
  selector: string;
  label: string;
  rect: { x: number; y: number; width: number; height: number };
  captured_at: string;
}
export interface DiagnosticServerObservation {
  captured_at: string;
  personality_agent_id?: string;
  ready?: boolean;
  run_in_flight?: boolean;
  generation?: string;
  readiness_reason?: string;
  status: string;
}
export interface FeedbackDiagnostics {
  version: 1;
  captured_at: string;
  source?: DiagnosticSource;
  client_events?: DiagnosticClientEvent[];
  selection?: DiagnosticSelection;
  server_observation?: DiagnosticServerObservation;
  browser: string;
  language: string;
  time_zone: string;
  utc_offset_minutes: number;
  online: boolean;
  visibility: string;
  viewport: {
    width: number;
    height: number;
    scale: number;
    pixel_ratio: number;
    scroll_x?: number;
    scroll_y?: number;
  };
  assets: string[];
  served_release?: string;
  tab_release?: string;
}
let origin: DiagnosticSource | undefined;
let recentEvents: DiagnosticClientEvent[] = [];
let stopEvidence: (() => void) | undefined;

function safePath(pathname: string): string {
  const clean = pathname.split(/[?#]/, 1)[0];
  return (
    /^\/w\/[^/]+(?:\/(?:messaging(?:\/.*)?|members|roles|apps))?\/?$/.test(
      clean,
    ) ||
    clean === "/direct" ||
    clean === "/feedback" ||
    clean === "/"
      ? clean
      : "/other"
  ).slice(0, 512);
}

/** Limit diagnostic prose; never retain URL credentials, query strings, or fragments. */
export function sanitizeFeedbackSummary(value: string): string {
  return value
    .replace(/\b(?:https?|wss?):\/\/[^\s<>"']+/gi, (raw) => {
      try {
        const url = new URL(raw);
        return `${url.origin}${url.pathname}`;
      } catch {
        return "[url]";
      }
    })
    .replace(/(^|[\s("'])(\/[^\s<>"'?#]*)[?#][^\s<>"']*/g, "$1$2")
    .replace(/\b(Bearer|Basic)\s+[^\s,;"']+/gi, "$1 [redacted]")
    .replace(
      /(["']?(?:access[_-]?token|refresh[_-]?token|id[_-]?token|token|api[_-]?key|password|passwd|secret|authorization|cookie)["']?\s*[:=]\s*)(?:"[^"]*"|'[^']*'|[^\s,;]+)/gi,
      "$1[redacted]",
    )
    .replace(
      /\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b/g,
      "[redacted]",
    )
    .replace(/\b(?:sk|ghp|github_pat)[_-][A-Za-z0-9_-]{12,}\b/g, "[redacted]")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 240);
}

function recordEvent(
  kind: DiagnosticClientEvent["kind"],
  summary: string,
  path?: string,
) {
  if (!stopEvidence) return;
  recentEvents.push({
    at: new Date().toISOString(),
    kind,
    path: safePath(path ?? window.location.pathname),
    summary: sanitizeFeedbackSummary(summary),
  });
  if (recentEvents.length > 24)
    recentEvents.splice(0, recentEvents.length - 24);
}

/** Scope the browser-only ring to the current identity and clear it on unmount. */
export function startFeedbackEvidence(): () => void {
  stopEvidence?.();
  const onClick = (event: MouseEvent) => {
    if (!(event.target instanceof Element)) return;
    const target = event.target.closest(
      "button, a, input, select, textarea, summary, [role='button'], [role='link']",
    );
    if (!target || target.closest("[data-feedback-ui]")) return;
    // Do not inspect accessible labels or DOM text: they can contain conversation data.
    let summary = target.tagName.toLowerCase();
    if (target instanceof HTMLAnchorElement) {
      try {
        const url = new URL(target.href, window.location.href);
        summary +=
          url.origin === window.location.origin
            ? ` → ${safePath(url.pathname)}`
            : " → external";
      } catch {
        /* A malformed target still has a useful element type. */
      }
    }
    recordEvent("click", summary);
  };
  const onError = (event: ErrorEvent) => {
    let location = "";
    if (event.filename) {
      try {
        const url = new URL(event.filename, window.location.href);
        location = ` (${url.origin}${url.pathname}:${event.lineno}:${event.colno})`;
      } catch {
        /* File location is optional. */
      }
    }
    recordEvent("error", `${event.message || "Browser error"}${location}`);
  };
  const onRejection = (event: PromiseRejectionEvent) => {
    const reason: unknown = event.reason;
    const summary =
      reason instanceof Error
        ? `${reason.name}: ${reason.message}`
        : typeof reason === "string"
          ? reason
          : "Unhandled promise rejection";
    recordEvent("unhandledrejection", summary);
  };
  document.addEventListener("click", onClick, true);
  window.addEventListener("error", onError);
  window.addEventListener("unhandledrejection", onRejection);
  const stop = () => {
    document.removeEventListener("click", onClick, true);
    window.removeEventListener("error", onError);
    window.removeEventListener("unhandledrejection", onRejection);
    if (stopEvidence === stop) {
      stopEvidence = undefined;
      resetFeedbackOrigin();
    }
  };
  stopEvidence = stop;
  return stop;
}

/** Refresh at capture time while the source page and its transports are still mounted. */
export function recordFeedbackOrigin(
  pathname: string,
  workspaceId?: string,
  includeFeedback = false,
) {
  if (pathname === "/feedback" && !includeFeedback) return;
  // Paths can contain invite secrets. Only capture known app routes and no query/hash.
  const path = safePath(pathname);
  if (origin?.path !== path)
    recordEvent("navigation", `Navigate to ${path}`, path);
  const workspace = useWorkspaceControl.getState();
  const messaging = useMessaging.getState();
  const conversation = useConversation.getState();
  const states: Record<string, string> = {
    workspace_selection: workspace.selectionStatus,
    workspace_list: workspace.listStatus,
  };
  if (workspace.errorCode)
    states.workspace_error = safeCode(workspace.errorCode);
  if (path.includes("/messaging")) {
    states.messaging_connection = messaging.connection;
    states.messaging_bootstrap_failed = String(messaging.bootstrapFailed);
    states.messaging_edit_failed = String(Boolean(messaging.editFailure));
    states.messaging_delete_failures = String(
      messaging.deleteFailedMessageIds.size,
    );
    states.messaging_notification_default = messaging.notificationDefaultLevel;
  }
  if (path === "/direct") {
    states.agent_connection = conversation.connection;
    states.agent_ready = conversation.ready;
    states.agent_status = conversation.status;
    if (conversation.lastError)
      states.agent_error = safeCode(conversation.lastError);
    const runId = conversation.conversation.runOrder.at(-1);
    if (runId) states.agent_latest_run_id = runId.slice(0, 128);
    states.agent_recoverable_drafts = String(
      conversation.recoverableDrafts.length,
    );
  }
  origin = {
    path: path.slice(0, 512),
    workspace_id: workspaceId,
    captured_at: new Date().toISOString(),
    states,
  };
}
export function resetFeedbackOrigin() {
  origin = undefined;
  recentEvents = [];
}
function safeCode(value: string) {
  return /^[a-z0-9_:-]{1,120}$/i.test(value) ? value : "present";
}
export function captureFeedbackDiagnostics(): FeedbackDiagnostics {
  let timeZone = "";
  try {
    timeZone = Intl.DateTimeFormat().resolvedOptions().timeZone;
  } catch {
    /* Optional browser feature. */
  }
  const viewport = window.visualViewport;
  const assets = Array.from(
    document.querySelectorAll<HTMLScriptElement>("script[src]"),
  )
    .flatMap((script) => {
      const url = new URL(script.src, window.location.href);
      return url.origin === window.location.origin &&
        /^\/assets\/[\w.-]+\.js$/.test(url.pathname)
        ? [url.pathname]
        : [];
    })
    .slice(0, 8);
  return {
    version: 1,
    ...(/^[a-f0-9]{40}$/.test(import.meta.env.VITE_SUMI_RELEASE_SHA ?? "")
      ? { tab_release: import.meta.env.VITE_SUMI_RELEASE_SHA }
      : {}),
    captured_at: new Date().toISOString(),
    ...(origin ? { source: structuredClone(origin) } : {}),
    client_events: structuredClone(recentEvents),
    browser: navigator.userAgent.slice(0, 512),
    language: navigator.language.slice(0, 64),
    time_zone: timeZone,
    utc_offset_minutes: -new Date().getTimezoneOffset(),
    online: navigator.onLine,
    visibility: document.visibilityState,
    viewport: {
      width: Math.round(viewport?.width ?? window.innerWidth),
      height: Math.round(viewport?.height ?? window.innerHeight),
      scale: viewport?.scale ?? 1,
      pixel_ratio: window.devicePixelRatio || 1,
      scroll_x: Math.round(window.scrollX),
      scroll_y: Math.round(window.scrollY),
    },
    assets,
  };
}
/** A deployment's manifest may differ from an already-open tab; retain both identifiers. */
export async function readServedRelease(): Promise<string | undefined> {
  try {
    const response = await fetch("/release.json", {
      cache: "no-store",
      signal: AbortSignal.timeout(3000),
    });
    if (!response.ok) return undefined;
    const data = await response.json();
    return typeof data.release_sha === "string" &&
      /^[a-f0-9]{40}$/.test(data.release_sha)
      ? data.release_sha
      : undefined;
  } catch {
    return undefined;
  }
}
