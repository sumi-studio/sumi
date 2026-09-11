import { useConversation } from "../agent/store";
import { useMessaging } from "../messaging/store";
import { useWorkspaceControl } from "../workspace/store";

export interface DiagnosticSource {
  path: string;
  workspace_id?: string;
  captured_at: string;
  states: Record<string, string>;
}
export interface FeedbackDiagnostics {
  version: 1;
  captured_at: string;
  source?: DiagnosticSource;
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
  };
  assets: string[];
  served_release?: string;
  tab_release?: string;
}
let origin: DiagnosticSource | undefined;

/** Called before leaving a page: keep its state, not the closed transport after unmount. */
export function recordFeedbackOrigin(pathname: string, workspaceId?: string) {
  if (pathname === "/feedback") return;
  // Paths can contain invite secrets. Only capture known app routes and no query/hash.
  const clean = pathname.split(/[?#]/, 1)[0];
  const path =
    /^\/w\/[^/]+(?:\/(?:messaging(?:\/.*)?|members|roles|apps))?\/?$/.test(
      clean,
    ) ||
    clean === "/direct" ||
    clean === "/"
      ? clean
      : "/other";
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
