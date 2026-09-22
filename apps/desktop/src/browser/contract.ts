/** Host-only port. The caller must authorize profile/tab access before handing it out.
 * Page text is untrusted website content, never Sumi instructions or authority. */
export interface TabRef {
  runtimeId: string;
  profileId: string;
  tabId: string;
}

export interface PageBinding {
  revision: number;
  observationId: string;
  url: string;
}

export interface VisibleTarget {
  id: string;
  tag: string;
  role: string;
  name: string;
  value?: string;
  bounds: { x: number; y: number; width: number; height: number };
}

export interface PageObservation {
  tab: TabRef;
  binding: PageBinding;
  title: string;
  text: string;
  targets: VisibleTarget[];
  truncated: boolean;
}

export type BrowserAction =
  | { kind: "click"; target: string }
  | { kind: "fill"; target: string; text: string }
  | { kind: "scroll"; x: number; y: number }
  | { kind: "navigate"; url: string };

/** Completion acknowledges dispatch, not completion of arbitrary website work.
 * Observe again to read the result and acquire fresh target handles. */
export interface ActionReceipt {
  tab: TabRef;
  status: "dispatched";
  revision: number;
}

export interface BrowserTabPort {
  observe(tab: TabRef): Promise<PageObservation>;
  act(
    tab: TabRef,
    binding: PageBinding,
    action: BrowserAction,
  ): Promise<ActionReceipt>;
}

export type BrowserErrorCode =
  | "invalid_request"
  | "runtime_unavailable"
  | "wrong_runtime"
  | "tab_closed"
  | "tab_unavailable"
  | "tab_busy"
  | "tab_navigating"
  | "stale_observation"
  | "target_unavailable"
  | "navigation_failed"
  | "operation_timed_out";

export class BrowserRuntimeError extends Error {
  readonly code: BrowserErrorCode;
  constructor(code: BrowserErrorCode, message: string) {
    super(message);
    this.code = code;
    this.name = "BrowserRuntimeError";
  }
}

export const LIMITS = Object.freeze({
  text: 16_000,
  targets: 100,
  nodes: 5_000,
  input: 8_000,
  url: 8_192,
  operationMs: 5_000,
  observationMs: 30_000,
  tabs: 16,
});

export function navigationURL(value: unknown): string {
  if (typeof value !== "string" || value.length > LIMITS.url) {
    throw new BrowserRuntimeError(
      "invalid_request",
      "Expected a bounded HTTP(S) URL.",
    );
  }
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new BrowserRuntimeError("invalid_request", "Invalid navigation URL.");
  }
  if (
    (url.protocol !== "https:" && url.protocol !== "http:") ||
    url.username ||
    url.password
  ) {
    throw new BrowserRuntimeError(
      "invalid_request",
      "Only HTTP(S) navigation without URL credentials is supported.",
    );
  }
  return url.href;
}

export function validateAction(action: BrowserAction): void {
  if (!action || typeof action !== "object") {
    throw new BrowserRuntimeError("invalid_request", "Expected an action.");
  }
  switch (action.kind) {
    case "navigate":
      navigationURL(action.url);
      return;
    case "click":
    case "fill":
      if (
        typeof action.target !== "string" ||
        !/^t\d{1,3}$/.test(action.target)
      )
        break;
      if (action.kind === "click") return;
      if (typeof action.text === "string" && action.text.length <= LIMITS.input)
        return;
      break;
    case "scroll":
      if (
        [action.x, action.y].every(
          (n) => Number.isFinite(n) && Math.abs(n) <= 10_000,
        )
      )
        return;
      break;
  }
  throw new BrowserRuntimeError(
    "invalid_request",
    "Unsupported or unbounded browser action.",
  );
}
