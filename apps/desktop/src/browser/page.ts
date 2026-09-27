import type { BrowserAction, LIMITS, VisibleTarget } from "./contract.js";

/** Serialized into a dedicated isolated world. No page script, arbitrary expression,
 * selector, Electron object, or Node function is accepted from the caller. */
export function pageOperation(request: {
  kind: "observe" | "act" | "check";
  observationId: string;
  url: string;
  deadline: number;
  limits: typeof LIMITS;
  action?: BrowserAction;
  guard?: boolean;
}) {
  type Snapshot = {
    id: string;
    expires: number;
    targets: Map<string, Element>;
    signatures: Map<string, string>;
    contexts: Map<string, string>;
  };
  const state = globalThis as typeof globalThis & {
    sumiBrowserSnapshot?: Snapshot;
  };
  if (Date.now() > request.deadline) return { error: "operation_timed_out" };
  if (location.href !== request.url) return { error: "stale_observation" };

  const visible = (element: Element) => {
    const rect = element.getBoundingClientRect();
    return (
      rect.width > 0 &&
      rect.height > 0 &&
      rect.right > 0 &&
      rect.bottom > 0 &&
      rect.left < innerWidth &&
      rect.top < innerHeight &&
      element.checkVisibility({ checkOpacity: true, checkVisibilityCSS: true })
    );
  };
  const nameOf = (element: Element) =>
    (
      element.getAttribute("aria-label") ??
      element.getAttribute("placeholder") ??
      element.textContent ??
      ""
    )
      .trim()
      .slice(0, 256);
  const controlValue = (element: Element) =>
    (element instanceof HTMLInputElement &&
      !["password", "file", "hidden"].includes(element.type)) ||
    element instanceof HTMLTextAreaElement ||
    element instanceof HTMLSelectElement
      ? element.value.slice(0, 512)
      : undefined;
  // What a decision was based on: each observed control's identity, label,
  // value, enabled state and visibility. Password contents are covered by length.
  const signature = (element: Element) =>
    JSON.stringify([
      element.isConnected && visible(element),
      element.tagName,
      element.getAttribute("role"),
      nameOf(element),
      element instanceof HTMLInputElement && element.type === "password"
        ? element.value.length
        : controlValue(element),
      element.matches(":disabled,[aria-disabled=true]"),
      element instanceof HTMLInputElement ? element.checked : null,
    ]);
  // Scoped guard, after browser-use/jev-ultrafast's snapshot guards (MIT):
  // the acted-on control's enclosing form/dialog/row text must be unchanged.
  // A control directly under <body> has no scope: page-wide live content
  // (clocks, tickers) must not block every action.
  const context = (element: Element) => {
    const scope =
      element.closest(
        'form,dialog,[role="dialog"],article,li,tr,[role="row"]',
      ) ?? element.parentElement;
    return scope &&
      scope !== document.body &&
      scope !== document.documentElement
      ? (scope.textContent ?? "").slice(0, 4000)
      : "";
  };
  if (request.kind === "observe") {
    const targets = new Map<string, Element>();
    const signatures = new Map<string, string>();
    const contexts = new Map<string, string>();
    const result: VisibleTarget[] = [];
    const lines: string[] = [];
    let chars = 0;
    let visited = 0;
    let truncated = false;
    const walker = document.createTreeWalker(
      document.body ?? document.documentElement,
      NodeFilter.SHOW_ELEMENT | NodeFilter.SHOW_TEXT,
    );
    for (let node = walker.nextNode(); node; node = walker.nextNode()) {
      if (++visited > request.limits.nodes || Date.now() > request.deadline) {
        truncated = true;
        break;
      }
      if (
        node.nodeType === Node.TEXT_NODE &&
        node.parentElement &&
        !node.parentElement.closest("script,style,noscript,textarea") &&
        visible(node.parentElement)
      ) {
        const range = document.createRange();
        range.selectNodeContents(node);
        const rect = range.getBoundingClientRect();
        if (
          rect.width <= 0 ||
          rect.height <= 0 ||
          rect.bottom <= 0 ||
          rect.top >= innerHeight
        )
          continue;
        const text = (node.textContent ?? "").trim();
        if (text) {
          const remaining = request.limits.text - chars;
          if (remaining > 0) {
            lines.push(text.slice(0, remaining));
            chars += Math.min(text.length, remaining) + 1;
          }
          if (text.length > remaining) truncated = true;
        }
      }
      if (
        !(node instanceof Element) ||
        !node.matches(
          "a[href],button,input,textarea,select,[role=button],[role=link]",
        )
      )
        continue;
      if (!visible(node)) continue;
      if (result.length >= request.limits.targets) {
        truncated = true;
        continue;
      }
      const id = `t${result.length}`;
      targets.set(id, node);
      signatures.set(id, signature(node));
      contexts.set(id, context(node));
      const rect = node.getBoundingClientRect();
      const target: VisibleTarget = {
        id,
        tag: node.tagName.toLowerCase(),
        role: (node.getAttribute("role") ?? "").slice(0, 100),
        name: nameOf(node),
        bounds: {
          x: rect.x,
          y: rect.y,
          width: rect.width,
          height: rect.height,
        },
      };
      if (node instanceof HTMLInputElement) target.type = node.type;
      const value = controlValue(node);
      if (value !== undefined) target.value = value;
      if (node instanceof HTMLSelectElement)
        target.options = [...node.options]
          .slice(0, request.limits.options)
          .map((option) => ({
            value: option.value.slice(0, 512),
            label: (option.label || option.text).trim().slice(0, 160),
            ...(option.disabled || option.closest("optgroup[disabled]")
              ? { disabled: true }
              : {}),
          }));
      result.push(target);
    }
    state.sumiBrowserSnapshot = {
      id: request.observationId,
      expires: Date.now() + request.limits.observationMs,
      targets,
      signatures,
      contexts,
    };
    return {
      title: document.title.slice(0, 512),
      text: lines.join("\n").slice(0, request.limits.text),
      targets: result,
      truncated,
    };
  }

  const snapshot = state.sumiBrowserSnapshot;
  if (
    !snapshot ||
    snapshot.id !== request.observationId ||
    snapshot.expires < Date.now()
  )
    return { error: "stale_observation" };
  // A binding is single-use. Failed dispatches also require a fresh observation.
  delete state.sumiBrowserSnapshot;
  if (request.guard || request.kind === "check") {
    for (const [id, element] of snapshot.targets)
      if (snapshot.signatures.get(id) !== signature(element))
        return { error: "page_changed" };
    if (request.kind === "check") return { ok: true };
  }
  const action = request.action;
  if (action?.kind === "scroll") {
    window.scrollBy({ left: action.x, top: action.y, behavior: "instant" });
    return { ok: true };
  }
  if (
    action?.kind !== "click" &&
    action?.kind !== "fill" &&
    action?.kind !== "select"
  )
    return { error: "invalid_request" };
  const element = snapshot.targets.get(action.target);
  if (
    request.guard &&
    element &&
    snapshot.contexts.get(action.target) !== context(element)
  )
    return { error: "page_changed" };
  if (
    !(element instanceof HTMLElement) ||
    !element.isConnected ||
    !visible(element) ||
    element.matches(":disabled,[aria-disabled=true]")
  ) {
    return { error: "target_unavailable" };
  }
  // Do not activate elements covered by dialogs or other page content.
  const rect = element.getBoundingClientRect();
  const x =
    Math.max(0, rect.left) +
    (Math.min(innerWidth, rect.right) - Math.max(0, rect.left)) / 2;
  const y =
    Math.max(0, rect.top) +
    (Math.min(innerHeight, rect.bottom) - Math.max(0, rect.top)) / 2;
  const top = document.elementFromPoint(x, y);
  if (!top || (top !== element && !element.contains(top)))
    return { error: "target_unavailable" };
  if (action.kind === "click") {
    element.click();
    return { ok: true };
  }
  if (action.kind === "select") {
    if (!(element instanceof HTMLSelectElement))
      return { error: "target_unavailable" };
    const option = [...element.options].find(
      (o) =>
        o.value === action.value &&
        !o.disabled &&
        !o.closest("optgroup[disabled]"),
    );
    if (!option) return { error: "target_unavailable" };
    element.focus();
    option.selected = true;
    element.dispatchEvent(new Event("input", { bubbles: true }));
    element.dispatchEvent(new Event("change", { bubbles: true }));
    return { ok: true };
  }
  if (
    !(element instanceof HTMLTextAreaElement) &&
    !(
      element instanceof HTMLInputElement &&
      ["text", "search", "email", "url", "tel", "password"].includes(
        element.type,
      )
    )
  ) {
    return { error: "target_unavailable" };
  }
  if (element.readOnly) return { error: "target_unavailable" };
  element.focus();
  // Native DOM setter works with controlled inputs without invoking a page's
  // replacement own-property setter. Events still reach the website normally.
  const prototype =
    element instanceof HTMLInputElement
      ? HTMLInputElement.prototype
      : HTMLTextAreaElement.prototype;
  Object.getOwnPropertyDescriptor(prototype, "value")?.set?.call(
    element,
    action.text,
  );
  element.dispatchEvent(
    new InputEvent("input", {
      bubbles: true,
      inputType: "insertText",
      data: action.text,
    }),
  );
  element.dispatchEvent(new Event("change", { bubbles: true }));
  return { ok: true };
}
