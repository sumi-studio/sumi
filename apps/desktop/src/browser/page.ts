import type { BrowserAction, LIMITS, VisibleTarget } from "./contract.js";

/** Serialized into a dedicated isolated world. No page script, arbitrary expression,
 * selector, Electron object, or Node function is accepted from the caller. */
export function pageOperation(request: {
  kind: "observe" | "act";
  observationId: string;
  url: string;
  deadline: number;
  limits: typeof LIMITS;
  action?: BrowserAction;
}) {
  type Snapshot = {
    id: string;
    expires: number;
    targets: Map<string, Element>;
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
  if (request.kind === "observe") {
    const targets = new Map<string, Element>();
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
      const rect = node.getBoundingClientRect();
      const target: VisibleTarget = {
        id,
        tag: node.tagName.toLowerCase(),
        role: (node.getAttribute("role") ?? "").slice(0, 100),
        name: (
          node.getAttribute("aria-label") ??
          node.getAttribute("placeholder") ??
          node.textContent ??
          ""
        )
          .trim()
          .slice(0, 256),
        bounds: {
          x: rect.x,
          y: rect.y,
          width: rect.width,
          height: rect.height,
        },
      };
      if (
        (node instanceof HTMLInputElement &&
          !["password", "file", "hidden"].includes(node.type)) ||
        node instanceof HTMLTextAreaElement ||
        node instanceof HTMLSelectElement
      ) {
        target.value = node.value.slice(0, 512);
      }
      result.push(target);
    }
    state.sumiBrowserSnapshot = {
      id: request.observationId,
      expires: Date.now() + request.limits.observationMs,
      targets,
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
  const action = request.action;
  if (action?.kind === "scroll") {
    window.scrollBy({ left: action.x, top: action.y, behavior: "instant" });
    return { ok: true };
  }
  if (action?.kind !== "click" && action?.kind !== "fill")
    return { error: "invalid_request" };
  const element = snapshot.targets.get(action.target);
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
