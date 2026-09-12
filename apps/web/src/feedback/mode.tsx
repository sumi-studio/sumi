import { Check, Crosshair, MessageSquarePlus, Minus, X } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { useAuth } from "../auth/auth-context";
import { isImeComposing } from "../lib/ime";
import { type Bootstrap, feedbackClient, type Thread } from "./api";
import {
  captureFeedbackDiagnostics,
  type DiagnosticAnnotation,
  type DiagnosticSelection,
  type FeedbackDiagnostics,
  MAX_FEEDBACK_ANNOTATIONS,
  recordFeedbackOrigin,
  sanitizeFeedbackSummary,
} from "./diagnostics";
import { NewThread } from "./inbox";
import "./mode.css";

export const OPEN_FEEDBACK_MODE = "sumi:feedback-mode";
export function openFeedbackMode() {
  window.dispatchEvent(new Event(OPEN_FEEDBACK_MODE));
}

/** A modeless panel: the current app and its transports remain mounted. */
export function FeedbackMode({
  pathname,
  workspaceId,
}: {
  pathname: string;
  workspaceId?: string;
}) {
  const { authenticated, user } = useAuth();
  const allowed = Boolean(authenticated && user);
  const [snapshot, setSnapshot] = useState<FeedbackDiagnostics>();
  const [open, setOpen] = useState(false);
  const [minimized, setMinimized] = useState(false);
  const [selecting, setSelecting] = useState(false);
  const [capture, setCapture] = useState(false);
  const [selection, setSelection] = useState<DiagnosticSelection>();
  const [hoverRect, setHoverRect] = useState<DiagnosticSelection["rect"]>();
  const [annotations, setAnnotations] = useState<DiagnosticAnnotation[]>([]);
  const [markers, setMarkers] = useState<
    { annotation: DiagnosticAnnotation; rect: DiagnosticSelection["rect"] }[]
  >([]);
  const [revealed, setRevealed] = useState<number>();
  const [revealNotice, setRevealNotice] = useState("");
  const anchors = useRef(
    new Map<string, { element?: Element; nestedScroll: string }>(),
  );
  const suppressClick = useRef(false);
  const [bootstrap, setBootstrap] = useState<Bootstrap>();
  const [loadError, setLoadError] = useState(false);
  const [created, setCreated] = useState<Thread>();
  const trigger = useRef<HTMLElement | null>(null);
  const panel = useRef<HTMLElement>(null);
  const lastEditor = useRef("feedback-mode-body");
  const [viewport, setViewport] = useState<{
    height: number;
    bottom: number;
  }>();
  const userId = user?.id;
  useEffect(() => {
    const visual = window.visualViewport;
    if (!visual) return;
    const update = () =>
      setViewport({
        height: visual.height - 16,
        bottom:
          Math.max(0, window.innerHeight - visual.height - visual.offsetTop) +
          (window.innerWidth <= 600 ? 8 : 20),
      });
    update();
    visual.addEventListener("resize", update);
    visual.addEventListener("scroll", update);
    return () => {
      visual.removeEventListener("resize", update);
      visual.removeEventListener("scroll", update);
    };
  }, []);
  // biome-ignore lint/correctness/useExhaustiveDependencies: A changed identity must discard the previous user's open panel.
  useEffect(() => {
    setOpen(false);
    setSnapshot(undefined);
    setSelection(undefined);
    setCreated(undefined);
    setSelecting(false);
    setMinimized(false);
    setCapture(false);
    setAnnotations([]);
    setMarkers([]);
    setRevealed(undefined);
    setRevealNotice("");
    anchors.current.clear();
  }, [userId, allowed]);
  useEffect(() => {
    if (!allowed) return;
    const show = () => {
      if (capture) return;
      trigger.current =
        document.activeElement instanceof HTMLElement
          ? document.activeElement
          : null;
      if (!open) {
        recordFeedbackOrigin(pathname, workspaceId, true);
        setSnapshot(captureFeedbackDiagnostics());
        setSelection(undefined);
        setCreated(undefined);
      }
      setOpen(true);
      setMinimized(false);
    };
    const shortcut = (event: KeyboardEvent) => {
      if (
        event.altKey &&
        event.shiftKey &&
        event.code === "KeyF" &&
        !event.repeat &&
        !isImeComposing(event)
      ) {
        event.preventDefault();
        show();
      }
    };
    window.addEventListener(OPEN_FEEDBACK_MODE, show);
    window.addEventListener("keydown", shortcut);
    return () => {
      window.removeEventListener(OPEN_FEEDBACK_MODE, show);
      window.removeEventListener("keydown", shortcut);
    };
  }, [allowed, open, pathname, workspaceId, capture]);
  useEffect(() => {
    if (!open || !allowed) return;
    let active = true;
    setBootstrap(undefined);
    setLoadError(false);
    void feedbackClient
      .bootstrap()
      .then((value) => {
        if (active) setBootstrap(value);
      })
      .catch(() => {
        if (active) setLoadError(true);
      });
    return () => {
      active = false;
    };
  }, [open, allowed]);
  useEffect(() => {
    if (!open || minimized || selecting || capture || !bootstrap) return;
    const frame = requestAnimationFrame(() =>
      panel.current
        ?.querySelector<HTMLTextAreaElement>(`#${lastEditor.current}`)
        ?.focus({ preventScroll: true }),
    );
    return () => cancelAnimationFrame(frame);
  }, [open, minimized, selecting, capture, bootstrap]);
  useEffect(() => {
    if (!open || !allowed) return;
    const clear = () => {
      suppressClick.current = false;
    };
    const click = (event: MouseEvent) => {
      if (!suppressClick.current) return;
      suppressClick.current = false;
      event.preventDefault();
      event.stopImmediatePropagation();
    };
    document.addEventListener("pointerdown", clear, true);
    document.addEventListener("click", click, true);
    return () => {
      clear();
      document.removeEventListener("pointerdown", clear, true);
      document.removeEventListener("click", click, true);
    };
  }, [open, allowed]);
  useEffect(() => {
    if (!selecting || !open || !allowed || capture) return;
    let drag:
      | { element: Element; x: number; y: number; pointerId: number }
      | undefined;
    const targetAt = (event: MouseEvent) => {
      const target = event.target;
      if (!(target instanceof Element)) return null;
      if (
        target.closest("[data-feedback-ui]") &&
        !target.closest("[data-feedback-selection-surface]")
      )
        return null;
      return (
        document
          .elementsFromPoint?.(event.clientX, event.clientY)
          .find((element) => !element.closest("[data-feedback-ui]")) ??
        (!target.closest("[data-feedback-ui]") ? target : null)
      );
    };
    const suppress = (event: Event) => {
      event.preventDefault();
      event.stopImmediatePropagation();
    };
    const finish = (
      element: Element,
      rect: DiagnosticSelection["rect"],
      kind: "element" | "region",
    ) => {
      if (annotations.length >= MAX_FEEDBACK_ANNOTATIONS) return;
      const next: DiagnosticSelection = {
        kind,
        path: pathname.split(/[?#]/, 1)[0],
        tag: kind === "element" ? element.tagName.toLowerCase() : "region",
        selector: kind === "element" ? selectorFor(element) : "",
        label: kind === "element" ? elementLabel(element) : "指定した範囲",
        rect: roundedRect(rect),
        scroll_x: Math.round(window.scrollX),
        scroll_y: Math.round(window.scrollY),
        viewport_width: window.innerWidth,
        viewport_height: window.innerHeight,
        captured_at: new Date().toISOString(),
      };
      anchors.current.set(annotationKey(next), {
        element,
        nestedScroll: nestedScrollSignature(),
      });
      setSelection(next);
      setSelecting(false);
      setHoverRect(undefined);
      setMinimized(false);
      setRevealNotice("");
    };
    const down = (event: PointerEvent) => {
      if (event.button !== 0 && event.button !== undefined) return;
      const element = targetAt(event);
      if (!element) return;
      suppress(event);
      drag = {
        element,
        x: event.clientX,
        y: event.clientY,
        pointerId: event.pointerId,
      };
      setHoverRect(element.getBoundingClientRect());
    };
    const move = (event: PointerEvent) => {
      if (drag) {
        if (event.pointerId !== drag.pointerId) return;
        suppress(event);
        setHoverRect(
          regionRect(drag.x, drag.y, event.clientX, event.clientY) ??
            drag.element.getBoundingClientRect(),
        );
      } else {
        setHoverRect(targetAt(event)?.getBoundingClientRect());
      }
    };
    const up = (event: PointerEvent) => {
      if (!drag || event.pointerId !== drag.pointerId) return;
      suppress(event);
      suppressClick.current = true;
      const region = regionRect(drag.x, drag.y, event.clientX, event.clientY);
      finish(
        drag.element,
        region ?? drag.element.getBoundingClientRect(),
        region ? "region" : "element",
      );
      drag = undefined;
    };
    // Keyboard activation and assistive technology can produce a click without pointer events.
    const click = (event: MouseEvent) => {
      const element = targetAt(event);
      if (!element) return;
      suppress(event);
      finish(element, element.getBoundingClientRect(), "element");
    };
    const cancel = () => {
      drag = undefined;
      setSelecting(false);
      setHoverRect(undefined);
    };
    const keydown = (event: KeyboardEvent) => {
      if (event.key === "Escape" && !isImeComposing(event)) {
        suppress(event);
        cancel();
      }
    };
    const touchmove = (event: TouchEvent) => {
      if (
        drag ||
        (event.target instanceof Element &&
          event.target.closest("[data-feedback-selection-surface]"))
      )
        event.preventDefault();
    };
    document.addEventListener("pointerdown", down, true);
    document.addEventListener("pointermove", move, true);
    document.addEventListener("pointerup", up, true);
    document.addEventListener("pointercancel", cancel, true);
    document.addEventListener("click", click, true);
    document.addEventListener("keydown", keydown, true);
    document.addEventListener("touchmove", touchmove, {
      capture: true,
      passive: false,
    });
    return () => {
      document.removeEventListener("pointerdown", down, true);
      document.removeEventListener("pointermove", move, true);
      document.removeEventListener("pointerup", up, true);
      document.removeEventListener("pointercancel", cancel, true);
      document.removeEventListener("click", click, true);
      document.removeEventListener("keydown", keydown, true);
      document.removeEventListener("touchmove", touchmove, true);
      setHoverRect(undefined);
    };
  }, [selecting, open, allowed, capture, pathname, annotations.length]);
  // biome-ignore lint/correctness/useExhaustiveDependencies: Navigation invalidates the current gesture and highlight.
  useEffect(() => {
    // Navigation invalidates an in-progress pointer gesture.
    setSelecting(false);
    setRevealed(undefined);
    setRevealNotice("");
  }, [pathname]);
  useEffect(() => {
    if (!open || !allowed || capture) {
      setMarkers([]);
      return;
    }
    let frame: number | undefined;
    const update = () => {
      frame = undefined;
      const nestedScroll = annotations.some(
        (annotation) =>
          annotation.kind === "region" && annotation.path === pathname,
      )
        ? nestedScrollSignature()
        : undefined;
      const next = annotations.flatMap((annotation) => {
        const rect = locateAnnotation(
          annotation,
          pathname,
          anchors.current,
          nestedScroll,
        );
        return rect &&
          rect.width > 0 &&
          rect.height > 0 &&
          rect.x + rect.width > 0 &&
          rect.y + rect.height > 0 &&
          rect.x < window.innerWidth &&
          rect.y < window.innerHeight
          ? [{ annotation, rect }]
          : [];
      });
      setMarkers((previous) =>
        JSON.stringify(previous) === JSON.stringify(next) ? previous : next,
      );
    };
    const schedule = () => {
      if (frame === undefined) frame = requestAnimationFrame(update);
    };
    const isFeedbackNode = (node: Node) =>
      Boolean(
        (node instanceof Element ? node : node.parentElement)?.closest(
          "[data-feedback-ui]",
        ),
      );
    const onScroll = (event: Event) => {
      if (!(event.target instanceof Node) || !isFeedbackNode(event.target))
        schedule();
    };
    update();
    document.addEventListener("scroll", onScroll, true);
    window.addEventListener("resize", schedule);
    const observer =
      typeof ResizeObserver !== "undefined"
        ? new ResizeObserver(schedule)
        : undefined;
    observer?.observe(document.body);
    const mutations = new MutationObserver((records) => {
      if (
        records.some(
          (record) =>
            !isFeedbackNode(record.target) &&
            [...record.addedNodes, ...record.removedNodes].some(
              (node) => !isFeedbackNode(node),
            ),
        )
      )
        schedule();
    });
    mutations.observe(document.body, { childList: true, subtree: true });
    return () => {
      if (frame !== undefined) cancelAnimationFrame(frame);
      document.removeEventListener("scroll", onScroll, true);
      window.removeEventListener("resize", schedule);
      observer?.disconnect();
      mutations.disconnect();
    };
  }, [annotations, pathname, open, allowed, capture]);
  const revealAnnotation = (annotation: DiagnosticAnnotation) => {
    const rect = locateAnnotation(annotation, pathname, anchors.current);
    if (!rect) {
      setRevealNotice("この箇所は、指定した元の画面と表示位置で確認できます。");
      return;
    }
    const element = locateElement(annotation, anchors.current);
    element?.scrollIntoView?.({
      block: "center",
      inline: "nearest",
      behavior: "instant",
    });
    setRevealed(annotation.number);
    setRevealNotice("");
    setMinimized(true);
  };
  if (!allowed || !user || !open) return null;
  const close = () => {
    if (capture) return;
    setOpen(false);
    setSelecting(false);
    trigger.current?.focus({ preventScroll: true });
  };
  return createPortal(
    <>
      {selecting && !capture && (
        <div
          className="feedback-selection-surface"
          data-feedback-ui
          data-feedback-selection-surface
        />
      )}
      {!capture &&
        !created &&
        markers.map(({ annotation, rect }) => (
          <div
            key={annotation.number}
            data-feedback-ui
            className={`feedback-annotation-marker${revealed === annotation.number ? " is-revealed" : ""}`}
            style={{
              left: rect.x,
              top: rect.y,
              width: rect.width,
              height: rect.height,
            }}
          >
            <button
              type="button"
              aria-label={`指定箇所 ${annotation.number} を確認`}
              onClick={() => revealAnnotation(annotation)}
            >
              {annotation.number}
            </button>
          </div>
        ))}
      {selecting && !capture && (
        <div className="feedback-select-instruction" data-feedback-ui>
          <Crosshair size={16} />
          <span>クリックして箇所を、ドラッグして範囲を指定</span>
          <button
            type="button"
            onClick={() => {
              setSelecting(false);
              setHoverRect(undefined);
            }}
          >
            キャンセル
          </button>
        </div>
      )}
      {selecting && !capture && hoverRect && (
        <div
          data-feedback-ui
          className="feedback-target-outline"
          style={{
            left: hoverRect.x,
            top: hoverRect.y,
            width: hoverRect.width,
            height: hoverRect.height,
          }}
        />
      )}
      {minimized && !selecting && !capture && (
        <button
          type="button"
          className="feedback-mode-resume"
          data-feedback-ui
          onClick={() => setMinimized(false)}
        >
          <MessageSquarePlus size={17} />
          フィードバックの続きを書く
        </button>
      )}
      <section
        ref={panel}
        role="dialog"
        aria-label="この画面からフィードバック"
        data-feedback-ui
        className={`feedback-app feedback-mode-panel${minimized || selecting || capture ? " is-concealed" : ""}`}
        style={
          viewport
            ? { maxHeight: viewport.height, bottom: viewport.bottom }
            : undefined
        }
        onFocusCapture={(event) => {
          if (event.target instanceof HTMLTextAreaElement)
            lastEditor.current = event.target.id;
        }}
        onKeyDown={(event) => {
          if (event.key === "Escape" && !isImeComposing(event)) {
            event.stopPropagation();
            close();
          }
        }}
      >
        {created ? (
          <div className="feedback-mode-success">
            <Check size={24} />
            <h2>送信しました</h2>
            <p>このまま作業を続けられます。返信はFeedbackに届きます。</p>
            <a href={`/feedback?thread=${encodeURIComponent(created.id)}`}>
              会話を開く
            </a>
            <button type="button" onClick={close}>
              閉じる
            </button>
          </div>
        ) : bootstrap ? (
          <NewThread
            actor={`human:${user.id}`}
            draftNamespace="in-place"
            initialDiagnostics={snapshot}
            selection={selection}
            onAnnotationsChange={setAnnotations}
            onRevealAnnotation={revealAnnotation}
            resolveAnnotation={(annotation: DiagnosticAnnotation) =>
              open && allowed
                ? locateAnnotation(annotation, pathname, anchors.current)
                : undefined
            }
            client={feedbackClient}
            recipient={bootstrap.recipient_name}
            enabled={bootstrap.available}
            onBack={close}
            onCreated={setCreated}
            onCaptureChange={setCapture}
            header={
              <>
                {revealNotice && (
                  <p className="feedback-reveal-notice" role="status">
                    {revealNotice}
                  </p>
                )}
                <header className="feedback-mode-heading">
                  <span>フィードバック</span>
                  <div>
                    <button
                      type="button"
                      aria-label="画面の場所を指定"
                      title={
                        annotations.length >= MAX_FEEDBACK_ANNOTATIONS
                          ? "指定できる箇所は10件までです"
                          : "場所を指す（クリック・ドラッグ）"
                      }
                      disabled={annotations.length >= MAX_FEEDBACK_ANNOTATIONS}
                      onClick={() => {
                        setRevealed(undefined);
                        setSelecting(true);
                      }}
                    >
                      <Crosshair size={17} />
                    </button>
                    <button
                      type="button"
                      aria-label="小さくして画面を操作"
                      title="小さくして画面を操作"
                      onClick={() => setMinimized(true)}
                    >
                      <Minus size={17} />
                    </button>
                    <button
                      type="button"
                      aria-label="閉じる（下書きを保存）"
                      title="閉じる（下書きを保存）"
                      onClick={close}
                    >
                      <X size={17} />
                    </button>
                  </div>
                </header>
              </>
            }
          />
        ) : (
          <div className="feedback-mode-loading">
            <p role="status">
              {loadError
                ? "読み込めませんでした。閉じてもう一度お試しください。"
                : "フィードバックを準備しています…"}
            </p>
            <button type="button" onClick={close}>
              閉じる
            </button>
          </div>
        )}
      </section>
    </>,
    document.body,
  );
}
function selectorFor(element: Element): string {
  const parts: string[] = [];
  let node: Element | null = element;
  while (node && parts.length < 5) {
    const tag = node.tagName.toLowerCase();
    // Prefer a structural path: DOM IDs can include private content or tokens.
    const parent: Element | null = node.parentElement;
    const index = parent
      ? Array.from(parent.children)
          .filter((child) => child.tagName === node?.tagName)
          .indexOf(node) + 1
      : 1;
    parts.unshift(`${tag}:nth-of-type(${index})`);
    node = parent;
  }
  return parts.join(" > ").slice(0, 512);
}

function roundedRect(
  rect: DiagnosticSelection["rect"],
): DiagnosticSelection["rect"] {
  return {
    x: Math.round(rect.x),
    y: Math.round(rect.y),
    width: Math.round(rect.width),
    height: Math.round(rect.height),
  };
}
function regionRect(
  x: number,
  y: number,
  endX: number,
  endY: number,
): DiagnosticSelection["rect"] | undefined {
  if (Math.hypot(endX - x, endY - y) < 6) return undefined;
  const startX = Math.round(Math.max(0, Math.min(window.innerWidth, x)));
  const startY = Math.round(Math.max(0, Math.min(window.innerHeight, y)));
  const finishX = Math.round(Math.max(0, Math.min(window.innerWidth, endX)));
  const finishY = Math.round(Math.max(0, Math.min(window.innerHeight, endY)));
  const rect = {
    x: Math.min(startX, finishX),
    y: Math.min(startY, finishY),
    width: Math.abs(finishX - startX),
    height: Math.abs(finishY - startY),
  };
  return rect.width >= 2 && rect.height >= 2 ? rect : undefined;
}
function elementLabel(element: Element): string {
  const isEditor =
    /^(INPUT|TEXTAREA|SELECT)$/.test(element.tagName) ||
    element.closest("[contenteditable]");
  return sanitizeFeedbackSummary(
    element.getAttribute("aria-label") ||
      (isEditor ? element.getAttribute("placeholder") : element.textContent) ||
      element.tagName.toLowerCase(),
  ).slice(0, 200);
}
function annotationKey(annotation: DiagnosticSelection): string {
  return `${annotation.captured_at}:${annotation.selector}:${JSON.stringify(annotation.rect)}`;
}
type Anchors = Map<string, { element?: Element; nestedScroll: string }>;
function nestedScrollSignature(): string {
  return Array.from(document.querySelectorAll("*"))
    .filter(
      (element) =>
        element !== document.documentElement &&
        element !== document.body &&
        !element.closest("[data-feedback-ui]") &&
        (element.scrollTop || element.scrollLeft),
    )
    .map(
      (element) =>
        `${selectorFor(element)}:${element.scrollLeft}:${element.scrollTop}`,
    )
    .join("|");
}
function locateElement(
  annotation: DiagnosticSelection,
  anchors: Anchors,
): Element | undefined {
  if (annotation.kind !== "element") return undefined;
  const original = anchors.get(annotationKey(annotation))?.element;
  if (original && !original.isConnected) return undefined;
  if (original?.isConnected && elementLabel(original) === annotation.label)
    return original;
  try {
    const candidates = document.querySelectorAll(annotation.selector);
    if (candidates.length !== 1) return undefined;
    const element = candidates[0];
    return !element.closest("[data-feedback-ui]") &&
      element.tagName.toLowerCase() === annotation.tag &&
      elementLabel(element) === annotation.label
      ? element
      : undefined;
  } catch {
    return undefined;
  }
}
function locateAnnotation(
  annotation: DiagnosticSelection,
  pathname: string,
  anchors: Anchors,
  nestedScroll?: string,
): DiagnosticSelection["rect"] | undefined {
  if (annotation.path !== pathname.split(/[?#]/, 1)[0]) return undefined;
  if (annotation.kind === "element")
    return locateElement(annotation, anchors)?.getBoundingClientRect();
  const original = anchors.get(annotationKey(annotation));
  if (
    !original?.element?.isConnected ||
    annotation.scroll_x !== Math.round(window.scrollX) ||
    annotation.scroll_y !== Math.round(window.scrollY) ||
    annotation.viewport_width !== window.innerWidth ||
    annotation.viewport_height !== window.innerHeight ||
    original.nestedScroll !== (nestedScroll ?? nestedScrollSignature())
  )
    return undefined;
  return annotation.rect;
}
