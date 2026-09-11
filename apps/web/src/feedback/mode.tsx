import { Check, Crosshair, MessageSquarePlus, Minus, X } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { useAuth } from "../auth/auth-context";
import { isImeComposing } from "../lib/ime";
import {
  participantInstallation,
  useParticipantApps,
} from "../participant/app-store";
import { type Bootstrap, feedbackClient, type Thread } from "./api";
import {
  captureFeedbackDiagnostics,
  type DiagnosticSelection,
  type FeedbackDiagnostics,
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
  const apps = useParticipantApps();
  const installed = participantInstallation(apps.installations, "feedback");
  const allowed = Boolean(
    authenticated &&
      user &&
      apps.owner?.kind === "participant" &&
      apps.owner.participant.kind === "human" &&
      apps.owner.participant.humanId === user.id &&
      installed !== "duplicate" &&
      installed?.state === "enabled",
  );
  const [snapshot, setSnapshot] = useState<FeedbackDiagnostics>();
  const [open, setOpen] = useState(false);
  const [minimized, setMinimized] = useState(false);
  const [selecting, setSelecting] = useState(false);
  const [capture, setCapture] = useState(false);
  const [selection, setSelection] = useState<DiagnosticSelection>();
  const [hoverRect, setHoverRect] = useState<DOMRect>();
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
  // biome-ignore lint/correctness/useExhaustiveDependencies: A changed identity or installation must discard the previous user's open panel.
  useEffect(() => {
    setOpen(false);
    setSnapshot(undefined);
    setSelection(undefined);
    setCreated(undefined);
    setSelecting(false);
    setMinimized(false);
    setCapture(false);
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
    if (!selecting || !open || !allowed) return;
    const validTarget = (target: EventTarget | null) =>
      target instanceof Element && !target.closest("[data-feedback-ui]")
        ? target
        : null;
    const move = (event: PointerEvent) => {
      const element = validTarget(event.target);
      setHoverRect(element?.getBoundingClientRect());
    };
    const suppressPointerAction = (event: PointerEvent) => {
      if (validTarget(event.target)) {
        event.preventDefault();
        event.stopPropagation();
      }
    };
    const pick = (event: MouseEvent) => {
      const element = validTarget(event.target);
      if (!element) return;
      event.preventDefault();
      event.stopPropagation();
      const rect = element.getBoundingClientRect();
      const selector = selectorFor(element);
      // The selected element is explicitly included; never read input values.
      const label =
        /^(INPUT|TEXTAREA|SELECT)$/.test(element.tagName) ||
        element.closest("[contenteditable]")
          ? element.getAttribute("aria-label") ||
            element.getAttribute("placeholder") ||
            element.tagName.toLowerCase()
          : element.getAttribute("aria-label") ||
            element.textContent ||
            element.tagName.toLowerCase();
      setSelection({
        tag: element.tagName.toLowerCase(),
        selector,
        label: sanitizeFeedbackSummary(label).slice(0, 200),
        rect: {
          x: Math.round(rect.x),
          y: Math.round(rect.y),
          width: Math.round(rect.width),
          height: Math.round(rect.height),
        },
        captured_at: new Date().toISOString(),
      });
      setSelecting(false);
      setHoverRect(undefined);
      setMinimized(false);
    };
    const cancel = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.preventDefault();
        setSelecting(false);
        setHoverRect(undefined);
      }
    };
    document.addEventListener("pointermove", move, true);
    document.addEventListener("pointerdown", suppressPointerAction, true);
    document.addEventListener("pointerup", suppressPointerAction, true);
    document.addEventListener("click", pick, true);
    document.addEventListener("keydown", cancel, true);
    return () => {
      document.removeEventListener("pointermove", move, true);
      document.removeEventListener("pointerdown", suppressPointerAction, true);
      document.removeEventListener("pointerup", suppressPointerAction, true);
      document.removeEventListener("click", pick, true);
      document.removeEventListener("keydown", cancel, true);
    };
  }, [selecting, open, allowed]);
  if (!allowed || !user || !open) return null;
  const close = () => {
    if (capture) return;
    setOpen(false);
    setSelecting(false);
    trigger.current?.focus({ preventScroll: true });
  };
  return createPortal(
    <>
      {selecting && (
        <div className="feedback-select-instruction" data-feedback-ui>
          <Crosshair size={16} />
          <span>気になる場所をクリックしてください</span>
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
      {selecting && hoverRect && (
        <div
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
            client={feedbackClient}
            recipient={bootstrap.recipient_name}
            enabled={bootstrap.enabled && bootstrap.available}
            onBack={close}
            onCreated={setCreated}
            onCaptureChange={setCapture}
            header={
              <header className="feedback-mode-heading">
                <span>フィードバック</span>
                <div>
                  <button
                    type="button"
                    aria-label="画面の場所を指定"
                    title="画面の場所を指定"
                    onClick={() => setSelecting(true)}
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
