import { forwardRef, useCallback, useEffect, useImperativeHandle, useRef, useState } from "react";
import { isImeComposing } from "../lib/ime";
import type { AgentAction } from "./model";
import { forwardsKey, MAX_TEXT, modifierBits, toOverlay, toRemotePoint, type Viewport } from "./screen-input";

export type ViewerInput =
  | { kind: "mouse"; event: "down" | "up" | "move"; x: number; y: number; button?: "left" | "middle" | "right"; buttons?: number; clickCount?: number; modifiers?: number }
  | { kind: "wheel"; x: number; y: number; dx: number; dy: number; modifiers?: number }
  | { kind: "key"; event: "down" | "up"; key: string; code?: string; modifiers?: number }
  | { kind: "text"; text: string };

export interface BrowserScreenHandle {
  paint(frame: { data: string }): void;
}

const BUTTONS = ["left", "middle", "right"] as const;

/**
 * The shared screen: the remote tab's own pixels (CDP screencast) scaled to
 * fit, with the person's pointer, wheel, keys, IME text and paste relayed in
 * remote CSS pixels. Frames are painted through a ref, not React state.
 */
export const BrowserScreen = forwardRef<
  BrowserScreenHandle,
  {
    viewport: Viewport;
    control: "agent" | "human";
    agent?: AgentAction;
    activeTab?: string;
    disabled?: boolean;
    onInput(input: ViewerInput): void;
    onUnsupported(code: string): void;
  }
>(function BrowserScreen({ viewport, control, agent, activeTab, disabled, onInput, onUnsupported }, ref) {
  const image = useRef<HTMLImageElement>(null);
  const keys = useRef<HTMLTextAreaElement>(null);
  const [painted, setPainted] = useState(false);
  const [focused, setFocused] = useState(false);
  const latest = useRef({ viewport, control, onInput, disabled });
  latest.current = { viewport, control, onInput, disabled };

  useImperativeHandle(
    ref,
    () => ({
      paint(frame) {
        if (!image.current) return;
        image.current.src = `data:image/jpeg;base64,${frame.data}`;
        setPainted(true);
      },
    }),
    [],
  );

  const point = useCallback((clientX: number, clientY: number) => {
    const box = image.current?.getBoundingClientRect();
    return box ? toRemotePoint(clientX, clientY, box, latest.current.viewport) : null;
  }, []);

  // Wheel needs a non-passive listener to keep the page itself from scrolling.
  useEffect(() => {
    const element = image.current;
    if (!element) return;
    let pending: { x: number; y: number; dx: number; dy: number; modifiers: number } | undefined;
    let frame = 0;
    const flush = () => {
      frame = 0;
      if (pending) latest.current.onInput({ kind: "wheel", ...pending });
      pending = undefined;
    };
    const onWheel = (event: WheelEvent) => {
      if (latest.current.disabled) return;
      event.preventDefault();
      const at = point(event.clientX, event.clientY);
      if (!at) return;
      const scale = event.deltaMode === 1 ? 40 : event.deltaMode === 2 ? latest.current.viewport.height : 1;
      pending = {
        ...at,
        dx: Math.max(-5000, Math.min(5000, (pending?.dx ?? 0) + event.deltaX * scale)),
        dy: Math.max(-5000, Math.min(5000, (pending?.dy ?? 0) + event.deltaY * scale)),
        modifiers: modifierBits(event),
      };
      frame ||= requestAnimationFrame(flush);
    };
    element.addEventListener("wheel", onWheel, { passive: false });
    return () => {
      element.removeEventListener("wheel", onWheel);
      cancelAnimationFrame(frame);
    };
  }, [point]);

  const moveFrame = useRef(0);
  const pendingMove = useRef<ViewerInput | null>(null);

  const pointer = (event: React.PointerEvent<HTMLImageElement>, kind: "down" | "up" | "move") => {
    if (disabled) return;
    const at = point(event.clientX, event.clientY);
    if (!at) return;
    const button = BUTTONS[event.button] ?? "left";
    const input: ViewerInput = {
      kind: "mouse",
      event: kind,
      ...at,
      button,
      buttons: event.buttons,
      clickCount: kind === "move" ? 0 : Math.max(1, Math.min(3, event.detail || 1)),
      modifiers: modifierBits(event),
    };
    if (kind === "move") {
      // Hover only matters while the person drives; the secretary's screen
      // is not disturbed by a resting pointer.
      if (control !== "human" && !event.buttons) return;
      pendingMove.current = input;
      moveFrame.current ||= requestAnimationFrame(() => {
        moveFrame.current = 0;
        if (pendingMove.current) onInput(pendingMove.current);
        pendingMove.current = null;
      });
      return;
    }
    if (kind === "down") {
      event.preventDefault();
      keys.current?.focus({ preventScroll: true });
      event.currentTarget.setPointerCapture(event.pointerId);
    }
    onInput(input);
  };

  const key = (event: React.KeyboardEvent<HTMLTextAreaElement>, kind: "down" | "up") => {
    if (disabled || isImeComposing(event) || !forwardsKey(event)) return;
    event.preventDefault();
    onInput({ kind: "key", event: kind, key: event.key, code: event.code, modifiers: modifierBits(event) });
  };

  const text = (value: string) => {
    if (!value || disabled) return;
    for (let i = 0; i < value.length; i += MAX_TEXT) onInput({ kind: "text", text: value.slice(i, i + MAX_TEXT) });
  };

  return (
    <div
      className="relative"
      // Fit inside the parent's box (a size container) at the remote
      // viewport's aspect ratio, never above its native size.
      style={{
        width: `min(100cqw, calc(100cqh * ${viewport.width / viewport.height}), ${viewport.width}px)`,
        aspectRatio: `${viewport.width} / ${viewport.height}`,
      }}
      onDragOver={(e) => e.preventDefault()}
      onDrop={(e) => {
        e.preventDefault();
        if (e.dataTransfer.files.length) onUnsupported("file_drag_drop");
      }}
    >
      <img
        ref={image}
        alt="秘書と共有しているブラウザの画面"
        draggable={false}
        data-testid="cloud-browser-frame"
        className={`absolute inset-0 size-full select-none rounded-md border bg-muted/40 object-contain ${
          focused ? "border-ring ring-2 ring-ring/40" : "border-border"
        } ${control === "human" ? "cursor-default" : "cursor-pointer"}`}
        style={{ visibility: painted ? "visible" : "hidden", touchAction: "none" }}
        onPointerDown={(e) => pointer(e, "down")}
        onPointerUp={(e) => pointer(e, "up")}
        onPointerMove={(e) => pointer(e, "move")}
        onContextMenu={(e) => e.preventDefault()}
        onDragStart={(e) => e.preventDefault()}
      />
      {painted ? null : (
        <div className="absolute inset-0 grid place-items-center rounded-md border border-border bg-muted/30 text-muted-foreground text-sm">
          画面を待っています…
        </div>
      )}
      {agent && agent.bounds && agent.tab === activeTab ? (
        <div
          aria-hidden
          className="pointer-events-none absolute rounded-sm border-2 border-sky-500 bg-sky-500/10"
          style={toOverlay(agent.bounds, viewport)}
        />
      ) : null}
      <textarea
        ref={keys}
        aria-label="共有ブラウザへのキーボード入力"
        data-testid="cloud-browser-keys"
        className="pointer-events-none absolute bottom-0 left-0 h-8 w-40 resize-none opacity-0"
        autoCapitalize="off"
        autoComplete="off"
        spellCheck={false}
        onFocus={() => setFocused(true)}
        onBlur={() => setFocused(false)}
        onKeyDown={(e) => key(e, "down")}
        onKeyUp={(e) => key(e, "up")}
        onCompositionEnd={(e) => {
          text(e.data);
          e.currentTarget.value = "";
        }}
        onInput={(e) => {
          const native = e.nativeEvent as InputEvent;
          if (native.isComposing || native.inputType === "insertCompositionText") return;
          if (native.inputType === "insertText" && native.data) text(native.data);
          e.currentTarget.value = "";
        }}
        onPaste={(e) => {
          e.preventDefault();
          text(e.clipboardData.getData("text/plain"));
        }}
        onDrop={(e) => e.preventDefault()}
      />
    </div>
  );
});
