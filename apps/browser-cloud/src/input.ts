import { VIEWPORT } from "./remote-browser.ts";

/** The person's input from the viewer, in the remote viewport's CSS pixels.
 * `move` never takes control from the secretary; everything else does. */
export type ViewerInput =
  | { kind: "mouse"; event: "down" | "up" | "move"; x: number; y: number; button?: "left" | "middle" | "right"; buttons?: number; clickCount?: number; modifiers?: number }
  | { kind: "wheel"; x: number; y: number; dx: number; dy: number; modifiers?: number }
  | { kind: "key"; event: "down" | "up"; key: string; code?: string; modifiers?: number }
  | { kind: "text"; text: string };

export interface CdpInput {
  method: "Input.dispatchMouseEvent" | "Input.dispatchKeyEvent" | "Input.insertText";
  params: Record<string, unknown>;
}

const KEYS: Record<string, number> = {
  Enter: 13, Tab: 9, Backspace: 8, Escape: 27, Delete: 46, Insert: 45,
  ArrowLeft: 37, ArrowUp: 38, ArrowRight: 39, ArrowDown: 40,
  Home: 36, End: 35, PageUp: 33, PageDown: 34, " ": 32,
  F5: 116,
};

/** Editing shortcuts Chrome headless only performs as explicit commands. */
const COMMANDS: Record<string, string> = { a: "selectAll", z: "undo", y: "redo" };

const MAX_TEXT = 2_000;

function point(x: unknown, y: unknown): { x: number; y: number } | null {
  if (typeof x !== "number" || typeof y !== "number" || !Number.isFinite(x) || !Number.isFinite(y)) return null;
  if (x < 0 || y < 0 || x > VIEWPORT.width || y > VIEWPORT.height) return null;
  return { x: Math.round(x * 10) / 10, y: Math.round(y * 10) / 10 };
}

function modifiers(value: unknown): number {
  return typeof value === "number" && Number.isInteger(value) ? value & 15 : 0;
}

/** Whether this input takes control before it is dispatched. */
export function takesControl(input: ViewerInput): boolean {
  return !(input.kind === "mouse" && input.event === "move");
}

/** Validate and translate one viewer input. Unknown or unbounded input
 * returns null and is not dispatched. */
export function toCdp(raw: unknown): CdpInput[] | null {
  if (!raw || typeof raw !== "object") return null;
  const input = raw as ViewerInput;
  switch (input.kind) {
    case "mouse": {
      const at = point(input.x, input.y);
      const type = { down: "mousePressed", up: "mouseReleased", move: "mouseMoved" }[input.event];
      const button = input.button ?? "left";
      if (!at || !type || !["left", "middle", "right"].includes(button)) return null;
      const clickCount = input.event === "move" ? 0 : Math.min(Math.max(Number(input.clickCount) || 1, 1), 3);
      return [{
        method: "Input.dispatchMouseEvent",
        params: {
          type, ...at, button: input.event === "move" && !input.buttons ? "none" : button,
          buttons: modifiers(input.buttons) & 7, clickCount, modifiers: modifiers(input.modifiers),
        },
      }];
    }
    case "wheel": {
      const at = point(input.x, input.y);
      if (!at || ![input.dx, input.dy].every((d) => typeof d === "number" && Math.abs(d) <= 5_000)) return null;
      return [{ method: "Input.dispatchMouseEvent", params: { type: "mouseWheel", ...at, deltaX: input.dx, deltaY: input.dy, modifiers: modifiers(input.modifiers) } }];
    }
    case "text": {
      if (typeof input.text !== "string" || !input.text || input.text.length > MAX_TEXT) return null;
      return [{ method: "Input.insertText", params: { text: input.text } }];
    }
    case "key": {
      if (typeof input.key !== "string" || input.key.length > 20 || (input.event !== "down" && input.event !== "up")) return null;
      const mods = modifiers(input.modifiers);
      const single = [...input.key].length === 1;
      const shortcut = (mods & (2 | 4)) !== 0 && single;
      // A printable key without Ctrl/Meta types its character (IME text
      // arrives separately as committed "text").
      const printable = single && !shortcut && input.key !== " ";
      const alnum = /^[a-z0-9]$/i.test(input.key);
      const keyCode = KEYS[input.key] ?? (shortcut || alnum ? input.key.toUpperCase().charCodeAt(0) : printable ? 0 : undefined);
      if (keyCode === undefined) return null;
      const code = typeof input.code === "string" && input.code.length <= 30 ? input.code : undefined;
      const base = { key: input.key, code, windowsVirtualKeyCode: keyCode, nativeVirtualKeyCode: keyCode, modifiers: mods };
      if (input.event === "up") return [{ method: "Input.dispatchKeyEvent", params: { type: "keyUp", ...base } }];
      const params: Record<string, unknown> = { type: "rawKeyDown", ...base };
      // Enter must carry text to submit forms; the space bar activates buttons.
      if (input.key === "Enter" && !(mods & (2 | 4))) Object.assign(params, { type: "keyDown", text: "\r", unmodifiedText: "\r" });
      if (input.key === " " && !mods) Object.assign(params, { type: "keyDown", text: " ", unmodifiedText: " " });
      if (printable) Object.assign(params, { type: "keyDown", text: input.key, unmodifiedText: input.key });
      const command = shortcut ? COMMANDS[input.key.toLowerCase()] : undefined;
      if (command) params.commands = [mods & 8 && command === "undo" ? "redo" : command];
      return [{ method: "Input.dispatchKeyEvent", params }];
    }
  }
  return null;
}
