import assert from "node:assert/strict";
import test from "node:test";
import { takesControl, toCdp } from "../src/input.ts";

test("pointer input maps to CDP inside the shared viewport only", () => {
  assert.deepEqual(toCdp({ kind: "mouse", event: "down", x: 100.26, y: 50, button: "left", buttons: 1 }), [
    {
      method: "Input.dispatchMouseEvent",
      params: { type: "mousePressed", x: 100.3, y: 50, button: "left", buttons: 1, clickCount: 1, modifiers: 0 },
    },
  ]);
  assert.equal(toCdp({ kind: "mouse", event: "down", x: 1281, y: 10 }), null);
  assert.equal(toCdp({ kind: "mouse", event: "down", x: Number.NaN, y: 10 }), null);
  assert.equal(toCdp({ kind: "mouse", event: "drag", x: 1, y: 1 }), null);
  const move = toCdp({ kind: "mouse", event: "move", x: 1, y: 1 });
  assert.equal((move?.[0]?.params as { button: string } | undefined)?.button, "none");
  assert.deepEqual(toCdp({ kind: "wheel", x: 10, y: 10, dx: 0, dy: 240 })?.[0]?.params, {
    type: "mouseWheel", x: 10, y: 10, deltaX: 0, deltaY: 240, modifiers: 0,
  });
  assert.equal(toCdp({ kind: "wheel", x: 10, y: 10, dx: 0, dy: 1e9 }), null);
});

test("only pointer movement leaves control with the secretary", () => {
  assert.equal(takesControl({ kind: "mouse", event: "move", x: 1, y: 1 }), false);
  assert.equal(takesControl({ kind: "mouse", event: "down", x: 1, y: 1 }), true);
  assert.equal(takesControl({ kind: "wheel", x: 1, y: 1, dx: 0, dy: 1 }), true);
  assert.equal(takesControl({ kind: "key", event: "down", key: "a" }), true);
  assert.equal(takesControl({ kind: "text", text: "あ" }), true);
});

test("committed IME text is inserted as text, bounded", () => {
  assert.deepEqual(toCdp({ kind: "text", text: "東京都の予定を確認" }), [
    { method: "Input.insertText", params: { text: "東京都の予定を確認" } },
  ]);
  assert.equal(toCdp({ kind: "text", text: "" }), null);
  assert.equal(toCdp({ kind: "text", text: "x".repeat(2001) }), null);
});

test("keys: printable characters type, Enter submits, shortcuts become editing commands", () => {
  const a = toCdp({ kind: "key", event: "down", key: "a", code: "KeyA" })?.[0]?.params;
  assert.equal(a?.type, "keyDown");
  assert.equal(a?.text, "a");
  const enter = toCdp({ kind: "key", event: "down", key: "Enter" })?.[0]?.params;
  assert.equal(enter?.text, "\r");
  const selectAll = toCdp({ kind: "key", event: "down", key: "a", modifiers: 2 })?.[0]?.params;
  assert.equal(selectAll?.type, "rawKeyDown");
  assert.equal(selectAll?.text, undefined);
  assert.deepEqual(selectAll?.commands, ["selectAll"]);
  assert.deepEqual(toCdp({ kind: "key", event: "down", key: "z", modifiers: 4 | 8 })?.[0]?.params.commands, ["redo"]);
  assert.equal(toCdp({ kind: "key", event: "down", key: "Backspace" })?.[0]?.params.windowsVirtualKeyCode, 8);
  assert.equal(toCdp({ kind: "key", event: "up", key: "a" })?.[0]?.params.type, "keyUp");
  assert.equal(toCdp({ kind: "key", event: "down", key: "Unidentified-long-key-name" }), null);
  assert.equal(toCdp({ kind: "key", event: "down", key: "AudioVolumeUp" }), null);
});
