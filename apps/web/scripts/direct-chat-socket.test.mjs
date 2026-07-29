import assert from "node:assert/strict";
import test from "node:test";
import {
  DirectChatSocket,
  isDirectChatCommand,
  parseDirectChatServerFrame,
} from "../src/lib/direct-chat-socket.ts";
import { DirectChatTimeline } from "../src/lib/direct-chat-timeline.ts";

class FakeWebSocket {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;
  static instances = [];
  readyState = FakeWebSocket.CONNECTING;
  sent = [];
  onopen;
  onerror;
  onmessage;
  onclose;
  constructor(url) { this.url = url; FakeWebSocket.instances.push(this); }
  send(payload) { this.sent.push(payload); }
  open() { this.readyState = FakeWebSocket.OPEN; this.onopen?.(); }
  receive(value) { this.onmessage?.({ data: JSON.stringify(value) }); }
  close() { if (this.readyState === FakeWebSocket.CLOSED) return; this.readyState = FakeWebSocket.CLOSED; this.onclose?.(); }
}

const originalWebSocket = globalThis.WebSocket;
const originalLocation = globalThis.location;
globalThis.WebSocket = FakeWebSocket;
Object.defineProperty(globalThis, "location", { configurable: true, value: { origin: "http://browser.test" } });
test.after(() => {
  globalThis.WebSocket = originalWebSocket;
  Object.defineProperty(globalThis, "location", { configurable: true, value: originalLocation });
});

const accepted = (key) => ({ type: "command_accepted", idempotency_key: key, command_id: "00000000-0000-4000-8000-000000000001", seq: 1 });
const event = (seq, value) => ({ type: "event", envelope: { seq, event: value } });

test("uses the session-resolved direct-chat route and sends no target or provenance", () => {
  FakeWebSocket.instances = [];
  const socket = new DirectChatSocket();
  socket.connect();
  const wire = FakeWebSocket.instances.at(-1);
  assert.equal(new URL(wire.url).pathname, "/direct-chat/ws");
  assert.equal(new URL(wire.url).search, "");
  wire.open();
  assert.equal(socket.sendCommand({ type: "user_message", text: "hello", attachments: [] }, "key-1"), true);
  const commands = wire.sent.map(JSON.parse).filter((frame) => frame.type === "command");
  assert.deepEqual(commands, [{ type: "command", idempotency_key: "key-1", command: { type: "user_message", text: "hello", attachments: [] } }]);
  assert.equal(JSON.stringify(commands).includes("personality_agent_id"), false);
  assert.equal(JSON.stringify(commands).includes("conversation_id"), false);
  assert.equal(isDirectChatCommand({ type: "user_message", text: "x", attachments: [], actor: "forged" }), false);
  assert.equal(isDirectChatCommand({ type: "approval_decision", request_id: "request-1", decision: { type: "approve_always", rule: { source: "project-policy" } } }), true);
  socket.close();
});

test("retries an uncertain command with its original key and stops after acceptance", () => {
  FakeWebSocket.instances = [];
  const socket = new DirectChatSocket();
  socket.connect();
  const first = FakeWebSocket.instances.at(-1);
  first.open();
  socket.sendCommand({ type: "abort" }, "stable-key");
  first.close();
  socket.connect();
  const second = FakeWebSocket.instances.at(-1);
  second.open();
  const resent = second.sent.map(JSON.parse).filter((frame) => frame.type === "command");
  assert.deepEqual(resent, [{ type: "command", idempotency_key: "stable-key", command: { type: "abort" } }]);
  second.receive(accepted("stable-key"));
  assert.deepEqual(socket.pendingIdempotencyKeys(), []);
  second.close();
  socket.connect();
  const third = FakeWebSocket.instances.at(-1);
  third.open();
  assert.equal(third.sent.map(JSON.parse).filter((frame) => frame.type === "command").length, 0);
  socket.close();
});

test("a terminal idempotency conflict clears its pending key without reconnect resend", () => {
  FakeWebSocket.instances = [];
  const socket = new DirectChatSocket();
  socket.connect();
  const first = FakeWebSocket.instances.at(-1);
  first.open();
  socket.sendCommand({ type: "abort" }, "conflicting-key");
  first.receive({ type: "command_rejected", idempotency_key: "conflicting-key", reject_reason: "idempotency_conflict" });
  assert.deepEqual(socket.pendingIdempotencyKeys(), []);
  first.close();
  socket.connect();
  const second = FakeWebSocket.instances.at(-1);
  second.open();
  assert.equal(second.sent.map(JSON.parse).filter((frame) => frame.type === "command").length, 0);
  socket.close();
});

test("tracks browser connection separately from authoritative agent readiness", () => {
  FakeWebSocket.instances = [];
  const socket = new DirectChatSocket();
  const connections = [];
  const readiness = [];
  socket.onConnection((state) => connections.push(state));
  socket.onReady((state) => readiness.push(state));
  socket.connect();
  const wire = FakeWebSocket.instances.at(-1);
  wire.open();
  wire.receive({ type: "direct_chat_status", status: "unavailable" });
  wire.receive({ type: "direct_chat_status", status: "ready" });
  assert.deepEqual(connections, ["connecting", "connected"]);
  assert.deepEqual(readiness, ["unknown", "not_ready", "ready"]);
  socket.close();
});

test("rejects legacy target-bearing and malformed server frames", () => {
  assert.equal(parseDirectChatServerFrame(event(1, { type: "agent_start" }), 0)?.type, "event");
  assert.equal(parseDirectChatServerFrame({ type: "event", envelope: { conversation_id: "legacy", seq: 1, event: { type: "agent_start" } } }, 0), undefined);
  assert.equal(parseDirectChatServerFrame({ type: "command_accepted", idempotency_key: "k" }, 0), undefined);
  for (const command_id of ["command-1", "00000000-0000-4000-8000-00000000000", "00000000-0000-4000-8000-00000000000G", "00000000-0000-4000-8000-000000000001 ", "0000000a-0000-4000-8000-000000000001".toUpperCase()]) {
    assert.equal(parseDirectChatServerFrame({ type: "command_accepted", idempotency_key: "k", command_id, seq: 1 }, 0), undefined);
  }
  assert.equal(parseDirectChatServerFrame(accepted("k"), 0)?.type, "command_accepted");
  assert.equal(parseDirectChatServerFrame({ type: "direct_chat_status", ready: true }, 0), undefined);
});

test("reconstructs and deduplicates durable messages and tool state after reload/replay", () => {
  const replay = [
    event(1, { type: "message_start", message_id: "user-1", message: { role: "user", content: [{ type: "text", text: "persisted user" }] } }),
    event(2, { type: "tool_execution_start", tool_call_id: "tool-1", tool_name: "read_file" }),
    event(3, { type: "tool_execution_end", tool_call_id: "tool-1" }),
    event(4, { type: "message_end", message_id: "assistant-1", message: { role: "assistant", content: [{ type: "text", text: "persisted assistant" }] } }),
  ];
  const timeline = new DirectChatTimeline();
  for (const frame of replay) timeline.apply(frame);
  for (const frame of replay) timeline.apply(frame);
  assert.deepEqual(timeline.items().map((item) => [item.kind, item.text]), [
    ["user", "persisted user"], ["tool", "Tool finished: tool-1"], ["assistant", "persisted assistant"],
  ]);
  const reloaded = new DirectChatTimeline();
  for (const frame of replay) reloaded.apply(frame);
  assert.deepEqual(reloaded.items(), timeline.items());
});

test("durable completion supersedes a volatile preview and drops late volatile replay", () => {
  const timeline = new DirectChatTimeline();
  timeline.apply({ type: "event", envelope: { event: { type: "message_update", message_id: "assistant-2", event: { type: "text_delta", delta: "draft" } } } });
  timeline.apply(event(1, { type: "message_end", message_id: "assistant-2", message: { role: "assistant", content: [{ type: "text", text: "durable" }] } }));
  timeline.apply({ type: "event", envelope: { event: { type: "message_update", message_id: "assistant-2", event: { type: "text_delta", delta: "late" } } } });
  assert.deepEqual(timeline.items().map((item) => [item.id, item.text]), [["message-assistant-2", "durable"]]);
});
