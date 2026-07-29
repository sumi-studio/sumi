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
const timestamp = "2026-07-28T00:00:00Z";
const usage = { input: 0, output: 0, cache_read: 0, cache_write: 0, reasoning: 0, total_tokens: 0 };
const assistantMessage = (content = []) => ({
  role: "assistant",
  content,
  model: "fixture",
  provider: "fixture",
  origin: { provider_instance_id: "fixture", protocol: "open_ai_responses", model: "fixture" },
  usage,
  stop_reason: "stop",
  error_message: null,
  provider_code: null,
  interrupted: false,
  timestamp,
});
const approvalRequest = (overrides = {}) => ({
  id: "request-1",
  tool_call_id: "call-1",
  tool_name: "read_file",
  action: { reviewable: "read fixture" },
  args_summary: "read fixture",
  ...overrides,
});

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
  assert.equal(isDirectChatCommand({ type: "approval_decision", request_id: "request-1", decision: { type: "approve_always", rule: { scope: [{ personalityAgentId: "literal-data", paid: true }] } } }), true);
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
  assert.equal(parseDirectChatServerFrame(event(1, { type: "agent_start", personality_agent_id: "internal" }), 0), undefined);
  assert.equal(parseDirectChatServerFrame(event(1, { type: "message_end", message_id: "message-1", message: { role: "assistant", content: [], tenant_id: "internal" } }), 0), undefined);
  assert.equal(parseDirectChatServerFrame(event(1, { type: "approval_requested", request: { id: "request-1", personality_agent_id: "internal", action: {} } }), 0), undefined);
  assert.equal(parseDirectChatServerFrame({ type: "command_accepted", idempotency_key: "k" }, 0), undefined);
  for (const command_id of ["command-1", "00000000-0000-4000-8000-00000000000", "00000000-0000-4000-8000-00000000000G", "00000000-0000-4000-8000-000000000001 ", "0000000a-0000-4000-8000-000000000001".toUpperCase()]) {
    assert.equal(parseDirectChatServerFrame({ type: "command_accepted", idempotency_key: "k", command_id, seq: 1 }, 0), undefined);
  }
  assert.equal(parseDirectChatServerFrame(accepted("k"), 0)?.type, "command_accepted");
  assert.equal(parseDirectChatServerFrame({ type: "direct_chat_status", ready: true }, 0), undefined);
});

test("rejects identity aliases and provenance only when they are structural fields", () => {
  const structuralLeaks = [
    { type: "agent_start", PersonalityAgentId: "internal" },
    {
      type: "message_end",
      message_id: "00000000-0000-4000-8000-000000000001",
      message: { ...assistantMessage(), tenantId: "internal" },
    },
    {
      type: "message_end",
      message_id: "00000000-0000-4000-8000-000000000001",
      message: assistantMessage([{ type: "text", text: "done", wire_item_index: 0, provenance: {} }]),
    },
    {
      type: "approval_requested",
      request: approvalRequest({ PAID: "internal" }),
    },
    {
      type: "approval_requested",
      request: approvalRequest({ action: { reviewable: "read", workspaceId: "internal" } }),
    },
    {
      type: "approval_requested",
      request: approvalRequest({
        audit: {
          outcome: "allow",
          risk: "low",
          authorization: "low",
          rationale: "ok",
          org_id: "internal",
        },
      }),
    },
  ];
  for (const leak of structuralLeaks) {
    assert.equal(parseDirectChatServerFrame(event(1, leak), 0), undefined);
  }

  const volatileLeaks = [
    {
      type: "message_update",
      message_id: "00000000-0000-4000-8000-000000000001",
      event: {
        type: "text_delta",
        content_index: 0,
        delta: "draft",
        organization_id: "internal",
      },
    },
    { type: "error", message: "failed", provenance: {} },
  ];
  for (const leak of volatileLeaks) {
    assert.equal(parseDirectChatServerFrame({ type: "event", envelope: { event: leak } }, 0), undefined);
  }
});

test("preserves identity-like keys and paid data inside explicit AnyJSON fields", () => {
  const opaque = {
    personality_agent_id: "literal-data",
    tenant_id: "literal-data",
    provenance: { source: "literal-data" },
    paid: true,
  };
  assert.equal(parseDirectChatServerFrame(event(1, {
    type: "tool_execution_start",
    tool_call_id: "tool-1",
    tool_name: "read_file",
    args: opaque,
  }), 0)?.type, "event");
  assert.equal(parseDirectChatServerFrame(event(1, {
    type: "tool_execution_end",
    tool_call_id: "tool-1",
    result: opaque,
    is_error: false,
  }), 0)?.type, "event");
  assert.equal(parseDirectChatServerFrame({
    type: "event",
    envelope: {
      event: { type: "tool_execution_update", tool_call_id: "tool-1", partial: opaque },
    },
  }, 0)?.type, "event");
  assert.equal(parseDirectChatServerFrame(event(1, {
    type: "approval_requested",
    request: approvalRequest({ action: { reviewable: opaque }, args_summary: opaque }),
  }), 0)?.type, "event");
  assert.equal(parseDirectChatServerFrame(event(1, {
    type: "message_end",
    message_id: "00000000-0000-4000-8000-000000000001",
    message: {
      role: "tool_result",
      tool_call_id: "call-1",
      tool_name: "read_file",
      content: [{ type: "text", text: JSON.stringify(opaque) }],
      details: opaque,
      is_error: false,
      timestamp,
    },
  }), 0)?.type, "event");
  assert.equal(parseDirectChatServerFrame(event(1, {
    type: "message_end",
    message_id: "00000000-0000-4000-8000-000000000001",
    message: {
      role: "user",
      content: [{ type: "text", text: JSON.stringify(opaque) }],
      timestamp,
    },
  }), 0)?.type, "event");
  assert.equal(parseDirectChatServerFrame({
    type: "event",
    envelope: {
      event: {
        type: "message_update",
        message_id: "00000000-0000-4000-8000-000000000001",
        event: {
          type: "tool_call_end",
          content_index: 0,
          tool_call: { id: "call-1", name: "read_file", arguments: opaque },
        },
      },
    },
  }, 0)?.type, "event");
  assert.equal(parseDirectChatServerFrame(event(1, {
    type: "approval_resolved",
    request_id: "request-1",
    resolution: { decision: { type: "approve_always", rule: opaque } },
  }), 0)?.type, "event");
});

test("accepts every exact event shape emitted by the browser E2E fixture", () => {
  const durable = [
    { type: "agent_start" },
    { type: "turn_start" },
    { type: "tool_execution_start", tool_call_id: "call-1", tool_name: "read_file", args: {} },
    { type: "tool_execution_end", tool_call_id: "call-1", result: "ok", is_error: false },
    { type: "steered", mode: "hard" },
    { type: "approval_requested", request: approvalRequest() },
    {
      type: "message_start",
      message_id: "00000000-0000-4000-8000-000000000002",
      message: assistantMessage(),
    },
    {
      type: "message_end",
      message_id: "00000000-0000-4000-8000-000000000002",
      message: {
        ...assistantMessage([{ type: "text", text: "Terminal replay", wire_item_index: 0 }]),
        stop_reason: "aborted",
        interrupted: true,
      },
    },
    { type: "turn_end", message: null, tool_results: [] },
    { type: "agent_end" },
  ];
  for (const fixtureEvent of durable) {
    assert.equal(parseDirectChatServerFrame(event(1, fixtureEvent), 0)?.type, "event");
  }
  for (const delta of ["streamed assistant", "abortable stream"]) {
    assert.equal(parseDirectChatServerFrame({
      type: "event",
      envelope: {
        event: {
          type: "message_update",
          message_id: "00000000-0000-4000-8000-000000000001",
          event: { type: "text_delta", content_index: 0, delta },
        },
      },
    }, 0)?.type, "event");
  }
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
