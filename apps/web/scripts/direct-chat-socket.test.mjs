import assert from "node:assert/strict";
import test from "node:test";
import { createConversationStore } from "../src/agent/store.ts";
import {
  DIRECT_CHAT_RUNTIME_UNAVAILABLE_CLOSE_CODE,
  DirectChatSocket,
  isDirectChatCommand,
  parseDirectChatServerFrame,
  resolveDirectChatURL,
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
  constructor(url) {
    this.url = url;
    FakeWebSocket.instances.push(this);
  }
  send(payload) {
    this.sent.push(payload);
  }
  open() {
    this.readyState = FakeWebSocket.OPEN;
    this.onopen?.();
  }
  receive(value) {
    this.onmessage?.({ data: JSON.stringify(value) });
  }
  close(code = 1005, reason = "") {
    if (this.readyState === FakeWebSocket.CLOSED) return;
    this.readyState = FakeWebSocket.CLOSED;
    this.onclose?.({ code, reason });
  }
  /** A close the browser synthesizes: no status, no reason, no cause. */
  drop() {
    this.close(1006, "");
  }
}

const originalWebSocket = globalThis.WebSocket;
const originalLocation = globalThis.location;
const installationId = "0198f0f4-9b72-7000-8000-000000000051";
const binding = { installationId, authorityEpoch: "1" };
globalThis.WebSocket = FakeWebSocket;
Object.defineProperty(globalThis, "location", {
  configurable: true,
  value: { origin: "http://browser.test" },
});
test.after(() => {
  globalThis.WebSocket = originalWebSocket;
  Object.defineProperty(globalThis, "location", {
    configurable: true,
    value: originalLocation,
  });
});

const accepted = (key, disposition) => ({
  type: "command_accepted",
  idempotency_key: key,
  command_id: "00000000-0000-4000-8000-000000000001",
  seq: 1,
  ...(disposition ? { disposition } : {}),
});
const event = (seq, value) => ({
  type: "event",
  envelope: { seq, event: value },
});
const timestamp = "2026-07-28T00:00:00Z";
const usage = {
  input: 0,
  output: 0,
  cache_read: 0,
  cache_write: 0,
  reasoning: 0,
  total_tokens: 0,
};
const assistantMessage = (content = []) => ({
  role: "assistant",
  content,
  model: "fixture",
  provider: "fixture",
  origin: {
    provider_instance_id: "fixture",
    protocol: "open_ai_responses",
    model: "fixture",
  },
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
  socket.bindInstallation(binding);
  socket.connect();
  const wire = FakeWebSocket.instances.at(-1);
  assert.equal(new URL(wire.url).pathname, "/direct-chat/ws");
  assert.equal(
    new URL(wire.url).search,
    `?installation_id=${installationId}&authority_epoch=1`,
  );
  assert.deepEqual(wire.sent, []);
  assert.equal(
    socket.sendCommand(
      { type: "user_message", text: "hello", attachments: [] },
      "key-1",
    ),
    true,
  );
  assert.deepEqual(wire.sent, []);
  wire.open();
  assert.deepEqual(wire.sent.map(JSON.parse), [
    { type: "hello", last_event_seq: 0 },
  ]);
  wire.receive({ type: "direct_chat_status", status: "unavailable" });
  assert.deepEqual(wire.sent.map(JSON.parse), [
    { type: "hello", last_event_seq: 0 },
  ]);
  wire.receive({ type: "direct_chat_status", status: "ready" });
  const commands = wire.sent
    .map(JSON.parse)
    .filter((frame) => frame.type === "command");
  assert.deepEqual(commands, [
    {
      type: "command",
      idempotency_key: "key-1",
      command: { type: "user_message", text: "hello", attachments: [] },
    },
  ]);
  assert.equal(
    JSON.stringify(commands).includes("personality_agent_id"),
    false,
  );
  assert.equal(JSON.stringify(commands).includes("conversation_id"), false);
  assert.equal(
    isDirectChatCommand({
      type: "user_message",
      text: "x",
      attachments: [],
      actor: "forged",
    }),
    false,
  );
  assert.equal(
    isDirectChatCommand({
      type: "approval_decision",
      request_id: "request-1",
      decision: { type: "approve_once" },
    }),
    true,
  );
  assert.equal(
    isDirectChatCommand({
      type: "approval_decision",
      request_id: "request-1",
      decision: { type: "deny_once" },
    }),
    true,
  );
  assert.equal(
    isDirectChatCommand({
      type: "approval_decision",
      request_id: "request-1",
      decision: { type: "approve_always", rule: { source: "project-policy" } },
    }),
    false,
  );
  assert.equal(
    isDirectChatCommand({
      type: "approval_decision",
      request_id: "request-1",
      decision: { type: "deny" },
    }),
    false,
  );
  socket.close();
});

test("rejects a path-prefixed API base instead of silently discarding it", () => {
  assert.throws(
    () =>
      resolveDirectChatURL({
        apiBaseURL: "http://browser.test/api",
        installationId,
        authorityEpoch: "1",
        pageOrigin: "http://browser.test",
      }),
    /must contain only an origin/,
  );
});

test("retries an uncertain command with its original key and stops after acceptance", () => {
  FakeWebSocket.instances = [];
  const socket = new DirectChatSocket();
  socket.bindInstallation(binding);
  socket.connect();
  const first = FakeWebSocket.instances.at(-1);
  first.open();
  first.receive({ type: "direct_chat_status", status: "ready" });
  socket.sendCommand({ type: "abort" }, "stable-key");
  first.close();
  socket.bindInstallation(binding);
  socket.connect();
  const second = FakeWebSocket.instances.at(-1);
  second.open();
  assert.deepEqual(second.sent.map(JSON.parse), [
    { type: "hello", last_event_seq: 0 },
  ]);
  second.receive({ type: "direct_chat_status", status: "unavailable" });
  assert.equal(
    second.sent.map(JSON.parse).filter((frame) => frame.type === "command")
      .length,
    0,
  );
  second.receive({ type: "direct_chat_status", status: "ready" });
  const resent = second.sent
    .map(JSON.parse)
    .filter((frame) => frame.type === "command");
  assert.deepEqual(resent, [
    {
      type: "command",
      idempotency_key: "stable-key",
      command: { type: "abort" },
    },
  ]);
  second.receive(accepted("stable-key"));
  assert.deepEqual(socket.pendingIdempotencyKeys(), []);
  second.close();
  socket.bindInstallation(binding);
  socket.connect();
  const third = FakeWebSocket.instances.at(-1);
  third.open();
  assert.equal(
    third.sent.map(JSON.parse).filter((frame) => frame.type === "command")
      .length,
    0,
  );
  socket.close();
});

test("a terminal idempotency conflict clears its pending key without reconnect resend", () => {
  FakeWebSocket.instances = [];
  const socket = new DirectChatSocket();
  socket.bindInstallation(binding);
  socket.connect();
  const first = FakeWebSocket.instances.at(-1);
  first.open();
  first.receive({ type: "direct_chat_status", status: "ready" });
  socket.sendCommand({ type: "abort" }, "conflicting-key");
  first.receive({
    type: "command_rejected",
    idempotency_key: "conflicting-key",
    reject_reason: "idempotency_conflict",
  });
  assert.deepEqual(socket.pendingIdempotencyKeys(), []);
  first.close();
  socket.bindInstallation(binding);
  socket.connect();
  const second = FakeWebSocket.instances.at(-1);
  second.open();
  assert.equal(
    second.sent.map(JSON.parse).filter((frame) => frame.type === "command")
      .length,
    0,
  );
  socket.close();
});

test("unavailable status retains pending commands without sending until ready", () => {
  FakeWebSocket.instances = [];
  const socket = new DirectChatSocket();
  socket.bindInstallation(binding);
  socket.connect();
  const wire = FakeWebSocket.instances.at(-1);
  wire.open();
  socket.sendCommand({ type: "abort" }, "unavailable-key");
  assert.deepEqual(socket.pendingIdempotencyKeys(), ["unavailable-key"]);
  assert.equal(
    wire.sent.map(JSON.parse).filter((frame) => frame.type === "command")
      .length,
    0,
  );
  wire.receive({ type: "direct_chat_status", status: "unavailable" });
  assert.deepEqual(socket.pendingIdempotencyKeys(), ["unavailable-key"]);
  assert.equal(
    wire.sent.map(JSON.parse).filter((frame) => frame.type === "command")
      .length,
    0,
  );
  wire.receive({ type: "direct_chat_status", status: "ready" });
  assert.deepEqual(
    wire.sent.map(JSON.parse).filter((frame) => frame.type === "command"),
    [
      {
        type: "command",
        idempotency_key: "unavailable-key",
        command: { type: "abort" },
      },
    ],
  );
  socket.close();
});

test("authority reset drops replay cursor and pending commands before reconnect", () => {
  FakeWebSocket.instances = [];
  const socket = new DirectChatSocket();
  socket.bindInstallation(binding);
  socket.connect();
  const first = FakeWebSocket.instances.at(-1);
  first.open();
  first.receive(event(1, { type: "agent_start" }));
  socket.sendCommand({ type: "abort" }, "old-authority-key");
  assert.deepEqual(socket.pendingIdempotencyKeys(), ["old-authority-key"]);

  socket.resetAuthority();

  assert.equal(first.readyState, FakeWebSocket.CLOSED);
  assert.deepEqual(socket.pendingIdempotencyKeys(), []);
  socket.bindInstallation(binding);
  socket.connect();
  const second = FakeWebSocket.instances.at(-1);
  second.open();
  assert.deepEqual(second.sent.map(JSON.parse), [
    { type: "hello", last_event_seq: 0 },
  ]);
  second.receive({ type: "direct_chat_status", status: "ready" });
  assert.equal(
    second.sent.map(JSON.parse).filter((frame) => frame.type === "command")
      .length,
    0,
  );
  socket.close();
});

test("installation replacement preserves the admitted cursor but fences pending retry", () => {
  FakeWebSocket.instances = [];
  const socket = new DirectChatSocket();
  socket.bindInstallation(binding);
  socket.connect();
  const first = FakeWebSocket.instances.at(-1);
  first.open();
  first.receive(event(1, { type: "agent_start" }));
  first.receive({ type: "direct_chat_status", status: "ready" });
  socket.sendCommand({ type: "abort" }, "old-installation-key");
  assert.deepEqual(socket.pendingIdempotencyKeys(), ["old-installation-key"]);

  const replacement = "0198f0f4-9b72-7000-8000-000000000052";
  socket.bindInstallation({ installationId: replacement, authorityEpoch: "1" });
  assert.equal(first.readyState, FakeWebSocket.CLOSED);
  assert.deepEqual(socket.pendingIdempotencyKeys(), []);
  socket.connect();
  const second = FakeWebSocket.instances.at(-1);
  assert.equal(
    new URL(second.url).search,
    `?installation_id=${replacement}&authority_epoch=1`,
  );
  second.open();
  assert.deepEqual(second.sent.map(JSON.parse), [
    { type: "hello", last_event_seq: 1 },
  ]);
  second.receive({ type: "direct_chat_status", status: "ready" });
  assert.equal(
    second.sent.map(JSON.parse).filter((frame) => frame.type === "command")
      .length,
    0,
  );
  socket.close();
});

test("same-installation suspend starts a fresh transport epoch without losing the cursor", () => {
  FakeWebSocket.instances = [];
  const socket = new DirectChatSocket();
  socket.bindInstallation(binding);
  socket.connect();
  const first = FakeWebSocket.instances.at(-1);
  first.open();
  first.receive(event(1, { type: "agent_start" }));
  first.receive({ type: "direct_chat_status", status: "ready" });
  socket.sendCommand({ type: "abort" }, "pre-disable-key");
  assert.deepEqual(socket.pendingIdempotencyKeys(), ["pre-disable-key"]);

  socket.suspendInstallation();

  assert.equal(first.readyState, FakeWebSocket.CLOSED);
  assert.deepEqual(socket.pendingIdempotencyKeys(), []);
  socket.bindInstallation({ installationId, authorityEpoch: "2" });
  socket.connect();
  const second = FakeWebSocket.instances.at(-1);
  second.open();
  assert.deepEqual(second.sent.map(JSON.parse), [
    { type: "hello", last_event_seq: 1 },
  ]);
  second.receive({ type: "direct_chat_status", status: "ready" });
  assert.equal(
    second.sent.map(JSON.parse).filter((frame) => frame.type === "command")
      .length,
    0,
  );
  socket.close();
});

test("same installation ID with a newer durable epoch fences stale retry without an explicit local disable", () => {
  FakeWebSocket.instances = [];
  const socket = new DirectChatSocket();
  socket.bindInstallation(binding);
  socket.connect();
  const stale = FakeWebSocket.instances.at(-1);
  stale.open();
  stale.receive(event(1, { type: "agent_start" }));
  stale.receive({ type: "direct_chat_status", status: "ready" });
  socket.sendCommand({ type: "abort" }, "other-tab-disable-key");
  assert.deepEqual(socket.pendingIdempotencyKeys(), ["other-tab-disable-key"]);

  socket.bindInstallation({ installationId, authorityEpoch: "2" });

  assert.equal(stale.readyState, FakeWebSocket.CLOSED);
  assert.deepEqual(socket.pendingIdempotencyKeys(), []);
  socket.connect();
  const current = FakeWebSocket.instances.at(-1);
  assert.equal(
    new URL(current.url).search,
    `?installation_id=${installationId}&authority_epoch=2`,
  );
  current.open();
  assert.deepEqual(current.sent.map(JSON.parse), [
    { type: "hello", last_event_seq: 1 },
  ]);
  current.receive({ type: "direct_chat_status", status: "ready" });
  assert.equal(
    current.sent.map(JSON.parse).filter((frame) => frame.type === "command")
      .length,
    0,
  );
  socket.close();
});

test("tracks browser connection separately from authoritative agent readiness", () => {
  FakeWebSocket.instances = [];
  const socket = new DirectChatSocket();
  socket.bindInstallation(binding);
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
  assert.equal(
    parseDirectChatServerFrame(event(1, { type: "agent_start" }), 0)?.type,
    "event",
  );
  assert.equal(
    parseDirectChatServerFrame(
      {
        type: "event",
        envelope: {
          conversation_id: "legacy",
          seq: 1,
          event: { type: "agent_start" },
        },
      },
      0,
    ),
    undefined,
  );
  assert.equal(
    parseDirectChatServerFrame(
      event(1, { type: "agent_start", personality_agent_id: "internal" }),
      0,
    ),
    undefined,
  );
  assert.equal(
    parseDirectChatServerFrame(
      event(1, {
        type: "message_end",
        message_id: "message-1",
        message: { role: "assistant", content: [], tenant_id: "internal" },
      }),
      0,
    ),
    undefined,
  );
  assert.equal(
    parseDirectChatServerFrame(
      event(1, {
        type: "approval_requested",
        request: {
          id: "request-1",
          personality_agent_id: "internal",
          action: {},
        },
      }),
      0,
    ),
    undefined,
  );
  assert.equal(
    parseDirectChatServerFrame(
      { type: "command_accepted", idempotency_key: "k" },
      0,
    ),
    undefined,
  );
  for (const command_id of [
    "command-1",
    "00000000-0000-4000-8000-00000000000",
    "00000000-0000-4000-8000-00000000000G",
    "00000000-0000-4000-8000-000000000001 ",
    "0000000a-0000-4000-8000-000000000001".toUpperCase(),
  ]) {
    assert.equal(
      parseDirectChatServerFrame(
        { type: "command_accepted", idempotency_key: "k", command_id, seq: 1 },
        0,
      ),
      undefined,
    );
  }
  assert.equal(
    parseDirectChatServerFrame(accepted("k"), 0)?.type,
    "command_accepted",
  );
  assert.equal(
    parseDirectChatServerFrame({ type: "direct_chat_status", ready: true }, 0),
    undefined,
  );
  assert.equal(
    parseDirectChatServerFrame({ ...accepted("x".repeat(1025)) }, 0),
    undefined,
  );
  assert.equal(
    parseDirectChatServerFrame(
      {
        type: "command_rejected",
        idempotency_key: "x".repeat(1025),
        reject_reason: "unavailable",
      },
      0,
    ),
    undefined,
  );
});

test("accepts only exact durable command disposition shapes", () => {
  const command_id = "00000000-0000-4000-8000-000000000001";
  for (const disposition of [
    {
      type: "command_disposition",
      command_id,
      command_seq: 7,
      status: "applied",
    },
    {
      type: "command_disposition",
      command_id,
      command_seq: 7,
      status: "superseded",
    },
    {
      type: "command_disposition",
      command_id,
      command_seq: 7,
      status: "rejected",
      reject_reason: "not_allowed",
    },
  ]) {
    assert.equal(
      parseDirectChatServerFrame(event(1, disposition), 0)?.type,
      "event",
    );
  }

  for (const disposition of [
    {
      type: "command_disposition",
      command_id,
      command_seq: 7,
      status: "rejected",
    },
    {
      type: "command_disposition",
      command_id,
      command_seq: 7,
      status: "applied",
      reject_reason: "not_allowed",
    },
    {
      type: "command_disposition",
      command_id,
      command_seq: 7,
      status: "rejected",
      reject_reason: "idempotency_conflict",
    },
    {
      type: "command_disposition",
      command_id,
      command_seq: -1,
      status: "applied",
    },
    {
      type: "command_disposition",
      command_id: "not-a-uuid",
      command_seq: 7,
      status: "applied",
    },
    {
      type: "command_disposition",
      command_id,
      command_seq: 7,
      status: "received",
    },
    {
      type: "command_disposition",
      command_id,
      command_seq: 7,
      status: "applied",
      extra: true,
    },
  ]) {
    assert.equal(
      parseDirectChatServerFrame(event(1, disposition), 0),
      undefined,
    );
  }
  assert.equal(
    parseDirectChatServerFrame(
      {
        type: "event",
        envelope: {
          event: {
            type: "command_disposition",
            command_id,
            command_seq: 7,
            status: "applied",
          },
        },
      },
      0,
    ),
    undefined,
  );
  assert.equal(
    parseDirectChatServerFrame(
      event(2, {
        type: "command_disposition",
        command_id,
        command_seq: 7,
        status: "applied",
      }),
      0,
    ),
    undefined,
  );
});

test("acceptance permits only an exactly correlated terminal disposition", () => {
  const command_id = "00000000-0000-4000-8000-000000000001";
  for (const disposition of [
    {
      type: "command_disposition",
      command_id,
      command_seq: 1,
      status: "applied",
    },
    {
      type: "command_disposition",
      command_id,
      command_seq: 1,
      status: "superseded",
    },
    {
      type: "command_disposition",
      command_id,
      command_seq: 1,
      status: "rejected",
      reject_reason: "not_allowed",
    },
  ]) {
    assert.equal(
      parseDirectChatServerFrame(accepted("key", disposition), 0)?.type,
      "command_accepted",
    );
  }

  for (const disposition of [
    {
      type: "command_disposition",
      command_id,
      command_seq: 2,
      status: "applied",
    },
    {
      type: "command_disposition",
      command_id: "00000000-0000-4000-8000-000000000002",
      command_seq: 1,
      status: "applied",
    },
    {
      type: "command_disposition",
      command_id,
      command_seq: 1,
      status: "received",
    },
    {
      type: "command_disposition",
      command_id,
      command_seq: 1,
      status: "rejected",
    },
    {
      type: "command_disposition",
      command_id,
      command_seq: 1,
      status: "applied",
      reject_reason: "not_allowed",
    },
    {
      type: "command_disposition",
      command_id,
      command_seq: 1,
      status: "applied",
      extra: true,
    },
  ]) {
    assert.equal(
      parseDirectChatServerFrame(accepted("key", disposition), 0),
      undefined,
    );
  }
  assert.equal(
    parseDirectChatServerFrame({ ...accepted("key"), disposition: null }, 0),
    undefined,
  );
});

test("durable disposition advances the socket replay cursor", () => {
  FakeWebSocket.instances = [];
  const socket = new DirectChatSocket();
  socket.bindInstallation(binding);
  socket.connect();
  const first = FakeWebSocket.instances.at(-1);
  first.open();
  first.receive(
    event(1, {
      type: "command_disposition",
      command_id: "00000000-0000-4000-8000-000000000001",
      command_seq: 1,
      status: "applied",
    }),
  );
  first.close();
  socket.bindInstallation(binding);
  socket.connect();
  const second = FakeWebSocket.instances.at(-1);
  second.open();
  assert.deepEqual(second.sent.map(JSON.parse), [
    { type: "hello", last_event_seq: 1 },
  ]);
  socket.close();
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
      message: assistantMessage([
        { type: "text", text: "done", wire_item_index: 0, provenance: {} },
      ]),
    },
    {
      type: "approval_requested",
      request: approvalRequest({ PAID: "internal" }),
    },
    {
      type: "approval_requested",
      request: approvalRequest({
        action: { reviewable: "read", workspaceId: "internal" },
      }),
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
    assert.equal(
      parseDirectChatServerFrame(
        { type: "event", envelope: { event: leak } },
        0,
      ),
      undefined,
    );
  }
});

test("preserves identity-like keys and paid data inside explicit AnyJSON fields", () => {
  const opaque = {
    personality_agent_id: "literal-data",
    tenant_id: "literal-data",
    provenance: { source: "literal-data" },
    paid: true,
  };
  assert.equal(
    parseDirectChatServerFrame(
      event(1, {
        type: "tool_execution_start",
        tool_call_id: "tool-1",
        tool_name: "read_file",
        args: opaque,
      }),
      0,
    )?.type,
    "event",
  );
  assert.equal(
    parseDirectChatServerFrame(
      event(1, {
        type: "tool_execution_end",
        tool_call_id: "tool-1",
        result: opaque,
        is_error: false,
      }),
      0,
    )?.type,
    "event",
  );
  assert.equal(
    parseDirectChatServerFrame(
      {
        type: "event",
        envelope: {
          event: {
            type: "tool_execution_update",
            tool_call_id: "tool-1",
            partial: opaque,
          },
        },
      },
      0,
    )?.type,
    "event",
  );
  assert.equal(
    parseDirectChatServerFrame(
      event(1, {
        type: "approval_requested",
        request: approvalRequest({
          action: { reviewable: opaque },
          args_summary: opaque,
        }),
      }),
      0,
    )?.type,
    "event",
  );
  assert.equal(
    parseDirectChatServerFrame(
      event(1, {
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
      }),
      0,
    )?.type,
    "event",
  );
  assert.equal(
    parseDirectChatServerFrame(
      event(1, {
        type: "message_end",
        message_id: "00000000-0000-4000-8000-000000000001",
        message: {
          role: "user",
          content: [{ type: "text", text: JSON.stringify(opaque) }],
          timestamp,
        },
      }),
      0,
    )?.type,
    "event",
  );
  assert.equal(
    parseDirectChatServerFrame(
      {
        type: "event",
        envelope: {
          event: {
            type: "message_update",
            message_id: "00000000-0000-4000-8000-000000000001",
            event: {
              type: "tool_call_end",
              content_index: 0,
              tool_call: {
                id: "call-1",
                name: "read_file",
                route: "normal",
                arguments: opaque,
              },
            },
          },
        },
      },
      0,
    )?.type,
    "event",
  );
  for (const toolCall of [
    { id: "call-1", name: "read_file", arguments: opaque },
    { id: "call-1", name: "read_file", route: "automatic", arguments: opaque },
    {
      id: "call-1",
      name: "read_file",
      route: "normal",
      arguments: opaque,
      extra: true,
    },
  ]) {
    assert.equal(
      parseDirectChatServerFrame(
        {
          type: "event",
          envelope: {
            event: {
              type: "message_update",
              message_id: "00000000-0000-4000-8000-000000000001",
              event: {
                type: "tool_call_end",
                content_index: 0,
                tool_call: toolCall,
              },
            },
          },
        },
        0,
      ),
      undefined,
    );
  }
  assert.equal(
    parseDirectChatServerFrame(
      event(1, {
        type: "approval_resolved",
        request_id: "request-1",
        resolution: { decision: { type: "approve_once" } },
      }),
      0,
    )?.type,
    "event",
  );
  assert.equal(
    parseDirectChatServerFrame(
      event(1, {
        type: "approval_resolved",
        request_id: "request-1",
        resolution: { rejected: { decision: { type: "approve_once" } } },
      }),
      0,
    )?.type,
    "event",
  );
  assert.equal(
    parseDirectChatServerFrame(
      event(1, {
        type: "approval_resolved",
        request_id: "request-1",
        resolution: { rejected: { decision: { type: "deny_once" } } },
      }),
      0,
    ),
    undefined,
  );
  assert.equal(
    parseDirectChatServerFrame(
      event(1, {
        type: "approval_resolved",
        request_id: "request-1",
        resolution: { decision: { type: "approve_always", rule: opaque } },
      }),
      0,
    ),
    undefined,
  );
});

test("accepts every exact event shape emitted by the browser E2E fixture", () => {
  const durable = [
    { type: "agent_start" },
    { type: "turn_start" },
    {
      type: "tool_execution_start",
      tool_call_id: "call-1",
      tool_name: "read_file",
      args: {},
    },
    {
      type: "tool_execution_end",
      tool_call_id: "call-1",
      result: "ok",
      is_error: false,
    },
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
        ...assistantMessage([
          { type: "text", text: "Terminal replay", wire_item_index: 0 },
        ]),
        stop_reason: "aborted",
        interrupted: true,
      },
    },
    { type: "turn_end", message: null, tool_results: [] },
    { type: "agent_end" },
  ];
  for (const fixtureEvent of durable) {
    assert.equal(
      parseDirectChatServerFrame(event(1, fixtureEvent), 0)?.type,
      "event",
    );
  }
  for (const delta of ["streamed assistant", "abortable stream"]) {
    assert.equal(
      parseDirectChatServerFrame(
        {
          type: "event",
          envelope: {
            event: {
              type: "message_update",
              message_id: "00000000-0000-4000-8000-000000000001",
              event: { type: "text_delta", content_index: 0, delta },
            },
          },
        },
        0,
      )?.type,
      "event",
    );
  }
});

test("validates RFC3339 calendar, time, fraction, and offset components exactly", () => {
  const retry = (retry_at) =>
    event(1, {
      type: "retry_scheduled",
      attempt: 1,
      delay_ms: 100,
      retry_at,
      error_message: "retry",
    });
  for (const invalid of [
    "2026-02-29T00:00:00Z",
    "1900-02-29T00:00:00Z",
    "2026-04-31T00:00:00Z",
    "2026-01-01T24:00:00Z",
    "2026-01-01T23:60:00Z",
    "2026-01-01T23:59:60Z",
    "2026-01-01T00:00:00+24:00",
    "2026-01-01T00:00:00-00:60",
  ]) {
    assert.equal(parseDirectChatServerFrame(retry(invalid), 0), undefined);
  }
  for (const valid of [
    "0000-01-01T00:00:00Z",
    "2024-02-29T23:59:59Z",
    "2024-02-29T23:59:59.123456789+23:59",
    "9999-12-31T00:00:00.0-00:00",
  ]) {
    assert.equal(parseDirectChatServerFrame(retry(valid), 0)?.type, "event");
  }
});

test("rejects RFC3339 and UUID values with trailing line terminators", () => {
  const uuid = "00000000-0000-4000-8000-000000000001";
  const retry = (retry_at) =>
    event(1, {
      type: "retry_scheduled",
      attempt: 1,
      delay_ms: 100,
      retry_at,
      error_message: "retry",
    });
  const messageStart = (message_id) =>
    event(1, {
      type: "message_start",
      message_id,
      message: assistantMessage(),
    });
  for (const suffix of ["\n", "\r\n", "\u2028", "\u2029"]) {
    assert.equal(
      parseDirectChatServerFrame(retry(`${timestamp}${suffix}`), 0),
      undefined,
    );
    assert.equal(
      parseDirectChatServerFrame(messageStart(`${uuid}${suffix}`), 0),
      undefined,
    );
    assert.equal(
      parseDirectChatServerFrame(
        {
          type: "command_accepted",
          idempotency_key: "key-1",
          command_id: `${uuid}${suffix}`,
          seq: 1,
        },
        0,
      ),
      undefined,
    );
  }
  assert.equal(parseDirectChatServerFrame(retry(timestamp), 0)?.type, "event");
  assert.equal(
    parseDirectChatServerFrame(messageStart(uuid), 0)?.type,
    "event",
  );
  assert.equal(
    parseDirectChatServerFrame(
      {
        type: "command_accepted",
        idempotency_key: "key-1",
        command_id: uuid,
        seq: 1,
      },
      0,
    )?.type,
    "command_accepted",
  );
});

test("reconstructs and deduplicates durable messages and tool state after reload/replay", () => {
  const replay = [
    event(1, {
      type: "message_start",
      message_id: "user-1",
      message: {
        role: "user",
        content: [{ type: "text", text: "persisted user" }],
      },
    }),
    event(2, {
      type: "tool_execution_start",
      tool_call_id: "tool-1",
      tool_name: "read_file",
    }),
    event(3, { type: "tool_execution_end", tool_call_id: "tool-1" }),
    event(4, {
      type: "message_end",
      message_id: "assistant-1",
      message: {
        role: "assistant",
        content: [{ type: "text", text: "persisted assistant" }],
      },
    }),
  ];
  const timeline = new DirectChatTimeline();
  for (const frame of replay) timeline.apply(frame);
  for (const frame of replay) timeline.apply(frame);
  assert.deepEqual(
    timeline.items().map((item) => [item.kind, item.text]),
    [
      ["user", "persisted user"],
      ["tool", "Tool finished: tool-1"],
      ["assistant", "persisted assistant"],
    ],
  );
  const reloaded = new DirectChatTimeline();
  for (const frame of replay) reloaded.apply(frame);
  assert.deepEqual(reloaded.items(), timeline.items());
});

test("durable completion supersedes a volatile preview and drops late volatile replay", () => {
  const timeline = new DirectChatTimeline();
  timeline.apply({
    type: "event",
    envelope: {
      event: {
        type: "message_update",
        message_id: "assistant-2",
        event: { type: "text_delta", delta: "draft" },
      },
    },
  });
  timeline.apply(
    event(1, {
      type: "message_end",
      message_id: "assistant-2",
      message: {
        role: "assistant",
        content: [{ type: "text", text: "durable" }],
      },
    }),
  );
  timeline.apply({
    type: "event",
    envelope: {
      event: {
        type: "message_update",
        message_id: "assistant-2",
        event: { type: "text_delta", delta: "late" },
      },
    },
  });
  assert.deepEqual(
    timeline.items().map((item) => [item.id, item.text]),
    [["message-assistant-2", "durable"]],
  );
});

test("only a cause the server states marks the agent unavailable", () => {
  FakeWebSocket.instances = [];
  const socket = new DirectChatSocket();
  socket.bindInstallation(binding);
  const connections = [];
  const readiness = [];
  socket.onConnection((state) => connections.push(state));
  socket.onReady((state) => readiness.push(state));
  socket.connect();

  // The API accepts the upgrade and then names the failed lazy spawn in the
  // close frame, which is the only channel a page can read a cause on.
  const wire = FakeWebSocket.instances.at(-1);
  wire.open();
  wire.close(DIRECT_CHAT_RUNTIME_UNAVAILABLE_CLOSE_CODE, "runtime_not_ready");

  assert.deepEqual(connections, ["connecting", "connected", "closed"]);
  assert.deepEqual(readiness, ["unknown", "not_ready"]);
  socket.close();
});

test("closes the browser cannot attribute never blame the agent runtime", () => {
  for (const [name, drive] of [
    // A refused upgrade: 401, 403, a disallowed origin, an offline network, a
    // DNS or TLS failure. The page sees one indistinguishable close.
    ["refused upgrade", (wire) => wire.drop()],
    // An established session dropped mid-flight.
    [
      "dropped session",
      (wire) => {
        wire.open();
        wire.receive({ type: "direct_chat_status", status: "ready" });
        wire.drop();
      },
    ],
    // A server close that carries a code, but not this contract's code.
    [
      "unrelated server close",
      (wire) => {
        wire.open();
        wire.close(1001, "going away");
      },
    ],
  ]) {
    FakeWebSocket.instances = [];
    const socket = new DirectChatSocket();
    socket.bindInstallation(binding);
    const readiness = [];
    socket.onReady((state) => readiness.push(state));
    socket.connect();
    drive(FakeWebSocket.instances.at(-1));
    assert.equal(readiness.at(-1), "unknown", name);
    assert.equal(readiness.includes("not_ready"), false, name);
    socket.close();
  }
});

// Readiness is a claim about now, so the only way to check that a past failure
// does not outlive its connection is to actually run the backoff timer the
// close armed. Capturing the timer keeps that deterministic.
function withCapturedRetryTimer(run) {
  const realSetTimeout = globalThis.setTimeout;
  const realClearTimeout = globalThis.clearTimeout;
  const scheduled = [];
  globalThis.setTimeout = (callback, delay) => {
    const handle = { callback, delay, cancelled: false, fired: false };
    scheduled.push(handle);
    return handle;
  };
  globalThis.clearTimeout = (handle) => {
    if (handle && typeof handle === "object") handle.cancelled = true;
    else realClearTimeout(handle);
  };
  try {
    return run(() => {
      const handle = scheduled.find(
        (entry) => !entry.fired && !entry.cancelled,
      );
      assert.ok(handle, "the socket never armed a reconnect attempt");
      handle.fired = true;
      handle.callback();
      return handle.delay;
    });
  } finally {
    globalThis.setTimeout = realSetTimeout;
    globalThis.clearTimeout = realClearTimeout;
  }
}

test("a stated runtime failure does not outlive the connection that stated it", () => {
  FakeWebSocket.instances = [];
  const socket = new DirectChatSocket();
  socket.bindInstallation(binding);
  const readiness = [];
  socket.onReady((state) => readiness.push(state));

  withCapturedRetryTimer((fireReconnect) => {
    socket.connect();
    const rejected = FakeWebSocket.instances.at(-1);
    rejected.open();
    rejected.close(
      DIRECT_CHAT_RUNTIME_UNAVAILABLE_CLOSE_CODE,
      "runtime_not_ready",
    );
    assert.equal(readiness.at(-1), "not_ready");

    fireReconnect();
    const retried = FakeWebSocket.instances.at(-1);
    assert.notEqual(retried, rejected, "the reconnect opened no new socket");
    retried.open();
    assert.equal(
      readiness.at(-1),
      "unknown",
      "a live connection must not inherit the previous connection's verdict",
    );
    retried.receive({ type: "direct_chat_status", status: "ready" });
    assert.equal(readiness.at(-1), "ready");
  });
  socket.close();
});

test("a reconnect the browser cannot attribute withdraws the stated cause", () => {
  FakeWebSocket.instances = [];
  const socket = new DirectChatSocket();
  socket.bindInstallation(binding);
  const readiness = [];
  socket.onReady((state) => readiness.push(state));

  withCapturedRetryTimer((fireReconnect) => {
    socket.connect();
    const rejected = FakeWebSocket.instances.at(-1);
    rejected.open();
    rejected.close(
      DIRECT_CHAT_RUNTIME_UNAVAILABLE_CLOSE_CODE,
      "runtime_not_ready",
    );
    assert.equal(readiness.at(-1), "not_ready");

    // API restart, sleep resume, offline: the retry never reaches a server
    // that can state anything, so the agent stops being the named cause.
    fireReconnect();
    FakeWebSocket.instances.at(-1).drop();
    assert.equal(readiness.at(-1), "unknown");
  });
  socket.close();
});

test("the mounted store surfaces a stated runtime failure and clears it on retry", async () => {
  FakeWebSocket.instances = [];
  const transport = new DirectChatSocket();
  const store = createConversationStore({ transport });
  const release = store.getState().acquireConnection(binding);
  await new Promise((resolve) => queueMicrotask(resolve));

  const rejected = FakeWebSocket.instances.at(-1);
  rejected.open();
  rejected.close(
    DIRECT_CHAT_RUNTIME_UNAVAILABLE_CLOSE_CODE,
    "runtime_not_ready",
  );
  assert.equal(store.getState().connection, "closed");
  assert.equal(store.getState().ready, "not_ready");

  // Exactly what the retry control in the chat screen invokes.
  store.getState().disconnect();
  store.getState().resumeMountedConnection();
  await new Promise((resolve) => queueMicrotask(resolve));

  const retried = FakeWebSocket.instances.at(-1);
  assert.notEqual(retried, rejected);
  retried.open();
  retried.receive({ type: "direct_chat_status", status: "ready" });
  assert.equal(store.getState().connection, "connected");
  assert.equal(store.getState().ready, "ready");
  release();
});
