/// <reference types="node" />

import assert from "node:assert/strict";
import test from "node:test";
import type { BrowserEventEnvelope } from "@sumi/api-client";
import type {
  DirectChatConnectionState,
  DirectChatReadyState,
  DirectChatServerFrame,
} from "../lib/direct-chat-socket";
import { createConversationStore, type DirectChatTransport } from "./store";
import { userMessageIdFromCommandId } from "./user-message-id";

test("UUIDv5 derivation matches the Rust USER_MESSAGE_ID_NAMESPACE contract", () => {
  assert.equal(
    userMessageIdFromCommandId("00000000-0000-4000-8000-000000000001"),
    "b508ee8b-fa35-59b0-8772-6f75ba135990",
  );
  assert.throws(() => userMessageIdFromCommandId("not-a-uuid"));
});

test("store gates commands and reconciles an accepted optimistic user message", () => {
  const transport = new FakeTransport();
  const store = createConversationStore({
    transport,
    idempotencyKey: () => "idem-1",
    reducerId: () => "error-1",
  });

  assert.equal(store.getState().sendMessage("before connect"), false);
  assert.equal(transport.sent.length, 0);

  store.getState().connect();
  assert.equal(store.getState().connection, "connected");
  assert.equal(store.getState().ready, "ready");
  assert.equal(store.getState().sendMessage("  hello  "), true);
  assert.deepEqual(transport.sent, [
    {
      command: { type: "user_message", text: "hello", attachments: [] },
      idempotencyKey: "idem-1",
    },
  ]);

  const commandId = "00000000-0000-4000-8000-000000000001";
  const messageId = userMessageIdFromCommandId(commandId);
  transport.emit({
    type: "command_accepted",
    idempotency_key: "idem-1",
    command_id: commandId,
    seq: 1,
  });
  let entry = store.getState().conversation.entries[messageId];
  assert.equal(entry?.kind, "user");
  if (entry?.kind === "user") {
    assert.equal(entry.delivery, "accepted");
    assert.equal(entry.timestamp, null);
  }
  assert.equal(
    store.getState().conversation.entries["optimistic:idem-1"],
    undefined,
  );

  const durable: BrowserEventEnvelope = {
    seq: 1,
    event: {
      type: "message_end",
      message_id: messageId,
      message: {
        role: "user",
        content: [{ type: "text", text: "hello" }],
        timestamp: "2026-07-30T12:00:00Z",
      },
    },
  };
  transport.emit({
    type: "event",
    envelope: durable,
  } as unknown as DirectChatServerFrame);
  entry = store.getState().conversation.entries[messageId];
  assert.equal(entry?.kind, "user");
  if (entry?.kind === "user") {
    assert.equal(entry.delivery, "durable");
    assert.equal(entry.timestamp, "2026-07-30T12:00:00Z");
    assert.equal(entry.idempotencyKey, undefined);
  }
  assert.deepEqual(store.getState().conversation.entryOrder, [messageId]);
});

test("rejection marks only its provisional action and preserves durable history", () => {
  const transport = new FakeTransport();
  let nextKey = "old";
  const store = createConversationStore({
    transport,
    idempotencyKey: () => nextKey,
    reducerId: () => "error-2",
  });
  store.getState().connect();

  const durableId = "00000000-0000-4000-8000-000000000020";
  transport.emit({
    type: "event",
    envelope: {
      seq: 1,
      event: {
        type: "message_end",
        message_id: durableId,
        message: {
          role: "user",
          content: [{ type: "text", text: "durable" }],
          timestamp: "2026-07-30T12:00:00Z",
        },
      },
    },
  });
  nextKey = "rejected";
  assert.equal(store.getState().sendMessage("provisional"), true);
  transport.emit({
    type: "command_rejected",
    idempotency_key: "rejected",
    reject_reason: "not_allowed",
  });

  const durable = store.getState().conversation.entries[durableId];
  const rejected = store.getState().conversation.entries["optimistic:rejected"];
  assert.equal(durable?.kind, "user");
  if (durable?.kind === "user") assert.equal(durable.delivery, "durable");
  assert.equal(rejected?.kind, "user");
  if (rejected?.kind === "user") {
    assert.equal(rejected.delivery, "rejected");
    assert.equal(rejected.rejectReason, "not_allowed");
  }
  assert.equal(store.getState().lastError, "Command rejected: not_allowed");
});

class FakeTransport implements DirectChatTransport {
  readonly sent: Array<{ command: unknown; idempotencyKey?: string }> = [];
  private readonly frameListeners = new Set<
    (frame: DirectChatServerFrame) => void
  >();
  private readonly connectionListeners = new Set<
    (state: DirectChatConnectionState) => void
  >();
  private readonly readyListeners = new Set<
    (state: DirectChatReadyState) => void
  >();

  connect() {
    for (const listener of this.connectionListeners) listener("connected");
    for (const listener of this.readyListeners) listener("ready");
  }

  close() {
    for (const listener of this.connectionListeners) listener("closed");
    for (const listener of this.readyListeners) listener("unknown");
  }

  sendCommand(command: unknown, idempotencyKey?: string) {
    this.sent.push({ command, idempotencyKey });
    return true;
  }

  onFrame(listener: (frame: DirectChatServerFrame) => void) {
    this.frameListeners.add(listener);
    return () => {
      this.frameListeners.delete(listener);
    };
  }

  onConnection(listener: (state: DirectChatConnectionState) => void) {
    this.connectionListeners.add(listener);
    return () => {
      this.connectionListeners.delete(listener);
    };
  }

  onReady(listener: (state: DirectChatReadyState) => void) {
    this.readyListeners.add(listener);
    return () => {
      this.readyListeners.delete(listener);
    };
  }

  emit(frame: DirectChatServerFrame) {
    for (const listener of this.frameListeners) listener(frame);
  }
}
