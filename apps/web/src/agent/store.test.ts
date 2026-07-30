/// <reference types="node" />

import assert from "node:assert/strict";
import test from "node:test";
import type { BrowserEventEnvelope } from "@sumi/api-client";
import type {
  DirectChatConnectionState,
  DirectChatReadyState,
  DirectChatServerFrame,
} from "../lib/direct-chat-socket";
import { PrivateOutbox, type PrivateOutboxStorage } from "./private-outbox";
import { createConversationStore, type DirectChatTransport } from "./store";
import { userMessageIdFromCommandId } from "./user-message-id";

const CommandId = "00000000-0000-4000-8000-000000000001";
const Timestamp = "2026-07-30T12:00:00Z";

test("UUIDv5 derivation matches the Rust USER_MESSAGE_ID_NAMESPACE contract", () => {
  assert.equal(
    userMessageIdFromCommandId("00000000-0000-4000-8000-000000000001"),
    "b508ee8b-fa35-59b0-8772-6f75ba135990",
  );
  assert.throws(() => userMessageIdFromCommandId("not-a-uuid"));
});

test("admission is provisional and canonical user arrival replaces it", () => {
  const transport = new FakeTransport();
  const outbox = new PrivateOutbox();
  const store = createConversationStore({
    transport,
    outbox,
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
  let entry = store.getState().conversation.entries["optimistic:idem-1"];
  assert.equal(entry?.kind, "user");
  if (entry?.kind === "user") {
    assert.equal(entry.delivery, "admitted");
    assert.equal(entry.timestamp, null);
  }
  assert.equal(store.getState().conversation.entries[messageId], undefined);
  assert.equal(outbox.findByCommand(commandId, 1)?.text, "hello");

  transport.emit(disposition(1, commandId, 1, "applied"));
  entry = store.getState().conversation.entries["optimistic:idem-1"];
  assert.equal(entry?.kind, "user");
  if (entry?.kind === "user") assert.equal(entry.delivery, "admitted");

  const durable: BrowserEventEnvelope = {
    seq: 2,
    event: {
      type: "message_end",
      message_id: messageId,
      message: userMessage("hello"),
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
    assert.equal(entry.timestamp, Timestamp);
    assert.equal(entry.idempotencyKey, undefined);
  }
  assert.equal(
    store.getState().conversation.entries["optimistic:idem-1"],
    undefined,
  );
  assert.deepEqual(store.getState().conversation.entryOrder, [messageId]);
  assert.deepEqual(outbox.entries(), []);

  // Duplicates after canonical convergence do not recreate private state.
  transport.emit({
    type: "command_accepted",
    idempotency_key: "idem-1",
    command_id: commandId,
    seq: 1,
  });
  transport.emit(disposition(2, commandId, 1, "applied"));
  assert.deepEqual(store.getState().conversation.entryOrder, [messageId]);
  assert.deepEqual(store.getState().recoverableDrafts, []);
});

test("disposition before admission is reconciled when the receipt arrives", () => {
  for (const status of ["applied", "superseded", "rejected"] as const) {
    const transport = new FakeTransport();
    const store = createConversationStore({
      transport,
      outbox: new PrivateOutbox(),
      idempotencyKey: () => `idem-${status}`,
    });
    store.getState().connect();
    assert.equal(store.getState().sendMessage(`text-${status}`), true);
    transport.emit(
      disposition(
        1,
        CommandId,
        7,
        status,
        status === "rejected" ? "not_allowed" : undefined,
      ),
    );
    assert.equal(
      store.getState().conversation.entries[`optimistic:idem-${status}`]?.kind,
      "user",
    );
    transport.emit(accepted(`idem-${status}`, CommandId, 7));

    const optimistic =
      store.getState().conversation.entries[`optimistic:idem-${status}`];
    if (status === "applied") {
      assert.equal(optimistic?.kind, "user");
      if (optimistic?.kind === "user") {
        assert.equal(optimistic.delivery, "admitted");
      }
      assert.deepEqual(store.getState().recoverableDrafts, []);
    } else {
      assert.equal(optimistic, undefined);
      assert.deepEqual(store.getState().recoverableDrafts, [
        {
          idempotencyKey: `idem-${status}`,
          text: `text-${status}`,
          reason: status === "superseded" ? "superseded" : "not_allowed",
          commandId: CommandId,
        },
      ]);
      // Replayed admission is idempotent after terminal recovery.
      transport.emit(accepted(`idem-${status}`, CommandId, 7));
      assert.equal(store.getState().lastError, null);
    }
  }
});

test("canonical arrival before receipt removes the provisional row on later correlation", () => {
  const transport = new FakeTransport();
  const outbox = new PrivateOutbox();
  const store = createConversationStore({
    transport,
    outbox,
    idempotencyKey: () => "canonical-first",
  });
  store.getState().connect();
  assert.equal(store.getState().sendMessage("canonical first"), true);

  const messageId = userMessageIdFromCommandId(CommandId);
  transport.emit({
    type: "event",
    envelope: {
      seq: 1,
      event: {
        type: "message_start",
        message_id: messageId,
        message: userMessage("canonical first"),
      },
    },
  });
  assert.deepEqual(store.getState().conversation.entryOrder, [
    "optimistic:canonical-first",
    messageId,
  ]);

  transport.emit(accepted("canonical-first", CommandId, 9));
  assert.deepEqual(store.getState().conversation.entryOrder, [messageId]);
  assert.deepEqual(outbox.entries(), []);
  transport.emit(disposition(2, CommandId, 9, "applied"));
  assert.deepEqual(store.getState().conversation.entryOrder, [messageId]);
});

test("all applied receipt, disposition, and canonical permutations converge", () => {
  const permutations = [
    ["receipt", "disposition", "canonical"],
    ["receipt", "canonical", "disposition"],
    ["disposition", "receipt", "canonical"],
    ["disposition", "canonical", "receipt"],
    ["canonical", "receipt", "disposition"],
    ["canonical", "disposition", "receipt"],
  ] as const;

  for (const [index, permutation] of permutations.entries()) {
    const key = `permutation-${index}`;
    const transport = new FakeTransport();
    const outbox = new PrivateOutbox();
    const store = createConversationStore({
      transport,
      outbox,
      idempotencyKey: () => key,
    });
    store.getState().connect();
    assert.equal(store.getState().sendMessage(`text-${index}`), true);
    let eventSeq = 0;
    for (const step of permutation) {
      if (step === "receipt") {
        const frame = accepted(key, CommandId, 11);
        transport.emit(frame);
        transport.emit(frame);
      } else if (step === "disposition") {
        const frame = disposition(++eventSeq, CommandId, 11, "applied");
        transport.emit(frame);
        transport.emit(frame);
      } else {
        const frame = canonicalFrame(
          ++eventSeq,
          CommandId,
          `canonical-${index}`,
        );
        transport.emit(frame);
        transport.emit(frame);
      }
    }

    const messageId = userMessageIdFromCommandId(CommandId);
    assert.deepEqual(store.getState().conversation.entryOrder, [messageId]);
    const canonical = store.getState().conversation.entries[messageId];
    assert.equal(canonical?.kind, "user");
    if (canonical?.kind === "user") {
      assert.equal(canonical.text, `canonical-${index}`);
      assert.equal(canonical.delivery, "durable");
    }
    assert.deepEqual(outbox.entries(), []);
    assert.deepEqual(store.getState().recoverableDrafts, []);
  }
});

test("terminal disposition after receipt recovers and removes local rows", () => {
  for (const status of ["superseded", "rejected"] as const) {
    const key = `terminal-${status}`;
    const transport = new FakeTransport();
    const store = createConversationStore({
      transport,
      outbox: new PrivateOutbox(),
      idempotencyKey: () => key,
    });
    store.getState().connect();
    assert.equal(store.getState().sendMessage(`recover-${status}`), true);
    transport.emit(accepted(key, CommandId, 13));
    transport.emit(
      disposition(
        1,
        CommandId,
        13,
        status,
        status === "rejected" ? "not_allowed" : undefined,
      ),
    );
    assert.equal(
      store.getState().conversation.entries[`optimistic:${key}`],
      undefined,
    );
    assert.deepEqual(store.getState().recoverableDrafts, [
      {
        idempotencyKey: key,
        text: `recover-${status}`,
        reason: status === "superseded" ? "superseded" : "not_allowed",
        commandId: CommandId,
      },
    ]);
  }
});

test("admitted text survives reload for applied replay but never becomes canonical", () => {
  const storage = new MemoryStorage();
  const first = new PrivateOutbox(storage);
  assert.equal(first.putPending("reload-key", "private text"), true);
  assert.equal(first.admit("reload-key", CommandId, 4).kind, "admitted");

  const transport = new FakeTransport();
  const store = createConversationStore({
    transport,
    outbox: new PrivateOutbox(storage),
  });
  transport.emit(disposition(1, CommandId, 4, "applied"));
  const optimistic =
    store.getState().conversation.entries["optimistic:reload-key"];
  assert.equal(optimistic?.kind, "user");
  if (optimistic?.kind === "user") {
    assert.equal(optimistic.delivery, "admitted");
    assert.equal(optimistic.text, "private text");
  }
  assert.equal(
    store.getState().conversation.entries[
      userMessageIdFromCommandId(CommandId)
    ],
    undefined,
  );

  transport.emit({
    type: "event",
    envelope: {
      seq: 2,
      event: {
        type: "message_end",
        message_id: userMessageIdFromCommandId(CommandId),
        message: userMessage("canonical text"),
      },
    },
  });
  const canonical =
    store.getState().conversation.entries[
      userMessageIdFromCommandId(CommandId)
    ];
  assert.equal(canonical?.kind, "user");
  if (canonical?.kind === "user")
    assert.equal(canonical.text, "canonical text");
  assert.equal(
    store.getState().conversation.entries["optimistic:reload-key"],
    undefined,
  );
  assert.deepEqual(new PrivateOutbox(storage).entries(), []);
});

test("pending text survives reload and is safely requeued with its original key", () => {
  const storage = new MemoryStorage();
  const first = new PrivateOutbox(storage);
  assert.equal(first.putPending("uncertain-key", "uncertain text"), true);

  const transport = new FakeTransport();
  const store = createConversationStore({
    transport,
    outbox: new PrivateOutbox(storage),
  });
  assert.deepEqual(store.getState().conversation.entryOrder, []);
  store.getState().connect();
  assert.deepEqual(transport.sent, [
    {
      command: {
        type: "user_message",
        text: "uncertain text",
        attachments: [],
      },
      idempotencyKey: "uncertain-key",
    },
  ]);
  assert.deepEqual(store.getState().conversation.entryOrder, []);

  transport.emit(accepted("uncertain-key", CommandId, 15));
  const optimistic =
    store.getState().conversation.entries["optimistic:uncertain-key"];
  assert.equal(optimistic?.kind, "user");
  if (optimistic?.kind === "user") {
    assert.equal(optimistic.delivery, "admitted");
  }
});

test("immediate rejection removes provisional history and supports restore/discard", () => {
  const transport = new FakeTransport();
  let nextKey = "restore";
  const store = createConversationStore({
    transport,
    outbox: new PrivateOutbox(),
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
        message: userMessage("durable"),
      },
    },
  });
  assert.equal(store.getState().sendMessage("restore me"), true);
  transport.emit({
    type: "command_rejected",
    idempotency_key: "restore",
    reject_reason: "not_allowed",
  });

  const durable = store.getState().conversation.entries[durableId];
  assert.equal(durable?.kind, "user");
  if (durable?.kind === "user") assert.equal(durable.delivery, "durable");
  assert.equal(
    store.getState().conversation.entries["optimistic:restore"],
    undefined,
  );
  assert.deepEqual(store.getState().recoverableDrafts, [
    {
      idempotencyKey: "restore",
      text: "restore me",
      reason: "not_allowed",
    },
  ]);
  assert.equal(store.getState().lastError, "Command rejected: not_allowed");
  assert.equal(store.getState().restoreDraft("restore"), "restore me");
  assert.deepEqual(store.getState().recoverableDrafts, []);

  nextKey = "discard";
  assert.equal(store.getState().sendMessage("discard me"), true);
  transport.emit({
    type: "command_rejected",
    idempotency_key: "discard",
    reject_reason: "unavailable",
  });
  assert.equal(store.getState().discardDraft("discard"), true);
  assert.equal(store.getState().discardDraft("discard"), false);
  assert.deepEqual(store.getState().recoverableDrafts, []);
});

test("client queue failure is immediately recoverable and never enters history", () => {
  const transport = new FakeTransport();
  transport.sendResult = false;
  const store = createConversationStore({
    transport,
    outbox: new PrivateOutbox(),
    idempotencyKey: () => "client-failure",
  });
  store.getState().connect();
  assert.equal(store.getState().sendMessage("keep this"), false);
  assert.equal(
    store.getState().conversation.entries["optimistic:client-failure"],
    undefined,
  );
  assert.deepEqual(store.getState().recoverableDrafts, [
    {
      idempotencyKey: "client-failure",
      text: "keep this",
      reason: "client_validation",
    },
  ]);
});

function accepted(
  idempotencyKey: string,
  commandId: string,
  commandSeq: number,
): DirectChatServerFrame {
  return {
    type: "command_accepted",
    idempotency_key: idempotencyKey,
    command_id: commandId,
    seq: commandSeq,
  };
}

function disposition(
  eventSeq: number,
  commandId: string,
  commandSeq: number,
  status: "applied" | "superseded" | "rejected",
  rejectReason?: "not_allowed",
): DirectChatServerFrame {
  return {
    type: "event",
    envelope: {
      seq: eventSeq,
      event: {
        type: "command_disposition",
        command_id: commandId,
        command_seq: commandSeq,
        status,
        ...(rejectReason ? { reject_reason: rejectReason } : {}),
      },
    },
  } as unknown as DirectChatServerFrame;
}

function userMessage(text: string) {
  return {
    role: "user" as const,
    content: [{ type: "text" as const, text }],
    timestamp: Timestamp,
  };
}

function canonicalFrame(
  eventSeq: number,
  commandId: string,
  text: string,
): DirectChatServerFrame {
  return {
    type: "event",
    envelope: {
      seq: eventSeq,
      event: {
        type: "message_end",
        message_id: userMessageIdFromCommandId(commandId),
        message: userMessage(text),
      },
    },
  } as unknown as DirectChatServerFrame;
}

class MemoryStorage implements PrivateOutboxStorage {
  private readonly values = new Map<string, string>();

  getItem(key: string) {
    return this.values.get(key) ?? null;
  }

  setItem(key: string, value: string) {
    this.values.set(key, value);
  }

  removeItem(key: string) {
    this.values.delete(key);
  }
}

class FakeTransport implements DirectChatTransport {
  readonly sent: Array<{ command: unknown; idempotencyKey?: string }> = [];
  sendResult = true;
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
    return this.sendResult;
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
