/// <reference types="node" />

import assert from "node:assert/strict";
import { test } from "vitest";
import {
  MaxPrivateOutboxEntries,
  MaxPrivateOutboxTextLength,
  PrivateOutbox,
  type PrivateOutboxStorage,
  PrivateOutboxStorageKey,
  PrivateOutboxVersion,
} from "./private-outbox";

const CommandId = "00000000-0000-4000-8000-000000000001";

test("persists a versioned admitted row and reloads only private state", () => {
  const storage = new MemoryStorage();
  const outbox = new PrivateOutbox(storage);
  assert.equal(outbox.putPending("key-1", "hello"), true);
  assert.deepEqual(outbox.admit("key-1", CommandId, 7), {
    kind: "admitted",
    entry: {
      state: "admitted",
      idempotencyKey: "key-1",
      text: "hello",
      commandId: CommandId,
      commandSeq: 7,
    },
  });
  assert.deepEqual(JSON.parse(storage.value(PrivateOutboxStorageKey) ?? ""), {
    version: PrivateOutboxVersion,
    entries: [
      {
        state: "admitted",
        idempotencyKey: "key-1",
        text: "hello",
        commandId: CommandId,
        commandSeq: 7,
      },
    ],
  });
  assert.deepEqual(new PrivateOutbox(storage).entries(), outbox.entries());
});

test("admission and recovery transitions are idempotent and reject conflicts", () => {
  const outbox = new PrivateOutbox();
  assert.equal(outbox.putPending("key-1", "hello"), true);
  assert.equal(outbox.putPending("key-1", "hello"), true);
  assert.equal(outbox.putPending("key-1", "changed"), false);
  assert.equal(outbox.admit("key-1", CommandId, 7).kind, "admitted");
  assert.equal(outbox.admit("key-1", CommandId, 7).kind, "admitted");
  assert.equal(
    outbox.admit("key-1", "00000000-0000-4000-8000-000000000002", 7).kind,
    "conflict",
  );
  assert.equal(outbox.admit("key-1", CommandId, 8).kind, "conflict");

  assert.deepEqual(outbox.recoverByCommand(CommandId, 7, "superseded"), {
    state: "recoverable",
    idempotencyKey: "key-1",
    text: "hello",
    reason: "superseded",
    commandId: CommandId,
    commandSeq: 7,
  });
  assert.equal(outbox.admit("key-1", CommandId, 7).kind, "already_recoverable");
  assert.equal(outbox.admit("key-1", CommandId, 8).kind, "conflict");
  assert.deepEqual(outbox.recoverableDrafts(), [
    {
      idempotencyKey: "key-1",
      text: "hello",
      reason: "superseded",
      commandId: CommandId,
    },
  ]);
  assert.equal(outbox.consumeRecoverable("key-1"), "hello");
  assert.equal(outbox.consumeRecoverable("key-1"), undefined);
  assert.deepEqual(outbox.entries(), []);
});

test("pre-admission recovery remains recoverable and removable", () => {
  const outbox = new PrivateOutbox();
  assert.equal(outbox.putPending("key-1", "hello"), true);
  assert.deepEqual(outbox.recoverByIdempotencyKey("key-1", "unavailable"), {
    state: "recoverable",
    idempotencyKey: "key-1",
    text: "hello",
    reason: "unavailable",
  });
  assert.equal(outbox.recoverByCommand(CommandId, 1, "rejected"), undefined);
  assert.equal(outbox.removeByIdempotencyKey("missing"), false);
  assert.equal(outbox.removeByIdempotencyKey("key-1"), true);
  assert.deepEqual(outbox.entries(), []);
});

test("discards malformed, unbounded, duplicate, and wrong-version storage", () => {
  const invalidJSON = new MemoryStorage();
  invalidJSON.setItem(PrivateOutboxStorageKey, "{not-json");
  assert.deepEqual(new PrivateOutbox(invalidJSON).entries(), []);
  assert.equal(invalidJSON.getItem(PrivateOutboxStorageKey), null);

  const malformed: unknown[] = [
    null,
    [],
    { version: 2, entries: [] },
    { version: 1, entries: [], extra: true },
    { version: 1, entries: "not-an-array" },
    {
      version: 1,
      entries: [
        { state: "pending", idempotencyKey: "same", text: "a" },
        { state: "pending", idempotencyKey: "same", text: "b" },
      ],
    },
    {
      version: 1,
      entries: [
        {
          state: "admitted",
          idempotencyKey: "key",
          text: "hello",
          commandId: "not-a-uuid",
          commandSeq: 1,
        },
      ],
    },
    {
      version: 1,
      entries: [
        {
          state: "recoverable",
          idempotencyKey: "key",
          text: "hello",
          reason: "",
        },
      ],
    },
    {
      version: 1,
      entries: Array.from(
        { length: MaxPrivateOutboxEntries + 1 },
        (_, index) => ({
          state: "pending",
          idempotencyKey: `key-${index}`,
          text: "x",
        }),
      ),
    },
    {
      version: 1,
      entries: [
        {
          state: "pending",
          idempotencyKey: "key",
          text: "x".repeat(MaxPrivateOutboxTextLength + 1),
        },
      ],
    },
  ];

  for (const value of malformed) {
    const storage = new MemoryStorage();
    storage.setItem(PrivateOutboxStorageKey, JSON.stringify(value));
    assert.deepEqual(new PrivateOutbox(storage).entries(), []);
    assert.equal(storage.getItem(PrivateOutboxStorageKey), null);
  }
});

test("enforces entry and aggregate text bounds before mutating", () => {
  const full = new PrivateOutbox();
  for (let index = 0; index < MaxPrivateOutboxEntries; index += 1) {
    assert.equal(full.putPending(`key-${index}`, "x"), true);
  }
  assert.equal(full.putPending("one-too-many", "x"), false);
  assert.equal(full.entries().length, MaxPrivateOutboxEntries);

  const textBound = new PrivateOutbox();
  assert.equal(
    textBound.putPending("max-text", "x".repeat(MaxPrivateOutboxTextLength)),
    true,
  );
  assert.equal(textBound.putPending("over-total", "x"), false);
  assert.equal(
    new PrivateOutbox().putPending(
      "over-entry",
      "x".repeat(MaxPrivateOutboxTextLength + 1),
    ),
    false,
  );
});

test("storage denial refuses an undurable pending command", () => {
  const storage: PrivateOutboxStorage = {
    getItem() {
      throw new Error("denied");
    },
    setItem() {
      throw new Error("denied");
    },
    removeItem() {
      throw new Error("denied");
    },
  };
  const outbox = new PrivateOutbox(storage);
  assert.equal(outbox.putPending("key", "hello"), false);
  assert.deepEqual(outbox.entries(), []);
});

test("failed transition persistence rolls memory back to the durable row", () => {
  const storage = new FailableMemoryStorage();
  const outbox = new PrivateOutbox(storage);
  assert.equal(outbox.putPending("key", "hello"), true);

  storage.failMutations = true;
  assert.deepEqual(outbox.admit("key", CommandId, 1), {
    kind: "persistence_failed",
    entry: {
      state: "admitted",
      idempotencyKey: "key",
      text: "hello",
      commandId: CommandId,
      commandSeq: 1,
    },
  });
  assert.deepEqual(outbox.entries(), [
    { state: "pending", idempotencyKey: "key", text: "hello" },
  ]);
  assert.deepEqual(new PrivateOutbox(storage).entries(), outbox.entries());

  assert.equal(outbox.recoverByIdempotencyKey("key", "unavailable"), undefined);
  assert.deepEqual(outbox.entries(), [
    { state: "pending", idempotencyKey: "key", text: "hello" },
  ]);
});

test("failed removal is not consumed or cleared in memory", () => {
  const storage = new FailableMemoryStorage();
  const outbox = new PrivateOutbox(storage);
  assert.equal(outbox.putPending("key", "hello"), true);
  assert.equal(
    outbox.recoverByIdempotencyKey("key", "unavailable")?.state,
    "recoverable",
  );

  storage.failMutations = true;
  assert.equal(outbox.removeByIdempotencyKey("key"), false);
  assert.equal(outbox.consumeRecoverable("key"), undefined);
  assert.deepEqual(outbox.recoverableDrafts(), [
    {
      idempotencyKey: "key",
      text: "hello",
      reason: "unavailable",
    },
  ]);
  assert.deepEqual(new PrivateOutbox(storage).entries(), outbox.entries());
});

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

  value(key: string) {
    return this.values.get(key);
  }
}

class FailableMemoryStorage extends MemoryStorage {
  failMutations = false;

  override setItem(key: string, value: string) {
    if (this.failMutations) throw new Error("storage denied");
    super.setItem(key, value);
  }

  override removeItem(key: string) {
    if (this.failMutations) throw new Error("storage denied");
    super.removeItem(key);
  }
}
