/// <reference types="node" />

import assert from "node:assert/strict";
import test from "node:test";
import type {
  ApprovalRequest,
  BrowserEventEnvelope,
  CommandDispositionEvent,
} from "@sumi/api-client";
import type {
  DirectChatConnectionState,
  DirectChatInstallationBinding,
  DirectChatReadyState,
  DirectChatServerFrame,
} from "../lib/direct-chat-socket";
import { PrivateOutbox, type PrivateOutboxStorage } from "./private-outbox";
import { createConversationStore, type DirectChatTransport } from "./store";
import { userMessageIdFromCommandId } from "./user-message-id";

const CommandId = "00000000-0000-4000-8000-000000000001";
const Timestamp = "2026-07-30T12:00:00Z";
const InstallationId = "0198f0f4-9b72-7000-8000-000000000051";
const InstallationBinding = {
  installationId: InstallationId,
  authorityEpoch: "1",
} satisfies DirectChatInstallationBinding;

test("UUIDv5 derivation matches the Rust USER_MESSAGE_ID_NAMESPACE contract", () => {
  assert.equal(
    userMessageIdFromCommandId("00000000-0000-4000-8000-000000000001"),
    "b508ee8b-fa35-59b0-8772-6f75ba135990",
  );
  assert.throws(() => userMessageIdFromCommandId("not-a-uuid"));
});

test("one mounted owner opens one connection and releases it promptly", async () => {
  const transport = new FakeTransport();
  const store = createConversationStore({ transport });

  const release = store.getState().acquireConnection(InstallationBinding);
  assert.equal(transport.connectCalls, 0);

  await flushConnectionMicrotasks();
  assert.equal(transport.connectCalls, 1);
  assert.equal(store.getState().connection, "connected");

  release();
  assert.equal(transport.closeCalls, 1);
  assert.equal(store.getState().connection, "closed");
});

test("StrictMode probe cleanup cannot create or close the live socket", async () => {
  const transport = new FakeTransport();
  const store = createConversationStore({ transport });
  const connectionStates: DirectChatConnectionState[] = [];
  const unsubscribe = store.subscribe((state) => {
    connectionStates.push(state.connection);
  });

  const releaseProbe = store.getState().acquireConnection(InstallationBinding);
  releaseProbe();
  const releaseLive = store.getState().acquireConnection(InstallationBinding);

  await flushConnectionMicrotasks();
  assert.equal(transport.connectCalls, 1);
  assert.equal(transport.closeCalls, 0);
  assert.equal(connectionStates.includes("closed"), false);

  releaseLive();
  assert.equal(transport.closeCalls, 1);
  unsubscribe();
});

test("a real unmount before deferred connect leaves no phantom socket", async () => {
  const transport = new FakeTransport();
  const store = createConversationStore({ transport });

  const release = store.getState().acquireConnection(InstallationBinding);
  release();
  await flushConnectionMicrotasks();

  assert.equal(transport.connectCalls, 0);
  assert.equal(transport.closeCalls, 0);
  assert.equal(store.getState().connection, "closed");
});

test("stale owner cleanup cannot close a later owner's connection", async () => {
  const transport = new FakeTransport();
  const store = createConversationStore({ transport });

  const releaseStale = store.getState().acquireConnection(InstallationBinding);
  const releaseCurrent = store
    .getState()
    .acquireConnection(InstallationBinding);
  await flushConnectionMicrotasks();

  releaseStale();
  assert.equal(transport.connectCalls, 1);
  assert.equal(transport.closeCalls, 0);

  releaseCurrent();
  assert.equal(transport.closeCalls, 1);
});

test("rapid authority switches connect only the latest mounted generation", async () => {
  const transport = new FakeTransport();
  const store = createConversationStore({ transport });
  const release = store.getState().acquireConnection(InstallationBinding);

  await flushConnectionMicrotasks();
  assert.equal(transport.connectCalls, 1);

  assert.equal(store.getState().resetAuthority(), true);
  store.getState().resumeMountedConnection();
  assert.equal(store.getState().resetAuthority(), true);
  store.getState().resumeMountedConnection();
  const releaseRebound = store
    .getState()
    .acquireConnection(InstallationBinding);
  await flushConnectionMicrotasks();

  assert.equal(transport.resetAuthorityCalls, 2);
  assert.equal(transport.connectCalls, 2);
  assert.equal(store.getState().connection, "connected");

  releaseRebound();
  release();
  assert.equal(transport.closeCalls, 3);
});

test("authority rebind invalidates a deferred pre-reset connection", async () => {
  const transport = new FakeTransport();
  const store = createConversationStore({ transport });
  const release = store.getState().acquireConnection(InstallationBinding);

  assert.equal(store.getState().resetAuthority(), true);
  store.getState().resumeMountedConnection();
  const releaseRebound = store
    .getState()
    .acquireConnection(InstallationBinding);
  await flushConnectionMicrotasks();

  assert.equal(transport.resetAuthorityCalls, 1);
  assert.equal(transport.connectCalls, 1);
  releaseRebound();
  release();
});

test("installation replacement fences unaccepted replay into a recoverable draft", async () => {
  const transport = new FakeTransport();
  const outbox = new PrivateOutbox();
  const store = createConversationStore({
    transport,
    outbox,
    idempotencyKey: () => "pending-before-reinstall",
  });
  const releaseOld = store.getState().acquireConnection(InstallationBinding);
  await flushConnectionMicrotasks();
  assert.equal(store.getState().sendMessage("keep this text"), true);
  assert.equal(transport.sent.length, 1);

  const replacement = "0198f0f4-9b72-7000-8000-000000000052";
  const replacementBinding = {
    installationId: replacement,
    authorityEpoch: "1",
  };
  const releaseNew = store.getState().acquireConnection(replacementBinding);
  await flushConnectionMicrotasks();

  assert.deepEqual(transport.installationBindings, [
    InstallationBinding,
    replacementBinding,
  ]);
  assert.deepEqual(outbox.entries(), [
    {
      state: "recoverable",
      idempotencyKey: "pending-before-reinstall",
      text: "keep this text",
      reason: "installation_changed",
    },
  ]);
  assert.equal(transport.sent.length, 1);
  assert.equal(
    Object.keys(store.getState().conversation.entries).some((key) =>
      key.startsWith("optimistic:"),
    ),
    false,
  );

  releaseNew();
  releaseOld();
});

test("disable and re-enable of the same installation starts a fresh authority epoch", async () => {
  const transport = new FakeTransport();
  const outbox = new PrivateOutbox();
  const store = createConversationStore({
    transport,
    outbox,
    idempotencyKey: () => "pending-before-disable",
  });
  const releaseOld = store.getState().acquireConnection(InstallationBinding);
  await flushConnectionMicrotasks();
  transport.emit(canonicalFrame(1, CommandId, "already admitted"));
  assert.equal(store.getState().sendMessage("recover this text"), true);
  assert.equal(transport.sent.length, 1);

  assert.equal(store.getState().suspendInstallation(InstallationBinding), true);

  assert.equal(transport.suspendInstallationCalls, 1);
  assert.deepEqual(outbox.entries(), [
    {
      state: "recoverable",
      idempotencyKey: "pending-before-disable",
      text: "recover this text",
      reason: "installation_suspended",
    },
  ]);
  assert.equal(
    store.getState().conversation.entries[userMessageIdFromCommandId(CommandId)]
      ?.kind,
    "user",
  );
  assert.equal(
    store.getState().conversation.entries["optimistic:pending-before-disable"],
    undefined,
  );

  releaseOld();
  const reenabledBinding = {
    installationId: InstallationId,
    authorityEpoch: "2",
  };
  const releaseReenabled = store.getState().acquireConnection(reenabledBinding);
  await flushConnectionMicrotasks();
  assert.deepEqual(transport.installationBindings, [
    InstallationBinding,
    reenabledBinding,
  ]);
  assert.equal(transport.connectCalls, 2);
  assert.equal(transport.sent.length, 1);
  releaseReenabled();
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

test("authority reset disposes conversation and private delivery state", () => {
  const transport = new FakeTransport();
  const outbox = new PrivateOutbox();
  const store = createConversationStore({
    transport,
    outbox,
    idempotencyKey: () => "old-authority-key",
  });
  store.getState().connect();
  assert.equal(store.getState().sendMessage("private old text"), true);
  transport.emit({
    type: "event",
    envelope: {
      seq: 1,
      event: { type: "agent_start" },
    },
  } as DirectChatServerFrame);
  assert.notEqual(store.getState().conversation.entryOrder.length, 0);
  assert.notEqual(outbox.entries().length, 0);

  store.getState().resetAuthority();

  assert.deepEqual(store.getState().conversation.entryOrder, []);
  assert.deepEqual(store.getState().conversation.entries, {});
  assert.deepEqual(store.getState().recoverableDrafts, []);
  assert.deepEqual(outbox.entries(), []);
  assert.equal(store.getState().connection, "closed");
  assert.equal(store.getState().ready, "unknown");
  const sentBeforeReconnect = transport.sent.length;
  store.getState().connect();
  assert.equal(transport.sent.length, sentBeforeReconnect);
});

test("authority reset releases approval latches from the previous principal", () => {
  const transport = new FakeTransport();
  const store = createConversationStore({
    transport,
    outbox: new PrivateOutbox(),
  });
  const request = approvalRequest("reused-request-id");

  store.getState().connect();
  transport.emit({
    type: "event",
    envelope: { seq: 1, event: { type: "approval_requested", request } },
  } as unknown as DirectChatServerFrame);
  assert.equal(
    store.getState().decideApproval(request.id, { type: "approve_once" }),
    true,
  );

  store.getState().resetAuthority();
  store.getState().connect();
  transport.emit({
    type: "event",
    envelope: { seq: 1, event: { type: "approval_requested", request } },
  } as unknown as DirectChatServerFrame);
  assert.equal(
    store.getState().decideApproval(request.id, { type: "deny_once" }),
    true,
  );
  assert.deepEqual(
    transport.sent.map((entry) => entry.command),
    [
      {
        type: "approval_decision",
        request_id: request.id,
        decision: { type: "approve_once" },
      },
      {
        type: "approval_decision",
        request_id: request.id,
        decision: { type: "deny_once" },
      },
    ],
  );
});

test("authority switch stays blocked and private text quarantined until erasure succeeds", () => {
  let stored: string | null = null;
  let failMutations = false;
  const storage: PrivateOutboxStorage = {
    getItem: () => stored,
    setItem(_key, value) {
      if (failMutations) throw new Error("session storage denied");
      stored = value;
    },
    removeItem() {
      if (failMutations) throw new Error("session storage denied");
      stored = null;
    },
  };
  const transport = new FakeTransport();
  const outbox = new PrivateOutbox(storage);
  const store = createConversationStore({
    transport,
    outbox,
    idempotencyKey: () => "prior-authority",
  });
  store.getState().connect();
  assert.equal(store.getState().sendMessage("prior authority text"), true);

  failMutations = true;
  assert.equal(store.getState().resetAuthority(), false);
  assert.deepEqual(store.getState().conversation.entries, {});
  assert.deepEqual(store.getState().recoverableDrafts, []);
  assert.equal(
    store.getState().lastError,
    "Private delivery state could not be cleared; authority switch was blocked",
  );
  assert.equal(
    new PrivateOutbox(storage).entries()[0]?.text,
    "prior authority text",
  );

  store.getState().connect();
  assert.equal(store.getState().connection, "closed");
  assert.equal(
    store.getState().lastError,
    "Private delivery state must be cleared before direct chat can reconnect",
  );

  failMutations = false;
  assert.equal(store.getState().resetAuthority(), true);
  assert.deepEqual(new PrivateOutbox(storage).entries(), []);
  store.getState().connect();
  assert.equal(store.getState().connection, "connected");
});

test("replayed disposition before admission is reconciled by its authoritative receipt", () => {
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
    transport.emit(
      accepted(`idem-${status}`, CommandId, 7, {
        type: "command_disposition",
        command_id: CommandId,
        command_seq: 7,
        status,
        ...(status === "rejected"
          ? { reject_reason: "not_allowed" as const }
          : {}),
      } as CommandDispositionEvent),
    );

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
    let dispositionSeen = false;
    const terminalDisposition: CommandDispositionEvent = {
      type: "command_disposition",
      command_id: CommandId,
      command_seq: 11,
      status: "applied",
    };
    for (const step of permutation) {
      if (step === "receipt") {
        const frame = accepted(
          key,
          CommandId,
          11,
          dispositionSeen ? terminalDisposition : undefined,
        );
        transport.emit(frame);
        transport.emit(frame);
      } else if (step === "disposition") {
        dispositionSeen = true;
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

test("lost receipt after more than 32 unrelated dispositions reconciles authoritatively", () => {
  for (const status of ["applied", "superseded", "rejected"] as const) {
    const key = `lost-receipt-${status}`;
    const transport = new FakeTransport();
    const outbox = new PrivateOutbox();
    const store = createConversationStore({
      transport,
      outbox,
      idempotencyKey: () => key,
    });
    store.getState().connect();
    assert.equal(store.getState().sendMessage(`lost-${status}`), true);

    const terminal: CommandDispositionEvent = {
      type: "command_disposition",
      command_id: CommandId,
      command_seq: 17,
      status,
      ...(status === "rejected"
        ? { reject_reason: "not_allowed" as const }
        : {}),
    } as CommandDispositionEvent;
    transport.emit({
      type: "event",
      envelope: { seq: 1, event: terminal },
    });
    for (let index = 0; index < 33; index++) {
      const unrelatedCommandId = `00000000-0000-4000-8000-${(index + 2)
        .toString(16)
        .padStart(12, "0")}`;
      transport.emit(
        disposition(index + 2, unrelatedCommandId, index + 100, "applied"),
      );
    }

    transport.emit(accepted(key, CommandId, 17, terminal));

    const optimistic =
      store.getState().conversation.entries[`optimistic:${key}`];
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
          idempotencyKey: key,
          text: `lost-${status}`,
          reason: status === "superseded" ? "superseded" : "not_allowed",
          commandId: CommandId,
        },
      ]);
    }
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

test("pending text from a previous page is surfaced for recovery and never replayed", () => {
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
  assert.deepEqual(transport.sent, []);
  assert.deepEqual(store.getState().recoverableDrafts, [
    {
      idempotencyKey: "uncertain-key",
      text: "uncertain text",
      reason: "unavailable",
    },
  ]);
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

test("rejection for a non-message command does not claim recovery persistence failed", () => {
  const transport = new FakeTransport();
  const store = createConversationStore({
    transport,
    outbox: new PrivateOutbox(),
  });
  store.getState().connect();

  transport.emit({
    type: "command_rejected",
    idempotency_key: "non-message-command",
    reject_reason: "not_allowed",
  });

  assert.equal(store.getState().lastError, "Command rejected: not_allowed");
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

test("durable outbox failure keeps the composer content unsent", () => {
  const storage: PrivateOutboxStorage = {
    getItem: () => null,
    setItem() {
      throw new Error("session storage denied");
    },
    removeItem() {},
  };
  const transport = new FakeTransport();
  const store = createConversationStore({
    transport,
    outbox: new PrivateOutbox(storage),
    idempotencyKey: () => "undurable",
  });
  store.getState().connect();

  assert.equal(store.getState().sendMessage("keep composing"), false);
  assert.deepEqual(transport.sent, []);
  assert.equal(
    store.getState().lastError,
    "Message could not be saved for recovery",
  );
});

test("admission persistence failure remains provisional and is surfaced", () => {
  let stored: string | null = null;
  let failMutations = false;
  const storage: PrivateOutboxStorage = {
    getItem: () => stored,
    setItem(_key, value) {
      if (failMutations) throw new Error("session storage denied");
      stored = value;
    },
    removeItem() {
      if (failMutations) throw new Error("session storage denied");
      stored = null;
    },
  };
  const transport = new FakeTransport();
  const outbox = new PrivateOutbox(storage);
  const store = createConversationStore({
    transport,
    outbox,
    idempotencyKey: () => "undurable-admission",
  });
  store.getState().connect();
  assert.equal(store.getState().sendMessage("keep provisional"), true);

  failMutations = true;
  transport.emit(accepted("undurable-admission", CommandId, 1));

  assert.equal(
    store.getState().lastError,
    "Command admission could not be saved for recovery",
  );
  assert.equal(
    store.getState().conversation.entries["optimistic:undurable-admission"]
      ?.kind,
    "user",
  );
  const optimistic =
    store.getState().conversation.entries["optimistic:undurable-admission"];
  if (optimistic?.kind === "user") {
    assert.equal(optimistic.delivery, "admitted");
  }
  assert.deepEqual(outbox.entries(), [
    {
      state: "pending",
      idempotencyKey: "undurable-admission",
      text: "keep provisional",
    },
  ]);
  assert.deepEqual(new PrivateOutbox(storage).entries(), outbox.entries());

  failMutations = false;
  const messageId = userMessageIdFromCommandId(CommandId);
  transport.emit({
    type: "event",
    envelope: {
      seq: 2,
      event: {
        type: "message_end",
        message_id: messageId,
        message: userMessage("keep provisional"),
      },
    },
  });
  assert.equal(
    store.getState().conversation.entries["optimistic:undurable-admission"],
    undefined,
  );
  assert.equal(store.getState().conversation.entries[messageId]?.kind, "user");
  assert.deepEqual(outbox.entries(), []);
  assert.deepEqual(new PrivateOutbox(storage).entries(), []);
});

test("canonical evidence clears an undurable optimistic row even when outbox cleanup fails", () => {
  let stored: string | null = null;
  let failMutations = false;
  const storage: PrivateOutboxStorage = {
    getItem: () => stored,
    setItem(_key, value) {
      if (failMutations) throw new Error("session storage denied");
      stored = value;
    },
    removeItem() {
      if (failMutations) throw new Error("session storage denied");
      stored = null;
    },
  };
  const transport = new FakeTransport();
  const outbox = new PrivateOutbox(storage);
  const store = createConversationStore({
    transport,
    outbox,
    idempotencyKey: () => "undurable-canonical",
  });
  store.getState().connect();
  assert.equal(store.getState().sendMessage("canonical evidence"), true);

  failMutations = true;
  transport.emit(accepted("undurable-canonical", CommandId, 1));
  transport.emit(canonicalFrame(1, CommandId, "canonical evidence"));

  assert.equal(
    store.getState().conversation.entries["optimistic:undurable-canonical"],
    undefined,
  );
  assert.equal(
    store.getState().conversation.entries[userMessageIdFromCommandId(CommandId)]
      ?.kind,
    "user",
  );
  assert.equal(
    store.getState().lastError,
    "Canonical message arrived, but local recovery state could not be cleared",
  );
  assert.equal(
    outbox.findByIdempotencyKey("undurable-canonical")?.state,
    "pending",
  );
});

test("terminal disposition clears an undurable optimistic row while reporting recovery failure", () => {
  let stored: string | null = null;
  let failMutations = false;
  const storage: PrivateOutboxStorage = {
    getItem: () => stored,
    setItem(_key, value) {
      if (failMutations) throw new Error("session storage denied");
      stored = value;
    },
    removeItem() {
      if (failMutations) throw new Error("session storage denied");
      stored = null;
    },
  };
  const transport = new FakeTransport();
  const outbox = new PrivateOutbox(storage);
  const store = createConversationStore({
    transport,
    outbox,
    idempotencyKey: () => "undurable-disposition",
  });
  store.getState().connect();
  assert.equal(store.getState().sendMessage("terminal evidence"), true);

  failMutations = true;
  transport.emit(accepted("undurable-disposition", CommandId, 1));
  transport.emit(disposition(1, CommandId, 1, "rejected", "not_allowed"));

  assert.equal(
    store.getState().conversation.entries["optimistic:undurable-disposition"],
    undefined,
  );
  assert.equal(
    store.getState().lastError,
    "Command outcome could not be saved for local recovery",
  );
  assert.equal(
    outbox.findByIdempotencyKey("undurable-disposition")?.state,
    "pending",
  );
});

test("failed local cleanup keeps the optimistic recovery row until retry succeeds", () => {
  let stored: string | null = null;
  let failMutations = false;
  const storage: PrivateOutboxStorage = {
    getItem: () => stored,
    setItem(_key, value) {
      if (failMutations) throw new Error("session storage denied");
      stored = value;
    },
    removeItem() {
      if (failMutations) throw new Error("session storage denied");
      stored = null;
    },
  };
  const transport = new FakeTransport();
  const outbox = new PrivateOutbox(storage);
  const store = createConversationStore({
    transport,
    outbox,
    idempotencyKey: () => "cleanup-retry",
  });
  store.getState().connect();
  assert.equal(store.getState().sendMessage("survive cleanup failure"), true);
  transport.emit(accepted("cleanup-retry", CommandId, 1));

  failMutations = true;
  transport.emit(canonicalFrame(1, CommandId, "survive cleanup failure"));

  assert.equal(
    store.getState().lastError,
    "Canonical message arrived, but local recovery state could not be cleared",
  );
  assert.equal(
    store.getState().conversation.entries["optimistic:cleanup-retry"]?.kind,
    "user",
  );
  assert.equal(
    store.getState().conversation.entries[userMessageIdFromCommandId(CommandId)]
      ?.kind,
    "user",
  );
  assert.equal(
    outbox.findByCommand(CommandId, 1)?.text,
    "survive cleanup failure",
  );
  assert.equal(
    new PrivateOutbox(storage).findByCommand(CommandId, 1)?.text,
    "survive cleanup failure",
  );

  const unrelatedCommandId = "00000000-0000-4000-8000-000000000002";
  transport.emit(disposition(2, unrelatedCommandId, 99, "applied"));
  assert.equal(
    store.getState().lastError,
    "Canonical message arrived, but local recovery state could not be cleared",
  );
  transport.emit(canonicalFrame(3, unrelatedCommandId, "unrelated"));
  assert.equal(
    store.getState().lastError,
    "Canonical message arrived, but local recovery state could not be cleared",
  );

  failMutations = false;
  transport.emit(disposition(4, CommandId, 1, "applied"));
  assert.equal(store.getState().lastError, null);
  assert.equal(
    store.getState().conversation.entries["optimistic:cleanup-retry"],
    undefined,
  );
  assert.deepEqual(outbox.entries(), []);
  assert.deepEqual(new PrivateOutbox(storage).entries(), []);
});

test("approval decision is synchronously latched until durable resolution", () => {
  const transport = new FakeTransport();
  const store = createConversationStore({
    transport,
    outbox: new PrivateOutbox(),
  });
  store.getState().connect();
  const request = approvalRequest("approval-latch");
  transport.emit({
    type: "event",
    envelope: { seq: 1, event: { type: "approval_requested", request } },
  } as unknown as DirectChatServerFrame);

  assert.equal(
    store.getState().decideApproval(request.id, { type: "approve_once" }),
    true,
  );
  assert.equal(store.getState().sendingApprovalRequestId, request.id);
  assert.equal(
    store.getState().decideApproval(request.id, { type: "deny_once" }),
    false,
  );
  assert.deepEqual(transport.sent, [
    {
      command: {
        type: "approval_decision",
        request_id: request.id,
        decision: { type: "approve_once" },
      },
      idempotencyKey: undefined,
    },
  ]);

  transport.emit({
    type: "event",
    envelope: {
      seq: 2,
      event: {
        type: "approval_resolved",
        request_id: request.id,
        resolution: { decision: { type: "approve_once" } },
      },
    },
  } as unknown as DirectChatServerFrame);
  assert.equal(store.getState().sendingApprovalRequestId, null);
  assert.equal(store.getState().approval, null);
});

test("a local approval queue failure releases only that request latch", () => {
  const transport = new FakeTransport();
  transport.sendResult = false;
  const store = createConversationStore({
    transport,
    outbox: new PrivateOutbox(),
  });
  store.getState().connect();
  const request = approvalRequest("approval-retry");
  transport.emit({
    type: "event",
    envelope: { seq: 1, event: { type: "approval_requested", request } },
  } as unknown as DirectChatServerFrame);

  assert.equal(
    store.getState().decideApproval(request.id, { type: "approve_once" }),
    false,
  );
  assert.equal(store.getState().sendingApprovalRequestId, null);
  transport.sendResult = true;
  assert.equal(
    store.getState().decideApproval(request.id, { type: "deny_once" }),
    true,
  );
  assert.deepEqual(
    transport.sent.map((entry) => entry.command),
    [
      {
        type: "approval_decision",
        request_id: request.id,
        decision: { type: "approve_once" },
      },
      {
        type: "approval_decision",
        request_id: request.id,
        decision: { type: "deny_once" },
      },
    ],
  );
});

function accepted(
  idempotencyKey: string,
  commandId: string,
  commandSeq: number,
  disposition?: CommandDispositionEvent,
): DirectChatServerFrame {
  return {
    type: "command_accepted",
    idempotency_key: idempotencyKey,
    command_id: commandId,
    seq: commandSeq,
    ...(disposition ? { disposition } : {}),
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

function approvalRequest(id: string): ApprovalRequest {
  return {
    id,
    tool_call_id: `call-${id}`,
    tool_name: "bash",
    action: { reviewable: { command: "git status" } },
    args_summary: { command: "git status" },
    reason: "shell access",
    audit: {
      outcome: "allow",
      risk: "low",
      authorization: "medium",
      rationale: "read only",
    },
  };
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

function flushConnectionMicrotasks(): Promise<void> {
  return new Promise((resolve) => queueMicrotask(resolve));
}

class FakeTransport implements DirectChatTransport {
  readonly sent: Array<{ command: unknown; idempotencyKey?: string }> = [];
  sendResult = true;
  connectCalls = 0;
  closeCalls = 0;
  resetAuthorityCalls = 0;
  suspendInstallationCalls = 0;
  readonly installationBindings: DirectChatInstallationBinding[] = [];
  private readonly frameListeners = new Set<
    (frame: DirectChatServerFrame) => void
  >();
  private readonly connectionListeners = new Set<
    (state: DirectChatConnectionState) => void
  >();
  private readonly readyListeners = new Set<
    (state: DirectChatReadyState) => void
  >();

  bindInstallation(binding: DirectChatInstallationBinding) {
    this.installationBindings.push(binding);
  }

  connect() {
    this.connectCalls += 1;
    for (const listener of this.connectionListeners) listener("connected");
    for (const listener of this.readyListeners) listener("ready");
  }

  close() {
    this.closeCalls += 1;
    for (const listener of this.connectionListeners) listener("closed");
    for (const listener of this.readyListeners) listener("unknown");
  }

  resetAuthority() {
    this.resetAuthorityCalls += 1;
    this.close();
  }

  suspendInstallation() {
    this.suspendInstallationCalls += 1;
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
