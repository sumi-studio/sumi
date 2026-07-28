import assert from "node:assert/strict";
import test from "node:test";
import { parseBrowserServerFrame } from "../src/lib/conversation-socket.ts";

const conversationID = "conversation-1";

test("accepts safe event and command response frames", () => {
  const durable = {
    type: "event",
    envelope: {
      conversation_id: conversationID,
      seq: 1,
      event: { type: "agent_start" },
    },
  };
  assert.equal(parseBrowserServerFrame(durable, conversationID, 0), durable);

  const volatile = {
    type: "event",
    envelope: {
      conversation_id: conversationID,
      event: {
        type: "message_update",
        message_id: "00000000-0000-4000-8000-000000000001",
        event: { type: "text_delta", delta: "hello" },
      },
    },
  };
  assert.equal(parseBrowserServerFrame(volatile, conversationID, 1), volatile);

  const accepted = {
    type: "command_accepted",
    envelope: {
      seq: 1,
      command_id: "00000000-0000-4000-8000-000000000001",
      command: { type: "abort" },
    },
  };
  assert.equal(parseBrowserServerFrame(accepted, conversationID, 0), accepted);
  const rejected = {
    type: "command_rejected",
    reject_reason: "not_allowed",
  };
  assert.equal(parseBrowserServerFrame(rejected, conversationID, 0), rejected);
});

test("rejects malformed events before they reach UI listeners", () => {
  const invalid = [
    { type: "event", envelope: {} },
    {
      type: "event",
      envelope: { conversation_id: conversationID, event: null },
    },
    {
      type: "event",
      envelope: {
        conversation_id: conversationID,
        event: { type: "message_update", message_id: "id" },
      },
    },
    {
      type: "event",
      envelope: {
        conversation_id: conversationID,
        seq: 1,
        event: {
          type: "message_end",
          message_id: "id",
          message: { role: "assistant", content: "not-an-array" },
        },
      },
    },
    {
      type: "event",
      envelope: {
        conversation_id: conversationID,
        seq: 1,
        event: {
          type: "message_end",
          message_id: "id",
          message: { role: "assistant", content: [null] },
        },
      },
    },
    {
      type: "event",
      envelope: {
        conversation_id: "other",
        seq: 1,
        event: { type: "agent_start" },
      },
    },
    {
      type: "event",
      envelope: {
        conversation_id: conversationID,
        seq: 1,
        event: { type: "future_event" },
      },
    },
    {
      type: "event",
      envelope: {
        conversation_id: conversationID,
        event: { type: "agent_start" },
      },
    },
    {
      type: "event",
      envelope: {
        conversation_id: conversationID,
        seq: 1,
        event: {
          type: "message_update",
          message_id: "id",
          event: { type: "text_delta", delta: "unsafe cursor" },
        },
      },
    },
  ];
  for (const frame of invalid) {
    assert.equal(parseBrowserServerFrame(frame, conversationID, 0), undefined);
  }
});

test("rejects unsafe or non-monotonic durable cursors", () => {
  for (const seq of [-1, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
    const frame = {
      type: "event",
      envelope: {
        conversation_id: conversationID,
        seq,
        event: { type: "agent_start" },
      },
    };
    assert.equal(parseBrowserServerFrame(frame, conversationID, 0), undefined);
  }

  for (const seq of [4, 5]) {
    const frame = {
      type: "event",
      envelope: {
        conversation_id: conversationID,
        seq,
        event: { type: "agent_start" },
      },
    };
    assert.equal(parseBrowserServerFrame(frame, conversationID, 5), undefined);
  }

  const gap = {
    type: "event",
    envelope: {
      conversation_id: conversationID,
      seq: 7,
      event: { type: "agent_start" },
    },
  };
  assert.equal(parseBrowserServerFrame(gap, conversationID, 5), undefined);

  const contiguous = {
    type: "event",
    envelope: {
      conversation_id: conversationID,
      seq: 6,
      event: { type: "agent_start" },
    },
  };
  assert.equal(
    parseBrowserServerFrame(contiguous, conversationID, 5),
    contiguous,
  );
});

test("rejects unknown reasons and malformed command acknowledgements", () => {
  assert.equal(
    parseBrowserServerFrame(
      { type: "command_rejected", reject_reason: "surprise" },
      conversationID,
      0,
    ),
    undefined,
  );
  assert.equal(
    parseBrowserServerFrame(
      {
        type: "command_accepted",
        envelope: {
          seq: 1,
          command_id: "not-a-uuid",
          command: { type: "abort" },
        },
      },
      conversationID,
      0,
    ),
    undefined,
  );
});
