/// <reference types="node" />

import assert from "node:assert/strict";
import test from "node:test";
import type {
  ApprovalRequest,
  BrowserEventEnvelope,
  PublicAssistantMessage,
  PublicMessage,
} from "@sumi/api-client";
import type { ChatItem, ConversationModel } from "./model";
import { collectAgentCopyText, projectConversation } from "./projection";
import { ConversationProjector, type TimelineExchange } from "./projector";
import {
  type AgentSession,
  createAgentSession,
  reduceEnvelope,
} from "./reducer";

const id = () => "deterministic";
const Timestamp = "2026-07-30T12:00:00Z";
const AssistantMessageId = "00000000-0000-4000-8000-000000000010";
const UserMessageId = "00000000-0000-4000-8000-000000000011";

function apply(
  session: AgentSession,
  envelope: BrowserEventEnvelope,
): AgentSession {
  return reduceEnvelope(session, envelope, { id }).session;
}

function assistantMessage(
  text: string,
  overrides: Partial<PublicAssistantMessage> = {},
): PublicAssistantMessage {
  return {
    role: "assistant",
    content: text ? [{ type: "text", text, wire_item_index: 0 }] : [],
    model: "gpt-5.6-terra",
    provider: "openai",
    origin: {
      provider_instance_id: "default",
      protocol: "open_ai_responses",
      model: "gpt-5.6-terra",
    },
    usage: {
      input: 1,
      output: 1,
      cache_read: 0,
      cache_write: 0,
      reasoning: 0,
      total_tokens: 2,
    },
    stop_reason: "stop",
    error_message: null,
    provider_code: null,
    interrupted: false,
    timestamp: Timestamp,
    ...overrides,
  };
}

function userMessage(text: string): PublicMessage {
  return {
    role: "user",
    content: [{ type: "text", text }],
    timestamp: Timestamp,
  };
}

function assistantToolCall(toolCallId: string): PublicAssistantMessage {
  return {
    ...assistantMessage(""),
    content: [
      {
        type: "tool_call",
        tool_call: {
          id: toolCallId,
          name: "bash",
          route: "normal",
          arguments: {},
        },
        wire_item_index: 0,
      },
    ],
  };
}

function approvalRequest(id: string, toolCallId: string): ApprovalRequest {
  return {
    id,
    tool_call_id: toolCallId,
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

function toExcerpt(text: string): string {
  return text
    .replace(/```[\s\S]*?```/g, " (コード) ")
    .replace(/\$\$[\s\S]*?\$\$/g, " (数式) ")
    .replace(/[#*`>|$_-]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 140);
}

/** Canonical exchange derivation: the scan the projector replaces. */
function canonicalExchanges(items: ChatItem[]): TimelineExchange[] {
  const exchanges: TimelineExchange[] = [];
  items.forEach((item, index) => {
    if (item.kind === "user") {
      const previous = exchanges.at(-1);
      if (previous) previous.endIndex = index - 1;
      exchanges.push({
        startIndex: index,
        endIndex: items.length - 1,
        tick: { id: item.id, title: item.text },
      });
      return;
    }
    if (item.kind === "prose") {
      const current = exchanges.at(-1);
      if (current && !current.tick.preview)
        current.tick.preview = toExcerpt(item.text);
    }
  });
  return exchanges;
}

/** Assert the projector output equals the canonical projection exactly. */
function assertMatchesCanonical(
  projector: ConversationProjector,
  model: ConversationModel,
  label: string,
) {
  const items = projectConversation(model);
  assert.deepEqual(projector.items, items, `${label}: items`);
  assert.deepEqual(
    projector.copyTextByRunId,
    collectAgentCopyText(model),
    `${label}: copyText`,
  );
  assert.deepEqual(
    projector.exchanges,
    canonicalExchanges(items),
    `${label}: exchanges`,
  );
  const canonicalIndex = new Map(items.map((item, index) => [item.id, index]));
  assert.deepEqual(
    new Map(projector.itemIndexById),
    canonicalIndex,
    `${label}: itemIndexById`,
  );
}

function drive(
  envelopes: BrowserEventEnvelope[],
  { updateEvery = 1 }: { updateEvery?: number } = {},
) {
  let session = createAgentSession();
  const projector = new ConversationProjector();
  envelopes.forEach((envelope, index) => {
    session = apply(session, envelope);
    if ((index + 1) % updateEvery !== 0 && index !== envelopes.length - 1)
      return;
    projector.update(session.conversation);
    assertMatchesCanonical(
      projector,
      session.conversation,
      `event ${index} (${envelope.event.type})`,
    );
  });
  return { session, projector };
}

type DurableEvent = Extract<BrowserEventEnvelope, { seq: number }>["event"];
type VolatileEvent = Exclude<BrowserEventEnvelope, { seq: number }>["event"];

function seq(n: number, event: DurableEvent): BrowserEventEnvelope {
  return { audience: "direct_chat", seq: n, event };
}

function live(event: VolatileEvent): BrowserEventEnvelope {
  return { audience: "direct_chat", event };
}

test("streamed text deltas keep the projection canonical per event", () => {
  drive([
    seq(1, { type: "agent_start" }),
    seq(2, {
      type: "message_start",
      message_id: AssistantMessageId,
      message: assistantMessage(""),
    }),
    live({
      type: "message_update",
      message_id: AssistantMessageId,
      event: { type: "text_start", content_index: 0 },
    }),
    live({
      type: "message_update",
      message_id: AssistantMessageId,
      event: { type: "text_delta", content_index: 0, delta: "hel" },
    }),
    live({
      type: "message_update",
      message_id: AssistantMessageId,
      event: { type: "text_delta", content_index: 0, delta: "lo" },
    }),
    live({
      type: "message_update",
      message_id: AssistantMessageId,
      event: { type: "text_end", content_index: 0, content: "hello" },
    }),
    seq(3, {
      type: "message_end",
      message_id: AssistantMessageId,
      message: assistantMessage("hello"),
    }),
    seq(4, { type: "agent_end" }),
  ]);
});

test("user message, tool calls, and approval resolution stay canonical", () => {
  drive([
    seq(1, { type: "agent_start" }),
    seq(2, {
      type: "message_start",
      message_id: UserMessageId,
      message: userMessage("一覧して"),
    }),
    seq(3, {
      type: "message_end",
      message_id: UserMessageId,
      message: userMessage("一覧して"),
    }),
    seq(4, {
      type: "message_start",
      message_id: AssistantMessageId,
      message: assistantToolCall("call-1"),
    }),
    seq(5, {
      type: "tool_execution_start",
      tool_call_id: "call-1",
      tool_name: "bash",
      args: { command: "ls" },
    }),
    seq(6, {
      type: "approval_requested",
      request: approvalRequest("req-1", "call-1"),
    }),
    seq(7, {
      type: "approval_resolved",
      request_id: "req-1",
      resolution: { decision: { type: "approve_once" } },
    }),
    seq(8, {
      type: "tool_execution_end",
      tool_call_id: "call-1",
      result: { content: "ok" },
      is_error: false,
    }),
    seq(9, {
      type: "message_end",
      message_id: AssistantMessageId,
      message: assistantToolCall("call-1"),
    }),
    live({
      type: "message_update",
      message_id: AssistantMessageId,
      event: { type: "text_start", content_index: 1 },
    }),
    live({
      type: "message_update",
      message_id: AssistantMessageId,
      event: { type: "text_delta", content_index: 1, delta: "done" },
    }),
    seq(10, {
      type: "message_end",
      message_id: AssistantMessageId,
      message: {
        ...assistantMessage(""),
        content: [
          {
            type: "tool_call",
            tool_call: {
              id: "call-1",
              name: "bash",
              route: "normal",
              arguments: {},
            },
            wire_item_index: 0,
          },
          { type: "text", text: "done", wire_item_index: 1 },
        ],
      },
    }),
    seq(11, { type: "agent_end" }),
  ]);
});

test("denied approval consumes the pending row like the canonical path", () => {
  drive([
    seq(1, { type: "agent_start" }),
    seq(2, {
      type: "message_start",
      message_id: AssistantMessageId,
      message: assistantToolCall("call-9"),
    }),
    seq(3, {
      type: "approval_requested",
      request: approvalRequest("req-9", "call-9"),
    }),
    seq(4, {
      type: "approval_resolved",
      request_id: "req-9",
      resolution: { decision: { type: "deny_once" } },
    }),
    seq(5, {
      type: "message_end",
      message_id: AssistantMessageId,
      message: assistantToolCall("call-9"),
    }),
  ]);
});

test("reasoning deltas and deferred tool outcomes stay canonical", () => {
  drive([
    seq(1, { type: "agent_start" }),
    live({
      type: "message_update",
      message_id: AssistantMessageId,
      event: { type: "reasoning_summary_delta", content_index: 0, delta: "考" },
    }),
    live({
      type: "message_update",
      message_id: AssistantMessageId,
      event: {
        type: "reasoning_summary_end",
        content_index: 0,
        content: "考え中",
      },
    }),
    // Tool outcome arrives before its call resolves to a run; reconciled later.
    seq(2, {
      type: "tool_execution_end",
      tool_call_id: "call-late",
      result: { content: "late" },
      is_error: false,
    }),
    seq(3, {
      type: "message_start",
      message_id: AssistantMessageId,
      message: assistantToolCall("call-late"),
    }),
    seq(4, {
      type: "tool_execution_start",
      tool_call_id: "call-late",
      tool_name: "bash",
      args: {},
    }),
    seq(5, {
      type: "message_end",
      message_id: AssistantMessageId,
      message: assistantToolCall("call-late"),
    }),
    seq(6, { type: "agent_end" }),
  ]);
});

test("durable message_end that removes volatile rows rebuilds canonically", () => {
  let session = createAgentSession();
  const projector = new ConversationProjector();
  const step = (envelope: BrowserEventEnvelope, label: string) => {
    session = apply(session, envelope);
    projector.update(session.conversation);
    assertMatchesCanonical(projector, session.conversation, label);
  };

  step(seq(1, { type: "agent_start" }), "agent_start");
  step(
    seq(2, {
      type: "message_start",
      message_id: AssistantMessageId,
      message: assistantMessage(""),
    }),
    "message_start",
  );
  step(
    live({
      type: "message_update",
      message_id: AssistantMessageId,
      event: { type: "text_delta", content_index: 0, delta: "a" },
    }),
    "delta 0",
  );
  step(
    live({
      type: "message_update",
      message_id: AssistantMessageId,
      event: { type: "text_delta", content_index: 1, delta: "b" },
    }),
    "delta 1",
  );
  // The durable body drops index 1 entirely: canonical cleanup removes it.
  step(
    seq(3, {
      type: "message_end",
      message_id: AssistantMessageId,
      message: {
        ...assistantMessage(""),
        content: [{ type: "text", text: "a", wire_item_index: 0 }],
      },
    }),
    "message_end without index 1",
  );
});

test("batched journals and mid-order inserts match canonical projection", () => {
  const envelopes: BrowserEventEnvelope[] = [
    seq(1, { type: "agent_start" }),
    seq(2, {
      type: "message_start",
      message_id: AssistantMessageId,
      message: assistantMessage(""),
    }),
    live({
      type: "message_update",
      message_id: AssistantMessageId,
      event: { type: "text_delta", content_index: 5, delta: "tail" },
    }),
    // Durable replay materializes an earlier content block beside its
    // neighbors — a mid-order insert rather than a tail append.
    seq(3, {
      type: "message_end",
      message_id: AssistantMessageId,
      message: {
        ...assistantMessage(""),
        content: [
          { type: "text", text: "head", wire_item_index: 0 },
          { type: "text", text: "tail", wire_item_index: 5 },
        ],
      },
    }),
    live({
      type: "message_update",
      message_id: AssistantMessageId,
      event: { type: "text_delta", content_index: 0, delta: "more" },
    }),
    seq(4, { type: "agent_end" }),
  ];
  // Update only every third event so journals batch across envelopes.
  drive(envelopes, { updateEvery: 3 });
});

test("model literals without a journal fall back to a full rebuild", () => {
  let session = createAgentSession();
  session = apply(session, seq(1, { type: "agent_start" }));
  session = apply(
    session,
    seq(2, {
      type: "message_start",
      message_id: AssistantMessageId,
      message: assistantMessage(""),
    }),
  );
  session = apply(
    session,
    live({
      type: "message_update",
      message_id: AssistantMessageId,
      event: { type: "text_delta", content_index: 0, delta: "hi" },
    }),
  );
  const literal: ConversationModel = {
    entryOrder: [...session.conversation.entryOrder],
    entries: { ...session.conversation.entries },
    runs: { ...session.conversation.runs },
    runOrder: [...session.conversation.runOrder],
  };
  const projector = new ConversationProjector();
  projector.update(literal);
  assertMatchesCanonical(projector, literal, "literal model");

  // Journaled writes after the rebuild still apply incrementally.
  session = apply(
    session,
    live({
      type: "message_update",
      message_id: AssistantMessageId,
      event: { type: "text_delta", content_index: 0, delta: "!" },
    }),
  );
  projector.update(session.conversation);
  assertMatchesCanonical(projector, session.conversation, "post-literal delta");
});

test("ignored envelopes leave the projected rows untouched", () => {
  let session = createAgentSession();
  session = apply(session, seq(1, { type: "agent_start" }));
  session = apply(
    session,
    seq(2, {
      type: "message_start",
      message_id: AssistantMessageId,
      message: assistantMessage(""),
    }),
  );
  const projector = new ConversationProjector();
  projector.update(session.conversation);
  const items = projector.items;
  // An already-seen seq is ignored entirely.
  const next = apply(
    session,
    seq(2, {
      type: "message_start",
      message_id: AssistantMessageId,
      message: assistantMessage(""),
    }),
  );
  assert.equal(next, session);
  projector.update(session.conversation);
  assert.equal(projector.items, items);
});

test("message_end dropping adjacent streamed blocks leaves no ghost rows", () => {
  let session = createAgentSession();
  const projector = new ConversationProjector();
  const step = (envelope: BrowserEventEnvelope, label: string) => {
    session = apply(session, envelope);
    projector.update(session.conversation);
    assertMatchesCanonical(projector, session.conversation, label);
  };

  step(seq(1, { type: "agent_start" }), "agent_start");
  step(
    seq(2, {
      type: "message_start",
      message_id: AssistantMessageId,
      message: assistantMessage(""),
    }),
    "message_start",
  );
  // Two adjacent streamed prose blocks that the durable copy drops entirely.
  step(
    live({
      type: "message_update",
      message_id: AssistantMessageId,
      event: { type: "text_delta", content_index: 0, delta: "first" },
    }),
    "delta 0",
  );
  step(
    live({
      type: "message_update",
      message_id: AssistantMessageId,
      event: { type: "text_delta", content_index: 1, delta: "second" },
    }),
    "delta 1",
  );
  // The durable copy merges to a single block at a new index; both streamed
  // blocks are stale and adjacent in entryOrder. In-place removal must not
  // skip the second one — the regression was a live-iterator splice dropping
  // every other adjacent stale entry.
  step(
    seq(3, {
      type: "message_end",
      message_id: AssistantMessageId,
      message: {
        ...assistantMessage(""),
        content: [{ type: "text", text: "merged", wire_item_index: 2 }],
      },
    }),
    "message_end dropping both blocks",
  );
  assert.equal(
    session.conversation.entryOrder.includes(`message:${AssistantMessageId}:1`),
    false,
    "adjacent stale streamed block survives in entryOrder",
  );
  assert.equal(
    session.conversation.entries[`message:${AssistantMessageId}:1`],
    undefined,
    "adjacent stale streamed block survives in entries",
  );
  step(seq(4, { type: "agent_end" }), "agent_end");
});

test("a wholesale session replacement clears a mounted projector", () => {
  let session = createAgentSession();
  const projector = new ConversationProjector();
  session = apply(session, seq(1, { type: "agent_start" }));
  session = apply(
    session,
    seq(2, {
      type: "message_start",
      message_id: UserMessageId,
      message: userMessage("previous authority transcript"),
    }),
  );
  session = apply(
    session,
    seq(3, {
      type: "message_end",
      message_id: UserMessageId,
      message: userMessage("previous authority transcript"),
    }),
  );
  projector.update(session.conversation);
  assert.equal(projector.items.length > 0, true);

  // resetAuthority installs createAgentSession()'s fresh model; its journal is
  // structural, so the projector rescans instead of keeping stale rows.
  projector.update(createAgentSession().conversation);
  assert.equal(
    projector.items.length,
    0,
    "stale transcript rows survive a session reset",
  );

  // A run-only write on the new session must not extend the old rows.
  let next = createAgentSession();
  next = apply(next, seq(1, { type: "agent_start" }));
  projector.update(next.conversation);
  assertMatchesCanonical(
    projector,
    next.conversation,
    "post-reset agent_start",
  );
});

test("a projector first mounted mid-stream takes a canonical snapshot", () => {
  let session = createAgentSession();
  session = apply(session, seq(1, { type: "agent_start" }));
  session = apply(
    session,
    seq(2, {
      type: "message_start",
      message_id: UserMessageId,
      message: userMessage("hi"),
    }),
  );
  const owner = new ConversationProjector();
  owner.update(session.conversation);

  // A second consumer arrives after the owner drained the journal; its first
  // update must still produce the full transcript, not an empty one.
  const late = new ConversationProjector();
  late.update(session.conversation);
  assertMatchesCanonical(late, session.conversation, "late-mounted projector");
  assert.deepEqual(late.items, owner.items);
});
