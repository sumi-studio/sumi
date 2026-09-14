import assert from "node:assert/strict";
import test from "node:test";
import type { PublicAssistantMessage } from "@sumi/api-client";
import type { DirectChatHistoryPage } from "../lib/direct-chat-history";
import { mergeHistory, projectHistory } from "./history";
import { createAgentSession, reduceEnvelope } from "./reducer";

const timestamp = "2026-09-12T00:00:00Z";
const message: PublicAssistantMessage = {
  role: "assistant",
  content: [
    {
      type: "tool_call",
      wire_item_index: 0,
      tool_call: {
        id: "old-call",
        name: "bash",
        route: "normal",
        arguments: {},
      },
    },
  ],
  model: "test",
  provider: "openai",
  origin: {
    provider_instance_id: "test",
    protocol: "open_ai_responses",
    model: "test",
  },
  usage: {
    input: 0,
    output: 0,
    cache_read: 0,
    cache_write: 0,
    reasoning: 0,
    total_tokens: 0,
  },
  stop_reason: "tool_use",
  error_message: null,
  provider_code: null,
  interrupted: false,
  timestamp,
};
test("a late old operation cannot attach to the current run and resolves when its page arrives", () => {
  let current = reduceEnvelope(createAgentSession(), {
    seq: 100,
    audience: "direct_chat",
    event: { type: "agent_start" },
  }).session;
  const result = {
    tool_call_id: "old-call",
    tool_name: "bash",
    content: [{ type: "text" as const, text: "Finished" }],
    details: {},
    is_error: false,
    timestamp,
  };
  current = reduceEnvelope(current, {
    seq: 101,
    audience: "direct_chat",
    event: {
      type: "tool_execution_end",
      tool_call_id: "old-call",
      result,
      is_error: false,
    },
  }).session;
  current = reduceEnvelope(current, {
    seq: 102,
    audience: "secretary",
    event: {
      type: "approval_operation_outcome",
      operation_id: "operation-old",
      tool_call_id: "old-call",
      status: "succeeded",
      executed: true,
      result,
    },
  }).session;
  assert.equal(current.conversation.runs["run:100"].trace.length, 0);
  assert.ok(current.unresolvedToolOutcomes["old-call"]);
  assert.ok(current.unresolvedApprovalOutcomes["operation-old"]);
  const page: DirectChatHistoryPage = {
    events: [
      {
        seq: 2,
        audience: "direct_chat",
        event: {
          type: "message_end",
          message_id: "00000000-0000-4000-8000-000000000001",
          message,
        },
      },
    ],
    context: [
      { seq: 1, audience: "direct_chat", event: { type: "agent_start" } },
    ],
    latestSeq: 102,
    beforeSeq: 2,
    hasMore: false,
    index: [],
    activeRun: null,
    pendingApprovals: [],
  };
  const older = projectHistory(page);
  const merged = mergeHistory(current, older.session, older.entrySeq);
  assert.equal(merged.activeRunId, "run:100");
  assert.equal(merged.conversation.runs["run:100"].trace.length, 0);
  const tool = merged.conversation.runs["run:1"].trace[0];
  assert.equal(tool.type, "tool");
  assert.equal(tool.status, "done");
  assert.equal(merged.toolRunIds["old-call"], "run:1");
  assert.deepEqual(merged.unresolvedApprovalOutcomes, {});
  assert.deepEqual(merged.unresolvedToolOutcomes, {});
});

test("context events that create rows stay out of the projected window", () => {
  const userText = (text: string) => ({
    role: "user" as const,
    content: [{ type: "text" as const, text }],
    timestamp,
  });
  const assistantText = (text: string) => ({
    ...message,
    content: [{ type: "text" as const, text, wire_item_index: 0 }],
    stop_reason: "stop" as const,
  });
  // The window's dependency context is a complete earlier turn plus a
  // tool activity + pending approval — all of which create entryOrder rows
  // that must not be marked visible by the replay loop.
  const page: DirectChatHistoryPage = {
    events: [
      { seq: 10, audience: "direct_chat", event: { type: "agent_start" } },
      {
        seq: 11,
        audience: "direct_chat",
        event: {
          type: "message_end",
          message_id: "win-u",
          message: userText("window question"),
        },
      },
      {
        seq: 12,
        audience: "direct_chat",
        event: {
          type: "message_end",
          message_id: "win-a",
          message: assistantText("window answer"),
        },
      },
      { seq: 13, audience: "direct_chat", event: { type: "agent_end" } },
    ],
    context: [
      { seq: 1, audience: "direct_chat", event: { type: "agent_start" } },
      {
        seq: 2,
        audience: "direct_chat",
        event: {
          type: "message_end",
          message_id: "ctx-u",
          message: userText("context question"),
        },
      },
      {
        seq: 3,
        audience: "direct_chat",
        event: {
          type: "tool_execution_start",
          tool_call_id: "ctx-call",
          tool_name: "bash",
          args: { command: "ls" },
        },
      },
      {
        seq: 4,
        audience: "direct_chat",
        event: {
          type: "approval_requested",
          request: {
            id: "ctx-req",
            tool_call_id: "ctx-call",
            tool_name: "bash",
            action: { reviewable: { command: "ls" } },
            args_summary: { command: "ls" },
            reason: "shell access",
            audit: {
              outcome: "allow",
              risk: "low",
              authorization: "medium",
              rationale: "read only",
            },
          },
        },
      },
      {
        seq: 5,
        audience: "direct_chat",
        event: {
          type: "message_end",
          message_id: "ctx-a",
          message: assistantText("context answer"),
        },
      },
      { seq: 6, audience: "direct_chat", event: { type: "agent_end" } },
    ],
    latestSeq: 13,
    beforeSeq: 10,
    hasMore: false,
    index: [],
    activeRun: null,
    pendingApprovals: [],
  };
  const older = projectHistory(page);
  const order = older.session.conversation.entryOrder;
  assert.deepEqual(
    order.filter(
      (id) =>
        id === "ctx-u" ||
        id.startsWith("message:ctx-") ||
        id.startsWith("trace:run:1") ||
        id.startsWith("approval:"),
    ),
    [],
    "context-created rows leaked into the visible window",
  );
  assert.ok(order.includes("win-u"));
  assert.ok(order.includes("message:win-a:0"));

  // Merge/live continuation: context ids must not resurface above the window.
  const live = reduceEnvelope(createAgentSession(), {
    seq: 14,
    audience: "direct_chat",
    event: { type: "agent_start" },
  }).session;
  const merged = mergeHistory(live, older.session, older.entrySeq);
  assert.deepEqual(
    merged.conversation.entryOrder.filter(
      (id) => id === "ctx-u" || id.startsWith("message:ctx-"),
    ),
    [],
    "context rows resurfaced through mergeHistory",
  );
  assert.ok(merged.conversation.entryOrder.includes("win-u"));
});

test("history decoder keeps sparse sequences and rejects state from beyond the snapshot", async () => {
  const { parseDirectChatHistoryPage } = await import(
    "../lib/direct-chat-history"
  );
  const raw = {
    events: [
      { seq: 10, audience: "direct_chat", event: { type: "agent_start" } },
    ],
    context: [],
    latest_seq: 20,
    before_seq: 10,
    has_more: true,
    index: [],
    active_run: null,
    pending_approvals: [],
    command_dispositions: [],
  };
  assert.equal(parseDirectChatHistoryPage(raw).events[0].seq, 10);
  assert.throws(() => parseDirectChatHistoryPage({ ...raw, latest_seq: 9 }));
  assert.throws(() =>
    parseDirectChatHistoryPage({ ...raw, has_more: true, before_seq: null }),
  );
  assert.throws(() =>
    parseDirectChatHistoryPage({ ...raw, pending_approvals: raw.events }),
  );
});
