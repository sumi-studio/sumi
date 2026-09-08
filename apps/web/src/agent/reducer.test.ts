/// <reference types="node" />

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import type {
  ApprovalRequest,
  BrowserEventEnvelope,
  PublicAssistantMessage,
  PublicMessage,
  PublicStreamEvent,
} from "@sumi/api-client";
import { projectConversation } from "./projection";
import { createAgentSession, reduceEnvelope } from "./reducer";

const id = () => "deterministic";

test("durable message_end replaces volatile text and durable replay is ignored", () => {
  let session = createAgentSession();
  session = apply(session, {
    audience: "direct_chat",
    seq: 1,
    event: { type: "agent_start" },
  });
  session = apply(session, {
    audience: "direct_chat",
    seq: 2,
    event: {
      type: "message_start",
      message_id: AssistantMessageId,
      message: assistantMessage(""),
    },
  });
  session = apply(session, {
    audience: "direct_chat",
    event: {
      type: "message_update",
      message_id: AssistantMessageId,
      event: { type: "text_delta", content_index: 0, delta: "volatile" },
    },
  });
  session = apply(session, {
    audience: "direct_chat",
    event: {
      type: "message_update",
      message_id: AssistantMessageId,
      event: {
        type: "thinking_delta",
        content_index: 1,
        delta: "private chain of thought",
      },
    },
  });
  session = apply(session, {
    audience: "direct_chat",
    event: {
      type: "message_update",
      message_id: AssistantMessageId,
      event: {
        type: "reasoning_summary_end",
        content_index: 2,
        content: "確認しました",
      },
    },
  });

  const completed: BrowserEventEnvelope = {
    audience: "direct_chat",
    seq: 3,
    event: {
      type: "message_end",
      message_id: AssistantMessageId,
      message: assistantMessage("durable truth"),
    },
  };
  session = apply(session, completed);
  const afterFirstEnd = session;
  session = apply(session, completed);

  assert.equal(session, afterFirstEnd);
  const prose = session.conversation.entries[`message:${AssistantMessageId}:0`];
  assert.equal(prose?.kind, "prose");
  if (prose?.kind === "prose") {
    assert.equal(prose.text, "durable truth");
    assert.equal(prose.streaming, false);
    assert.equal(prose.timestamp, Timestamp);
  }
  const run = session.conversation.runs["run:1"];
  assert.equal(
    run.trace.filter((entry) => entry.type === "reasoning").length,
    1,
  );
  assert.equal(
    run.trace.some(
      (entry) =>
        entry.type === "reasoning" &&
        entry.text.includes("private chain of thought"),
    ),
    false,
  );
  assert.deepEqual(
    projectConversation(session.conversation).map((item) => item.kind),
    ["agent-run", "prose", "trace"],
  );
});

test("tool call projections retain normal and elevated as distinct routes", () => {
  let session = createAgentSession();
  session = apply(session, {
    audience: "direct_chat",
    seq: 1,
    event: { type: "agent_start" },
  });
  for (const [contentIndex, route] of (
    ["normal", "elevated"] as const
  ).entries()) {
    session = apply(session, {
      audience: "direct_chat",
      event: {
        type: "message_update",
        message_id: AssistantMessageId,
        event: {
          type: "tool_call_end",
          content_index: contentIndex,
          tool_call: {
            id: `call-${route}`,
            name: "read_file",
            route,
            arguments: { path: `${route}.txt` },
          },
        },
      },
    });
  }

  const routes = session.conversation.runs["run:1"].trace
    .filter((trace) => trace.type === "tool")
    .map((trace) => trace.route);
  assert.deepEqual(routes, ["normal", "elevated"]);
});

test("late empty message_start preserves update-before-start prose until authoritative end", () => {
  let session = createAgentSession();
  session = apply(session, {
    audience: "direct_chat",
    seq: 1,
    event: { type: "agent_start" },
  });
  session = apply(session, {
    audience: "direct_chat",
    event: {
      type: "message_update",
      message_id: AssistantMessageId,
      event: { type: "text_delta", content_index: 0, delta: "before start" },
    },
  });
  session = apply(session, {
    audience: "direct_chat",
    seq: 2,
    event: {
      type: "message_start",
      message_id: AssistantMessageId,
      message: assistantMessage(""),
    },
  });
  session = apply(session, {
    audience: "direct_chat",
    seq: 3,
    event: {
      type: "message_start",
      message_id: AssistantMessageId,
      message: assistantMessage(""),
    },
  });
  session = apply(session, {
    audience: "direct_chat",
    event: {
      type: "message_update",
      message_id: AssistantMessageId,
      event: { type: "text_delta", content_index: 0, delta: " and after" },
    },
  });

  const streaming =
    session.conversation.entries[`message:${AssistantMessageId}:0`];
  assert.equal(streaming?.kind, "prose");
  if (streaming?.kind === "prose") {
    assert.equal(streaming.text, "before start and after");
    assert.equal(streaming.streaming, true);
  }

  session = apply(session, {
    audience: "direct_chat",
    seq: 4,
    event: {
      type: "message_end",
      message_id: AssistantMessageId,
      message: assistantMessage(""),
    },
  });
  assert.equal(
    session.conversation.entries[`message:${AssistantMessageId}:0`],
    undefined,
  );
});

test("durable empty provider failure is visible instead of disappearing", () => {
  let session = createAgentSession();
  session = apply(session, {
    audience: "direct_chat",
    seq: 1,
    event: { type: "agent_start" },
  });
  session = apply(session, {
    audience: "direct_chat",
    seq: 2,
    event: {
      type: "message_start",
      message_id: AssistantMessageId,
      message: assistantMessage(""),
    },
  });
  const failed: BrowserEventEnvelope = {
    audience: "direct_chat",
    seq: 3,
    event: {
      type: "message_end",
      message_id: AssistantMessageId,
      message: {
        ...assistantMessage(""),
        origin: {
          provider_instance_id: "opencode-go",
          protocol: "open_ai_chat_completions",
          model: "kimi-k2.7-code",
        },
        stop_reason: "error",
        error_message: "Provider request failed",
        provider_code: "provider_error",
      },
    },
  };

  session = apply(session, failed);
  const afterFirstEnd = session;
  session = apply(session, failed);

  assert.equal(session, afterFirstEnd);
  assert.equal(
    session.conversation.entries[`message:${AssistantMessageId}:0`],
    undefined,
  );
  const error =
    session.conversation.entries[`message-error:${AssistantMessageId}`];
  assert.deepEqual(error, {
    kind: "error",
    id: `message-error:${AssistantMessageId}`,
    runId: "run:1",
    message: "Provider request failed (provider_error)",
    retryable: false,
  });
  assert.deepEqual(
    projectConversation(session.conversation).map((item) => item.kind),
    ["agent-run", "error"],
  );
});

test("durable tool start and end upsert without volatile tool-call events", () => {
  let session = createAgentSession();
  session = apply(session, {
    audience: "direct_chat",
    seq: 10,
    event: { type: "agent_start" },
  });
  session = apply(session, {
    audience: "direct_chat",
    seq: 11,
    event: {
      type: "tool_execution_start",
      tool_call_id: "call-1",
      tool_name: "read_file",
      args: { path: "README.md" },
    },
  });
  session = apply(session, {
    audience: "direct_chat",
    seq: 12,
    event: {
      type: "tool_execution_end",
      tool_call_id: "call-1",
      result: { content: "ok" },
      is_error: false,
    },
  });
  session = apply(session, {
    audience: "direct_chat",
    seq: 13,
    event: {
      type: "tool_execution_end",
      tool_call_id: "end-without-start",
      result: "failed",
      is_error: true,
    },
  });

  const trace = session.conversation.runs["run:10"].trace;
  const first = trace.find(
    (entry) => entry.type === "tool" && entry.id === "call-1",
  );
  const second = trace.find(
    (entry) => entry.type === "tool" && entry.id === "end-without-start",
  );
  assert.equal(first?.type, "tool");
  if (first?.type === "tool") {
    assert.equal(first.name, "read_file");
    assert.equal(first.status, "done");
    assert.equal(first.label, "read_fileを完了");
    assert.deepEqual(first.args, { path: "README.md" });
    assert.deepEqual(first.result, { content: "ok" });
  }
  assert.equal(second?.type, "tool");
  if (second?.type === "tool") assert.equal(second.status, "error");
});

test("valid SDUI materializes while adversarial SDUI validation stays inert", () => {
  const valid = {
    type: "list",
    props: { title: "Tasks", items: [{ text: "Bounded" }] },
  };
  const tooManyItems = {
    type: "list",
    props: {
      items: Array.from({ length: 101 }, (_, index) => ({
        text: `item-${index}`,
      })),
    },
  };
  const tooManyActions = {
    type: "reminder",
    props: {
      title: "Reminder",
      at: "2026-08-01T09:00:00Z",
      actions: Array.from({ length: 9 }, (_, index) => ({
        label: `action-${index}`,
        action: `action:${index}`,
      })),
    },
  };
  const oversizedString = {
    type: "confirm",
    props: {
      title: "x".repeat(257),
      confirm: { label: "yes", action: "confirm" },
      cancel: { label: "no", action: "cancel" },
    },
  };
  let deeplyNested: Record<string, unknown> = {
    type: "list",
    props: { items: [] },
  };
  for (let depth = 0; depth < 10_000; depth += 1) {
    deeplyNested = {
      type: "list",
      props: { items: [] },
      children: [deeplyNested],
    };
  }

  const payloads: Array<{ name: string; node: unknown; accepted: boolean }> = [
    { name: "valid", node: valid, accepted: true },
    { name: "deep children", node: deeplyNested, accepted: false },
    { name: "wide items", node: tooManyItems, accepted: false },
    { name: "wide actions", node: tooManyActions, accepted: false },
    { name: "oversized string", node: oversizedString, accepted: false },
    {
      name: "prototype constructor",
      node: { type: "constructor", props: {} },
      accepted: true,
    },
    {
      name: "prototype toString",
      node: { type: "toString", props: {} },
      accepted: true,
    },
  ];

  for (const { name, node, accepted } of payloads) {
    let session = createAgentSession();
    session = apply(session, {
      audience: "direct_chat",
      seq: 60,
      event: { type: "agent_start" },
    });
    assert.doesNotThrow(() => {
      session = apply(session, {
        audience: "direct_chat",
        seq: 61,
        event: {
          type: "tool_execution_end",
          tool_call_id: `call-${name}`,
          result: { sdui: node } as never,
          is_error: false,
        },
      });
    });
    const card = session.conversation.entries[`card:call-${name}`];
    assert.equal(card?.kind === "card", accepted, name);
  }
});

test("approval request and resolution preserve structured decision", () => {
  let session = createAgentSession();
  session = apply(session, {
    audience: "direct_chat",
    seq: 20,
    event: { type: "agent_start" },
  });
  const request: ApprovalRequest = {
    id: "approval-1",
    tool_call_id: "call-approval",
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
  session = apply(session, {
    audience: "direct_chat",
    seq: 21,
    event: { type: "approval_requested", request },
  });
  session = apply(session, {
    audience: "direct_chat",
    seq: 22,
    event: {
      type: "approval_resolved",
      request_id: request.id,
      resolution: { decision: { type: "approve_once" } },
    },
  });

  assert.equal(session.approval, null);
  const entry = session.conversation.entries[`approval:${request.id}`];
  assert.equal(entry?.kind, "approval");
  if (entry?.kind === "approval") {
    assert.equal(entry.status, "allowed");
    assert.deepEqual(entry.decision, { type: "approve_once" });
    assert.deepEqual(entry.request, request);
  }
});

test("execution rejection preserves the Human's exact approval decision", () => {
  let session = createAgentSession();
  const request = approvalRequest("approval-rejected", "call-rejected");
  session = apply(session, {
    audience: "direct_chat",
    seq: 30,
    event: { type: "agent_start" },
  });
  session = apply(session, {
    audience: "direct_chat",
    seq: 31,
    event: { type: "approval_requested", request },
  });
  session = apply(session, {
    audience: "direct_chat",
    seq: 32,
    event: {
      type: "approval_resolved",
      request_id: request.id,
      resolution: { rejected: { decision: { type: "approve_once" } } },
    },
  });

  assert.equal(session.approval, null);
  const entry = session.conversation.entries[`approval:${request.id}`];
  assert.equal(entry?.kind, "approval");
  if (entry?.kind === "approval") {
    assert.equal(entry.status, "rejected");
    assert.deepEqual(entry.decision, { type: "approve_once" });
  }
});

test("cancelled approval closes its linked pending tool trace and durable replay is ignored", () => {
  let session = createAgentSession();
  const request = approvalRequest("approval-cancelled", "call-cancelled");
  session = apply(session, {
    audience: "direct_chat",
    seq: 40,
    event: { type: "agent_start" },
  });
  session = apply(session, {
    audience: "direct_chat",
    seq: 41,
    event: {
      type: "message_end",
      message_id: "00000000-0000-4000-8000-000000000012",
      message: assistantToolCall("call-cancelled"),
    },
  });
  session = apply(session, {
    audience: "direct_chat",
    seq: 42,
    event: { type: "approval_requested", request },
  });
  const resolution: BrowserEventEnvelope = {
    audience: "direct_chat",
    seq: 43,
    event: {
      type: "approval_resolved",
      request_id: request.id,
      resolution: "cancelled",
    },
  };
  session = apply(session, resolution);
  const resolved = session;
  session = apply(session, resolution);

  assert.equal(session, resolved);
  const approval = session.conversation.entries[`approval:${request.id}`];
  const tool = session.conversation.runs["run:40"].trace.find(
    (entry) => entry.type === "tool" && entry.id === request.tool_call_id,
  );
  assert.equal(approval?.kind, "approval");
  if (approval?.kind === "approval") assert.equal(approval.status, "cancelled");
  assert.equal(tool?.type, "tool");
  if (tool?.type === "tool") assert.equal(tool.status, "cancelled");
});

test("denied approval closes its linked running tool trace", () => {
  let session = createAgentSession();
  const request = approvalRequest("approval-denied", "call-denied");
  session = apply(session, {
    audience: "direct_chat",
    seq: 50,
    event: { type: "agent_start" },
  });
  session = apply(session, {
    audience: "direct_chat",
    seq: 51,
    event: {
      type: "tool_execution_start",
      tool_call_id: request.tool_call_id,
      tool_name: request.tool_name,
      args: {},
    },
  });
  session = apply(session, {
    audience: "direct_chat",
    seq: 52,
    event: { type: "approval_requested", request },
  });
  session = apply(session, {
    audience: "direct_chat",
    seq: 53,
    event: {
      type: "approval_resolved",
      request_id: request.id,
      resolution: { decision: { type: "deny_once" } },
    },
  });

  const tool = session.conversation.runs["run:50"].trace.find(
    (entry) => entry.type === "tool" && entry.id === request.tool_call_id,
  );
  assert.equal(tool?.type, "tool");
  if (tool?.type === "tool") assert.equal(tool.status, "cancelled");
});

test("durable user messages materialize once under their server message id", () => {
  let session = createAgentSession();
  const envelope: BrowserEventEnvelope = {
    audience: "direct_chat",
    seq: 30,
    event: {
      type: "message_end",
      message_id: UserMessageId,
      message: userMessage("hello"),
    },
  };
  session = apply(session, envelope);
  session = apply(session, envelope);
  assert.deepEqual(session.conversation.entryOrder, [UserMessageId]);
  assert.equal(session.conversation.entries[UserMessageId]?.kind, "user");
});

test("durable command disposition advances the cursor without entering conversation", () => {
  let session = createAgentSession();
  const envelope: BrowserEventEnvelope = {
    audience: "direct_chat",
    seq: 30,
    event: {
      type: "command_disposition",
      command_id: CommandId,
      command_seq: 9,
      status: "superseded",
    },
  };
  session = apply(session, envelope);
  const afterFirst = session;
  session = apply(session, envelope);

  assert.equal(session, afterFirst);
  assert.equal(session.lastDurableSeq, 30);
  assert.deepEqual(session.conversation.entryOrder, []);
  assert.deepEqual(session.conversation.runOrder, []);
  assert.equal("commandDispositions" in session, false);
});

test("lifetime command dispositions retain no reducer correlation history", () => {
  let session = createAgentSession();
  for (let sequence = 1; sequence <= 10_000; sequence++) {
    session = apply(session, {
      audience: "direct_chat",
      seq: sequence,
      event: {
        type: "command_disposition",
        command_id: CommandId,
        command_seq: sequence,
        status: "applied",
      },
    });
  }

  assert.equal("commandDispositions" in session, false);
  assert.equal(session.lastDurableSeq, 10_000);
  assert.deepEqual(session.conversation.entryOrder, []);
  assert.deepEqual(session.conversation.runOrder, []);
});

function apply(
  session: ReturnType<typeof createAgentSession>,
  envelope: BrowserEventEnvelope,
) {
  return reduceEnvelope(session, envelope, { id }).session;
}

const CommandId = "00000000-0000-4000-8000-000000000001";

function assistantMessage(text: string): PublicAssistantMessage {
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

function userMessage(text: string): PublicMessage {
  return {
    role: "user",
    content: [{ type: "text", text }],
    timestamp: Timestamp,
  };
}

const AssistantMessageId = "00000000-0000-4000-8000-000000000010";
const UserMessageId = "00000000-0000-4000-8000-000000000011";
const Timestamp = "2026-07-30T12:00:00Z";

test("prose, public summary, tool operation, intervening prose and result retain their event positions", () => {
  let session = createAgentSession();
  const update = (event: PublicStreamEvent) => {
    session = apply(session, {
      audience: "secretary",
      event: { type: "message_update", message_id: AssistantMessageId, event },
    });
  };
  session = apply(session, {
    audience: "secretary",
    seq: 1,
    event: { type: "agent_start" },
  });
  update({ type: "text_delta", content_index: 0, delta: "First" });
  update({
    type: "reasoning_summary_end",
    content_index: 1,
    content: "Check the source",
  });
  update({
    type: "tool_call_end",
    content_index: 2,
    tool_call: {
      id: "read",
      name: "read_file",
      route: "normal",
      arguments: { path: "note" },
    },
  });
  update({ type: "text_delta", content_index: 3, delta: "While waiting" });
  session = apply(session, {
    audience: "secretary",
    seq: 2,
    event: {
      type: "tool_execution_end",
      tool_call_id: "read",
      result: "Source result",
      is_error: false,
    },
  });
  update({ type: "text_delta", content_index: 4, delta: "After result" });
  const labels = projectConversation(session.conversation)
    .filter((row) => row.kind !== "agent-run")
    .map((row) =>
      row.kind === "prose"
        ? row.text
        : row.kind === "trace"
          ? row.trace.type === "reasoning"
            ? row.trace.text
            : `${row.traceId}:${row.phase}`
          : row.kind,
    );
  assert.deepEqual(labels, [
    "First",
    "Check the source",
    "read:activity",
    "While waiting",
    "read:result",
    "After result",
  ]);
  assert.equal(session.conversation.runs["run:1"].audience, "secretary");
});

test("canonical message replay preserves text/tool wire order without grouping all prose", () => {
  let session = apply(createAgentSession(), {
    audience: "direct_chat",
    seq: 1,
    event: { type: "agent_start" },
  });
  const message: PublicAssistantMessage = {
    ...assistantMessage(""),
    content: [
      { type: "text", text: "Before", wire_item_index: 0 },
      {
        type: "tool_call",
        wire_item_index: 1,
        tool_call: {
          id: "read",
          name: "read_file",
          route: "normal",
          arguments: {},
        },
      },
      { type: "text", text: "After", wire_item_index: 2 },
    ],
  };
  session = apply(session, {
    audience: "direct_chat",
    seq: 2,
    event: { type: "message_end", message_id: AssistantMessageId, message },
  });
  assert.deepEqual(
    projectConversation(session.conversation)
      .filter((row) => row.kind !== "agent-run")
      .map((row) => (row.kind === "prose" ? row.text : row.kind)),
    ["Before", "trace", "After"],
  );
});

test("external source remains attached to its own log input across replay", () => {
  const fixture = JSON.parse(
    readFileSync(
      new URL(
        "../../../../contracts/agent-events-fixtures.json",
        import.meta.url,
      ),
      "utf8",
    ),
  ).external_dm;
  const message: PublicMessage = {
    role: "user",
    content: [{ type: "text", text: "External speaker" }],
    timestamp: Timestamp,
    incoming_source: fixture.wire.provenance,
  };
  const envelope: BrowserEventEnvelope = {
    audience: "secretary",
    seq: 1,
    event: { type: "message_end", message_id: AssistantMessageId, message },
  };
  const session = apply(createAgentSession(), envelope);
  const entry = session.conversation.entries[AssistantMessageId];
  assert.equal(entry.kind, "user");
  if (entry.kind !== "user") throw new Error("missing input");
  assert.deepEqual(entry.source, fixture.wire.provenance);
  assert.equal(entry.idempotencyKey, undefined);
  assert.equal(apply(session, envelope), session);
});

test("durable summary upgrades the live block and replay restores its position", () => {
  const start: BrowserEventEnvelope = {
    audience: "secretary",
    seq: 1,
    event: { type: "agent_start" },
  };
  const summary: BrowserEventEnvelope = {
    audience: "secretary",
    seq: 2,
    event: {
      type: "reasoning_summary",
      message_id: AssistantMessageId,
      content_index: 0,
      wire_item_index: 1,
      content: "Check first",
    },
  };
  const operation: BrowserEventEnvelope = {
    audience: "secretary",
    seq: 3,
    event: {
      type: "tool_execution_start",
      tool_call_id: "check",
      tool_name: "read_file",
      args: { path: "note" },
    },
  };
  let live = apply(createAgentSession(), start);
  live = apply(live, {
    audience: "secretary",
    event: {
      type: "message_update",
      message_id: AssistantMessageId,
      event: {
        type: "reasoning_summary_delta",
        content_index: 0,
        delta: "Check",
      },
    },
  });
  const ids = live.conversation.entryOrder;
  live = apply(live, summary);
  assert.deepEqual(live.conversation.entryOrder, ids);
  live = apply(live, operation);
  let replay = createAgentSession();
  for (const frame of [start, summary, operation])
    replay = apply(replay, frame);
  assert.deepEqual(
    projectConversation(replay.conversation),
    projectConversation(live.conversation),
  );
  assert.equal(apply(live, summary), live);
});

test("summary slot zero and text wire zero do not collide during canonical replay", () => {
  const start: BrowserEventEnvelope = {
    audience: "secretary",
    seq: 1,
    event: { type: "agent_start" },
  };
  const summary: BrowserEventEnvelope = {
    audience: "secretary",
    seq: 2,
    event: {
      type: "reasoning_summary",
      message_id: AssistantMessageId,
      content_index: 0,
      wire_item_index: 1,
      content: "Reason",
    },
  };
  const end: BrowserEventEnvelope = {
    audience: "secretary",
    seq: 3,
    event: {
      type: "message_end",
      message_id: AssistantMessageId,
      message: {
        ...assistantMessage(""),
        content: [
          { type: "text", text: "Before", wire_item_index: 0 },
          { type: "text", text: "After", wire_item_index: 2 },
        ],
      },
    },
  };
  let live = apply(createAgentSession(), start);
  live = apply(live, {
    audience: "secretary",
    event: {
      type: "message_update",
      message_id: AssistantMessageId,
      event: { type: "text_delta", content_index: 0, delta: "Before" },
    },
  });
  live = apply(live, summary);
  live = apply(live, {
    audience: "secretary",
    event: {
      type: "message_update",
      message_id: AssistantMessageId,
      event: { type: "text_delta", content_index: 2, delta: "After" },
    },
  });
  const liveOrder = live.conversation.entryOrder;
  live = apply(live, end);
  assert.deepEqual(live.conversation.entryOrder, liveOrder);
  let replay = createAgentSession();
  for (const frame of [start, summary, end]) replay = apply(replay, frame);
  assert.deepEqual(
    projectConversation(replay.conversation),
    projectConversation(live.conversation),
  );
});

test("canonical rejected operation stays between its surrounding prose blocks", () => {
  let session = apply(createAgentSession(), {
    audience: "direct_chat",
    seq: 1,
    event: { type: "agent_start" },
  });
  const message: PublicAssistantMessage = {
    ...assistantMessage(""),
    content: [
      { type: "text", text: "Before", wire_item_index: 0 },
      {
        type: "rejected_tool_call",
        wire_item_index: 1,
        rejected: { id: "bad", name: "read_file", error: "invalid_json" },
      },
      { type: "text", text: "After", wire_item_index: 2 },
    ],
  };
  session = apply(session, {
    audience: "direct_chat",
    seq: 2,
    event: { type: "message_end", message_id: AssistantMessageId, message },
  });
  assert.deepEqual(
    projectConversation(session.conversation)
      .filter((row) => row.kind !== "agent-run")
      .map((row) => (row.kind === "prose" ? row.text : row.kind)),
    ["Before", "trace", "After"],
  );
});
