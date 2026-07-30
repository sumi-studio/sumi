/// <reference types="node" />

import assert from "node:assert/strict";
import test from "node:test";
import type {
  ApprovalRequest,
  BrowserEventEnvelope,
  PublicAssistantMessage,
  PublicMessage,
} from "@sumi/api-client";
import { projectConversation } from "./projection";
import { createAgentSession, reduceEnvelope } from "./reducer";

const id = () => "deterministic";

test("durable message_end replaces volatile text and durable replay is ignored", () => {
  let session = createAgentSession();
  session = apply(session, {
    seq: 1,
    event: { type: "agent_start" },
  });
  session = apply(session, {
    seq: 2,
    event: {
      type: "message_start",
      message_id: AssistantMessageId,
      message: assistantMessage(""),
    },
  });
  session = apply(session, {
    event: {
      type: "message_update",
      message_id: AssistantMessageId,
      event: { type: "text_delta", content_index: 0, delta: "volatile" },
    },
  });
  session = apply(session, {
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
  const prose = session.conversation.entries[`message:${AssistantMessageId}`];
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
    ["agent-run", "prose"],
  );
});

test("durable tool start and end upsert without volatile tool-call events", () => {
  let session = createAgentSession();
  session = apply(session, {
    seq: 10,
    event: { type: "agent_start" },
  });
  session = apply(session, {
    seq: 11,
    event: {
      type: "tool_execution_start",
      tool_call_id: "call-1",
      tool_name: "read_file",
      args: { path: "README.md" },
    },
  });
  session = apply(session, {
    seq: 12,
    event: {
      type: "tool_execution_end",
      tool_call_id: "call-1",
      result: { content: "ok" },
      is_error: false,
    },
  });
  session = apply(session, {
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

test("approval request and resolution preserve structured decision", () => {
  let session = createAgentSession();
  session = apply(session, {
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
    seq: 21,
    event: { type: "approval_requested", request },
  });
  session = apply(session, {
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

test("cancelled approval closes its linked pending tool trace and durable replay is ignored", () => {
  let session = createAgentSession();
  const request = approvalRequest("approval-cancelled", "call-cancelled");
  session = apply(session, {
    seq: 40,
    event: { type: "agent_start" },
  });
  session = apply(session, {
    seq: 41,
    event: {
      type: "message_end",
      message_id: "00000000-0000-4000-8000-000000000012",
      message: assistantToolCall("call-cancelled"),
    },
  });
  session = apply(session, {
    seq: 42,
    event: { type: "approval_requested", request },
  });
  const resolution: BrowserEventEnvelope = {
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
    seq: 50,
    event: { type: "agent_start" },
  });
  session = apply(session, {
    seq: 51,
    event: {
      type: "tool_execution_start",
      tool_call_id: request.tool_call_id,
      tool_name: request.tool_name,
      args: {},
    },
  });
  session = apply(session, {
    seq: 52,
    event: { type: "approval_requested", request },
  });
  session = apply(session, {
    seq: 53,
    event: {
      type: "approval_resolved",
      request_id: request.id,
      resolution: { decision: { type: "deny" } },
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

function apply(
  session: ReturnType<typeof createAgentSession>,
  envelope: BrowserEventEnvelope,
) {
  return reduceEnvelope(session, envelope, { id }).session;
}

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
        tool_call: { id: toolCallId, name: "bash", arguments: {} },
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
