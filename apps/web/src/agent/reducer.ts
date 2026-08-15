import type {
  AnyJSON,
  ApprovalDecision,
  ApprovalRequest,
  BrowserEventEnvelope,
  PublicAssistantMessage,
  PublicMessage,
  PublicStreamEvent,
  ReviewProjection,
  ToolCall,
} from "@sumi/api-client";
import { parseCatalogSduiNode } from "@sumi/sdui";
import { secureRandomUUID } from "../lib/random-uuid";
import type {
  AgentRun,
  AgentTraceEvent,
  ConversationEntry,
  ConversationModel,
} from "./model";
import { createEmptyConversation } from "./model";

interface MessageStream {
  textByIndex: Record<number, string>;
}

export interface AgentSession {
  conversation: ConversationModel;
  status: "idle" | "streaming";
  approval: ApprovalRequest | null;
  activeRunId: string | null;
  lastDurableSeq: number;
  completedMessageIds: Record<string, true>;
  messageRunIds: Record<string, string | null>;
  toolRunIds: Record<string, string>;
  approvalRunIds: Record<string, string>;
  messageStreams: Record<string, MessageStream>;
}

export interface ReducerContext {
  id: () => string;
}

export type ReduceEnvelopeResult =
  | { kind: "applied"; session: AgentSession }
  | { kind: "ignored"; session: AgentSession };

export function createAgentSession(
  conversation = createEmptyConversation(),
): AgentSession {
  return {
    conversation,
    status: "idle",
    approval: null,
    activeRunId: null,
    lastDurableSeq: 0,
    completedMessageIds: {},
    messageRunIds: {},
    toolRunIds: {},
    approvalRunIds: {},
    messageStreams: {},
  };
}

export function reduceEnvelope(
  current: AgentSession,
  envelope: BrowserEventEnvelope,
  context: ReducerContext = { id: secureRandomUUID },
): ReduceEnvelopeResult {
  if ("seq" in envelope && envelope.seq <= current.lastDurableSeq) {
    return { kind: "ignored", session: current };
  }

  let session =
    "seq" in envelope
      ? {
          ...current,
          lastDurableSeq: envelope.seq,
        }
      : current;
  const event = envelope.event;

  switch (event.type) {
    case "agent_start": {
      if (!("seq" in envelope)) return { kind: "ignored", session: current };
      const runId = `run:${envelope.seq}`;
      const run: AgentRun = {
        kind: "agent-run",
        id: runId,
        startedSeq: envelope.seq,
        endedSeq: null,
        status: "running",
        trace: [],
      };
      session = {
        ...session,
        status: "streaming",
        activeRunId: runId,
        conversation: upsertRun(session.conversation, run),
      };
      break;
    }
    case "agent_end": {
      if (!("seq" in envelope)) return { kind: "ignored", session: current };
      const activeRunId = session.activeRunId;
      session = {
        ...session,
        status: "idle",
        activeRunId: null,
        approval: null,
        conversation: activeRunId
          ? patchRun(session.conversation, activeRunId, (run) => ({
              ...run,
              endedSeq: envelope.seq,
              status: "complete",
              trace: run.trace.map(finalizeTrace),
            }))
          : session.conversation,
      };
      break;
    }
    case "message_start":
      session = applyMessage(session, event.message_id, event.message, false);
      break;
    case "message_update":
      session = applyMessageUpdate(session, event.message_id, event.event);
      break;
    case "message_end":
      session = applyMessage(session, event.message_id, event.message, true);
      break;
    case "tool_execution_start":
      session = applyToolStart(
        session,
        event.tool_call_id,
        event.tool_name,
        event.args,
      );
      break;
    case "tool_execution_update":
      // Partial tool output has no stable display semantics. The durable end
      // event remains the source of truth.
      break;
    case "tool_execution_end":
      session = applyToolEnd(
        session,
        event.tool_call_id,
        event.result,
        event.is_error,
      );
      break;
    case "approval_requested":
      session = applyApprovalRequested(session, event.request);
      break;
    case "approval_resolved":
      session = applyApprovalResolved(
        session,
        event.request_id,
        event.resolution,
      );
      break;
    case "steered": {
      if (!("seq" in envelope)) return { kind: "ignored", session: current };
      session = {
        ...session,
        conversation: upsertEntry(session.conversation, {
          kind: "steer",
          id: `steer:${envelope.seq}`,
          runId: session.activeRunId,
          mode: event.mode,
        }),
      };
      break;
    }
    case "error":
      session = {
        ...session,
        conversation: upsertEntry(session.conversation, {
          kind: "error",
          id: `error:${context.id()}`,
          runId: session.activeRunId,
          message: event.message,
          retryable: false,
        }),
      };
      break;
    case "retry_scheduled": {
      if (!("seq" in envelope)) return { kind: "ignored", session: current };
      const runId = session.activeRunId;
      if (runId) {
        session = {
          ...session,
          conversation: upsertTrace(session.conversation, runId, {
            type: "error",
            id: `retry:${envelope.seq}`,
            message: event.error_message,
          }),
        };
      }
      break;
    }
    case "command_disposition":
      break;
    case "turn_start":
    case "turn_end":
    case "memory_maintenance":
      break;
  }

  return { kind: "applied", session };
}

function applyMessage(
  session: AgentSession,
  messageId: string,
  message: PublicMessage,
  complete: boolean,
): AgentSession {
  if (message.role === "user") {
    const entry: ConversationEntry = {
      kind: "user",
      id: messageId,
      text: publicText(message),
      attachments: [],
      timestamp: message.timestamp,
      delivery: "durable",
    };
    return {
      ...session,
      completedMessageIds: complete
        ? { ...session.completedMessageIds, [messageId]: true }
        : session.completedMessageIds,
      conversation: upsertEntry(session.conversation, entry),
    };
  }
  if (message.role === "tool_result") return session;

  const runId =
    session.messageRunIds[messageId] === undefined
      ? session.activeRunId
      : session.messageRunIds[messageId];
  let conversation = session.conversation;
  const finalText = assistantText(message);
  const entryId = `message:${messageId}`;
  if (
    finalText.length > 0 ||
    (complete && conversation.entries[entryId]?.kind === "prose")
  ) {
    conversation =
      complete && finalText.length === 0
        ? removeEntry(conversation, entryId)
        : upsertEntry(conversation, {
            kind: "prose",
            id: entryId,
            runId,
            messageId,
            text: finalText,
            streaming: !complete,
            interrupted: complete ? message.interrupted : false,
            timestamp: message.timestamp,
          });
  }
  if (complete && message.stop_reason === "error") {
    const detail = message.error_message?.trim() || "Provider request failed";
    const providerCode = message.provider_code?.trim();
    conversation = upsertEntry(conversation, {
      kind: "error",
      id: `message-error:${messageId}`,
      runId,
      message: providerCode ? `${detail} (${providerCode})` : detail,
      retryable: false,
    });
  }

  for (const content of message.content) {
    if (content.type === "tool_call") {
      ({ conversation } = upsertToolCall(
        conversation,
        runId,
        content.tool_call,
      ));
      if (runId) {
        session = {
          ...session,
          toolRunIds: {
            ...session.toolRunIds,
            [content.tool_call.id]: runId,
          },
        };
      }
    } else if (content.type === "rejected_tool_call" && runId) {
      conversation = upsertTrace(conversation, runId, {
        type: "error",
        id: `rejected-tool:${content.rejected.id}`,
        message: `${content.rejected.name}: ${content.rejected.error}`,
      });
    }
  }

  if (complete && runId) {
    conversation = patchRun(conversation, runId, (run) => ({
      ...run,
      trace: run.trace.map((trace) =>
        trace.type === "reasoning" &&
        trace.id.startsWith(`reasoning:${messageId}:`) &&
        trace.status === "streaming"
          ? { ...trace, status: "incomplete" }
          : trace,
      ),
    }));
  }

  const { [messageId]: _discardedStream, ...messageStreams } =
    session.messageStreams;
  return {
    ...session,
    conversation,
    messageRunIds: { ...session.messageRunIds, [messageId]: runId },
    messageStreams: complete ? messageStreams : session.messageStreams,
    completedMessageIds: complete
      ? { ...session.completedMessageIds, [messageId]: true }
      : session.completedMessageIds,
  };
}

function applyMessageUpdate(
  session: AgentSession,
  messageId: string,
  event: PublicStreamEvent,
): AgentSession {
  if (session.completedMessageIds[messageId]) return session;
  const runId =
    session.messageRunIds[messageId] === undefined
      ? session.activeRunId
      : session.messageRunIds[messageId];
  const currentStream = session.messageStreams[messageId] ?? {
    textByIndex: {},
  };
  let stream = currentStream;
  let conversation = session.conversation;

  switch (event.type) {
    case "text_start":
      stream = {
        ...stream,
        textByIndex: { ...stream.textByIndex, [event.content_index]: "" },
      };
      break;
    case "text_delta":
      stream = {
        ...stream,
        textByIndex: {
          ...stream.textByIndex,
          [event.content_index]:
            (stream.textByIndex[event.content_index] ?? "") + event.delta,
        },
      };
      break;
    case "text_end":
      stream = {
        ...stream,
        textByIndex: {
          ...stream.textByIndex,
          [event.content_index]: event.content,
        },
      };
      break;
    case "reasoning_summary_start":
    case "reasoning_summary_delta":
    case "reasoning_summary_end": {
      if (!runId) break;
      const traceId = `reasoning:${messageId}:${event.content_index}`;
      const previous = findTrace(conversation, runId, traceId);
      const previousText = previous?.type === "reasoning" ? previous.text : "";
      const nextText =
        event.type === "reasoning_summary_delta"
          ? previousText + event.delta
          : event.type === "reasoning_summary_end"
            ? event.content
            : "";
      conversation = upsertTrace(conversation, runId, {
        type: "reasoning",
        id: traceId,
        contentIndex: event.content_index,
        text: nextText,
        status:
          event.type === "reasoning_summary_end" ? "complete" : "streaming",
      });
      break;
    }
    case "tool_call_end": {
      ({ conversation } = upsertToolCall(conversation, runId, event.tool_call));
      if (runId) {
        session = {
          ...session,
          toolRunIds: {
            ...session.toolRunIds,
            [event.tool_call.id]: runId,
          },
        };
      }
      break;
    }
    case "tool_call_rejected":
      if (runId) {
        conversation = upsertTrace(conversation, runId, {
          type: "error",
          id: `rejected-tool:${event.rejected.id}`,
          message: `${event.rejected.name}: ${event.rejected.error}`,
        });
      }
      break;
    case "thinking_start":
    case "thinking_delta":
    case "thinking_end":
    case "tool_call_start":
    case "tool_call_delta":
    case "tool_call_preview":
      break;
  }

  if (
    event.type === "text_start" ||
    event.type === "text_delta" ||
    event.type === "text_end"
  ) {
    conversation = upsertEntry(conversation, {
      kind: "prose",
      id: `message:${messageId}`,
      runId,
      messageId,
      text: joinedText(stream.textByIndex),
      streaming: true,
      interrupted: false,
      timestamp: null,
    });
  }

  return {
    ...session,
    conversation,
    messageRunIds: { ...session.messageRunIds, [messageId]: runId },
    messageStreams: { ...session.messageStreams, [messageId]: stream },
  };
}

function applyToolStart(
  session: AgentSession,
  toolCallId: string,
  toolName: string,
  args: Record<string, AnyJSON>,
): AgentSession {
  const runId = session.toolRunIds[toolCallId] ?? session.activeRunId;
  if (!runId) return session;
  const existing = findTrace(session.conversation, runId, toolCallId);
  return {
    ...session,
    toolRunIds: { ...session.toolRunIds, [toolCallId]: runId },
    conversation: upsertTrace(session.conversation, runId, {
      type: "tool",
      id: toolCallId,
      name: toolName,
      label: existing?.type === "tool" ? existing.label : `${toolName}を実行中`,
      args,
      result: existing?.type === "tool" ? existing.result : undefined,
      status: "running",
    }),
  };
}

function applyToolEnd(
  session: AgentSession,
  toolCallId: string,
  result: AnyJSON,
  isError: boolean,
): AgentSession {
  const runId = session.toolRunIds[toolCallId] ?? session.activeRunId;
  if (!runId) return session;
  const existing = findTrace(session.conversation, runId, toolCallId);
  const tool =
    existing?.type === "tool"
      ? existing
      : {
          type: "tool" as const,
          id: toolCallId,
          name: toolCallId,
          args: {},
          result: undefined,
          label: toolCallId,
          status: "running" as const,
        };
  let conversation = upsertTrace(session.conversation, runId, {
    ...tool,
    result,
    label:
      resultLabel(result) ?? `${tool.name}${isError ? "でエラー" : "を完了"}`,
    status: isError ? "error" : "done",
  });
  const parsed = sduiResult(result);
  if (!isError && parsed) {
    const cardId = `card:${toolCallId}`;
    conversation = upsertTrace(conversation, runId, {
      type: "artifact",
      id: cardId,
      label: resultLabel(result) ?? "結果を表示しました",
    });
    conversation = upsertEntry(conversation, {
      kind: "card",
      id: cardId,
      runId,
      toolCallId,
      node: parsed,
      timestamp: null,
    });
  }
  return {
    ...session,
    toolRunIds: { ...session.toolRunIds, [toolCallId]: runId },
    conversation,
  };
}

function applyApprovalRequested(
  session: AgentSession,
  request: ApprovalRequest,
): AgentSession {
  const runId = session.toolRunIds[request.tool_call_id] ?? session.activeRunId;
  const summary = approvalSummary(request.action, request.args_summary);
  let conversation = upsertEntry(session.conversation, {
    kind: "approval",
    id: `approval:${request.id}`,
    runId,
    requestId: request.id,
    request,
    summary,
    reason: request.reason ?? null,
    status: "pending",
    decision: null,
    timestamp: null,
  });
  if (runId) {
    conversation = upsertTrace(conversation, runId, {
      type: "approval",
      id: request.id,
      toolCallId: request.tool_call_id,
      summary,
      status: "pending",
      decision: null,
    });
  }
  return {
    ...session,
    conversation,
    approval: request,
    approvalRunIds: runId
      ? { ...session.approvalRunIds, [request.id]: runId }
      : session.approvalRunIds,
  };
}

function applyApprovalResolved(
  session: AgentSession,
  requestId: string,
  resolution: "cancelled" | { decision: ApprovalDecision },
): AgentSession {
  const decision = resolution === "cancelled" ? null : resolution.decision;
  const status =
    resolution === "cancelled"
      ? "cancelled"
      : resolution.decision.type === "deny_once"
        ? "denied"
        : "allowed";
  const approvalEntry = session.conversation.entries[`approval:${requestId}`];
  const runId =
    session.approvalRunIds[requestId] ??
    (approvalEntry?.kind === "approval" ? approvalEntry.runId : null);
  let conversation = patchEntry(
    session.conversation,
    `approval:${requestId}`,
    (entry) =>
      entry.kind === "approval" ? { ...entry, status, decision } : entry,
  );
  if (runId) {
    const trace = findTrace(conversation, runId, requestId);
    if (trace?.type !== "approval" && approvalEntry?.kind !== "approval") {
      return {
        ...session,
        conversation,
        approval: session.approval?.id === requestId ? null : session.approval,
      };
    }
    conversation = upsertTrace(conversation, runId, {
      type: "approval",
      id: requestId,
      toolCallId:
        trace?.type === "approval"
          ? trace.toolCallId
          : approvalEntry?.kind === "approval"
            ? approvalEntry.request.tool_call_id
            : "",
      summary:
        trace?.type === "approval"
          ? trace.summary
          : approvalEntry?.kind === "approval"
            ? approvalEntry.summary
            : "承認",
      status,
      decision,
    });
    if (status !== "allowed" && approvalEntry?.kind === "approval") {
      conversation = patchRun(conversation, runId, (run) => ({
        ...run,
        trace: run.trace.map((entry) =>
          entry.type === "tool" &&
          entry.id === approvalEntry.request.tool_call_id &&
          (entry.status === "pending" || entry.status === "running")
            ? { ...entry, status: "cancelled" }
            : entry,
        ),
      }));
    }
  }
  return {
    ...session,
    conversation,
    approval: session.approval?.id === requestId ? null : session.approval,
  };
}

function publicText(message: PublicMessage): string {
  return message.content
    .filter((content) => content.type === "text")
    .map((content) => content.text)
    .join("");
}

function assistantText(message: PublicAssistantMessage): string {
  return message.content
    .filter((content) => content.type === "text")
    .sort((left, right) => left.wire_item_index - right.wire_item_index)
    .map((content) => content.text)
    .join("");
}

function joinedText(textByIndex: Record<number, string>): string {
  return Object.entries(textByIndex)
    .sort(([left], [right]) => Number(left) - Number(right))
    .map(([, text]) => text)
    .join("");
}

function upsertToolCall(
  conversation: ConversationModel,
  runId: string | null,
  toolCall: ToolCall,
): { conversation: ConversationModel } {
  if (!runId) return { conversation };
  const existing = findTrace(conversation, runId, toolCall.id);
  return {
    conversation: upsertTrace(conversation, runId, {
      type: "tool",
      id: toolCall.id,
      name: toolCall.name,
      label:
        existing?.type === "tool" ? existing.label : `${toolCall.name}を準備中`,
      args: toolCall.arguments,
      result: existing?.type === "tool" ? existing.result : undefined,
      status: existing?.type === "tool" ? existing.status : "pending",
    }),
  };
}

function approvalSummary(
  action: ReviewProjection,
  argsSummary: AnyJSON,
): string {
  if ("insufficient_evidence" in action) {
    return action.insufficient_evidence.reason;
  }
  if (typeof action.reviewable === "string") return action.reviewable;
  return safeJson(action.reviewable ?? argsSummary);
}

function safeJson(value: AnyJSON): string {
  try {
    return JSON.stringify(value);
  } catch {
    return "内容を確認してください";
  }
}

function resultLabel(result: AnyJSON): string | null {
  if (
    typeof result === "object" &&
    result !== null &&
    !Array.isArray(result) &&
    typeof result.label === "string"
  ) {
    return result.label;
  }
  return null;
}

function sduiResult(result: AnyJSON) {
  if (
    typeof result !== "object" ||
    result === null ||
    Array.isArray(result) ||
    !("sdui" in result)
  ) {
    return null;
  }
  return parseCatalogSduiNode(result.sdui);
}

function finalizeTrace(trace: AgentTraceEvent): AgentTraceEvent {
  if (trace.type === "reasoning" && trace.status === "streaming") {
    return { ...trace, status: "incomplete" };
  }
  return trace;
}

function upsertRun(model: ConversationModel, run: AgentRun): ConversationModel {
  return {
    ...model,
    runOrder: model.runs[run.id] ? model.runOrder : [...model.runOrder, run.id],
    runs: { ...model.runs, [run.id]: run },
  };
}

function patchRun(
  model: ConversationModel,
  runId: string,
  patch: (run: AgentRun) => AgentRun,
): ConversationModel {
  const run = model.runs[runId];
  return run
    ? { ...model, runs: { ...model.runs, [runId]: patch(run) } }
    : model;
}

function upsertTrace(
  model: ConversationModel,
  runId: string,
  trace: AgentTraceEvent,
): ConversationModel {
  return patchRun(model, runId, (run) => {
    const exists = run.trace.some((entry) => entry.id === trace.id);
    return {
      ...run,
      trace: exists
        ? run.trace.map((entry) => (entry.id === trace.id ? trace : entry))
        : [...run.trace, trace],
    };
  });
}

function findTrace(
  model: ConversationModel,
  runId: string,
  traceId: string,
): AgentTraceEvent | undefined {
  return model.runs[runId]?.trace.find((entry) => entry.id === traceId);
}

export function upsertEntry(
  model: ConversationModel,
  entry: ConversationEntry,
): ConversationModel {
  return {
    ...model,
    entryOrder: model.entries[entry.id]
      ? model.entryOrder
      : [...model.entryOrder, entry.id],
    entries: { ...model.entries, [entry.id]: entry },
  };
}

export function removeEntry(
  model: ConversationModel,
  entryId: string,
): ConversationModel {
  if (!model.entries[entryId]) return model;
  const { [entryId]: _removed, ...entries } = model.entries;
  return {
    ...model,
    entryOrder: model.entryOrder.filter((id) => id !== entryId),
    entries,
  };
}

export function patchEntry(
  model: ConversationModel,
  entryId: string,
  patch: (entry: ConversationEntry) => ConversationEntry,
): ConversationModel {
  const entry = model.entries[entryId];
  return entry
    ? { ...model, entries: { ...model.entries, [entryId]: patch(entry) } }
    : model;
}
