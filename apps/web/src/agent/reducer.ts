import type {
  AnyJSON,
  ApprovalDecision,
  ApprovalOperationOutcomeEvent,
  ApprovalOperationProvenanceV2,
  ApprovalRequest,
  BrowserEventEnvelope,
  ExternalProvenanceV2,
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
  ConversationChanges,
  ConversationEntry,
  ConversationModel,
} from "./model";
import { createConversationChanges, createEmptyConversation } from "./model";

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
  approvalOperations: Record<
    string,
    { toolCallId: string; completed: boolean }
  >;
  messageStreams: Record<string, MessageStream>;
  unresolvedToolOutcomes: Record<
    string,
    { result: AnyJSON; isError: boolean; toolName?: string }
  >;
  unresolvedApprovalOutcomes: Record<string, ApprovalOperationOutcomeEvent>;
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
    approvalOperations: {},
    messageStreams: {},
    unresolvedToolOutcomes: {},
    unresolvedApprovalOutcomes: {},
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
        audience: envelope.audience,
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
    case "reasoning_summary": {
      const runId =
        session.messageRunIds[event.message_id] ?? session.activeRunId;
      if (runId)
        session = {
          ...session,
          conversation: upsertTrace(
            session.conversation,
            runId,
            {
              type: "reasoning",
              id: `reasoning:${event.message_id}:${event.content_index}`,
              contentIndex: event.content_index,
              text: event.content,
              status: "complete",
            },
            {
              messageId: event.message_id,
              contentIndex: event.wire_item_index,
            },
          ),
        };
      break;
    }
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
    case "tool_execution_update": {
      const runId =
        session.toolRunIds[event.tool_call_id] ?? session.activeRunId;
      const tool = runId
        ? findTrace(session.conversation, runId, event.tool_call_id)
        : undefined;
      if (runId && tool?.type === "tool" && tool.status === "running")
        session = {
          ...session,
          conversation: upsertTrace(session.conversation, runId, {
            ...tool,
            progress: event.partial,
          }),
        };
      break;
    }
    case "tool_execution_end":
      session = applyToolEnd(
        session,
        event.tool_call_id,
        event.result,
        event.is_error,
      );
      break;
    case "approval_operation_outcome":
      session = applyApprovalOperationOutcome(session, event);
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

  return { kind: "applied", session: reconcileDeferredTools(session) };
}

function applyMessage(
  session: AgentSession,
  messageId: string,
  message: PublicMessage,
  complete: boolean,
): AgentSession {
  if (message.role === "user") {
    const source = message.incoming_source;
    if (source && isApprovalOperationSource(source)) {
      return complete
        ? applyApprovalOperationOutcome(session, {
            type: "approval_operation_outcome",
            ...source.source,
          })
        : session;
    }
    const entry: ConversationEntry = {
      kind: "user",
      id: messageId,
      text: publicText(message),
      attachments: [],
      timestamp: message.timestamp,
      delivery: "durable",
      ...(source ? { source } : {}),
    };
    // Session bookkeeping records are mutable within the session for the
    // same reason the model containers are: per-event copies made each
    // streamed token O(history).
    if (complete) session.completedMessageIds[messageId] = true;
    return {
      ...session,
      conversation: upsertEntry(session.conversation, entry),
    };
  }
  if (message.role === "tool_result") {
    // Pre-execution review denials have a durable result message but no
    // ToolExecutionEnd. Admit its actual public receipt into the same call.
    return complete
      ? applyToolEnd(
          session,
          message.tool_call_id,
          { content: message.content, details: message.details },
          message.is_error,
          message.tool_name,
        )
      : session;
  }

  const runId =
    session.messageRunIds[messageId] === undefined
      ? session.activeRunId
      : session.messageRunIds[messageId];
  let conversation = session.conversation;
  // Each wire text block retains its own place among operations and summaries.
  for (const content of [...message.content].sort(
    (a, b) => a.wire_item_index - b.wire_item_index,
  )) {
    if (content.type === "tool_call") {
      ({ conversation } = upsertToolCall(
        conversation,
        runId,
        content.tool_call,
        { messageId, contentIndex: content.wire_item_index },
        true,
      ));
      if (runId) session.toolRunIds[content.tool_call.id] = runId;
      continue;
    }
    if (content.type === "rejected_tool_call" && runId) {
      conversation = upsertTrace(
        conversation,
        runId,
        {
          type: "error",
          id: `rejected-tool:${content.rejected.id}`,
          message: `${content.rejected.name}: ${content.rejected.error}`,
        },
        { messageId, contentIndex: content.wire_item_index },
        true,
      );
      continue;
    }
    if (content.type !== "text") continue;
    const entryId = `message:${messageId}:${content.wire_item_index}`;
    if (content.text.length === 0) {
      if (complete) conversation = removeEntry(conversation, entryId);
      continue;
    }
    conversation = upsertMessageEntry(conversation, {
      kind: "prose",
      id: entryId,
      runId,
      messageId,
      contentIndex: content.wire_item_index,
      text: content.text,
      streaming: !complete,
      interrupted: complete ? message.interrupted : false,
      timestamp: message.timestamp,
    });
  }
  if (complete) {
    const canonicalIds = new Set(
      message.content
        .filter((item) => item.type === "text" && item.text.length > 0)
        .map((item) => `message:${messageId}:${item.wire_item_index}`),
    );
    // entryOrder mutates in place under removeEntry; iterate a snapshot so
    // adjacent stale blocks cannot slip past the live iterator.
    for (const id of [...conversation.entryOrder]) {
      const entry = conversation.entries[id];
      if (
        entry?.kind === "prose" &&
        entry.messageId === messageId &&
        !canonicalIds.has(id)
      )
        conversation = removeEntry(conversation, id);
    }
  }
  if (complete && message.stop_reason === "error") {
    const detail = message.error_message?.trim() || "Provider request failed";
    const providerCode = message.provider_code?.trim();
    const cause =
      providerCode === "no_model_connection"
        ? "no_model_connection"
        : undefined;
    conversation = upsertEntry(conversation, {
      kind: "error",
      id: `message-error:${messageId}`,
      runId,
      message:
        providerCode && cause === undefined
          ? `${detail} (${providerCode})`
          : detail,
      retryable: false,
      ...(cause === undefined ? {} : { cause }),
    });
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

  if (complete) {
    delete session.messageStreams[messageId];
    session.completedMessageIds[messageId] = true;
  }
  session.messageRunIds[messageId] = runId;
  return { ...session, conversation };
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
      ({ conversation } = upsertToolCall(conversation, runId, event.tool_call, {
        messageId,
        contentIndex: event.content_index,
      }));
      if (runId) {
        session.toolRunIds[event.tool_call.id] = runId;
        session = { ...session };
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
      id: `message:${messageId}:${event.content_index}`,
      runId,
      messageId,
      contentIndex: event.content_index,
      text: stream.textByIndex[event.content_index] ?? "",
      streaming: true,
      interrupted: false,
      timestamp: null,
    });
  }

  session.messageRunIds[messageId] = runId;
  session.messageStreams[messageId] = stream;
  return { ...session, conversation };
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
  session.toolRunIds[toolCallId] = runId;
  return {
    ...session,
    conversation: upsertTrace(session.conversation, runId, {
      type: "tool",
      id: toolCallId,
      name: toolName,
      route: existing?.type === "tool" ? existing.route : null,
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
  toolName?: string,
  operationFinal = false,
): AgentSession {
  const runId = session.toolRunIds[toolCallId];
  if (!runId) {
    session.unresolvedToolOutcomes[toolCallId] = {
      result,
      isError,
      ...(toolName ? { toolName } : {}),
    };
    return { ...session };
  }
  const existing = findTrace(session.conversation, runId, toolCallId);
  const tool =
    existing?.type === "tool"
      ? existing
      : {
          type: "tool" as const,
          id: toolCallId,
          name: toolName ?? toolCallId,
          route: null,
          args: {},
          result: undefined,
          label: toolCallId,
          status: "running" as const,
        };
  const operationId =
    operationFinal || isError ? null : awaitingApprovalOperation(result);
  if (operationId && session.approvalOperations[operationId]?.completed)
    return session;
  if (operationId) {
    session.approvalOperations[operationId] = { toolCallId, completed: false };
    session = { ...session };
  }
  let conversation = upsertTrace(session.conversation, runId, {
    ...tool,
    progress: undefined,
    result,
    label: operationId
      ? "承認待ち"
      : (resultLabel(result) ??
        `${tool.name}${isError ? "でエラー" : "を完了"}`),
    status: operationId ? "pending" : isError ? "error" : "done",
  });
  const parsed = sduiResult(result);
  if (!operationId && !isError && parsed) {
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
  session.toolRunIds[toolCallId] = runId;
  return { ...session, conversation };
}

function isApprovalOperationSource(
  source: ExternalProvenanceV2 | ApprovalOperationProvenanceV2,
): source is ApprovalOperationProvenanceV2 {
  return source.source.surface === "approval_operation";
}
function awaitingApprovalOperation(result: AnyJSON): string | null {
  if (typeof result !== "object" || !result || Array.isArray(result))
    return null;
  const details = result.details;
  return typeof details === "object" &&
    details !== null &&
    !Array.isArray(details) &&
    details.status === "awaiting_approval" &&
    details.executed === false &&
    typeof details.operation_id === "string" &&
    details.operation_id.length > 0
    ? details.operation_id
    : null;
}
function applyApprovalOperationOutcome(
  session: AgentSession,
  outcome: ApprovalOperationOutcomeEvent,
): AgentSession {
  const known = session.approvalOperations[outcome.operation_id];
  const belongsToAnotherOperation = Object.entries(
    session.approvalOperations,
  ).some(
    ([id, operation]) =>
      operation.toolCallId === outcome.tool_call_id &&
      id !== outcome.operation_id,
  );
  if (
    belongsToAnotherOperation ||
    known?.completed ||
    (known && known.toolCallId !== outcome.tool_call_id)
  )
    return session;
  if (!session.toolRunIds[outcome.tool_call_id]) {
    session.unresolvedApprovalOutcomes[outcome.operation_id] = outcome;
    return { ...session };
  }
  const next = applyToolEnd(
    session,
    outcome.tool_call_id,
    {
      content: outcome.result.content,
      details: outcome.result.details,
      ...(outcome.status === "indeterminate"
        ? { label: "実行結果を確認できません" }
        : {}),
    },
    outcome.result.is_error,
    outcome.result.tool_name,
    true,
  );
  // A terminal operation also closes a pending decision card during history
  // replay, without inventing a Human decision that was never supplied.
  const approvalStatus =
    outcome.status === "denied"
      ? "denied"
      : outcome.status === "cancelled" || outcome.status === "expired"
        ? "cancelled"
        : "allowed";
  let conversation = patchEntry(
    next.conversation,
    `approval:${outcome.operation_id}`,
    (entry) =>
      entry.kind === "approval" && entry.status === "pending"
        ? { ...entry, status: approvalStatus }
        : entry,
  );
  const approvalRunId = next.approvalRunIds[outcome.operation_id];
  if (approvalRunId)
    conversation = patchRun(conversation, approvalRunId, (run) => ({
      ...run,
      trace: run.trace.map((trace) =>
        trace.type === "approval" &&
        trace.id === outcome.operation_id &&
        trace.status === "pending"
          ? { ...trace, status: approvalStatus }
          : trace,
      ),
    }));
  next.approvalOperations[outcome.operation_id] = {
    toolCallId: outcome.tool_call_id,
    completed: true,
  };
  return {
    ...next,
    conversation,
    approval: next.approval?.id === outcome.operation_id ? null : next.approval,
  };
}

export function reconcileDeferredTools(session: AgentSession): AgentSession {
  let next = session;
  for (const [callId, payload] of Object.entries(next.unresolvedToolOutcomes)) {
    if (!next.toolRunIds[callId]) continue;
    delete next.unresolvedToolOutcomes[callId];
    next = applyToolEnd(
      next,
      callId,
      payload.result,
      payload.isError,
      payload.toolName,
    );
  }
  for (const [operationId, outcome] of Object.entries(
    next.unresolvedApprovalOutcomes,
  )) {
    if (!next.toolRunIds[outcome.tool_call_id]) continue;
    delete next.unresolvedApprovalOutcomes[operationId];
    next = applyApprovalOperationOutcome(next, outcome);
  }
  return next;
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
  if (runId) session.approvalRunIds[request.id] = runId;
  return {
    ...session,
    conversation,
    approval: request,
  };
}

function applyApprovalResolved(
  session: AgentSession,
  requestId: string,
  resolution:
    | "cancelled"
    | { decision: ApprovalDecision }
    | { rejected: { decision: ApprovalDecision } },
): AgentSession {
  const decision =
    resolution === "cancelled"
      ? null
      : "rejected" in resolution
        ? resolution.rejected.decision
        : resolution.decision;
  const status =
    resolution === "cancelled"
      ? "cancelled"
      : "rejected" in resolution
        ? "rejected"
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
      const tool = findTrace(
        conversation,
        runId,
        approvalEntry.request.tool_call_id,
      );
      if (
        tool?.type === "tool" &&
        (tool.status === "pending" || tool.status === "running")
      ) {
        conversation = upsertTrace(conversation, runId, {
          ...tool,
          status: "cancelled",
          label: `${tool.name}を中止`,
        });
      }
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

function upsertToolCall(
  conversation: ConversationModel,
  runId: string | null,
  toolCall: ToolCall,
  position?: { messageId: string; contentIndex: number },
  reconcile = false,
): { conversation: ConversationModel } {
  if (!runId) return { conversation };
  const existing = findTrace(conversation, runId, toolCall.id);
  return {
    conversation: upsertTrace(
      conversation,
      runId,
      {
        type: "tool",
        id: toolCall.id,
        name: toolCall.name,
        route: toolCall.route,
        label:
          existing?.type === "tool"
            ? existing.label
            : `${toolCall.name}を準備中`,
        args: toolCall.arguments,
        result: existing?.type === "tool" ? existing.result : undefined,
        status: existing?.type === "tool" ? existing.status : "pending",
      },
      position,
      reconcile,
    ),
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
  const existed = !!model.runs[run.id];
  model.runs[run.id] = run;
  if (!existed) model.runOrder.push(run.id);
  const changes = writeJournal(model);
  changes.changedRunIds.add(run.id);
  return { ...model, changes };
}

function patchRun(
  model: ConversationModel,
  runId: string,
  patch: (run: AgentRun) => AgentRun,
): ConversationModel {
  const run = model.runs[runId];
  if (!run) return model;
  model.runs[runId] = patch(run);
  const changes = writeJournal(model);
  changes.changedRunIds.add(runId);
  return { ...model, changes };
}

function upsertTrace(
  model: ConversationModel,
  runId: string,
  trace: AgentTraceEvent,
  position?: { messageId: string; contentIndex: number },
  reconcile = false,
): ConversationModel {
  let next = patchRun(model, runId, (run) => {
    const exists = run.trace.some((entry) => entry.id === trace.id);
    return {
      ...run,
      trace: exists
        ? run.trace.map((entry) => (entry.id === trace.id ? trace : entry))
        : [...run.trace, trace],
    };
  });
  if (trace.type !== "approval" && trace.type !== "artifact") {
    const result =
      trace.type === "tool" &&
      (trace.status === "done" ||
        trace.status === "error" ||
        trace.status === "cancelled");
    const phase = result ? "result" : "activity";
    const existingEntry = next.entries[`trace:${runId}:${trace.id}:${phase}`];
    next = (reconcile ? upsertMessageEntry : upsertEntry)(next, {
      ...(existingEntry?.kind === "trace" ? existingEntry : {}),
      ...(phase === "activity" ? position : {}),
      kind: "trace",
      id: `trace:${runId}:${trace.id}:${phase}`,
      runId,
      traceId: trace.id,
      phase,
      ...(trace.type === "tool" && phase === "activity"
        ? {
            inputArgs:
              (
                next.entries[`trace:${runId}:${trace.id}:${phase}`] as
                  | Extract<ConversationEntry, { kind: "trace" }>
                  | undefined
              )?.inputArgs ?? trace.args,
          }
        : {}),
    });
  }
  return next;
}

function findTrace(
  model: ConversationModel,
  runId: string,
  traceId: string,
): AgentTraceEvent | undefined {
  return model.runs[runId]?.trace.find((entry) => entry.id === traceId);
}

// Replayed completed content can arrive after its durable summary. Insert a
// newly materialized block beside its known message-content neighbors without
// moving live rows or pulling tool execution outcomes across intervening text.
function upsertMessageEntry(
  model: ConversationModel,
  entry: ConversationEntry,
): ConversationModel {
  const existed = !!model.entries[entry.id];
  const next = upsertEntry(model, entry);
  if (
    existed ||
    !("messageId" in entry) ||
    entry.messageId === undefined ||
    !("contentIndex" in entry) ||
    entry.contentIndex === undefined
  )
    return next;
  const contentIndex = entry.contentIndex;
  const neighbors = model.entryOrder.flatMap((id, index) => {
    const other = model.entries[id];
    return other &&
      "messageId" in other &&
      other.messageId === entry.messageId &&
      "contentIndex" in other &&
      other.contentIndex !== undefined
      ? [{ index, contentIndex: other.contentIndex }]
      : [];
  });
  const after = neighbors.find((other) => other.contentIndex > contentIndex);
  const before = neighbors.findLast(
    (other) => other.contentIndex < contentIndex,
  );
  const at =
    after?.index ?? (before ? before.index + 1 : model.entryOrder.length);
  // upsertEntry already appended the new id at the tail; move it into place.
  next.entryOrder.splice(next.entryOrder.indexOf(entry.id), 1);
  next.entryOrder.splice(at, 0, entry.id);
  next.changes?.orderOps.push({ op: "move", id: entry.id, index: at });
  return next;
}

/**
 * The journal that records this write. A model built without one (ad-hoc
 * literal) gets a structural journal so projection consumers fall back to a
 * full rescan rather than trusting an incomplete diff.
 */
function writeJournal(model: ConversationModel): ConversationChanges {
  return model.changes ?? createConversationChanges(true);
}

export function upsertEntry(
  model: ConversationModel,
  entry: ConversationEntry,
): ConversationModel {
  // Entries and their order mutate in place: copying a lifetime-length
  // record for every streamed token made each delta O(history). The journal
  // records the write so incremental consumers see exactly what moved.
  const existed = !!model.entries[entry.id];
  model.entries[entry.id] = entry;
  const changes = writeJournal(model);
  if (!existed) {
    model.entryOrder.push(entry.id);
    changes.orderOps.push({
      op: "insert",
      id: entry.id,
      index: model.entryOrder.length - 1,
    });
    changes.addedEntryIds.add(entry.id);
  } else {
    changes.changedEntryIds.add(entry.id);
  }
  return { ...model, changes };
}

export function removeEntry(
  model: ConversationModel,
  entryId: string,
): ConversationModel {
  if (!model.entries[entryId]) return model;
  delete model.entries[entryId];
  const index = model.entryOrder.indexOf(entryId);
  const changes = writeJournal(model);
  if (index >= 0) {
    model.entryOrder.splice(index, 1);
    changes.orderOps.push({ op: "remove", id: entryId });
  }
  changes.removedEntryIds.add(entryId);
  return { ...model, changes };
}

export function patchEntry(
  model: ConversationModel,
  entryId: string,
  patch: (entry: ConversationEntry) => ConversationEntry,
): ConversationModel {
  const entry = model.entries[entryId];
  if (!entry) return model;
  model.entries[entryId] = patch(entry);
  const changes = writeJournal(model);
  changes.changedEntryIds.add(entryId);
  return { ...model, changes };
}
