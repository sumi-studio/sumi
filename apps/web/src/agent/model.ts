import type {
  AnyJSON,
  ApprovalDecision,
  ApprovalRequest,
  ExternalProvenanceV2,
  SteerMode,
  ToolCall,
} from "@sumi/api-client";
import type { SduiNode } from "@sumi/sdui";

export type AgentTraceEvent =
  | {
      type: "reasoning";
      id: string;
      contentIndex: number;
      /** Provider-authored summary for display. Raw thinking is never stored here. */
      text: string;
      status: "streaming" | "complete" | "incomplete";
    }
  | {
      type: "tool";
      id: string;
      name: string;
      /** Known once the canonical ToolCall projection arrives. */
      route: ToolCall["route"] | null;
      label: string;
      args: Record<string, AnyJSON>;
      result: AnyJSON | undefined;
      progress?: AnyJSON;
      approvalResolution?: "denied" | "rejected" | "cancelled";
      status: "pending" | "running" | "done" | "error" | "cancelled";
    }
  | {
      type: "approval";
      id: string;
      toolCallId: string;
      summary: string;
      status: "pending" | "allowed" | "denied" | "rejected" | "cancelled";
      decision: ApprovalDecision | null;
    }
  | { type: "artifact"; id: string; label: string }
  | { type: "error"; id: string; message: string };

/**
 * A display grouping derived from durable agent_start/agent_end events.
 *
 * The public wire deliberately does not expose run timestamps, duration, or an
 * outcome, so the browser must not manufacture those fields.
 */
export interface AgentRun {
  kind: "agent-run";
  id: string;
  startedSeq: number;
  audience: "direct_chat" | "secretary";
  endedSeq: number | null;
  status: "running" | "complete";
  trace: AgentTraceEvent[];
}

export type UserDelivery = "pending" | "admitted" | "rejected" | "durable";

/**
 * Private, recoverable composer input. This is local browser state and never a
 * canonical conversation entry.
 */
export interface RecoverableDraft {
  idempotencyKey: string;
  text: string;
  reason: string;
  commandId?: string;
}

export type ConversationEntry =
  | {
      kind: "trace";
      id: string;
      runId: string;
      traceId: string;
      phase: "activity" | "result";
      inputArgs?: Record<string, AnyJSON>;
      messageId?: string;
      contentIndex?: number;
    }
  | {
      kind: "user";
      source?: ExternalProvenanceV2;
      id: string;
      text: string;
      /** v1 direct chat accepts no attachments. */
      attachments: [];
      timestamp: string | null;
      delivery: UserDelivery;
      idempotencyKey?: string;
      rejectReason?: string;
    }
  | {
      kind: "prose";
      id: string;
      runId: string | null;
      messageId: string;
      contentIndex?: number;
      text: string;
      streaming: boolean;
      interrupted: boolean;
      timestamp: string | null;
    }
  | {
      kind: "card";
      id: string;
      runId: string | null;
      toolCallId: string;
      node: SduiNode;
      timestamp: null;
    }
  | {
      kind: "approval";
      id: string;
      runId: string | null;
      requestId: string;
      request: ApprovalRequest;
      summary: string;
      reason: string | null;
      status: "pending" | "allowed" | "denied" | "rejected" | "cancelled";
      decision: ApprovalDecision | null;
      timestamp: null;
    }
  | {
      kind: "steer";
      id: string;
      runId: string | null;
      mode: SteerMode;
    }
  | {
      kind: "error";
      id: string;
      runId: string | null;
      message: string;
      retryable: false;
      /**
       * Bounded failure classification carried on the wire's provider_code.
       * "no_model_connection" = no usable model connection is selected; the
       * row renders localized guidance plus the connection-settings sheet.
       * Unknown/absent codes keep the generic presentation.
       */
      cause?: "no_model_connection";
    };

/**
 * One position edit applied to `entryOrder`. `insert`/`move` carry the id's
 * index after the edit; `remove` needs no index because the consumer knows
 * where it kept the row.
 */
export type ConversationOrderOp =
  | { op: "insert"; id: string; index: number }
  | { op: "remove"; id: string }
  | { op: "move"; id: string; index: number };

/**
 * Write journal for a span of model transitions. The reducer's model
 * containers (`entries`, `runs`, `entryOrder`, `runOrder`) are mutated in
 * place within a session — rebuilding 6,000-key records on every streamed
 * token dominated the stream hot path — so the wrapper object alone cannot
 * say what changed. The journal accumulates ids and order edits written
 * through the model helpers until a projection consumes them, letting
 * incremental views recompute only the affected rows.
 */
export interface ConversationChanges {
  /** Entries inserted into `entryOrder`. */
  addedEntryIds: Set<string>;
  /** Entries removed from the model. */
  removedEntryIds: Set<string>;
  /** Entries whose value was replaced in place. */
  changedEntryIds: Set<string>;
  /** Runs whose value was replaced in place. */
  changedRunIds: Set<string>;
  /** `entryOrder` edits in application order. */
  orderOps: ConversationOrderOp[];
  /**
   * True when the sets above do not describe the transition — a rebuilt
   * model (history merge, snapshot filter) rather than journaled writes.
   * Consumers must treat every row as dirty.
   */
  structural: boolean;
}

export function createConversationChanges(
  structural = false,
): ConversationChanges {
  return {
    addedEntryIds: new Set(),
    removedEntryIds: new Set(),
    changedEntryIds: new Set(),
    changedRunIds: new Set(),
    orderOps: [],
    structural,
  };
}

/** One normalized projection of the personality agent's canonical life log. */
export interface ConversationModel {
  entryOrder: string[];
  entries: Record<string, ConversationEntry>;
  runOrder: string[];
  runs: Record<string, AgentRun>;
  /**
   * Write journal for the transition that produced this model and every
   * unconsumed successor. Absent on ad-hoc literals; helpers treat a missing
   * journal as structural so consumers stay conservative.
   */
  changes?: ConversationChanges;
}

export type ChatItem =
  | (Extract<ConversationEntry, { kind: "trace" }> & { trace: AgentTraceEvent })
  | AgentRun
  | Extract<
      ConversationEntry,
      { kind: "user" | "approval" | "steer" | "error" }
    >
  | (Extract<ConversationEntry, { kind: "prose" }> & {
      agentMessageFinal: boolean;
    })
  | Extract<ConversationEntry, { kind: "card" }>;

export function createEmptyConversation(): ConversationModel {
  return {
    entryOrder: [],
    entries: {},
    runOrder: [],
    runs: {},
    // Structural: a model that did not arrive through journaled writes must be
    // rescanned, not treated as "no changes". This is what lets consumers tell
    // a wholesale session replacement (resetAuthority, fresh sessions) apart
    // from an unchanged stream.
    changes: createConversationChanges(true),
  };
}
