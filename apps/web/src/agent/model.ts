import type {
  AnyJSON,
  ApprovalDecision,
  ApprovalRequest,
  SteerMode,
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
      label: string;
      args: Record<string, AnyJSON>;
      result: AnyJSON | undefined;
      status: "pending" | "running" | "done" | "error" | "cancelled";
    }
  | {
      type: "approval";
      id: string;
      toolCallId: string;
      summary: string;
      status: "pending" | "allowed" | "denied" | "cancelled";
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
  endedSeq: number | null;
  status: "running" | "complete";
  trace: AgentTraceEvent[];
}

export type UserDelivery = "pending" | "accepted" | "rejected" | "durable";

export type ConversationEntry =
  | {
      kind: "user";
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
      status: "pending" | "allowed" | "denied" | "cancelled";
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
    };

/** One normalized projection of the personality agent's canonical life log. */
export interface ConversationModel {
  entryOrder: string[];
  entries: Record<string, ConversationEntry>;
  runOrder: string[];
  runs: Record<string, AgentRun>;
}

export type ChatItem =
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
  return { entryOrder: [], entries: {}, runOrder: [], runs: {} };
}
