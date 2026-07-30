import type { ApprovalDecision, BrowserEventEnvelope } from "@sumi/api-client";
import { create } from "zustand";
import {
  type DirectChatConnectionState,
  type DirectChatReadyState,
  type DirectChatServerFrame,
  DirectChatSocket,
} from "../lib/direct-chat-socket";
import type { ConversationEntry, ConversationModel } from "./model";
import {
  type AgentSession,
  createAgentSession,
  patchEntry,
  reduceEnvelope,
  upsertEntry,
} from "./reducer";
import { userMessageIdFromCommandId } from "./user-message-id";

export interface DirectChatTransport {
  connect(): void;
  close(): void;
  sendCommand(command: unknown, idempotencyKey?: string): boolean;
  onFrame(listener: (frame: DirectChatServerFrame) => void): () => void;
  onConnection(
    listener: (state: DirectChatConnectionState) => void,
  ): () => void;
  onReady(listener: (state: DirectChatReadyState) => void): () => void;
}

export interface ConversationState {
  conversation: ConversationModel;
  status: AgentSession["status"];
  running: boolean;
  approval: AgentSession["approval"];
  connection: DirectChatConnectionState;
  ready: DirectChatReadyState;
  lastError: string | null;
  connect: () => void;
  disconnect: () => void;
  sendMessage: (text: string) => boolean;
  abort: () => boolean;
  decideApproval: (requestId: string, decision: ApprovalDecision) => boolean;
}

export interface ConversationStoreDependencies {
  transport: DirectChatTransport;
  idempotencyKey?: () => string;
  reducerId?: () => string;
}

export function createConversationStore({
  transport,
  idempotencyKey = () => crypto.randomUUID(),
  reducerId = () => crypto.randomUUID(),
}: ConversationStoreDependencies) {
  let session = createAgentSession();
  let connection: DirectChatConnectionState = "connecting";
  let ready: DirectChatReadyState = "unknown";
  let started = false;

  return create<ConversationState>((set) => {
    const publish = (lastError?: string | null) => {
      set((state) => ({
        conversation: session.conversation,
        status: session.status,
        running: session.status === "streaming",
        approval: session.approval,
        connection,
        ready,
        lastError: lastError === undefined ? state.lastError : lastError,
      }));
    };

    transport.onConnection((next) => {
      connection = next;
      publish(next === "connected" ? null : undefined);
    });
    transport.onReady((next) => {
      ready = next;
      publish();
    });
    transport.onFrame((frame) => {
      if (frame.type === "event") {
        // DirectChatSocket has already structurally validated the generated
        // browser contract before exposing this frame.
        const envelope = frame.envelope as unknown as BrowserEventEnvelope;
        session = reduceEnvelope(session, envelope, {
          id: reducerId,
        }).session;
        publish(
          envelope.event.type === "error" ? envelope.event.message : undefined,
        );
        return;
      }
      if (frame.type === "command_accepted") {
        const optimistic = findOptimistic(
          session.conversation,
          frame.idempotency_key,
        );
        if (!optimistic) return;
        try {
          const durableId = userMessageIdFromCommandId(frame.command_id);
          session = {
            ...session,
            conversation: acceptOptimistic(
              session.conversation,
              optimistic.id,
              durableId,
            ),
          };
          publish(null);
        } catch (error) {
          publish(
            error instanceof Error
              ? error.message
              : "accepted command_id could not be reconciled",
          );
        }
        return;
      }
      if (frame.type === "command_rejected") {
        const optimistic = findOptimistic(
          session.conversation,
          frame.idempotency_key,
        );
        if (optimistic) {
          session = {
            ...session,
            conversation: patchEntry(
              session.conversation,
              optimistic.id,
              (entry) =>
                entry.kind === "user" && entry.delivery !== "durable"
                  ? {
                      ...entry,
                      delivery: "rejected",
                      rejectReason: frame.reject_reason,
                    }
                  : entry,
            ),
          };
        }
        publish(`Command rejected: ${frame.reject_reason}`);
      }
    });

    return {
      conversation: session.conversation,
      status: session.status,
      running: false,
      approval: null,
      connection,
      ready,
      lastError: null,
      connect() {
        if (started) return;
        started = true;
        connection = "connecting";
        ready = "unknown";
        publish(null);
        transport.connect();
      },
      disconnect() {
        if (!started) return;
        started = false;
        transport.close();
        connection = "closed";
        ready = "unknown";
        publish();
      },
      sendMessage(text) {
        const normalized = text.trim();
        if (!normalized) {
          publish("Message is empty");
          return false;
        }
        if (connection !== "connected" || ready !== "ready") {
          publish("Direct chat is not ready");
          return false;
        }
        const key = idempotencyKey();
        const optimistic: ConversationEntry = {
          kind: "user",
          id: `optimistic:${key}`,
          text: normalized,
          attachments: [],
          timestamp: null,
          delivery: "pending",
          idempotencyKey: key,
        };
        session = {
          ...session,
          conversation: upsertEntry(session.conversation, optimistic),
        };
        publish(null);
        const sent = transport.sendCommand(
          { type: "user_message", text: normalized, attachments: [] },
          key,
        );
        if (!sent) {
          session = {
            ...session,
            conversation: patchEntry(
              session.conversation,
              optimistic.id,
              (entry) =>
                entry.kind === "user"
                  ? {
                      ...entry,
                      delivery: "rejected",
                      rejectReason: "client_validation",
                    }
                  : entry,
            ),
          };
          publish("Message could not be queued");
        }
        return sent;
      },
      abort() {
        if (
          connection !== "connected" ||
          ready !== "ready" ||
          session.status !== "streaming"
        ) {
          publish("There is no connected run to stop");
          return false;
        }
        const sent = transport.sendCommand({ type: "abort" });
        if (!sent) publish("Stop command could not be queued");
        return sent;
      },
      decideApproval(requestId, decision) {
        const entry = session.conversation.entries[`approval:${requestId}`];
        if (
          connection !== "connected" ||
          ready !== "ready" ||
          entry?.kind !== "approval" ||
          entry.status !== "pending" ||
          session.approval?.id !== requestId
        ) {
          publish("Approval is no longer actionable");
          return false;
        }
        const sent = transport.sendCommand({
          type: "approval_decision",
          request_id: requestId,
          decision,
        });
        if (!sent) publish("Approval decision could not be queued");
        return sent;
      },
    };
  });
}

function findOptimistic(
  model: ConversationModel,
  idempotencyKey: string,
): Extract<ConversationEntry, { kind: "user" }> | undefined {
  return Object.values(model.entries).find(
    (entry): entry is Extract<ConversationEntry, { kind: "user" }> =>
      entry.kind === "user" &&
      entry.delivery !== "durable" &&
      entry.idempotencyKey === idempotencyKey,
  );
}

function acceptOptimistic(
  model: ConversationModel,
  optimisticId: string,
  durableId: string,
): ConversationModel {
  const optimistic = model.entries[optimisticId];
  if (optimistic?.kind !== "user" || optimistic.delivery === "durable") {
    return model;
  }
  const existing = model.entries[durableId];
  const entries = { ...model.entries };
  delete entries[optimisticId];
  if (!existing) {
    entries[durableId] = {
      ...optimistic,
      id: durableId,
      delivery: "accepted",
    };
  }
  return {
    ...model,
    entryOrder: model.entryOrder.flatMap((id) =>
      id !== optimisticId ? [id] : existing ? [] : [durableId],
    ),
    entries,
  };
}

export const useConversation = createConversationStore({
  transport: new DirectChatSocket(),
});
