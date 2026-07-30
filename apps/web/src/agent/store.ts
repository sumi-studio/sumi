import type { ApprovalDecision, BrowserEventEnvelope } from "@sumi/api-client";
import { create } from "zustand";
import {
  type DirectChatConnectionState,
  type DirectChatReadyState,
  type DirectChatServerFrame,
  DirectChatSocket,
} from "../lib/direct-chat-socket";
import { secureRandomUUID } from "../lib/random-uuid";
import type {
  ConversationEntry,
  ConversationModel,
  RecoverableDraft,
} from "./model";
import { PrivateOutbox } from "./private-outbox";
import {
  type AgentSession,
  commandDispositionKey,
  createAgentSession,
  reduceEnvelope,
  removeEntry,
  upsertEntry,
} from "./reducer";
import { userMessageIdFromCommandId } from "./user-message-id";

export interface DirectChatTransport {
  connect(): void;
  close(): void;
  resetAuthority?(): void;
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
  /** The request whose decision was accepted locally and is awaiting durability. */
  sendingApprovalRequestId: string | null;
  connection: DirectChatConnectionState;
  ready: DirectChatReadyState;
  lastError: string | null;
  recoverableDrafts: RecoverableDraft[];
  connect: () => void;
  disconnect: () => void;
  resetAuthority: () => void;
  sendMessage: (text: string) => boolean;
  restoreDraft: (idempotencyKey: string) => string | undefined;
  discardDraft: (idempotencyKey: string) => boolean;
  abort: () => boolean;
  decideApproval: (requestId: string, decision: ApprovalDecision) => boolean;
}

export interface ConversationStoreDependencies {
  transport: DirectChatTransport;
  outbox?: PrivateOutbox;
  idempotencyKey?: () => string;
  reducerId?: () => string;
}

export function createConversationStore({
  transport,
  outbox = new PrivateOutbox(),
  idempotencyKey = secureRandomUUID,
  reducerId = secureRandomUUID,
}: ConversationStoreDependencies) {
  let session = createAgentSession();
  let connection: DirectChatConnectionState = "connecting";
  let ready: DirectChatReadyState = "unknown";
  let started = false;
  const approvalSubmissionLatches = new Set<string>();

  // Pending rows restored from a prior page are intentionally not replayed:
  // the user never saw a durable admission and an automatic retry could
  // duplicate a command. Make the text explicit and recoverable instead.
  for (const entry of outbox.entries()) {
    if (entry.state === "pending") {
      outbox.recoverByIdempotencyKey(entry.idempotencyKey, "unavailable");
    }
  }

  return create<ConversationState>((set) => {
    const publish = (lastError?: string | null) => {
      set((state) => ({
        conversation: session.conversation,
        status: session.status,
        running: session.status === "streaming",
        approval: session.approval,
        sendingApprovalRequestId:
          session.approval && approvalSubmissionLatches.has(session.approval.id)
            ? session.approval.id
            : null,
        connection,
        ready,
        lastError: lastError === undefined ? state.lastError : lastError,
        recoverableDrafts: outbox.recoverableDrafts(),
      }));
    };

    const removeOptimistic = (idempotencyKey: string) => {
      const optimistic = findOptimistic(session.conversation, idempotencyKey);
      if (!optimistic) return;
      session = {
        ...session,
        conversation: removeEntry(session.conversation, optimistic.id),
      };
    };

    const ensureOptimistic = (
      entry: { idempotencyKey: string; text: string },
      delivery: "pending" | "admitted",
    ) => {
      const optimistic = findOptimistic(
        session.conversation,
        entry.idempotencyKey,
      );
      const next: ConversationEntry = {
        kind: "user",
        id: optimistic?.id ?? `optimistic:${entry.idempotencyKey}`,
        text: entry.text,
        attachments: [],
        timestamp: null,
        delivery,
        idempotencyKey: entry.idempotencyKey,
      };
      session = {
        ...session,
        conversation: upsertEntry(session.conversation, next),
      };
    };

    const applyDisposition = (
      disposition: Extract<
        BrowserEventEnvelope["event"],
        { type: "command_disposition" }
      >,
    ) => {
      const entry = outbox.findByCommand(
        disposition.command_id,
        disposition.command_seq,
      );
      if (!entry) return;
      if (disposition.status === "applied") {
        const canonicalMessageId = userMessageIdFromCommandId(
          disposition.command_id,
        );
        if (isCanonicalUserMessage(session.conversation, canonicalMessageId)) {
          outbox.removeByIdempotencyKey(entry.idempotencyKey);
          removeOptimistic(entry.idempotencyKey);
        } else {
          ensureOptimistic(entry, "admitted");
        }
        return;
      }
      outbox.recoverByCommand(
        disposition.command_id,
        disposition.command_seq,
        disposition.status === "superseded"
          ? "superseded"
          : (disposition.reject_reason ?? "rejected"),
      );
      removeOptimistic(entry.idempotencyKey);
    };

    const reconcileCanonicalMessage = (messageId: string) => {
      for (const entry of outbox.entries()) {
        if (entry.state !== "admitted") continue;
        if (userMessageIdFromCommandId(entry.commandId) !== messageId) continue;
        outbox.removeByIdempotencyKey(entry.idempotencyKey);
        removeOptimistic(entry.idempotencyKey);
      }
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
        const reduced = reduceEnvelope(session, envelope, {
          id: reducerId,
        });
        session = reduced.session;
        if (reduced.kind === "applied") {
          if (envelope.event.type === "approval_resolved") {
            approvalSubmissionLatches.delete(envelope.event.request_id);
          }
          if (envelope.event.type === "command_disposition") {
            applyDisposition(envelope.event);
          } else if (
            (envelope.event.type === "message_start" ||
              envelope.event.type === "message_end") &&
            envelope.event.message.role === "user"
          ) {
            reconcileCanonicalMessage(envelope.event.message_id);
          }
        }
        publish(
          envelope.event.type === "error" ? envelope.event.message : undefined,
        );
        return;
      }
      if (frame.type === "command_accepted") {
        try {
          const canonicalMessageId = userMessageIdFromCommandId(
            frame.command_id,
          );
          const admitted = outbox.admit(
            frame.idempotency_key,
            frame.command_id,
            frame.seq,
          );
          if (admitted.kind === "missing") return;
          if (admitted.kind === "already_recoverable") return;
          if (admitted.kind === "conflict") {
            publish("Command admission could not be reconciled");
            return;
          }
          if (
            isCanonicalUserMessage(session.conversation, canonicalMessageId)
          ) {
            outbox.removeByIdempotencyKey(frame.idempotency_key);
            removeOptimistic(frame.idempotency_key);
            publish(null);
            return;
          }
          const disposition =
            session.commandDispositions[
              commandDispositionKey(frame.command_id, frame.seq)
            ];
          if (disposition) {
            applyDisposition(disposition);
          } else {
            ensureOptimistic(admitted.entry, "admitted");
          }
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
        outbox.recoverByIdempotencyKey(
          frame.idempotency_key,
          frame.reject_reason,
        );
        removeOptimistic(frame.idempotency_key);
        publish(`Command rejected: ${frame.reject_reason}`);
      }
    });

    return {
      conversation: session.conversation,
      status: session.status,
      running: false,
      approval: null,
      sendingApprovalRequestId: null,
      connection,
      ready,
      lastError: null,
      recoverableDrafts: outbox.recoverableDrafts(),
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
      resetAuthority() {
        started = false;
        if (transport.resetAuthority) {
          transport.resetAuthority();
        } else {
          transport.close();
        }
        outbox.clear();
        session = createAgentSession();
        connection = "closed";
        ready = "unknown";
        publish(null);
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
        if (!outbox.putPending(key, normalized)) {
          publish("Message could not be saved for recovery");
          return false;
        }
        ensureOptimistic({ idempotencyKey: key, text: normalized }, "pending");
        publish(null);
        const sent = transport.sendCommand(
          { type: "user_message", text: normalized, attachments: [] },
          key,
        );
        if (!sent) {
          outbox.recoverByIdempotencyKey(key, "client_validation");
          removeOptimistic(key);
          publish("Message could not be queued");
        }
        return sent;
      },
      restoreDraft(idempotencyKey) {
        const text = outbox.consumeRecoverable(idempotencyKey);
        if (text !== undefined) publish(null);
        return text;
      },
      discardDraft(idempotencyKey) {
        const entry = outbox.findByIdempotencyKey(idempotencyKey);
        if (entry?.state !== "recoverable") return false;
        const removed = outbox.removeByIdempotencyKey(idempotencyKey);
        if (removed) publish();
        return removed;
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
        if (approvalSubmissionLatches.has(requestId)) {
          publish();
          return false;
        }
        // This latch is set before sendCommand because transports are allowed
        // to synchronously return or emit frames. It prevents conflicting
        // decisions during the durable-resolution gap.
        approvalSubmissionLatches.add(requestId);
        publish(null);
        const sent = transport.sendCommand({
          type: "approval_decision",
          request_id: requestId,
          decision,
        });
        if (!sent) {
          approvalSubmissionLatches.delete(requestId);
          publish("Approval decision could not be queued");
        }
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

function isCanonicalUserMessage(
  model: ConversationModel,
  messageId: string,
): boolean {
  return model.entries[messageId]?.kind === "user";
}

export const useConversation = createConversationStore({
  transport: new DirectChatSocket(),
});
