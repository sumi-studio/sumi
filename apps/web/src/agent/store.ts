import type { ApprovalDecision, BrowserEventEnvelope } from "@sumi/api-client";
import { create } from "zustand";
import {
  createDirectChatHistoryAPI,
  type DirectChatHistoryAPI,
  type DirectChatHistoryQuery,
  type DirectChatIndexEntry,
} from "../lib/direct-chat-history";
import {
  type DirectChatConnectionState,
  type DirectChatInstallationBinding,
  type DirectChatReadyState,
  type DirectChatServerFrame,
  DirectChatSocket,
} from "../lib/direct-chat-socket";
import { secureRandomUUID } from "../lib/random-uuid";
import { mergeHistory, projectHistory } from "./history";
import type {
  ConversationEntry,
  ConversationModel,
  RecoverableDraft,
} from "./model";
import { PrivateOutbox, type PrivateOutboxEntry } from "./private-outbox";
import { projectConversation } from "./projection";
import {
  type AgentSession,
  createAgentSession,
  reduceEnvelope,
  removeEntry,
  upsertEntry,
} from "./reducer";
import { userMessageIdFromCommandId } from "./user-message-id";

export interface DirectChatTransport {
  bindInstallation(binding: DirectChatInstallationBinding): void;
  suspendInstallation(): void;
  connect(): void;
  close(): void;
  resetAuthority?(): void;
  setReplayCursor?(seq: number): void;
  sendCommand(command: unknown, idempotencyKey?: string): boolean;
  onFrame(listener: (frame: DirectChatServerFrame) => void): () => void;
  onConnection(
    listener: (state: DirectChatConnectionState) => void,
  ): () => void;
  onReady(listener: (state: DirectChatReadyState) => void): () => void;
}

export interface DirectChatHistoryGap {
  anchorItemId: string;
  beforeSeq: number;
  afterSeq: number;
}
export interface DirectChatHistoryState {
  initialized: boolean;
  loading: boolean;
  loadingOlder: boolean;
  loadingAround: string | null;
  error: string | null;
  hasMore: boolean;
  beforeSeq: number | null;
  index: DirectChatIndexEntry[];
  gaps: DirectChatHistoryGap[];
}
const emptyHistory = (): DirectChatHistoryState => ({
  initialized: false,
  loading: false,
  loadingOlder: false,
  loadingAround: null,
  error: null,
  hasMore: false,
  beforeSeq: null,
  index: [],
  gaps: [],
});

export interface ConversationState {
  conversation: ConversationModel;
  history: DirectChatHistoryState;
  loadOlder: () => Promise<void>;
  loadAround: (messageId: string) => Promise<boolean>;
  loadGap: (beforeSeq: number) => Promise<void>;
  retryHistory: () => Promise<void>;
  status: AgentSession["status"];
  running: boolean;
  approval: AgentSession["approval"];
  /** The request whose decision was accepted locally and is awaiting durability. */
  sendingApprovalRequestId: string | null;
  connection: DirectChatConnectionState;
  ready: DirectChatReadyState;
  lastError: string | null;
  recoverableDrafts: RecoverableDraft[];
  acquireConnection: (binding: DirectChatInstallationBinding) => () => void;
  suspendInstallation: (binding: DirectChatInstallationBinding) => boolean;
  connect: () => void;
  disconnect: () => void;
  resumeMountedConnection: () => void;
  resetAuthority: () => boolean;
  sendMessage: (text: string) => boolean;
  restoreDraft: (idempotencyKey: string) => string | undefined;
  discardDraft: (idempotencyKey: string) => boolean;
  abort: () => boolean;
  decideApproval: (requestId: string, decision: ApprovalDecision) => boolean;
}

export interface ConversationStoreDependencies {
  transport: DirectChatTransport;
  historyAPI?: DirectChatHistoryAPI;
  outbox?: PrivateOutbox;
  idempotencyKey?: () => string;
  reducerId?: () => string;
}

type PrivateReconciliation =
  | { kind: "unmatched" }
  | { kind: "reconciled" }
  | { kind: "error"; message: string };

export function createConversationStore({
  transport,
  historyAPI,
  outbox = new PrivateOutbox(),
  idempotencyKey = secureRandomUUID,
  reducerId = secureRandomUUID,
}: ConversationStoreDependencies) {
  let session = createAgentSession();
  let history = emptyHistory();
  let historyRequest: AbortController | null = null;
  let failedHistory: {
    query: DirectChatHistoryQuery;
    mode: "initial" | "older" | "around" | "gap";
  } | null = null;
  const entrySequences = new Map<string, number>();
  let ranges: { start: number; end: number; anchorItemId: string }[] = [];

  let connection: DirectChatConnectionState = "connecting";
  let ready: DirectChatReadyState = "unknown";
  let started = false;
  let privateStateQuarantined = false;
  const connectionOwners = new Set<symbol>();
  let connectionGeneration = 0;
  let pendingConnectionGeneration: number | null = null;
  let boundInstallation: DirectChatInstallationBinding | null = null;
  const approvalSubmissionLatches = new Map<string, string>();
  const undurableAdmissions = new Map<
    string,
    Extract<PrivateOutboxEntry, { state: "admitted" }>
  >();

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
        history,
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
        recoverableDrafts: privateStateQuarantined
          ? []
          : outbox.recoverableDrafts(),
      }));
    };

    const cancelHistory = () => {
      historyRequest?.abort();
      historyRequest = null;
      history = {
        ...history,
        loading: false,
        loadingOlder: false,
        loadingAround: null,
      };
    };
    const mergeIndex = (incoming: DirectChatIndexEntry[]) => {
      const rows = new Map(history.index.map((row) => [row.id, row]));
      for (const row of incoming) rows.set(row.id, row);
      const index = [...rows.values()].sort((a, b) => a.seq - b.seq);
      return JSON.stringify(index) === JSON.stringify(history.index)
        ? history.index
        : index;
    };
    const recordRange = (start: number, end: number, anchorItemId: string) => {
      const ordered = [...ranges, { start, end, anchorItemId }].sort(
        (a, b) => a.start - b.start,
      );
      ranges = [];
      for (const range of ordered) {
        const last = ranges.at(-1);
        if (last && range.start <= last.end) {
          last.end = Math.max(last.end, range.end);
          if (!last.anchorItemId) last.anchorItemId = range.anchorItemId;
        } else ranges.push({ ...range });
      }
      return ranges.slice(1).flatMap((range, i) =>
        range.anchorItemId
          ? [
              {
                anchorItemId: range.anchorItemId,
                beforeSeq: range.start,
                afterSeq: ranges[i].end,
              },
            ]
          : [],
      );
    };
    const loadHistoryPage = async (
      query: DirectChatHistoryQuery,
      mode: "initial" | "older" | "around" | "gap",
    ): Promise<boolean> => {
      if (!historyAPI || !boundInstallation) return false;
      if (mode !== "initial" && !history.initialized) return false;
      if (historyRequest) {
        if (mode !== "around") return false;
        cancelHistory();
      }
      failedHistory = null;
      const binding = { ...boundInstallation };
      const controller = new AbortController();
      historyRequest = controller;
      history = {
        ...history,
        loading: mode === "initial",
        loadingOlder: mode === "older" || mode === "gap",
        loadingAround:
          mode === "around" ? (query.aroundMessageId ?? null) : null,
        error: null,
      };
      publish();
      try {
        const admittedIds = [
          ...new Set(
            [...outbox.entries(), ...undurableAdmissions.values()].flatMap(
              (entry) => (entry.state === "admitted" ? [entry.commandId] : []),
            ),
          ),
        ];
        const requestQuery =
          mode === "initial"
            ? {
                ...query,
                ...(admittedIds.length ? { commandIds: admittedIds } : {}),
              }
            : { ...query, includeIndex: false };
        const page = await historyAPI.page(
          binding,
          requestQuery,
          controller.signal,
        );
        if (
          controller.signal.aborted ||
          historyRequest !== controller ||
          !sameInstallationBinding(boundInstallation, binding)
        )
          return false;
        const projected = projectHistory(page);
        for (const [id, seq] of projected.entrySeq)
          if (!entrySequences.has(id)) entrySequences.set(id, seq);
        session =
          mode === "initial"
            ? projected.session
            : mergeHistory(session, projected.session, entrySequences);
        const firstVisibleSeq = Math.min(
          ...page.events.map((event) => event.seq),
        );
        const firstRow = projectConversation(
          projected.session.conversation,
        ).find(
          (row) =>
            row.kind !== "agent-run" &&
            (projected.entrySeq.get(row.id) ??
              (row.kind === "trace"
                ? projected.entrySeq.get(
                    `trace:${row.runId}:${row.traceId}:result`,
                  )
                : undefined) ??
              -1) >= firstVisibleSeq,
        );
        const start = page.hasMore ? (page.beforeSeq ?? 0) : 0;
        const end =
          mode === "initial"
            ? page.latestSeq
            : (query.beforeSeq ??
              Math.max(0, ...page.events.map((event) => event.seq)));
        const gaps = recordRange(start, end, firstRow?.id ?? "");
        const advancesOldest =
          history.beforeSeq === null ||
          page.beforeSeq === null ||
          page.beforeSeq < history.beforeSeq;
        history = {
          ...history,
          initialized: true,
          loading: false,
          loadingOlder: false,
          loadingAround: null,
          error: null,
          index: mergeIndex(page.index),
          gaps,
          ...(mode === "initial" || advancesOldest
            ? { beforeSeq: page.beforeSeq, hasMore: page.hasMore }
            : {}),
        };
        if (mode === "initial") {
          if (!transport.setReplayCursor)
            throw new Error("履歴の接続を開始できませんでした。");
          for (const event of [
            ...page.events,
            ...(page.commandDispositions ?? []),
          ]) {
            if (event.event.type === "command_disposition") {
              const result = applyDisposition(event.event);
              if (result.kind === "error") throw new Error(result.message);
            }
          }
          for (const row of page.index) {
            const result = reconcileCanonicalMessage(row.id);
            if (result.kind === "error") throw new Error(result.message);
          }
          transport.setReplayCursor(page.latestSeq);
        }
        historyRequest = null;
        publish(null);
        return (
          !query.aroundMessageId ||
          !!session.conversation.entries[query.aroundMessageId]
        );
      } catch (error) {
        if (controller.signal.aborted || historyRequest !== controller)
          return false;
        historyRequest = null;
        failedHistory = { query, mode };
        history = {
          ...history,
          loading: false,
          loadingOlder: false,
          loadingAround: null,
          error:
            error instanceof Error
              ? error.message
              : "会話の履歴を読み込めませんでした。",
        };
        if (mode === "initial") {
          started = false;
          connection = "closed";
          history = { ...history, initialized: false };
        }
        publish(mode === "initial" ? history.error : undefined);
        return false;
      }
    };

    const cancelPendingConnection = () => {
      connectionGeneration += 1;
      pendingConnectionGeneration = null;
    };

    const startConnection = () => {
      if (started) return;
      if (privateStateQuarantined) {
        publish(
          "Private delivery state must be cleared before direct chat can reconnect",
        );
        return;
      }
      started = true;
      connection = "connecting";
      ready = "unknown";
      publish(null);
      if (historyAPI && !history.initialized) {
        void loadHistoryPage({}, "initial").then((ok) => {
          if (ok && started && boundInstallation) transport.connect();
        });
      } else transport.connect();
    };

    const stopConnection = () => {
      cancelHistory();
      const wasStarted = started;
      started = false;
      if (wasStarted) transport.close();
      connection = "closed";
      ready = "unknown";
      publish();
    };

    const scheduleMountedConnection = () => {
      if (
        connectionOwners.size === 0 ||
        started ||
        pendingConnectionGeneration !== null ||
        privateStateQuarantined
      ) {
        return;
      }
      connection = "connecting";
      ready = "unknown";
      publish();
      const generation = ++connectionGeneration;
      pendingConnectionGeneration = generation;
      queueMicrotask(() => {
        if (pendingConnectionGeneration === generation) {
          pendingConnectionGeneration = null;
        }
        if (
          connectionGeneration !== generation ||
          connectionOwners.size === 0 ||
          started
        ) {
          return;
        }
        startConnection();
      });
    };

    const settleOwnerlessConnection = () => {
      const generation = connectionGeneration;
      queueMicrotask(() => {
        if (
          connectionGeneration !== generation ||
          connectionOwners.size !== 0 ||
          pendingConnectionGeneration !== null ||
          started
        ) {
          return;
        }
        connection = "closed";
        ready = "unknown";
        publish();
      });
    };

    const bindInstallation = (
      binding: DirectChatInstallationBinding,
    ): boolean => {
      const normalized = normalizeInstallationBinding(binding);
      if (!normalized) {
        publish("Direct chat installation is unavailable");
        return false;
      }
      if (sameInstallationBinding(boundInstallation, normalized)) return true;

      cancelPendingConnection();
      if (started) stopConnection();
      if (
        !recoverPendingCommands(
          "installation_changed",
          "Pending direct-chat text could not be fenced before the app installation changed",
        )
      )
        return false;
      transport.bindInstallation(normalized);
      boundInstallation = normalized;
      publish(null);
      return true;
    };

    const removeOptimistic = (idempotencyKey: string) => {
      const optimistic = findOptimistic(session.conversation, idempotencyKey);
      if (!optimistic) return;
      session = {
        ...session,
        conversation: removeEntry(session.conversation, optimistic.id),
      };
    };

    const recoverPendingCommands = (
      reason: string,
      failureMessage: string,
    ): boolean => {
      let recoveryFailed = false;
      for (const entry of outbox.entries()) {
        if (entry.state !== "pending") continue;
        const recovered = outbox.recoverByIdempotencyKey(
          entry.idempotencyKey,
          reason,
        );
        if (!recovered) {
          recoveryFailed = true;
          continue;
        }
        removeOptimistic(entry.idempotencyKey);
      }
      if (!recoveryFailed) return true;
      privateStateQuarantined = true;
      publish(failureMessage);
      return false;
    };

    const suspendInstallation = (
      binding: DirectChatInstallationBinding,
    ): boolean => {
      const normalized = normalizeInstallationBinding(binding);
      if (
        !normalized ||
        !sameInstallationBinding(boundInstallation, normalized)
      )
        return true;

      cancelPendingConnection();
      if (started) stopConnection();
      transport.suspendInstallation();
      boundInstallation = null;
      approvalSubmissionLatches.clear();
      const recovered = recoverPendingCommands(
        "installation_suspended",
        "Pending direct-chat text could not be recovered after the app was disabled",
      );
      if (recovered) publish(null);
      return recovered;
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
    ): PrivateReconciliation => {
      const correlationKey = commandCorrelationKey(
        disposition.command_id,
        disposition.command_seq,
      );
      const undurableEntry = undurableAdmissions.get(correlationKey);
      const entry =
        outbox.findByCommand(disposition.command_id, disposition.command_seq) ??
        undurableEntry;
      if (!entry) return { kind: "unmatched" };
      if (disposition.status === "applied") {
        const canonicalMessageId = userMessageIdFromCommandId(
          disposition.command_id,
        );
        if (isCanonicalUserMessage(session.conversation, canonicalMessageId)) {
          const cleared = outbox.removeByIdempotencyKey(entry.idempotencyKey);
          if (undurableEntry) {
            undurableAdmissions.delete(correlationKey);
            removeOptimistic(entry.idempotencyKey);
          } else if (cleared) {
            removeOptimistic(entry.idempotencyKey);
          }
          return cleared
            ? { kind: "reconciled" }
            : {
                kind: "error",
                message:
                  "Message was applied, but local recovery state could not be cleared",
              };
        } else {
          ensureOptimistic(entry, "admitted");
        }
        return { kind: "unmatched" };
      }
      const reason =
        disposition.status === "superseded"
          ? "superseded"
          : (disposition.reject_reason ?? "rejected");
      const recovered = undurableEntry
        ? outbox.recoverByIdempotencyKey(entry.idempotencyKey, reason)
        : outbox.recoverByCommand(
            disposition.command_id,
            disposition.command_seq,
            reason,
          );
      if (undurableEntry) {
        undurableAdmissions.delete(correlationKey);
        removeOptimistic(entry.idempotencyKey);
      } else if (recovered) {
        removeOptimistic(entry.idempotencyKey);
      }
      return recovered
        ? { kind: "reconciled" }
        : {
            kind: "error",
            message: "Command outcome could not be saved for local recovery",
          };
    };

    const reconcileCanonicalMessage = (
      messageId: string,
    ): PrivateReconciliation => {
      let matched = false;
      let error: string | null = null;
      for (const entry of outbox.entries()) {
        if (entry.state !== "admitted") continue;
        if (userMessageIdFromCommandId(entry.commandId) !== messageId) continue;
        matched = true;
        if (outbox.removeByIdempotencyKey(entry.idempotencyKey)) {
          removeOptimistic(entry.idempotencyKey);
        } else {
          error =
            "Canonical message arrived, but local recovery state could not be cleared";
        }
      }
      for (const [correlationKey, entry] of undurableAdmissions) {
        if (userMessageIdFromCommandId(entry.commandId) !== messageId) continue;
        matched = true;
        const cleared = outbox.removeByIdempotencyKey(entry.idempotencyKey);
        undurableAdmissions.delete(correlationKey);
        removeOptimistic(entry.idempotencyKey);
        if (!cleared) {
          error =
            "Canonical message arrived, but local recovery state could not be cleared";
        }
      }
      if (error) return { kind: "error", message: error };
      return matched ? { kind: "reconciled" } : { kind: "unmatched" };
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
        const beforeEntries = session.conversation.entries;
        const reduced = reduceEnvelope(session, envelope, {
          id: reducerId,
        });
        session = reduced.session;
        for (const id of session.conversation.entryOrder)
          if (!beforeEntries[id] && !entrySequences.has(id))
            entrySequences.set(
              id,
              "seq" in envelope ? envelope.seq : session.lastDurableSeq + 0.5,
            );
        if (
          "seq" in envelope &&
          envelope.event.type === "message_end" &&
          envelope.event.message.role === "user" &&
          envelope.event.message.incoming_source?.source.surface !==
            "approval_operation"
        ) {
          const title = envelope.event.message.content
            .filter((item) => item.type === "text")
            .map((item) => item.text)
            .join("")
            .slice(0, 160);
          history = {
            ...history,
            index: mergeIndex([
              { id: envelope.event.message_id, seq: envelope.seq, title },
            ]),
          };
        }

        let reconciliation: PrivateReconciliation = { kind: "unmatched" };
        if (reduced.kind === "applied") {
          if (envelope.event.type === "approval_resolved") {
            approvalSubmissionLatches.delete(envelope.event.request_id);
          }
          if (envelope.event.type === "command_disposition") {
            reconciliation = applyDisposition(envelope.event);
          } else if (
            (envelope.event.type === "message_start" ||
              envelope.event.type === "message_end") &&
            envelope.event.message.role === "user"
          ) {
            reconciliation = reconcileCanonicalMessage(
              envelope.event.message_id,
            );
          }
        }
        publish(
          reconciliation.kind === "error"
            ? reconciliation.message
            : reconciliation.kind === "reconciled"
              ? null
              : envelope.event.type === "error"
                ? envelope.event.message
                : undefined,
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
          if (admitted.kind === "persistence_failed") {
            const correlationKey = commandCorrelationKey(
              frame.command_id,
              frame.seq,
            );
            undurableAdmissions.set(correlationKey, admitted.entry);
            ensureOptimistic(admitted.entry, "admitted");
            if (frame.disposition) {
              const reconciliation = applyDisposition(frame.disposition);
              if (reconciliation.kind === "error") {
                publish(reconciliation.message);
                return;
              }
            }
            publish(
              undurableAdmissions.has(correlationKey)
                ? "Command admission could not be saved for recovery"
                : null,
            );
            return;
          }
          if (admitted.kind === "conflict") {
            publish("Command admission could not be reconciled");
            return;
          }
          undurableAdmissions.delete(
            commandCorrelationKey(frame.command_id, frame.seq),
          );
          if (
            isCanonicalUserMessage(session.conversation, canonicalMessageId)
          ) {
            const cleared = outbox.removeByIdempotencyKey(
              frame.idempotency_key,
            );
            if (cleared) {
              removeOptimistic(frame.idempotency_key);
            }
            publish(
              cleared
                ? null
                : "Message was admitted, but local recovery state could not be cleared",
            );
            return;
          }
          if (frame.disposition) {
            const reconciliation = applyDisposition(frame.disposition);
            if (reconciliation.kind === "error") {
              publish(reconciliation.message);
              return;
            }
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
        for (const [requestId, key] of approvalSubmissionLatches) {
          if (key === frame.idempotency_key) {
            approvalSubmissionLatches.delete(requestId);
          }
        }
        const entry = outbox.findByIdempotencyKey(frame.idempotency_key);
        if (!entry) {
          publish(`Command rejected: ${frame.reject_reason}`);
          return;
        }
        const recovered = outbox.recoverByIdempotencyKey(
          frame.idempotency_key,
          frame.reject_reason,
        );
        if (recovered) {
          removeOptimistic(frame.idempotency_key);
        }
        publish(
          recovered
            ? `Command rejected: ${frame.reject_reason}`
            : "Command rejection could not be saved for local recovery",
        );
      }
    });

    return {
      history,
      async loadOlder() {
        if (history.hasMore && history.beforeSeq !== null)
          await loadHistoryPage({ beforeSeq: history.beforeSeq }, "older");
      },
      async loadAround(messageId) {
        if (session.conversation.entries[messageId]) return true;
        return loadHistoryPage({ aroundMessageId: messageId }, "around");
      },
      async retryHistory() {
        const failed = failedHistory;
        if (!failed) return;
        if (failed.mode === "initial") {
          startConnection();
          return;
        }
        await loadHistoryPage(failed.query, failed.mode);
      },
      async loadGap(beforeSeq) {
        await loadHistoryPage({ beforeSeq }, "gap");
      },
      conversation: session.conversation,
      status: session.status,
      running: false,
      approval: null,
      sendingApprovalRequestId: null,
      connection,
      ready,
      lastError: null,
      recoverableDrafts: outbox.recoverableDrafts(),
      acquireConnection(binding) {
        if (!bindInstallation(binding)) return () => undefined;
        const owner = Symbol("direct-chat-connection-owner");
        connectionOwners.add(owner);
        scheduleMountedConnection();
        let released = false;
        return () => {
          if (released) return;
          released = true;
          connectionOwners.delete(owner);
          if (connectionOwners.size !== 0) return;
          cancelPendingConnection();
          if (started) {
            stopConnection();
          } else {
            // A StrictMode probe releases and reacquires in one task. Settle
            // the display state only after a later owner has had that chance.
            settleOwnerlessConnection();
          }
        };
      },
      suspendInstallation,
      connect() {
        cancelPendingConnection();
        startConnection();
      },
      disconnect() {
        cancelPendingConnection();
        stopConnection();
      },
      resumeMountedConnection() {
        if (boundInstallation !== null) scheduleMountedConnection();
      },
      resetAuthority() {
        cancelHistory();
        history = emptyHistory();
        failedHistory = null;
        entrySequences.clear();
        ranges = [];
        cancelPendingConnection();
        started = false;
        if (transport.resetAuthority) {
          transport.resetAuthority();
        } else {
          transport.close();
        }
        boundInstallation = null;
        const cleared = outbox.clear();
        privateStateQuarantined = !cleared;
        undurableAdmissions.clear();
        approvalSubmissionLatches.clear();
        session = createAgentSession();
        connection = "closed";
        ready = "unknown";
        publish(
          cleared
            ? null
            : "Private delivery state could not be cleared; authority switch was blocked",
        );
        return cleared;
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
        if (privateStateQuarantined) return undefined;
        const text = outbox.consumeRecoverable(idempotencyKey);
        if (text !== undefined) publish(null);
        return text;
      },
      discardDraft(idempotencyKey) {
        if (privateStateQuarantined) return false;
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
        const key = idempotencyKey();
        // This latch is set before sendCommand because transports are allowed
        // to synchronously return or emit frames. It prevents conflicting
        // decisions until resolution or rejection of this exact submission.
        approvalSubmissionLatches.set(requestId, key);
        publish(null);
        const sent = transport.sendCommand(
          {
            type: "approval_decision",
            request_id: requestId,
            decision,
          },
          key,
        );
        if (!sent && approvalSubmissionLatches.get(requestId) === key) {
          approvalSubmissionLatches.delete(requestId);
          publish("Approval decision could not be queued");
        }
        return sent;
      },
    };
  });
}

function commandCorrelationKey(commandId: string, commandSeq: number): string {
  return `${commandId}:${commandSeq}`;
}

const MAX_AUTHORITY_EPOCH = 9_223_372_036_854_775_807n;

function normalizeInstallationBinding(
  binding: DirectChatInstallationBinding,
): DirectChatInstallationBinding | null {
  const installationId = binding.installationId.trim();
  const authorityEpoch = binding.authorityEpoch.trim();
  if (
    !installationId ||
    !/^[1-9][0-9]*$/.test(authorityEpoch) ||
    authorityEpoch.length > 19 ||
    BigInt(authorityEpoch) > MAX_AUTHORITY_EPOCH
  ) {
    return null;
  }
  return { installationId, authorityEpoch };
}

function sameInstallationBinding(
  left: DirectChatInstallationBinding | null,
  right: DirectChatInstallationBinding,
): boolean {
  return (
    left?.installationId === right.installationId &&
    left.authorityEpoch === right.authorityEpoch
  );
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
  historyAPI: createDirectChatHistoryAPI(),
});
