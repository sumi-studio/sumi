import type {
  DirectChatHistoryPage,
  DurableHistoryEvent,
} from "../lib/direct-chat-history";
import type { ConversationModel } from "./model";
import {
  type AgentSession,
  createAgentSession,
  reconcileDeferredTools,
  reduceEnvelope,
} from "./reducer";

export interface ProjectedHistory {
  session: AgentSession;
  entrySeq: Map<string, number>;
}
/** Replay one bounded window privately, publishing no oldest-first intermediate UI. */
export function projectHistory(page: DirectChatHistoryPage): ProjectedHistory {
  const bySeq = new Map<number, DurableHistoryEvent>();
  const visible = new Set(
    [...page.events, ...page.pendingApprovals].map((event) => event.seq),
  );
  for (const event of [
    ...page.context,
    ...page.events,
    ...page.pendingApprovals,
    ...(page.activeRun ? [page.activeRun] : []),
  ])
    bySeq.set(event.seq, event);
  let session = createAgentSession();
  const entrySeq = new Map<string, number>();
  const visibleIds = new Set<string>();
  for (const event of [...bySeq.values()].sort((a, b) => a.seq - b.seq)) {
    const before = session.conversation.entries;
    session = reduceEnvelope(session, event).session;
    for (const id of session.conversation.entryOrder) {
      if (!entrySeq.has(id)) entrySeq.set(id, event.seq);
      if (
        visible.has(event.seq) &&
        before[id] !== session.conversation.entries[id]
      )
        visibleIds.add(id);
    }
  }
  const pending = page.pendingApprovals.at(-1)?.event;
  session = {
    ...session,
    lastDurableSeq: page.latestSeq,
    status: page.activeRun ? "streaming" : "idle",
    activeRunId: page.activeRun ? `run:${page.activeRun.seq}` : null,
    approval: pending?.type === "approval_requested" ? pending.request : null,
    conversation: {
      ...session.conversation,
      entryOrder: session.conversation.entryOrder.filter((id) =>
        visibleIds.has(id),
      ),
      entries: Object.fromEntries(
        Object.entries(session.conversation.entries).filter(([id]) =>
          visibleIds.has(id),
        ),
      ),
    },
  };
  return { session, entrySeq };
}
/** Existing rows and live state win over snapshots fetched while a stream advances. */
export function mergeHistory(
  current: AgentSession,
  older: AgentSession,
  entrySeq: Map<string, number>,
): AgentSession {
  const entries = {
    ...older.conversation.entries,
    ...current.conversation.entries,
  };
  const originalOrder = [
    ...older.conversation.entryOrder,
    ...current.conversation.entryOrder.filter(
      (id) => !older.conversation.entries[id],
    ),
  ];
  const order = originalOrder.sort(
    (a, b) => (entrySeq.get(a) ?? Infinity) - (entrySeq.get(b) ?? Infinity),
  );
  const runs = { ...older.conversation.runs, ...current.conversation.runs };
  for (const [id, oldRun] of Object.entries(older.conversation.runs)) {
    const liveRun = current.conversation.runs[id];
    if (!liveRun) continue;
    const traceById = new Map(oldRun.trace.map((trace) => [trace.id, trace]));
    for (const trace of liveRun.trace) traceById.set(trace.id, trace);
    runs[id] = { ...liveRun, trace: [...traceById.values()] };
  }
  const conversation: ConversationModel = {
    entries,
    entryOrder: order,
    runs,
    runOrder: Object.keys(runs).sort(
      (a, b) => runs[a].startedSeq - runs[b].startedSeq,
    ),
  };
  return reconcileDeferredTools({
    ...current,
    conversation,
    completedMessageIds: {
      ...older.completedMessageIds,
      ...current.completedMessageIds,
    },
    messageRunIds: { ...older.messageRunIds, ...current.messageRunIds },
    toolRunIds: { ...older.toolRunIds, ...current.toolRunIds },
    approvalRunIds: { ...older.approvalRunIds, ...current.approvalRunIds },
    unresolvedToolOutcomes: {
      ...older.unresolvedToolOutcomes,
      ...current.unresolvedToolOutcomes,
    },
    unresolvedApprovalOutcomes: {
      ...older.unresolvedApprovalOutcomes,
      ...current.unresolvedApprovalOutcomes,
    },
    approvalOperations: {
      ...older.approvalOperations,
      ...current.approvalOperations,
    },
  });
}
