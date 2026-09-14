import type { ChatItem, ConversationEntry, ConversationModel } from "./model";

type ApprovalEntry = Extract<ConversationEntry, { kind: "approval" }>;

/**
 * Cross-entry lookup tables the canonical projection consults while turning
 * entries into rows: which prose closes each run's reply, which trace entry
 * renders a tool call, and which approval answers each tool call. The
 * incremental projector keeps the same tables updated per write journal so
 * both paths project identical items.
 */
export interface ProjectionGlobals {
  /** runId (or `message:${messageId}` for runless prose) → last prose id. */
  finalProseByRun: Map<string, string>;
  /** `[runId, traceId]` → the entry id that renders this tool call. */
  toolEntries: Map<string, string>;
  /** `[runId, toolCallId]` → latest approval entry for the call. */
  approvals: Map<string, ApprovalEntry>;
}

export function projectionKey(parts: readonly (string | null)[]): string {
  return JSON.stringify(parts);
}

export function collectProjectionGlobals(
  model: ConversationModel,
): ProjectionGlobals {
  const finalProseByRun = new Map<string, string>();
  for (const entryId of model.entryOrder) {
    const entry = model.entries[entryId];
    if (entry?.kind === "prose") {
      finalProseByRun.set(
        entry.runId ?? `message:${entry.messageId}`,
        entry.id,
      );
    }
  }

  // One operation keeps its invocation slot while its trace receives progress
  // and the eventual receipt. Prefer the invocation even when replay supplied
  // a terminal entry first; do not consume any intervening prose.
  const toolEntries = new Map<string, string>();
  const approvals = new Map<string, ApprovalEntry>();
  for (const entryId of model.entryOrder) {
    const entry = model.entries[entryId];
    if (entry?.kind === "approval") {
      approvals.set(
        projectionKey([entry.runId, entry.request.tool_call_id]),
        entry,
      );
    }
    if (entry?.kind !== "trace") continue;
    const trace = model.runs[entry.runId]?.trace.find(
      (trace) => trace.id === entry.traceId,
    );
    if (trace?.type !== "tool") continue;
    const key = projectionKey([entry.runId, entry.traceId]);
    if (!toolEntries.has(key) || entry.phase === "activity")
      toolEntries.set(key, entryId);
  }
  return { finalProseByRun, toolEntries, approvals };
}

/**
 * Project one entry into its row, or null when the entry is suppressed (a
 * tool trace represented by its invocation row, a resolved approval already
 * covered by that row, an empty reasoning trace).
 */
export function projectEntry(
  model: ConversationModel,
  entry: ConversationEntry,
  globals: ProjectionGlobals,
): ChatItem | null {
  if (entry.kind === "trace") {
    const trace = model.runs[entry.runId]?.trace.find(
      (trace) => trace.id === entry.traceId,
    );
    if (trace?.type === "tool") {
      if (
        globals.toolEntries.get(projectionKey([entry.runId, entry.traceId])) !==
        entry.id
      )
        return null;
      const approval = globals.approvals.get(
        projectionKey([entry.runId, trace.id]),
      );
      return {
        ...entry,
        id: `trace:${entry.runId}:${trace.id}:activity`,
        phase:
          trace.status === "pending" || trace.status === "running"
            ? "activity"
            : "result",
        trace:
          approval &&
          (approval.status === "denied" ||
            approval.status === "rejected" ||
            approval.status === "cancelled")
            ? { ...trace, approvalResolution: approval.status }
            : trace,
      };
    }
    if (trace && (trace.type !== "reasoning" || trace.text.trim())) {
      return { ...entry, trace };
    }
    return null;
  }
  if (
    entry.kind === "approval" &&
    entry.status !== "pending" &&
    globals.toolEntries.has(
      projectionKey([entry.runId, entry.request.tool_call_id]),
    )
  )
    return null;
  if (entry.kind === "prose") {
    return {
      ...entry,
      agentMessageFinal:
        globals.finalProseByRun.get(
          entry.runId ?? `message:${entry.messageId}`,
        ) === entry.id,
    };
  }
  return entry;
}

/** Derives scroll order without creating a second transcript authority. */
export function projectConversation(model: ConversationModel): ChatItem[] {
  const globals = collectProjectionGlobals(model);

  const projected: ChatItem[] = [];
  const insertedRuns = new Set<string>();
  for (const entryId of model.entryOrder) {
    const entry = model.entries[entryId];
    if (!entry) continue;
    const runId = "runId" in entry ? entry.runId : null;
    if (runId && !insertedRuns.has(runId)) {
      const run = model.runs[runId];
      if (run) {
        projected.push(run);
        insertedRuns.add(runId);
      }
    }
    const item = projectEntry(model, entry, globals);
    if (item) projected.push(item);
  }
  for (const runId of model.runOrder) {
    if (insertedRuns.has(runId)) continue;
    const run = model.runs[runId];
    if (run) projected.push(run);
  }
  return projected;
}

export function collectAgentCopyText(
  model: ConversationModel,
): Map<string, string> {
  const textByRun = new Map<string, string>();
  for (const entryId of model.entryOrder) {
    const entry = model.entries[entryId];
    if (entry?.kind !== "prose" || entry.runId === null) continue;
    const previous = textByRun.get(entry.runId);
    textByRun.set(
      entry.runId,
      previous ? `${previous}\n\n${entry.text}` : entry.text,
    );
  }
  return textByRun;
}
