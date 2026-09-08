import type { ChatItem, ConversationEntry, ConversationModel } from "./model";

/** Derives scroll order without creating a second transcript authority. */
export function projectConversation(model: ConversationModel): ChatItem[] {
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
  const approvals = new Map<
    string,
    Extract<ConversationEntry, { kind: "approval" }>
  >();
  for (const entryId of model.entryOrder) {
    const entry = model.entries[entryId];
    if (entry?.kind === "approval") {
      approvals.set(
        JSON.stringify([entry.runId, entry.request.tool_call_id]),
        entry,
      );
    }
    if (entry?.kind !== "trace") continue;
    const trace = model.runs[entry.runId]?.trace.find(
      (trace) => trace.id === entry.traceId,
    );
    if (trace?.type !== "tool") continue;
    const key = JSON.stringify([entry.runId, entry.traceId]);
    if (!toolEntries.has(key) || entry.phase === "activity")
      toolEntries.set(key, entryId);
  }

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
    if (entry.kind === "trace") {
      const trace = model.runs[entry.runId]?.trace.find(
        (trace) => trace.id === entry.traceId,
      );
      if (trace?.type === "tool") {
        if (
          toolEntries.get(JSON.stringify([entry.runId, entry.traceId])) !==
          entryId
        )
          continue;
        const approval = approvals.get(JSON.stringify([entry.runId, trace.id]));
        projected.push({
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
        });
      } else if (trace && (trace.type !== "reasoning" || trace.text.trim())) {
        projected.push({ ...entry, trace });
      }
    } else {
      if (
        entry.kind === "approval" &&
        entry.status !== "pending" &&
        toolEntries.has(
          JSON.stringify([entry.runId, entry.request.tool_call_id]),
        )
      )
        continue;
      projected.push(
        entry.kind === "prose"
          ? {
              ...entry,
              agentMessageFinal:
                finalProseByRun.get(
                  entry.runId ?? `message:${entry.messageId}`,
                ) === entry.id,
            }
          : entry,
      );
    }
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
