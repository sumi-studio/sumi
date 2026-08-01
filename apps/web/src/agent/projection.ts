import type { ChatItem, ConversationModel } from "./model";

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
