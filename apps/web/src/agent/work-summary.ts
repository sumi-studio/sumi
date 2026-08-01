import type { AgentRun, AgentTraceEvent } from "./model";

export type { AgentRun } from "./model";

export function getInspectableTrace(
  trace: AgentTraceEvent[],
): AgentTraceEvent[] {
  return trace.filter(
    (event) => event.type !== "reasoning" || event.text.trim().length > 0,
  );
}

export function hasInspectableTrace(trace: AgentTraceEvent[]): boolean {
  return trace.some(
    (event) => event.type !== "reasoning" || event.text.trim().length > 0,
  );
}

/** The public contract supports activity state, but not duration or outcome. */
export function describeAgentRun(run: AgentRun): string {
  return run.status === "running" ? "作業中" : "作業が終了しました";
}
