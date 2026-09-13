import type { ToolSpec } from "./provider.ts";

/**
 * Tool registry. The core hands `specs` to the model and routes each tool
 * call through the state service operation ledger. For the slice, only
 * state-internal tools are registered: their effects commit atomically
 * inside the claim transaction (no crash window between effect and receipt).
 *
 * External-side-effect tools (send email, post to Slack, call a paid API)
 * are deliberately NOT in this registry — they need the authorized-tool
 * contract (durable intent → guarded execution → receipt) before the model
 * may invoke them. See progress.md.
 */

export interface RegisteredTool extends ToolSpec {
  /** Marked internal: effect applied atomically by the state service. */
  readonly internal: true;
}

export const INTERNAL_TOOLS: RegisteredTool[] = [
  {
    internal: true,
    name: "schedule.set",
    description:
      "Schedule a future wake-up for yourself. The payload becomes a durable input at wake_at. Use for reminders and follow-ups.",
    parameters: {
      type: "object",
      properties: {
        wake_at: { type: "string", description: "RFC3339 timestamp to wake" },
        payload: {
          type: "object",
          description: "input payload delivered at wake",
        },
      },
      required: ["wake_at"],
    },
  },
  {
    internal: true,
    name: "journal.note",
    description:
      "Append a durable note to your journal. Use for facts, decisions, or memories worth keeping across restarts.",
    parameters: {
      type: "object",
      properties: {
        text: { type: "string", description: "the note content" },
        kind: { type: "string", description: "optional note kind/tag" },
      },
      required: ["text"],
    },
  },
  {
    internal: true,
    name: "job.start",
    description:
      "Start a background job that keeps running even if you stop. Returns a job record with a job_id; the result arrives later as a 'job_completed' input — do not wait for it in this turn. Use for commands or tasks that may outlive this turn.",
    parameters: {
      type: "object",
      properties: {
        command: {
          type: "array",
          items: { type: "string" },
          description:
            'executable and arguments, e.g. ["bash","-lc","make test"] — never a shell string',
        },
        cwd: {
          type: "string",
          description: "working directory, relative to the workspace root",
        },
        timeout_ms: {
          type: "integer",
          description: "max run time in milliseconds (<= 3600000)",
        },
      },
      required: ["command"],
    },
  },
  {
    internal: true,
    name: "job.status",
    description:
      "Read a job's current status and, once finished, its recorded result. Reading a result never re-runs the job.",
    parameters: {
      type: "object",
      properties: {
        job_id: { type: "string", description: "id from job.start" },
      },
      required: ["job_id"],
    },
  },
  {
    internal: true,
    name: "job.cancel",
    description:
      "Ask to cancel a job. A queued job is cancelled immediately; a running job is asked to stop and its runner reports the real outcome (the command may already have finished).",
    parameters: {
      type: "object",
      properties: {
        job_id: { type: "string", description: "id from job.start" },
      },
      required: ["job_id"],
    },
  },
];

/** Model-visible specs for the registered internal tools. */
export function toolSpecs(): ToolSpec[] {
  return INTERNAL_TOOLS.map(({ name, description, parameters }) => ({
    name,
    description,
    parameters,
  }));
}
