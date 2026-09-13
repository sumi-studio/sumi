import type { ToolSpec } from "./provider.ts";

/**
 * Tool registry. The core hands `specs` to the model and routes each tool
 * call through the state service operation ledger. For the slice, only
 * state-internal tools are registered: their effects commit atomically
 * inside the claim transaction (no crash window between effect and receipt).
 *
 * message.send is outward-facing — speaking into the shared channel as the
 * secretary — so the state service holds every call for an explicit human
 * decision before its effect may run (ADR 0013). The model may also elevate
 * any call itself via the route field of the provider envelope; it can
 * never lower an intrinsic requirement.
 *
 * Other external-side-effect tools (send email, call a paid API) are
 * deliberately NOT in this registry — they need an external executor before
 * the model may invoke them.
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
    name: "message.send",
    description:
      "Send a message into the shared channel as yourself. This is an outward-facing act: every call waits for an explicit human approval before it is delivered. The call returns once decided.",
    parameters: {
      type: "object",
      properties: {
        text: { type: "string", description: "the message text" },
      },
      required: ["text"],
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
