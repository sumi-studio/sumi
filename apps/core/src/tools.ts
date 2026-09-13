import type { ToolSpec } from "./provider.ts";

/**
 * Tool registry. The core hands `specs` to the model and routes each tool
 * call through the state service operation ledger. For the slice, only
 * state-internal tools are registered: their effects commit atomically
 * inside the claim transaction (no crash window between effect and receipt).
 * `messaging.send` is state-internal too — its effect is delegated to the
 * Messaging domain, which applies the append in the same claim transaction.
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
    name: "messaging.send",
    description:
      "Post a message into a shared Messaging place (channel, DM, or group DM) as yourself, so the people and secretaries there see it. Use the place_id shown in the input's marker; pass its message_id as reply_to to answer that message directly. This is for genuinely replying — do not post merely to acknowledge ambient messages.",
    parameters: {
      type: "object",
      properties: {
        place_id: {
          type: "string",
          description: "the Messaging place id to post into",
        },
        content: { type: "string", description: "the message text" },
        reply_to: {
          type: "string",
          description: "optional message_id in the same place to reply to",
        },
      },
      required: ["place_id", "content"],
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
];

/** Model-visible specs for the registered internal tools. */
export function toolSpecs(): ToolSpec[] {
  return INTERNAL_TOOLS.map(({ name, description, parameters }) => ({
    name,
    description,
    parameters,
  }));
}
