import type { CommandDispositionEvent } from "../src/generated/agent-events";

const commandId = "00000000-0000-4000-8000-000000000001";

export const appliedDisposition: CommandDispositionEvent = {
  type: "command_disposition",
  command_id: commandId,
  command_seq: 1,
  status: "applied",
};

export const supersededDisposition: CommandDispositionEvent = {
  type: "command_disposition",
  command_id: commandId,
  command_seq: 2,
  status: "superseded",
};

export const rejectedDisposition: CommandDispositionEvent = {
  type: "command_disposition",
  command_id: commandId,
  command_seq: 3,
  status: "rejected",
  reject_reason: "schema_violation",
};

// @ts-expect-error rejected dispositions must include a reject reason.
export const rejectedWithoutReason: CommandDispositionEvent = {
  type: "command_disposition",
  command_id: commandId,
  command_seq: 4,
  status: "rejected",
};

export const appliedWithReason: CommandDispositionEvent = {
  type: "command_disposition",
  command_id: commandId,
  command_seq: 5,
  status: "applied",
  // @ts-expect-error applied dispositions must not include a reject reason.
  reject_reason: "schema_violation",
};

export const supersededWithReason: CommandDispositionEvent = {
  type: "command_disposition",
  command_id: commandId,
  command_seq: 6,
  status: "superseded",
  // @ts-expect-error superseded dispositions must not include a reject reason.
  reject_reason: "schema_violation",
};
