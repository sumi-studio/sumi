/**
 * Domain types for the shared secretary core. These mirror the Go
 * `internal/agentstate` wire contract exactly (snake_case JSON).
 */

export type Json = Record<string, unknown>;

export interface WriterLease {
  persona_id: string;
  generation: number;
  holder_id: string;
  acquired_at: string;
  expires_at: string;
}

export type InputAttention = "reply" | "observe" | "defer";

export interface Input {
  persona_id: string;
  input_id: string;
  kind: string;
  payload: Json;
  actor_kind: string;
  actor_id: string;
  source_surface: string;
  thread_id: string;
  occurred_at: string | null;
  attention: InputAttention;
  status: "queued" | "claimed" | "done";
  claimed_generation: number | null;
  turn_id: string | null;
  created_at: string;
  done_at: string | null;
}

export interface Turn {
  persona_id: string;
  turn_id: string;
  input_id: string;
  generation: number;
  attempt: number;
  status: "running" | "done" | "interrupted" | "failed";
  started_at: string;
  finished_at: string | null;
  output: Json | null;
  usage: Json | null;
  error: string | null;
}

export interface Event {
  persona_id: string;
  seq: number;
  turn_id: string;
  kind: string;
  payload: Json;
  created_at: string;
}

export interface EventInput {
  kind: string;
  payload: Json;
}

export interface Operation {
  persona_id: string;
  operation_id: string;
  turn_id: string;
  tool: string;
  idempotency_key: string;
  request: Json;
  status: "running" | "done" | "failed";
  response: Json | null;
  claimed_generation: number;
  created_at: string;
  completed_at: string | null;
}

export interface Schedule {
  persona_id: string;
  schedule_id: string;
  wake_at: string;
  payload: Json;
  miss_policy: "fire_late" | "coalesce" | "expire" | "report_missed";
  status: "pending" | "claimed" | "fired" | "cancelled" | "expired";
  claimed_generation: number | null;
  created_at: string;
  fired_at: string | null;
}

export interface OutboxEntry {
  persona_id: string;
  seq: number;
  kind: string;
  payload: Json;
  created_at: string;
  delivered_at: string | null;
}

export interface PersonaState {
  persona: {
    persona_id: string;
    human_id: string | null;
    display_name: string;
    created_at: string;
  };
  lease: WriterLease | null;
  queued_inputs: number;
  running_turn: string | null;
  pending_schedules: number;
  latest_event_seq: number;
}

/**
 * One decided tool call inside a durable plan. call_id is the model's own
 * identifier (kept verbatim for later provider tool_calls reconstruction);
 * the call's position in calls is its durable identity.
 */
export interface PlanCall {
  call_id?: string;
  tool: string;
  request: Json;
}

/** The model's decision for one input, persisted before any effect runs. */
export interface Decision {
  text: string;
  calls: PlanCall[];
  usage: Json;
}

/**
 * Durable record of one input's decision — one row per input, immutable.
 * A retried attempt continues this plan instead of re-planning.
 */
export interface TurnPlan {
  persona_id: string;
  input_id: string;
  turn_id: string;
  generation: number;
  plan: Decision;
  created_at: string;
}

export interface LoadResult {
  turn: Turn | null;
  input: Input | null;
  context: Event[];
  /** The input's recorded decision — null when none has been saved yet. */
  plan: TurnPlan | null;
}

export interface RecoverResult {
  interrupted_turns: string[];
  requeued_inputs: string[];
  released_schedule_claims: string[];
}

export interface CommitRequest {
  outcome: "complete" | "fail";
  events: EventInput[];
  output?: Json;
  usage?: Json;
  error?: string;
  retryable?: boolean;
}
