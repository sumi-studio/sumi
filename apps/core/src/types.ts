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
  /** Retryable-failed inputs requeue with a future claim time (backoff). */
  not_before: string | null;
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
    /** Placement authority: active | sealed | staged | transferred. */
    authority: string;
    /** The transfer that last changed authority, when one is in flight. */
    transfer_id: string | null;
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

/**
 * The model's decision in one round of a turn, persisted before any of that
 * round's effects run. A round with zero calls is final — its text is the
 * reply, informed by the committed tool results of earlier rounds.
 */
export interface Decision {
  text: string;
  calls: PlanCall[];
  usage: Json;
}

/**
 * Durable record of one input's decisions — one row per input. `plan` is
 * the append-only list of rounds: a recorded round never changes, a new
 * round may only be appended by the live turn. A retried attempt continues
 * the recorded rounds instead of re-planning them; the model is consulted
 * again only for the first round not yet recorded.
 */
export interface TurnPlan {
  persona_id: string;
  input_id: string;
  turn_id: string;
  generation: number;
  plan: Decision[];
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
  /**
   * Provider-supplied retry pacing (Retry-After) for a retryable
   * failure: the requeue's not_before is at least now+retry_after_ms
   * (server clamps). Absent/0 = the default per-attempt backoff.
   */
  retry_after_ms?: number;
}

/**
 * Secretary-independent background execution (M09). A job belongs to the
 * persona but NOT to the writer generation: its lifecycle is owned by a
 * runner claim (claimed_by + claim_expires_at), so a job started under one
 * secretary generation can complete after that process stopped and resumed.
 *
 * Lifecycle: queued → running → done|failed|cancelled. cancel_requested is
 * the running→cancelled transit state; 'lost' marks an expired runner claim
 * whose outcome is indeterminate — it is never silently re-executed. Every
 * terminal transition enqueues exactly one 'job:<job_id>' notification input
 * into the secretary's ordinary input stream.
 */
export type JobStatus =
  | "queued"
  | "running"
  | "cancel_requested"
  | "done"
  | "failed"
  | "cancelled"
  | "lost";

export interface Job {
  persona_id: string;
  job_id: string;
  /** Executor family; 'subprocess' is the implemented local kind. */
  kind: string;
  request: Json;
  status: JobStatus;
  claimed_by: string | null;
  claim_expires_at: string | null;
  created_by: string;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  cancel_requested_at: string | null;
  result: Json | null;
  error: string | null;
  notified_at: string | null;
}

/** Request shape for kind 'subprocess': an executable + argv, no shell. */
export interface SubprocessJobRequest {
  command: string[];
  cwd?: string;
  env?: Record<string, string>;
  timeout_ms?: number;
}

/** Terminal statuses a runner may report to completeJob. */
export type JobTerminalReport = "done" | "failed" | "cancelled";
