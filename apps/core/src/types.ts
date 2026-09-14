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
  /** "waiting" parks the input behind a pending tool approval. */
  status: "queued" | "claimed" | "waiting" | "done";
  claimed_generation: number | null;
  turn_id: string | null;
  created_at: string;
  done_at: string | null;
  /** Retryable-failed inputs requeue with a future claim time (backoff). */
  not_before: string | null;
  /** Set while the input is parked behind a pending human decision. */
  waiting_since: string | null;
  /** Total milliseconds spent waiting on human decisions — excluded from
   *  the provider retry budget (a person's thinking time is not model
   *  failure). Durable; survives restarts. */
  waited_ms: number;
}

export interface Turn {
  persona_id: string;
  turn_id: string;
  input_id: string;
  generation: number;
  attempt: number;
  status: "running" | "awaiting" | "done" | "interrupted" | "failed";
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
  /** "awaiting_approval" parks the op behind a pending human decision. */
  status: "running" | "awaiting_approval" | "done" | "failed";
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
  waiting_inputs: number;
  running_turn: string | null;
  pending_approvals: number;
  pending_schedules: number;
  latest_event_seq: number;
}

/**
 * Invocation route (ADR 0013 §1): "normal" executes under the agent's own
 * authority; "elevated" explicitly asks a human for a one-shot decision.
 * Immutable once recorded — part of the durable decision.
 */
export type ToolRoute = "normal" | "elevated";

/**
 * One decided tool call inside a durable plan. call_id is the model's own
 * identifier (kept verbatim for later provider tool_calls reconstruction);
 * the call's position in calls is its durable identity. route is the
 * invocation route recorded with the decision — required, never defaulted.
 */
export interface PlanCall {
  call_id?: string;
  tool: string;
  route: ToolRoute;
  request: Json;
}

/**
 * Durable record of one planned call's human decision (ADR 0013). Created
 * pending when a gated call is first claimed; the authenticated decision
 * command resolves it exactly once — approve_once grants
 * agent_own_with_human_consent provenance consumed by the executing claim,
 * deny_once finalizes the operation failed. Never silently retried.
 */
export interface Approval {
  approval_id: string;
  persona_id: string;
  input_id: string;
  call_index: number;
  operation_id: string;
  turn_id: string;
  tool: string;
  route: ToolRoute;
  /** Why approval was required: the tool's intrinsic registration, or the
   *  recorded call's elevated route. */
  required_by: "intrinsic" | "route";
  request: Json;
  action_digest: string;
  status: "pending" | "approved" | "denied";
  decision: "approve_once" | "deny_once" | null;
  decision_id: string | null;
  decided_by_kind: string | null;
  decided_by_id: string | null;
  provenance: string | null;
  decided_at: string | null;
  consumed_at: string | null;
  created_at: string;
}

/** The authenticated human's one-shot decision command on an approval. */
export interface ApprovalDecision {
  decision: "approve_once" | "deny_once";
  /** The deciding command's identity — identical replays are idempotent. */
  decision_id: string;
  decided_by_kind: "human";
  decided_by_id: string;
}

/**
 * The persona's resolved model connection for the core. The selection is
 * authoritative: "unset"/"none"/"chatgpt" or an "api" binding carrying the
 * connection's identity, version, and — only when the credential store is
 * armed — the decrypted key. Never a substituted model/provider.
 */
export interface ModelBinding {
  selection: "unset" | "none" | "api" | "chatgpt";
  connection?: {
    id: string;
    name: string;
    preset: string;
    base_url: string;
    model: string;
    version: string;
  };
  api_key?: string;
  credential_available?: boolean;
  reason?: string;
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
  outcome: "complete" | "fail" | "await";
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
