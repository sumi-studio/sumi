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
    /** Placement authority: active | sealed | staged | transferred. */
    authority: string;
    /** The transfer that last changed authority, when one is in flight. */
    transfer_id: string | null;
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
 * authoritative: "unset"/"none"/"chatgpt"/"needs_rebinding" or an "api"
 * binding carrying the connection's identity, version, and — only when the
 * credential store is armed — the decrypted key. Never a substituted
 * model/provider.
 */
export interface ModelBinding {
  // "needs_rebinding": the persona arrived by transfer carrying a model
  // selection intent; no model may run until the destination's bound
  // human selects a matching connection or the intent is cleared.
  selection: "unset" | "none" | "api" | "chatgpt" | "needs_rebinding";
  /** The carried non-secret intent, echoed when selection is needs_rebinding. */
  intent?: { kind: string; connection?: Record<string, unknown> };
  connection?: {
    id: string;
    name: string;
    preset: string;
    base_url: string;
    model: string;
    version: string;
    /**
     * Per-connection extra request headers, present only when the
     * credential store is armed — they are sealed with the credential and
     * sent only to this connection's endpoint. Never logged or echoed
     * into state/events.
     */
    extra_headers?: Record<string, string>;
    /**
     * The connection's requested bound on generated tokens (Anthropic
     * max_tokens / Responses max_output_tokens). Absent = protocol
     * default.
     */
    max_output_tokens?: number;
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
  /**
   * Applied L1 replacement blocks. Each renders at the journal position
   * where its events were — the core interleaves them with the raw tail
   * by sequence position.
   */
  memory: MemoryBlock[];
  /**
   * Older raw records outside the send cap — still stored and readable
   * through conversation_history; null when nothing was left out.
   */
  omitted: OmittedRange | null;
  /**
   * Older applied memory blocks outside the memory cap — stored, their
   * originals readable; null when every applied block was admitted.
   */
  memory_omitted?: OmittedMemory | null;
  /** The input's recorded decision — null when none has been saved yet. */
  plan: TurnPlan | null;
}

/** The extent of applied memory blocks left outside the sent context. */
export interface OmittedMemory {
  count: number;
  first_chunk_seq: number;
  last_chunk_seq: number;
  first_seq: number;
  last_seq: number;
  first_time: string;
  last_time: string;
  est_tokens: number;
}

/** The extent of raw records left outside the sent context. */
export interface OmittedRange {
  count: number;
  first_seq: number;
  last_seq: number;
  first_time: string;
  last_time: string;
}

/** One sealed journal range and its replacement lifecycle. Layer-2 chunks
 * are consolidation targets: `sources` names the accepted fragments they
 * consume, in order (null for ordinary L0→L1 chunks). */
export interface MemoryChunk {
  persona_id: string;
  chunk_seq: number;
  layer: number;
  sources: number[] | null;
  first_seq: number;
  last_seq: number;
  est_tokens: number;
  status:
    | "sealed"
    | "preparing"
    | "prepared"
    | "applied"
    | "kept"
    | "failed"
    | "superseded";
  replacement: string | null;
  replacement_est_tokens: number | null;
  /** Recorded preparation failures (the only thing that spends the budget). */
  attempts: number;
  /** Claims that ended without any recorded outcome (host lifecycle). */
  interruptions: number;
  last_error: string | null;
  claimed_generation: number | null;
  claimed_at: string | null;
  not_before: string | null;
  created_at: string;
  prepared_at: string | null;
  applied_at: string | null;
}

/** An applied chunk as it appears in the sent context. */
export interface MemoryBlock {
  chunk_seq: number;
  layer: number;
  first_seq: number;
  last_seq: number;
  /** When the first and last covered events were recorded. */
  first_time: string;
  last_time: string;
  text: string;
  est_tokens: number;
}

/** The journal as the model sees it: raw window plus applied blocks. */
export interface RenderedContext {
  events: Event[];
  memory: MemoryBlock[];
  omitted: OmittedRange | null;
  memory_omitted?: OmittedMemory | null;
}

/** The memory layer's current shape (read-only observability). */
export interface MemoryStatus {
  live_raw_tokens: number;
  applied_tokens: number;
  sealed: number;
  preparing: number;
  prepared: number;
  applied: number;
  kept: number;
  failed: number;
  /** Sources replaced by an applied upper-layer block (kept durable). */
  superseded: number;
  /** Chunks a claim could take now (sealed past backoff, or orphaned). */
  claimable: number;
  /** Earliest time a chunk becomes claimable; null when none waits. */
  next_claimable_at: string | null;
  /** Applied blocks left outside the memory cap. */
  applied_omitted: number;
  covered_seq: number;
  latest_seq: number;
  chunk_min_tokens: number;
  live_limit_tokens: number;
  memory_send_cap_tokens: number;
}

/**
 * A chunk claimed for asynchronous preparation, with everything the branch
 * needs: for a layer-1 target the covered events verbatim, for an
 * upper-layer target the selected source fragments' accepted texts with
 * their locators — plus the rendered parent context at claim time.
 */
export interface ClaimedMemoryChunk {
  chunk: MemoryChunk | null;
  target_events: Event[];
  target_fragments: MemoryBlock[];
  context: RenderedContext;
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

/**
 * Call sessions are the durable authority record for the secretary's
 * presence in a place's LiveKit room (migration 0055). The core creates one
 * through the delegated call.join effect; a per-placement media bridge
 * claims it and drives media under that claim's epoch.
 */
export type CallSessionStatus =
  | "requested"
  | "claimed"
  | "active"
  | "ending"
  | "ended"
  | "interrupted"
  | "revoked"
  | "failed";

export interface CallSession {
  session_id: string;
  workspace_id: string;
  place_id: string;
  personality_agent_id: string;
  room_sid?: string;
  status: CallSessionStatus;
  epoch: number;
  claimed_by?: string;
  claim_expires_at?: string;
  requested_by: string;
  created_at: string;
  updated_at: string;
  ended_at?: string;
  end_reason?: string;
}

/**
 * The secretary's committed speech for one session. Status records what is
 * known about playback — 'emitted' means the bridge finished rendering the
 * audio, never that a listener heard it. A claim epoch bump marks every
 * non-terminal utterance from earlier epochs 'unknown': committed intent is
 * durable but never auto-replayed.
 */
export type CallUtteranceStatus =
  | "intended"
  | "dequeued"
  | "emitting"
  | "emitted"
  | "interrupted"
  | "expired"
  | "failed"
  | "unknown";

export interface CallUtterance {
  utterance_id: string;
  session_id: string;
  session_epoch: number;
  seq: number;
  text: string;
  status: CallUtteranceStatus;
  detail?: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

/** Short-lived LiveKit credential for one session epoch. */
export interface CallTicket {
  url: string;
  token: string;
  room: string;
  identity: string;
}
