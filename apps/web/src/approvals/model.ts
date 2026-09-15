/**
 * The durable secretary approval inbox the new TypeScript/PostgreSQL core
 * parks on (ADR 0013). `GET /me/approvals` is the record: every entry is a
 * real core_tool_approvals row for a persona bound to the signed-in human,
 * with the suspended input's provenance so the human can see what will run.
 */

export type ApprovalStatus = "pending" | "approved" | "denied";
export type ApprovalDecision = "approve_once" | "deny_once";

export interface ApprovalPlaceContext {
  id: string;
  kind?: string;
  name?: string;
}

export interface ApprovalInputContext {
  input_id: string;
  kind: string;
  surface: string;
  thread_id?: string;
  workspace_id?: string;
  actor_kind?: string;
  actor_id?: string;
  actor_name?: string;
  place?: ApprovalPlaceContext;
  text?: string;
  occurred_at?: string;
}

export interface CoreApproval {
  approval_id: string;
  persona_id: string;
  input_id: string;
  call_index: number;
  operation_id: string;
  turn_id: string;
  tool: string;
  route: "normal" | "elevated" | string;
  required_by: "intrinsic" | "route" | string;
  request: Record<string, unknown>;
  action_digest: string;
  status: ApprovalStatus;
  decision: ApprovalDecision | null;
  decision_id?: string | null;
  decided_by_kind?: string | null;
  decided_by_id?: string | null;
  provenance?: string | null;
  decided_at: string | null;
  consumed_at: string | null;
  prior_decision?: ApprovalDecision | null;
  prior_decided_by_kind?: string | null;
  prior_decided_by_id?: string | null;
  prior_decided_at?: string | null;
  created_at: string;
  secretary_name: string;
  input?: ApprovalInputContext;
}

export interface ApprovalListResponse {
  approvals: CoreApproval[];
  /**
   * The session human the rows belong to, echoed by the server so the client
   * projection can prove its data describes the currently signed-in account —
   * not a stale snapshot a previous login left behind.
   */
  human?: string;
}

/** A browser decision never names its decider — the session does. */
export interface ApprovalDecisionRequest {
  decision: ApprovalDecision;
  decision_id: string;
}
