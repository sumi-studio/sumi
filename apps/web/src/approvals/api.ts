import type {
  ApprovalDecision,
  ApprovalListResponse,
  CoreApproval,
} from "./model";

const REQUEST_TIMEOUT_MS = 15_000;

export class ApprovalsAPIError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(code: string, status: number) {
    super(code);
    this.name = "ApprovalsAPIError";
    this.code = code;
    this.status = status;
  }
}

async function readError(response: Response): Promise<ApprovalsAPIError> {
  let code = "approval_request_failed";
  try {
    const body = (await response.json()) as { error?: unknown };
    if (typeof body.error === "string") code = body.error;
  } catch {
    // The status code is still the authoritative non-sensitive signal.
  }
  return new ApprovalsAPIError(code, response.status);
}

/** The session human's whole approval inbox: pending first, then recent. */
export async function listCoreApprovals(
  signal?: AbortSignal,
): Promise<CoreApproval[]> {
  const response = await fetch("/me/approvals", {
    credentials: "include",
    cache: "no-store",
    headers: { Accept: "application/json" },
    signal: signal
      ? AbortSignal.any([signal, AbortSignal.timeout(REQUEST_TIMEOUT_MS)])
      : AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });
  if (!response.ok) throw await readError(response);
  const body = (await response.json()) as ApprovalListResponse;
  return Array.isArray(body.approvals) ? body.approvals : [];
}

/**
 * Record the session human's one-shot decision. decision_id is the command's
 * idempotency identity: a retried identical send replays the stored decision;
 * a different decision on a resolved approval is a 409 conflict.
 */
export async function decideCoreApproval(
  approvalId: string,
  decision: ApprovalDecision,
  decisionId: string,
): Promise<CoreApproval> {
  const response = await fetch(
    `/me/approvals/${encodeURIComponent(approvalId)}/decision`,
    {
      method: "POST",
      credentials: "include",
      cache: "no-store",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ decision, decision_id: decisionId }),
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    },
  );
  if (!response.ok) throw await readError(response);
  const body = (await response.json()) as { approval: CoreApproval };
  return body.approval;
}
