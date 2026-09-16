import { AuthAPIError, fetchCSRFToken } from "./session-client";

/**
 * The registering browser's view of one secretary-move session. Proof fields
 * (activate/retire) and the bound Local source never appear here — they
 * belong to the grant holder, the Local command.
 */
export interface SecretaryTransferSession {
  session_id: string;
  status:
    | "awaiting_bundle"
    | "staged"
    | "provisioned"
    | "activated"
    | "cancelled"
    | "expired";
  admit_until: string;
  claim_until?: string;
  retired: boolean;
  state_only: true;
  not_included: Array<{ name: string; owner: string; reason: string }>;
  arrival?: {
    persona_id: string;
    content_sha256: string;
    continuity: Record<string, number>;
    rows: Record<string, number>;
    model_connection_required: boolean;
  };
}

export interface SecretaryTransferCreated {
  move_url: string;
  session: SecretaryTransferSession;
}

interface FlowAuthority {
  flowId: string;
  nonce: string;
}

async function postTransfer(
  path: string,
  body: Record<string, string>,
): Promise<Record<string, unknown>> {
  const csrfToken = await fetchCSRFToken();
  const response = await fetch(path, {
    method: "POST",
    credentials: "include",
    cache: "no-store",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      "X-CSRF-Token": csrfToken,
    },
    body: JSON.stringify(body),
  });
  const parsed: unknown = await response.json().catch(() => null);
  if (!response.ok) {
    const detail =
      parsed !== null &&
      typeof parsed === "object" &&
      "error" in parsed &&
      typeof parsed.error === "string"
        ? parsed.error
        : "transfer request failed";
    const error = new AuthAPIError(detail, response.status);
    if (
      response.status === 409 &&
      parsed !== null &&
      typeof parsed === "object" &&
      "session" in parsed &&
      isTransferSession(parsed.session)
    ) {
      // A 409 create answer names the open session so a lost move-URL reply
      // still recovers its status; the grant is never repeated.
      (error as AuthAPIError & { session?: SecretaryTransferSession }).session =
        parsed.session;
    }
    throw error;
  }
  return (parsed ?? {}) as Record<string, unknown>;
}

function isTransferSession(value: unknown): value is SecretaryTransferSession {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as SecretaryTransferSession).session_id === "string" &&
    typeof (value as SecretaryTransferSession).status === "string"
  );
}

/**
 * Opens a move session for the credential the pending registration flow
 * verified and returns the one-time move URL. A 409 answer means the
 * credential already has an open session — its status comes back on the
 * error so the registrant can continue or replace it.
 */
export async function createSecretaryTransfer(
  flow: FlowAuthority,
): Promise<SecretaryTransferCreated> {
  const body = await postTransfer("/api/secretary-transfer/sessions", {
    flow_id: flow.flowId,
    nonce: flow.nonce,
  });
  const session = body.session;
  if (
    typeof body.move_url !== "string" ||
    body.move_url === "" ||
    !isTransferSession(session)
  ) {
    throw new AuthAPIError("Invalid transfer response.", 0);
  }
  return { move_url: body.move_url, session };
}

/** Reads the credential's current session — the open one, else the most
 * recent — for the same verified flow. 404 means no move exists. */
export async function readSecretaryTransfer(
  flow: FlowAuthority,
): Promise<SecretaryTransferSession | null> {
  try {
    const body = await postTransfer(
      "/api/secretary-transfer/registrant/session",
      { flow_id: flow.flowId, nonce: flow.nonce },
    );
    return isTransferSession(body) ? body : null;
  } catch (error) {
    if (error instanceof AuthAPIError && error.status === 404) {
      return null;
    }
    throw error;
  }
}

/**
 * The registrant's cancel. expect_status pins the open status the person was
 * shown so replacing an unused move URL can never discard a secretary that
 * arrived meanwhile.
 */
export async function cancelSecretaryTransfer(
  flow: FlowAuthority,
  sessionId: string,
  expectStatus?: "awaiting_bundle" | "staged",
): Promise<SecretaryTransferSession> {
  const body = await postTransfer(
    `/api/secretary-transfer/sessions/${sessionId}/cancel`,
    expectStatus
      ? { flow_id: flow.flowId, nonce: flow.nonce, expect_status: expectStatus }
      : { flow_id: flow.flowId, nonce: flow.nonce },
  );
  if (!isTransferSession(body)) {
    throw new AuthAPIError("Invalid transfer response.", 0);
  }
  return body;
}

export function isTransferPendingError(error: unknown): boolean {
  return error instanceof AuthAPIError && error.message === "transfer_pending";
}

export function isTransferUnavailableError(error: unknown): boolean {
  return (
    error instanceof AuthAPIError && error.message === "transfer_unavailable"
  );
}

/** The move URL is a credential. It lives in sessionStorage only — it never
 * enters URLs, logs, or persistent storage beyond the tab's own lifetime. */
const moveURLKey = "sumi.registration.move-url.v1";

export interface SavedMoveURL {
  flowId: string;
  sessionId: string;
  moveURL: string;
}

export function saveMoveURL(
  flowId: string,
  sessionId: string,
  moveURL: string,
): void {
  try {
    sessionStorage.setItem(
      moveURLKey,
      JSON.stringify({ flowId, sessionId, moveURL }),
    );
  } catch {
    // The in-memory value still covers this page lifetime.
  }
}

export function loadMoveURL(flowId: string): SavedMoveURL | null {
  try {
    const parsed: unknown = JSON.parse(
      sessionStorage.getItem(moveURLKey) ?? "null",
    );
    if (
      typeof parsed === "object" &&
      parsed !== null &&
      (parsed as { flowId?: unknown }).flowId === flowId &&
      typeof (parsed as { sessionId?: unknown }).sessionId === "string" &&
      typeof (parsed as { moveURL?: unknown }).moveURL === "string"
    ) {
      return parsed as SavedMoveURL;
    }
  } catch {
    // Unreadable storage answers as absent.
  }
  return null;
}

export function clearMoveURL(flowId: string): void {
  try {
    if (loadMoveURL(flowId) !== null) {
      sessionStorage.removeItem(moveURLKey);
    }
  } catch {
    // Clearing is best-effort.
  }
}
