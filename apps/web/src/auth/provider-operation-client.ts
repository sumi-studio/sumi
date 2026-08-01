import { postAuthJSON } from "./session-client";

export type ManagedProvider = "google.com" | "github.com";
export type ProviderOperation = "link" | "unlink";

export interface ProviderOperationResult {
  operationId: string;
  outcome:
    | "client_operation_required"
    | "provider_linked"
    | "provider_already_linked"
    | "provider_unlinked"
    | "credential_in_use"
    | "firebase_operation_failed"
    | "cancelled";
  clientOperation?: "firebase_link_with_credential";
  completionTokenNotBefore?: string;
  expiresAt?: string;
  noticeRequired: boolean;
}

export async function startProviderOperation({
  provider,
  operation,
  nonce,
  idToken,
}: {
  provider: ManagedProvider;
  operation: ProviderOperation;
  nonce: string;
  idToken: string;
}): Promise<ProviderOperationResult> {
  return parseProviderOperationResult(
    await postAuthJSON("/auth/providers/operations", {
      provider,
      operation,
      decision_path: "account_settings",
      nonce,
      id_token: idToken,
    }),
  );
}

export async function completeProviderOperation({
  operationId,
  nonce,
  idToken,
}: {
  operationId: string;
  nonce: string;
  idToken: string;
}): Promise<ProviderOperationResult> {
  return parseProviderOperationResult(
    await postAuthJSON("/auth/providers/operations/complete", {
      operation_id: operationId,
      nonce,
      id_token: idToken,
    }),
  );
}

export async function failProviderOperation({
  operationId,
  nonce,
  outcome,
}: {
  operationId: string;
  nonce: string;
  outcome: "credential_in_use" | "firebase_operation_failed" | "cancelled";
}): Promise<void> {
  parseProviderOperationResult(
    await postAuthJSON("/auth/providers/operations/fail", {
      operation_id: operationId,
      nonce,
      outcome,
    }),
  );
}

export async function statusProviderOperation({
  operationId,
  nonce,
}: {
  operationId: string;
  nonce: string;
}): Promise<ProviderOperationResult> {
  return parseProviderOperationResult(
    await postAuthJSON("/auth/providers/operations/status", {
      operation_id: operationId,
      nonce,
    }),
  );
}

function parseProviderOperationResult(value: unknown): ProviderOperationResult {
  if (!isObject(value)) throw new Error("Invalid provider operation response.");
  const operationId = value.operation_id;
  const outcome = value.outcome;
  const clientOperation = value.client_operation;
  const completionTokenNotBefore = value.completion_token_not_before;
  const expiresAt = value.expires_at;
  const noticeRequired = value.notice_required;
  if (
    typeof operationId !== "string" ||
    operationId.length === 0 ||
    operationId.length > 128 ||
    !isProviderOutcome(outcome) ||
    (clientOperation !== undefined &&
      clientOperation !== "firebase_link_with_credential") ||
    (completionTokenNotBefore !== undefined &&
      !isTimestamp(completionTokenNotBefore)) ||
    (expiresAt !== undefined && !isTimestamp(expiresAt)) ||
    (noticeRequired !== undefined && typeof noticeRequired !== "boolean")
  ) {
    throw new Error("Invalid provider operation response.");
  }
  return {
    operationId,
    outcome,
    ...(clientOperation ? { clientOperation } : {}),
    ...(completionTokenNotBefore ? { completionTokenNotBefore } : {}),
    ...(expiresAt ? { expiresAt } : {}),
    noticeRequired: noticeRequired === true,
  };
}

function isProviderOutcome(
  value: unknown,
): value is ProviderOperationResult["outcome"] {
  return (
    value === "client_operation_required" ||
    value === "provider_linked" ||
    value === "provider_already_linked" ||
    value === "provider_unlinked" ||
    value === "credential_in_use" ||
    value === "firebase_operation_failed" ||
    value === "cancelled"
  );
}

function isTimestamp(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length <= 64 &&
    Number.isFinite(Date.parse(value))
  );
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
