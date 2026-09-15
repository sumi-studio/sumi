import { getIdToken, signInWithCustomToken, type User } from "firebase/auth";
import {
  type AuthFlowResult,
  type AuthIntent,
  createAuthFlowNonce,
  type EmailChallengeStatus,
  parseEmailChallenge,
  resolveAuthFlow,
  startAuthFlow,
} from "./auth-flow-client";
import {
  cleanupPendingEmailFlowStorage,
  clearActiveEmailFlowState,
  clearPendingEmailFlow,
  consumePendingCredentialRecovery,
  createEmailFlowState,
  loadActiveEmailFlowState,
  loadPendingEmailFlow,
  type PendingCredentialRecovery,
  type PendingEmailAuthFlow,
  type SerializedOAuthCredential,
  saveActiveEmailFlowState,
  savePendingEmailFlow,
} from "./auth-flow-state";
import {
  readWorkspaceInvitation,
  restoreWorkspaceInvitation,
} from "./enrollment-invitation-state";
import { getFirebaseAuth } from "./firebase";
import { AuthAPIError, getSumiSession, postAuthJSON } from "./session-client";

/** The emailed link opens this path; its secrets stay in the URL fragment. */
export const emailLinkPath = "/email-sign-in";
const emailLinkKey = "sumi.auth.email-link.v1";

export interface ActiveEmailCodeFlow {
  state: string;
  flow: PendingEmailAuthFlow;
}

export interface EmailCodeStart {
  active: ActiveEmailCodeFlow;
  challenge: EmailChallengeStatus;
}

export interface EmailLinkParams {
  challengeId: string;
  token: string;
}

export type EmailLinkState =
  | "usable"
  | "proved_here"
  | "consumed"
  | "superseded"
  | "expired"
  | "completed";

export interface EmailLinkInspection {
  flowId: string;
  intent: AuthIntent;
  email: string;
  state: EmailLinkState;
  sameBrowser: boolean;
  session: "none" | "same_account" | "other_account";
  expiresAt: string;
}

export interface EmailProof {
  active: ActiveEmailCodeFlow;
  customToken: string;
  /**
   * The Human the mailbox proof resolved to. Absent for proofs that precede
   * identity resolution; the resolve result still carries it.
   */
  humanId?: string;
}

export interface EmailProofCompletion {
  flow: PendingEmailAuthFlow;
  result: Exclude<AuthFlowResult, { outcome: "proof_required" }>;
  firebaseUser: User;
}

let capturedEmailLink: EmailLinkParams | null = null;

/**
 * Moves an opened email link out of the address bar before the router reads
 * it. Opening or scanning the link changes nothing on the server.
 */
export function captureEmailLinkFromLocation(): void {
  try {
    const url = new URL(globalThis.location.href);
    if (url.pathname !== emailLinkPath) return;
    const params = new URLSearchParams(url.hash.slice(1));
    const link = {
      challengeId: params.get("challenge") ?? "",
      token: params.get("token") ?? "",
    };
    history.replaceState(null, "", "/");
    if (!isEmailLinkParams(link)) return;
    capturedEmailLink = link;
    sessionStorage.setItem(emailLinkKey, JSON.stringify(link));
  } catch {
    // The in-memory capture still serves this page lifetime.
  }
}

export function pendingEmailLink(): EmailLinkParams | null {
  if (capturedEmailLink) return capturedEmailLink;
  try {
    const parsed: unknown = JSON.parse(
      sessionStorage.getItem(emailLinkKey) ?? "null",
    );
    return isEmailLinkParams(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

export function clearPendingEmailLink(): void {
  capturedEmailLink = null;
  try {
    sessionStorage.removeItem(emailLinkKey);
  } catch {
    // The link is single-page state; an unremovable copy stays harmless.
  }
}

export function loadActiveEmailCodeFlow(): ActiveEmailCodeFlow | null {
  const state = loadActiveEmailFlowState();
  if (!state) return null;
  const flow = loadPendingEmailFlow(state);
  if (!flow) {
    clearActiveEmailFlowState(state);
    return null;
  }
  return { state, flow };
}

export function abandonEmailCodeFlow(active: ActiveEmailCodeFlow): void {
  clearPendingEmailFlow(active.state);
  clearActiveEmailFlowState(active.state);
}

export async function beginEmailCodeAuth(
  rawEmail: string,
  intent: AuthIntent,
  recovery?: {
    provider: "google.com" | "github.com";
    requestedIntent: AuthIntent;
    credential: SerializedOAuthCredential;
  },
): Promise<EmailCodeStart> {
  cleanupPendingEmailFlowStorage();
  const email = rawEmail.trim();
  if (!email || email.length > 320) {
    throw new AuthAPIError("Invalid email address.", 0);
  }
  const state = createEmailFlowState();
  const nonce = createAuthFlowNonce();
  const request = {
    intent,
    provider: "email_code" as const,
    email,
    continuation: "/",
    nonce,
  };
  // The nonce makes start idempotent: a lost response is retried without
  // queueing a second email.
  const started = await retryAmbiguous(() => startAuthFlow(request));
  if (!started.emailChallenge) {
    throw new AuthAPIError("Invalid authentication flow response.", 0);
  }
  const workspaceInviteCode = readWorkspaceInvitation();
  const flow: PendingEmailAuthFlow = {
    flowId: started.flowId,
    nonce,
    intent,
    provider: "email_code",
    email: started.emailChallenge.email,
    expiresAt: started.expiresAt,
    stage: "code_sent",
    ...(workspaceInviteCode ? { workspaceInviteCode } : {}),
    ...(recovery
      ? { credentialRecovery: boundedRecovery(recovery, started.expiresAt) }
      : {}),
  };
  savePendingEmailFlow(state, flow);
  saveActiveEmailFlowState(state);
  return { active: { state, flow }, challenge: started.emailChallenge };
}

function boundedRecovery(
  recovery: Omit<PendingCredentialRecovery, "version" | "expiresAt">,
  flowExpiresAt: string,
): PendingCredentialRecovery {
  const flowExpiry = Date.parse(flowExpiresAt);
  if (!Number.isFinite(flowExpiry)) {
    throw new AuthAPIError("Invalid authentication flow expiry.", 0);
  }
  return {
    version: 1,
    ...recovery,
    expiresAt: new Date(
      Math.min(flowExpiry, Date.now() + 10 * 60_000),
    ).toISOString(),
  };
}

export async function readEmailCodeStatus(
  flow: PendingEmailAuthFlow,
): Promise<EmailChallengeStatus> {
  return parseFlowChallenge(
    await postAuthJSON("/auth/email/status", {
      flow_id: flow.flowId,
      nonce: flow.nonce,
    }),
    flow,
  );
}

export async function resendEmailCode(
  flow: PendingEmailAuthFlow,
): Promise<EmailChallengeStatus> {
  return parseFlowChallenge(
    await postAuthJSON("/auth/email/resend", {
      flow_id: flow.flowId,
      nonce: flow.nonce,
    }),
    flow,
  );
}

/**
 * A lost verify response is retried with the same code: the server returns
 * the committed proof instead of counting another attempt.
 */
export async function verifyEmailCode(
  active: ActiveEmailCodeFlow,
  code: string,
): Promise<EmailProof> {
  const body = await retryAmbiguous(() =>
    postAuthJSON("/auth/email/verify", {
      flow_id: active.flow.flowId,
      nonce: active.flow.nonce,
      code,
    }),
  );
  return {
    active,
    customToken: parseEmailProof(body, active.flow.flowId),
    humanId: parseHumanId(body),
  };
}

export async function inspectEmailLink(
  link: EmailLinkParams,
): Promise<EmailLinkInspection> {
  const active = loadActiveEmailCodeFlow();
  const body = await postAuthJSON("/auth/email/link/inspect", {
    challenge_id: link.challengeId,
    token: link.token,
    ...(active ? { nonce: active.flow.nonce } : {}),
  });
  return parseEmailLinkInspection(body);
}

/**
 * Completes the link in this browser. Another browser must pass adopt after
 * the person explicitly chose to continue here; that moves the flow to a new
 * nonce and the original tab reports that it continued elsewhere.
 */
export async function completeEmailLink(
  link: EmailLinkParams,
  inspection: EmailLinkInspection,
  adopt: boolean,
): Promise<EmailProof> {
  let active = loadActiveEmailCodeFlow();
  const sameBrowser =
    inspection.sameBrowser && active?.flow.flowId === inspection.flowId;
  if (!sameBrowser || !active) {
    if (!adopt) throw new AuthAPIError("link_adoption_required", 409);
    const state = createEmailFlowState();
    const flow: PendingEmailAuthFlow = {
      flowId: inspection.flowId,
      nonce: createAuthFlowNonce(),
      intent: inspection.intent,
      provider: "email_code",
      email: inspection.email,
      expiresAt: inspection.expiresAt,
      stage: "code_sent",
    };
    // Persist the new authority first so a lost response retries with it.
    savePendingEmailFlow(state, flow);
    saveActiveEmailFlowState(state);
    active = { state, flow };
  }
  const authority = active;
  const body = await retryAmbiguous(() =>
    postAuthJSON("/auth/email/link/complete", {
      challenge_id: link.challengeId,
      token: link.token,
      nonce: authority.flow.nonce,
      adopt: !sameBrowser,
    }),
  );
  return {
    active: authority,
    customToken: parseEmailProof(body, inspection.flowId),
    humanId: parseHumanId(body),
  };
}

/**
 * Exchanges the Sumi-minted custom token with Firebase and resolves the flow.
 * The pending record stays until the resolution and its session exist, so
 * every failure before that is an ordinary retry of the same proof.
 */
export async function finishEmailProof(
  { active, customToken }: EmailProof,
  options?: { switchFromUserId?: string },
): Promise<EmailProofCompletion> {
  const { state, flow } = active;
  const credential = await signInWithCustomToken(
    getFirebaseAuth(),
    customToken,
  );
  const request = {
    flowId: flow.flowId,
    nonce: flow.nonce,
    idToken: await getIdToken(credential.user, true),
  };
  const result = await resolveAuthFlow({
    ...request,
    switchFromUserId: options?.switchFromUserId,
  });
  await ensureEmailCompletionSession(request, result, options);
  // Claim a pending provider credential before any provider mutation.
  if (flow.credentialRecovery) consumePendingCredentialRecovery(state, flow);
  else clearPendingEmailFlow(state);
  clearActiveEmailFlowState(state);
  clearPendingEmailLink();
  if (flow.workspaceInviteCode) {
    restoreWorkspaceInvitation(flow.workspaceInviteCode);
  }
  return { flow, result, firebaseUser: credential.user };
}

/**
 * A terminal resolve can commit on the server while its response, and with it
 * the session cookie, never reaches this browser; status recovery then reports
 * the outcome without a session. Inside the server's completion replay window
 * the same flow authority and Firebase principal resolve again to the same
 * Human, which re-issues the session and grants nothing new.
 */
export async function ensureEmailCompletionSession(
  request: { flowId: string; nonce: string; idToken: string },
  result: AuthFlowResult,
  options?: { switchFromUserId?: string },
): Promise<void> {
  if (result.outcome !== "signed_in" && result.outcome !== "account_created") {
    return;
  }
  const session = await getSumiSession();
  if (session.authenticated) {
    // A session may only stand in for this completion when it belongs to
    // the Human the flow resolved to. Any other account is the person's
    // later choice: completing over it needs their explicit switch, which
    // the server also enforces through session_active.
    if (session.user.id === result.humanId) return;
    throw new AuthAPIError("session_active", 409);
  }
  const replayed = await resolveAuthFlow({
    ...request,
    switchFromUserId: options?.switchFromUserId,
  });
  if (replayed.outcome !== result.outcome) invalidResponse();
  const next = await getSumiSession();
  if (!next.authenticated) {
    throw new AuthAPIError("Sumi session was not established.", 0);
  }
  if (next.user.id !== replayed.humanId) {
    // The replay could not mint its own session because this jar now
    // belongs to a different Human.
    throw new AuthAPIError("session_active", 409);
  }
}

async function retryAmbiguous<T>(operation: () => Promise<T>): Promise<T> {
  try {
    return await operation();
  } catch (error) {
    if (
      error instanceof AuthAPIError &&
      error.status !== 0 &&
      error.status < 500
    ) {
      throw error;
    }
    return operation();
  }
}

function parseFlowChallenge(
  value: unknown,
  flow: PendingEmailAuthFlow,
): EmailChallengeStatus {
  const challenge = parseEmailChallenge(value);
  if (challenge.flowId !== flow.flowId) invalidResponse();
  return challenge;
}

function parseEmailProof(value: unknown, flowId: string): string {
  if (
    !isObject(value) ||
    value.flow_id !== flowId ||
    typeof value.custom_token !== "string" ||
    value.custom_token.length === 0 ||
    value.custom_token.length > 8 * 1024
  ) {
    invalidResponse();
  }
  return value.custom_token;
}

function parseEmailLinkInspection(value: unknown): EmailLinkInspection {
  if (!isObject(value)) invalidResponse();
  const { flow_id, intent, email, state, same_browser, session, expires_at } =
    value;
  if (
    typeof flow_id !== "string" ||
    flow_id.length === 0 ||
    flow_id.length > 256 ||
    (intent !== "sign_in" && intent !== "sign_up") ||
    typeof email !== "string" ||
    email.length === 0 ||
    email.length > 320 ||
    (state !== "usable" &&
      state !== "proved_here" &&
      state !== "consumed" &&
      state !== "superseded" &&
      state !== "expired" &&
      state !== "completed") ||
    typeof same_browser !== "boolean" ||
    (session !== "none" &&
      session !== "same_account" &&
      session !== "other_account") ||
    typeof expires_at !== "string" ||
    !Number.isFinite(Date.parse(expires_at))
  ) {
    invalidResponse();
  }
  return {
    flowId: flow_id,
    intent,
    email,
    state,
    sameBrowser: same_browser,
    session,
    expiresAt: expires_at,
  };
}

function parseHumanId(value: unknown): string | undefined {
  if (!isObject(value)) return undefined;
  const humanId = value.human_id;
  if (
    typeof humanId !== "string" ||
    humanId.length === 0 ||
    humanId.length > 256
  ) {
    return undefined;
  }
  return humanId;
}

function isEmailLinkParams(value: unknown): value is EmailLinkParams {
  return (
    isObject(value) &&
    typeof value.challengeId === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
      value.challengeId,
    ) &&
    typeof value.token === "string" &&
    /^[A-Za-z0-9_-]{43}$/.test(value.token)
  );
}

function invalidResponse(): never {
  throw new AuthAPIError("Invalid authentication flow response.", 0);
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

captureEmailLinkFromLocation();
