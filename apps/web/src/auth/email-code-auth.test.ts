// @vitest-environment jsdom

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { loadPendingEmailFlow } from "./auth-flow-state";
import type { AuthAPIError } from "./session-client";

const emailMocks = vi.hoisted(() => ({
  getIdToken: vi.fn(),
  signInWithCustomToken: vi.fn(),
}));

vi.mock("firebase/auth", () => ({
  getIdToken: emailMocks.getIdToken,
  signInWithCustomToken: emailMocks.signInWithCustomToken,
}));

vi.mock("./firebase", () => ({
  getFirebaseAuth: () => ({}),
}));

const challengeId = "0198f0f4-9b72-7000-8000-000000000030";
const token = "t".repeat(43);

function challenge(flowId = "flow-id") {
  return {
    flow_id: flowId,
    flow_status: "pending",
    email: "person@example.com",
    flow_expires_at: new Date(Date.now() + 30 * 60_000).toISOString(),
    challenge_expires_at: new Date(Date.now() + 10 * 60_000).toISOString(),
    attempts_remaining: 5,
    delivery: "pending",
    resend_available_at: new Date(Date.now() + 30_000).toISOString(),
  };
}

type Route = (body: Record<string, unknown>) => Response | Promise<Response>;

const signedIn = {
  flow_id: "flow-id",
  outcome: "signed_in",
  continuation: "/",
  expires_at: new Date(Date.now() + 30 * 60_000).toISOString(),
  human_id: "human-id",
};

const authenticatedSession = {
  authenticated: true,
  authority_binding_id: `${"a".repeat(42)}A`,
  user: { id: "human-id", display_name: "Person" },
};

function stubAuthAPI(routes: Record<string, Route[]>) {
  const calls: Array<{ path: string; body: Record<string, unknown> }> = [];
  const fetchMock = vi.fn(
    async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input);
      if (path === "/auth/csrf") {
        return Response.json({ csrf_token: "c".repeat(43) });
      }
      const body = JSON.parse(String(init?.body ?? "{}")) as Record<
        string,
        unknown
      >;
      calls.push({ path, body });
      const handler = routes[path]?.shift();
      if (!handler) throw new Error(`unexpected ${path}`);
      return handler(body);
    },
  );
  vi.stubGlobal("fetch", fetchMock);
  return calls;
}

beforeEach(() => {
  localStorage.clear();
  sessionStorage.clear();
  history.replaceState(null, "", "/");
  vi.resetModules();
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.clearAllMocks();
});

describe("email code flow", () => {
  it("retries a lost start response with the same nonce and keeps the flow in this browser", async () => {
    const calls = stubAuthAPI({
      "/auth/flows": [
        () => {
          throw new TypeError("network lost");
        },
        () =>
          Response.json({
            flow_id: "flow-id",
            outcome: "proof_required",
            expires_at: new Date(Date.now() + 30 * 60_000).toISOString(),
            email_challenge: challenge(),
          }),
      ],
    });
    const { beginEmailCodeAuth, loadActiveEmailCodeFlow } = await import(
      "./email-code-auth"
    );

    const started = await beginEmailCodeAuth("person@example.com", "sign_in");

    expect(calls).toHaveLength(2);
    expect(calls[0].body.nonce).toBe(calls[1].body.nonce);
    expect(calls[0].body.provider).toBe("email_code");
    expect(loadActiveEmailCodeFlow()?.flow).toEqual(started.active.flow);
    expect(started.challenge.delivery).toBe("pending");
  });

  it("keeps the pending flow when Firebase resolution fails so the proof can be retried", async () => {
    const calls = stubAuthAPI({
      "/auth/flows": [
        () =>
          Response.json({
            flow_id: "flow-id",
            outcome: "proof_required",
            expires_at: new Date(Date.now() + 30 * 60_000).toISOString(),
            email_challenge: challenge(),
          }),
      ],
      "/auth/email/verify": [
        () =>
          Response.json({
            flow_id: "flow-id",
            intent: "sign_in",
            custom_token: "custom-1",
          }),
        () =>
          Response.json({
            flow_id: "flow-id",
            intent: "sign_in",
            custom_token: "custom-2",
          }),
      ],
      "/auth/flows/resolve": [
        () => Response.json({ error: "provider_unavailable" }, { status: 409 }),
        () => Response.json(signedIn),
      ],
      "/auth/flows/status": [
        () => Response.json({ error: "invalid_flow" }, { status: 400 }),
      ],
      "/auth/session": [() => Response.json(authenticatedSession)],
    });
    emailMocks.signInWithCustomToken.mockImplementation(
      async (_auth, custom: string) => ({
        user: {
          uid: "firebase-uid",
          email: "person@example.com",
          customToken: custom,
        },
      }),
    );
    emailMocks.getIdToken.mockResolvedValue("id-token");
    const module = await import("./email-code-auth");
    const started = await module.beginEmailCodeAuth(
      "person@example.com",
      "sign_in",
    );

    const firstProof = await module.verifyEmailCode(started.active, "123456");
    await expect(module.finishEmailProof(firstProof)).rejects.toThrow();
    expect(loadPendingEmailFlow(started.active.state)).not.toBeNull();

    const retry = await module.verifyEmailCode(started.active, "123456");
    const completed = await module.finishEmailProof(retry);

    expect(completed.result.outcome).toBe("signed_in");
    expect(emailMocks.signInWithCustomToken).toHaveBeenLastCalledWith(
      {},
      "custom-2",
    );
    expect(loadPendingEmailFlow(started.active.state)).toBeNull();
    expect(module.loadActiveEmailCodeFlow()).toBeNull();
    const resolveBodies = calls.filter(
      (call) => call.path === "/auth/flows/resolve",
    );
    expect(resolveBodies.map((call) => call.body.id_token)).toEqual([
      "id-token",
      "id-token",
    ]);
  });

  async function provedFlow(routes: Record<string, Route[]>) {
    const calls = stubAuthAPI({
      "/auth/flows": [
        () =>
          Response.json({
            flow_id: "flow-id",
            outcome: "proof_required",
            expires_at: new Date(Date.now() + 30 * 60_000).toISOString(),
            email_challenge: challenge(),
          }),
      ],
      "/auth/email/verify": [
        () =>
          Response.json({
            flow_id: "flow-id",
            intent: "sign_in",
            custom_token: "custom-1",
          }),
      ],
      ...routes,
    });
    emailMocks.signInWithCustomToken.mockResolvedValue({
      user: { uid: "firebase-uid", email: "person@example.com" },
    });
    emailMocks.getIdToken.mockResolvedValue("id-token");
    const module = await import("./email-code-auth");
    const started = await module.beginEmailCodeAuth(
      "person@example.com",
      "sign_in",
    );
    const proof = await module.verifyEmailCode(started.active, "123456");
    return { calls, module, started, proof };
  }

  it("replays a committed resolve whose session cookie never arrived", async () => {
    const { calls, module, started, proof } = await provedFlow({
      "/auth/flows/resolve": [
        () => {
          throw new TypeError("connection reset after commit");
        },
        () => Response.json(signedIn),
      ],
      "/auth/flows/status": [() => Response.json(signedIn)],
      "/auth/session": [
        () => Response.json({ authenticated: false }),
        () => Response.json(authenticatedSession),
      ],
    });

    const completed = await module.finishEmailProof(proof);

    expect(completed.result.outcome).toBe("signed_in");
    expect(
      calls.filter((call) => call.path === "/auth/flows/resolve"),
    ).toHaveLength(2);
    expect(loadPendingEmailFlow(started.active.state)).toBeNull();
    expect(module.loadActiveEmailCodeFlow()).toBeNull();
  });

  it("keeps the flow authority when the replay also loses its session", async () => {
    const { module, started, proof } = await provedFlow({
      "/auth/flows/resolve": [
        () => {
          throw new TypeError("connection reset after commit");
        },
        () => {
          throw new TypeError("connection reset after replay");
        },
      ],
      "/auth/flows/status": [
        () => Response.json(signedIn),
        () => Response.json(signedIn),
      ],
      "/auth/session": [
        () => Response.json({ authenticated: false }),
        () => Response.json({ authenticated: false }),
      ],
    });

    await expect(module.finishEmailProof(proof)).rejects.toThrow(
      "Sumi session was not established.",
    );
    expect(loadPendingEmailFlow(started.active.state)).not.toBeNull();
    expect(module.loadActiveEmailCodeFlow()?.flow.flowId).toBe("flow-id");
  });

  it("carries the linked providers of an account email cannot sign in to", async () => {
    stubAuthAPI({
      "/auth/flows": [
        () =>
          Response.json({
            flow_id: "flow-id",
            outcome: "proof_required",
            expires_at: new Date(Date.now() + 30 * 60_000).toISOString(),
            email_challenge: challenge(),
          }),
      ],
      "/auth/email/verify": [
        () =>
          Response.json(
            {
              error: "email_unverified_account",
              sign_in_providers: ["github.com", "password", 7],
            },
            { status: 409 },
          ),
      ],
    });
    const module = await import("./email-code-auth");
    const { getAuthErrorMessage } = await import("./auth-errors");
    const sessionClient = await import("./session-client");
    const started = await module.beginEmailCodeAuth(
      "person@example.com",
      "sign_in",
    );

    const error = await module
      .verifyEmailCode(started.active, "123456")
      .catch((caught: unknown) => caught);

    expect((error as AuthAPIError).details.signInProviders).toEqual([
      "github.com",
    ]);
    expect(getAuthErrorMessage(error)).toBe(
      "このアカウントはメールアドレスが確認済みではないため、メールではログインできません。このアカウントに連携済みのGitHubでログインしてください。",
    );
    expect(
      getAuthErrorMessage(
        new sessionClient.AuthAPIError("email_unverified_account", 409, {
          signInProviders: [],
        }),
      ),
    ).toContain("管理者にお問い合わせください");
  });

  it("does not replay when the lost response already delivered the session", async () => {
    const { calls, module, proof } = await provedFlow({
      "/auth/flows/resolve": [() => new Response("{", { status: 200 })],
      "/auth/flows/status": [() => Response.json(signedIn)],
      "/auth/session": [() => Response.json(authenticatedSession)],
    });

    const completed = await module.finishEmailProof(proof);

    expect(completed.result.outcome).toBe("signed_in");
    expect(
      calls.filter((call) => call.path === "/auth/flows/resolve"),
    ).toHaveLength(1);
  });
});

describe("email link landing", () => {
  it("moves link secrets out of the address bar and ignores malformed links", async () => {
    history.replaceState(
      null,
      "",
      `/email-sign-in#challenge=${challengeId}&token=${token}`,
    );
    const module = await import("./email-code-auth");

    expect(globalThis.location.pathname).toBe("/");
    expect(globalThis.location.hash).toBe("");
    expect(module.pendingEmailLink()).toEqual({ challengeId, token });

    module.clearPendingEmailLink();
    history.replaceState(null, "", "/email-sign-in#challenge=bad&token=short");
    vi.resetModules();
    const reloaded = await import("./email-code-auth");
    expect(globalThis.location.pathname).toBe("/");
    expect(reloaded.pendingEmailLink()).toBeNull();
  });

  it("persists a new authority before another browser adopts the flow", async () => {
    const calls = stubAuthAPI({
      "/auth/email/link/complete": [
        (body) => {
          const stored = localStorage.getItem("sumi.auth.email-flow-active.v1");
          expect(stored).not.toBeNull();
          expect(body.adopt).toBe(true);
          return Response.json({
            flow_id: "flow-id",
            intent: "sign_in",
            custom_token: "custom",
          });
        },
      ],
    });
    const module = await import("./email-code-auth");
    const link = { challengeId, token };
    const inspected = {
      flowId: "flow-id",
      intent: "sign_in" as const,
      email: "person@example.com",
      state: "usable" as const,
      sameBrowser: false,
      session: "none" as const,
      expiresAt: new Date(Date.now() + 10 * 60_000).toISOString(),
    };

    await expect(
      module.completeEmailLink(link, inspected, false),
    ).rejects.toThrow("link_adoption_required");
    expect(calls).toHaveLength(0);

    const proof = await module.completeEmailLink(link, inspected, true);
    expect(proof.customToken).toBe("custom");
    expect(proof.active.flow.flowId).toBe("flow-id");
    expect(calls[0].body.nonce).toBe(proof.active.flow.nonce);
    expect(module.loadActiveEmailCodeFlow()?.flow.nonce).toBe(
      proof.active.flow.nonce,
    );
  });
});
