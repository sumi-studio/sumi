import { afterEach, describe, expect, it, vi } from "vitest";
import {
  AuthFlowRecoveryFailedError,
  confirmAuthFlow,
  resolveAuthFlow,
  startAuthFlow,
} from "./auth-flow-client";

const csrf = "c".repeat(43);
const nonce = "n".repeat(43);

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("Koseki browser auth-flow client", () => {
  it("preserves sign-up intent and provider exactly when starting", async () => {
    const fetchMock = mockAuthPost({
      flow_id: "flow-1",
      outcome: "proof_required",
      expires_at: "2026-08-01T01:00:00Z",
    });

    await startAuthFlow({
      intent: "sign_up",
      provider: "github.com",
      continuation: "/",
      nonce,
    });

    expect(fetchMock.mock.calls[1]?.[0]).toBe("/auth/flows");
    expect(JSON.parse(String(fetchMock.mock.calls[1]?.[1]?.body))).toEqual({
      intent: "sign_up",
      provider: "github.com",
      continuation: "/",
      nonce,
    });
  });

  it("returns an explicit mismatch action without changing intent", async () => {
    const fetchMock = mockAuthPost({
      flow_id: "flow-1",
      outcome: "confirmation_required",
      next_action: "create_account",
      continuation: "/",
      expires_at: "2026-08-01T01:00:00Z",
    });

    await expect(
      resolveAuthFlow({ flowId: "flow-1", nonce, idToken: "id-token" }),
    ).resolves.toMatchObject({
      outcome: "confirmation_required",
      nextAction: "create_account",
    });
    expect(fetchMock.mock.calls[1]?.[0]).toBe("/auth/flows/resolve");
    expect(JSON.parse(String(fetchMock.mock.calls[1]?.[1]?.body))).toEqual({
      flow_id: "flow-1",
      nonce,
      id_token: "id-token",
    });
  });

  it("sends only the server-requested action to explicit confirmation", async () => {
    const fetchMock = mockAuthPost({
      flow_id: "flow-1",
      outcome: "account_created",
      continuation: "/",
      expires_at: "2026-08-01T01:00:00Z",
    });

    await confirmAuthFlow({
      flowId: "flow-1",
      nonce,
      action: "create_account",
    });

    expect(fetchMock.mock.calls[1]?.[0]).toBe("/auth/flows/confirm");
    expect(JSON.parse(String(fetchMock.mock.calls[1]?.[1]?.body))).toEqual({
      flow_id: "flow-1",
      nonce,
      action: "create_account",
    });
  });

  it("recovers an ambiguous resolve through canonical flow status", async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(csrfResponse())
      .mockRejectedValueOnce(new TypeError("connection reset"))
      .mockResolvedValueOnce(csrfResponse())
      .mockResolvedValueOnce(
        flowResponse({
          flow_id: "flow-1",
          outcome: "confirmation_required",
          next_action: "create_account",
          continuation: "/",
          expires_at: "2026-08-01T01:00:00Z",
        }),
      );
    vi.stubGlobal("fetch", fetchMock);

    await expect(
      resolveAuthFlow({ flowId: "flow-1", nonce, idToken: "id-token" }),
    ).resolves.toMatchObject({
      outcome: "confirmation_required",
      nextAction: "create_account",
    });
    expect(fetchMock.mock.calls.map(([path]) => path)).toEqual([
      "/auth/csrf",
      "/auth/flows/resolve",
      "/auth/csrf",
      "/auth/flows/status",
    ]);
  });

  it("recovers an ambiguous confirm only from a terminal status", async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(csrfResponse())
      .mockRejectedValueOnce(new DOMException("timed out", "TimeoutError"))
      .mockResolvedValueOnce(csrfResponse())
      .mockResolvedValueOnce(
        flowResponse({
          flow_id: "flow-1",
          outcome: "account_created",
          continuation: "/",
          expires_at: "2026-08-01T01:00:00Z",
        }),
      );
    vi.stubGlobal("fetch", fetchMock);

    await expect(
      confirmAuthFlow({
        flowId: "flow-1",
        nonce,
        action: "create_account",
      }),
    ).resolves.toMatchObject({ outcome: "account_created" });
  });

  it("assures Sumi logout when ambiguous status cannot prove the outcome", async () => {
    const mutationError = new TypeError("connection reset");
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(csrfResponse())
      .mockRejectedValueOnce(mutationError)
      .mockResolvedValueOnce(csrfResponse())
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ error: "unavailable" }), { status: 503 }),
      )
      .mockResolvedValueOnce(csrfResponse())
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(
      resolveAuthFlow({ flowId: "flow-1", nonce, idToken: "id-token" }),
    ).rejects.toBe(mutationError);
    expect(fetchMock.mock.calls.map(([path]) => path)).toEqual([
      "/auth/csrf",
      "/auth/flows/resolve",
      "/auth/csrf",
      "/auth/flows/status",
      "/auth/csrf",
      "/auth/logout",
    ]);
  });

  it("fails closed explicitly when recovery logout also fails", async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(csrfResponse())
      .mockRejectedValueOnce(new TypeError("connection reset"))
      .mockResolvedValueOnce(csrfResponse())
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ error: "unavailable" }), { status: 503 }),
      )
      .mockResolvedValueOnce(csrfResponse())
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ error: "logout unavailable" }), {
          status: 503,
        }),
      );
    vi.stubGlobal("fetch", fetchMock);

    await expect(
      confirmAuthFlow({
        flowId: "flow-1",
        nonce,
        action: "create_account",
      }),
    ).rejects.toBeInstanceOf(AuthFlowRecoveryFailedError);
  });
});

function mockAuthPost(result: Record<string, string>) {
  const fetchMock = vi
    .fn<typeof fetch>()
    .mockResolvedValueOnce(
      new Response(JSON.stringify({ csrf_token: csrf }), { status: 200 }),
    )
    .mockResolvedValueOnce(
      new Response(JSON.stringify(result), { status: 200 }),
    );
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

function csrfResponse(): Response {
  return new Response(JSON.stringify({ csrf_token: csrf }), { status: 200 });
}

function flowResponse(result: Record<string, string>): Response {
  return new Response(JSON.stringify(result), { status: 200 });
}
