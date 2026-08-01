import { afterEach, describe, expect, it, vi } from "vitest";
import {
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
