import { describe, expect, it, vi } from "vitest";
import { createModelConnectionsAPI } from "./model-connections";

const status = {
  connected: true,
  connectionId: "connection-1",
  accountId: "account-1",
  model: "gpt-6-astra",
  effort: "medium",
  reconnectRequired: false,
  activation: "next_start",
};
const flow = {
  loginId: "login-1",
  verificationUrl: "https://auth.openai.com/codex/device",
  userCode: "ABCD-EFGH",
  expiresAt: "2099-01-01T00:00:00Z",
  intervalMs: 5000,
  status: "pending",
};

const csrfToken = "c".repeat(43);
function transport(...responses: Response[]) {
  return vi.fn<typeof fetch>(async (url) => {
    if (url === "/auth/csrf") return Response.json({ csrf_token: csrfToken });
    const response = responses.shift();
    if (!response) throw new Error("Unexpected request");
    return response;
  });
}

describe("ChatGPT connection transport", () => {
  it("uses session-bound routes and carries only model selection, not a caller-owned Human identity", async () => {
    const fetcher = transport(
      Response.json(status),
      Response.json(flow),
      Response.json({ ...flow, status: "completed", connection: status }),
      Response.json({ ...status, effort: "high" }),
      new Response(null, { status: 204 }),
      new Response(null, { status: 204 }),
    );
    const api = createModelConnectionsAPI(fetcher);
    const signal = new AbortController().signal;
    await api.status(signal);
    await api.startLogin(signal);
    await api.loginStatus("login-1", signal);
    await api.selectModel("connection-1", "high", signal);
    await api.cancelLogin("login-1", signal);
    await api.disconnect(signal);
    const calls = fetcher.mock.calls.filter(([url]) => url !== "/auth/csrf");
    expect(
      fetcher.mock.calls.filter(([url]) => url === "/auth/csrf"),
    ).toHaveLength(4);
    for (const [, init] of calls) {
      expect(new Headers(init?.headers).get("X-CSRF-Token")).toBe(
        init?.method === "GET" ? null : csrfToken,
      );
    }
    expect(calls.map(([url, init]) => [url, init?.method])).toEqual([
      ["/api/model-connections/chatgpt", "GET"],
      ["/api/model-connections/chatgpt/login", "POST"],
      ["/api/model-connections/chatgpt/login/login-1", "GET"],
      ["/api/model-connections/chatgpt/model", "PUT"],
      ["/api/model-connections/chatgpt/login/login-1", "DELETE"],
      ["/api/model-connections/chatgpt", "DELETE"],
    ]);
    expect(JSON.parse(String(calls[3][1]?.body))).toEqual({
      connectionId: "connection-1",
      model: "gpt-6-astra",
      effort: "high",
    });
    for (const [, init] of fetcher.mock.calls)
      expect(init).toMatchObject({ credentials: "include", cache: "no-store" });
  });

  it("does not retain unexpected credential fields in browser state and rejects unsafe login URLs", async () => {
    const fetcher = transport(
      Response.json({
        ...status,
        accessToken: "must-not-enter-state",
        refreshToken: "also-private",
      }),
      Response.json({ ...flow, verificationUrl: "javascript:alert(1)" }),
    );
    const api = createModelConnectionsAPI(fetcher);
    expect(await api.status(new AbortController().signal)).toEqual(status);
    await expect(
      api.startLogin(new AbortController().signal),
    ).rejects.toThrow();
  });

  it("surfaces safe server errors without pretending a mutation succeeded", async () => {
    const api = createModelConnectionsAPI(
      transport(
        Response.json(
          { error: { message: "再接続が必要です。" } },
          { status: 409 },
        ),
      ),
    );
    await expect(
      api.selectModel("old-connection", "medium", new AbortController().signal),
    ).rejects.toThrow("再接続が必要です。");
  });
});
