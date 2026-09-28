import { expect, it, vi } from "vitest";
import {
  APIConnectionError,
  createAPIConnectionsClient,
} from "./api-connections";

async function saveFailure(response: Response) {
  const client = createAPIConnectionsClient(async (path) =>
    path === "/auth/csrf"
      ? Response.json({ csrf_token: "a".repeat(43) })
      : response,
  );
  return client
    .save(
      {
        name: "n",
        preset: "openai-chat",
        baseUrl: "https://x.example",
        model: "m",
      },
      undefined,
      new AbortController().signal,
    )
    .then(
      () => expect.unreachable(),
      (error: unknown) => error,
    );
}

it("shows the API's input-validation reason for a 400", async () => {
  const error = await saveFailure(
    Response.json(
      {
        error: {
          message: "接続の入力内容を確認してください。",
          detail:
            'extra header "Authorization" is reserved by the request itself',
        },
      },
      { status: 400 },
    ),
  );
  expect(error).toBeInstanceOf(APIConnectionError);
  expect((error as APIConnectionError).message).toBe(
    '接続を変更できませんでした。入力内容を確認してください。 extra header "Authorization" is reserved by the request itself',
  );
});

it("never carries a diagnostic body into the displayable message", async () => {
  const diagnostic = "dial tcp 10.0.0.7:5432: connection refused";
  const cases = [
    Response.json(
      { error: { message: diagnostic, detail: diagnostic } },
      { status: 503 },
    ),
    Response.json({ error: { detail: diagnostic } }, { status: 502 }),
    new Response(`<html>${diagnostic}</html>`, { status: 400 }),
    Response.json(
      { error: { detail: { nested: diagnostic } } },
      { status: 400 },
    ),
    Response.json(
      { error: { detail: `${diagnostic}\n${"x".repeat(20)}` } },
      { status: 400 },
    ),
    Response.json(
      { error: { detail: diagnostic.repeat(20) } },
      { status: 400 },
    ),
    Response.json({ error: diagnostic }, { status: 401 }),
  ];
  for (const response of cases) {
    const error = await saveFailure(response);
    expect(error).toBeInstanceOf(APIConnectionError);
    expect((error as APIConnectionError).message).not.toContain("dial tcp");
    expect((error as APIConnectionError).status).toBe(response.status);
  }
  expect(
    ((await saveFailure(new Response(null, { status: 404 }))) as Error).message,
  ).toBe("接続が見つかりません。状態を更新してください。");
});

it("uses session-bound BYOK routes, explicit none and write-only credential submission", async () => {
  const connection = {
    id: "00000000-0000-4000-8000-000000000001",
    name: "Personal",
    preset: "openai-chat",
    baseUrl: "https://provider.example/v1",
    model: "custom-model",
  };
  const calls: Array<[string, RequestInit | undefined]> = [];
  const fetcher = vi.fn(async (path: RequestInfo | URL, init?: RequestInit) => {
    calls.push([String(path), init]);
    if (path === "/auth/csrf")
      return Response.json({ csrf_token: "a".repeat(43) });
    if (init?.method === "DELETE") return new Response(null, { status: 204 });
    if (path === "/api/model-connections")
      return Response.json({
        available: true,
        connections: [connection],
        selection: { kind: "none" },
        activation: "next_start",
      });
    return Response.json(connection);
  });
  const client = createAPIConnectionsClient(fetcher);
  const signal = new AbortController().signal;
  expect((await client.list(signal)).selection).toEqual({ kind: "none" });
  const { id, ...input } = connection;
  expect(
    await client.save(
      { ...input, apiKey: "synthetic-test-only" },
      undefined,
      signal,
    ),
  ).toEqual(connection);
  await client.select({ kind: "none" }, signal);
  await client.remove(id, signal);
  const mutations = calls.filter(
    ([, init]) => init?.method && init.method !== "GET",
  );
  expect(mutations.map(([path]) => path)).toEqual([
    "/api/model-connections/api",
    "/api/model-connections/selection",
    `/api/model-connections/api/${id}`,
  ]);
  for (const [, init] of mutations) {
    expect(init?.credentials).toBe("include");
    expect(init?.cache).toBe("no-store");
    expect(new Headers(init?.headers).get("X-CSRF-Token")).toBe("a".repeat(43));
  }
  expect(JSON.parse(String(mutations[1]?.[1]?.body))).toEqual({ kind: "none" });
});

it("drives the ChatGPT sign-in endpoints and shows only fixed failure text", async () => {
  const calls: { path: string; method: string; body?: string }[] = [];
  const replies: Response[] = [
    Response.json({
      loginId: "l/1",
      status: "pending",
      verificationUrl: "https://auth.openai.com/codex/device",
      userCode: "ABCD-1234",
      expiresAt: "2026-09-28T10:15:00Z",
      intervalMs: 5000,
    }),
    Response.json(
      {
        error: {
          code: "device_login_unavailable",
          message: "upstream said something private",
        },
      },
      { status: 502 },
    ),
    Response.json({ error: { message: "x" } }, { status: 409 }),
  ];
  const client = createAPIConnectionsClient(async (input, init) => {
    const path = String(input);
    if (path === "/auth/csrf")
      return Response.json({ csrf_token: "a".repeat(43) });
    calls.push({
      path,
      method: init?.method ?? "GET",
      body: init?.body as string | undefined,
    });
    return (
      replies.shift() ??
      Response.json({
        loginId: "l/1",
        status: "cancelled",
        expiresAt: "2026-09-28T10:15:00Z",
        intervalMs: 5000,
      })
    );
  });
  const signal = new AbortController().signal;
  const login = await client.beginChatGPTLogin("attempt-1", "conn-1", signal);
  expect(login.userCode).toBe("ABCD-1234");
  expect(calls[0]).toEqual({
    path: "/api/model-connections/chatgpt/login",
    method: "POST",
    body: JSON.stringify({ loginId: "attempt-1", connectionId: "conn-1" }),
  });
  const refused = await client
    .chatGPTLogin(login.loginId, signal)
    .catch((e: unknown) => e);
  expect(calls[1]?.path).toBe("/api/model-connections/chatgpt/login/l%2F1");
  expect(refused).toBeInstanceOf(APIConnectionError);
  expect((refused as Error).message).toContain("デバイスコード");
  expect((refused as Error).message).not.toContain("private");
  const disabled = await client
    .beginChatGPTLogin("attempt-2", undefined, signal)
    .catch((e: unknown) => e);
  expect((disabled as APIConnectionError).status).toBe(409);
  expect((disabled as Error).message).toContain("利用できません");
  await client.cancelChatGPTLogin("l/1", signal);
  expect(calls[3]).toMatchObject({
    path: "/api/model-connections/chatgpt/login/l%2F1",
    method: "DELETE",
  });
});
