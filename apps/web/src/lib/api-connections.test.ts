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
