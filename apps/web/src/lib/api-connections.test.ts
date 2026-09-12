import { expect, it, vi } from "vitest";
import { createAPIConnectionsClient } from "./api-connections";

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
