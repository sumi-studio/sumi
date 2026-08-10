import assert from "node:assert/strict";
import test from "node:test";
import { classifyPath } from "./route-policy.mjs";
import { handleRequest } from "./worker.mjs";

test("only the browser API surface and exact health reach the origin", () => {
  for (const path of [
    "/auth/session",
    "/direct-chat/ws",
    "/messaging/bootstrap",
    "/messaging/ws",
    "/health",
  ]) {
    assert.equal(classifyPath(path), "origin", path);
  }
  for (const path of [
    "/auth",
    "/direct-chat",
    "/messaging",
    "/agent",
    "/agent/ws",
    "/agent/future",
    "/health/more",
    "/local-control",
    "/local-control/v1/runtime-state",
    "/ready",
    "/ready/more",
    "/mcp-app-sandbox.html",
  ]) {
    assert.equal(classifyPath(path), "deny", path);
  }
  for (const path of ["/", "/workspace/settings", "/unknown-api"]) {
    assert.equal(classifyPath(path), "asset", path);
  }
});

test("origin responses stream through unchanged with cache bypass", async () => {
  const expected = new Response("ok", {
    headers: [
      ["Cache-Control", "no-store"],
      ["Set-Cookie", "sumi_session=opaque; Secure; HttpOnly"],
    ],
  });
  let observed;
  const actual = await handleRequest(
    new Request("https://sumi.example/messaging/bootstrap"),
    { ASSETS: { fetch: () => assert.fail("asset fallback used") } },
    async (request) => {
      observed = request;
      return expected;
    },
  );
  assert.equal(actual, expected);
  assert.equal(observed.cache, "no-store");
});

test("origin failure is a non-cacheable 503, never the SPA", async () => {
  const response = await handleRequest(
    new Request("https://sumi.example/auth/session"),
    { ASSETS: { fetch: () => assert.fail("asset fallback used") } },
    async () => {
      throw new Error("tunnel offline");
    },
  );
  assert.equal(response.status, 503);
  assert.equal(response.headers.get("Cache-Control"), "no-store");
  assert.deepEqual(await response.json(), { error: "origin_unavailable" });
});

test("a live Tunnel connector with an unavailable origin is also normalized", async () => {
  for (const status of [502, 504, 521, 522, 523, 524, 530]) {
    const response = await handleRequest(
      new Request("https://sumi.example/messaging/bootstrap"),
      { ASSETS: { fetch: () => assert.fail("asset fallback used") } },
      async () => new Response("Cloudflare gateway page", { status }),
    );
    assert.equal(response.status, 503, status);
    assert.equal(response.headers.get("Cache-Control"), "no-store", status);
    assert.deepEqual(await response.json(), { error: "origin_unavailable" });
  }
});

test("an application 503 keeps its typed failure instead of becoming a Tunnel error", async () => {
  const expected = new Response('{"error":"calls_unavailable"}', {
    status: 503,
    headers: {
      "Cache-Control": "no-store",
      "Content-Type": "application/json",
    },
  });
  const response = await handleRequest(
    new Request("https://sumi.example/messaging/calls/token"),
    { ASSETS: { fetch: () => assert.fail("asset fallback used") } },
    async () => expected,
  );
  assert.equal(response, expected);
  assert.deepEqual(await response.json(), { error: "calls_unavailable" });
});

test("denied surfaces never call origin or assets", async () => {
  for (const path of [
    "/auth",
    "/direct-chat",
    "/messaging",
    "/agent",
    "/agent/ws",
    "/health/more",
    "/local-control",
    "/local-control/v1/messaging:open",
    "/ready",
    "/ready/more",
    "/mcp-app-sandbox.html",
  ]) {
    const response = await handleRequest(
      new Request(`https://sumi.example${path}`),
      { ASSETS: { fetch: () => assert.fail("asset fallback used") } },
      async () => assert.fail("origin used"),
    );
    assert.equal(response.status, 404, path);
    assert.equal(response.headers.get("Cache-Control"), "no-store", path);
  }
});
