import assert from "node:assert/strict";
import test from "node:test";
import { classifyPath, decidePath } from "../cloudflare/route-policy.ts";

test("browser APIs, private paths, static assets, and navigation stay distinct", () => {
  for (const path of [
    "/auth/session",
    "/direct-chat/ws",
    "/messaging/bootstrap",
    "/workspaces",
    "/workspace-invites/redeem",
    "/apps/catalog",
    "/app-installations",
    "/health",
  ]) {
    assert.equal(classifyPath(path), "origin", path);
  }

  for (const path of [
    "/auth",
    "/agent/ws",
    "/local-control/v1",
    "/ready",
    "/mcp-app-sandbox.html",
    "/src/main.ts",
    "/.git/config",
    "/%252e%2567it%252fconfig",
    "/local-control%252Fv1%252Fruntime-state%253Apublish",
    "/src%252Fmain%252Ets",
    "/mcp-app-sandbox%252Ehtml",
    "/safe/%252e%252e/src/main.ts",
    "/%25252e%252567it%25252fconfig",
    "/malformed%",
    "/assets/app.js.map",
    "/messaging/internal.ts",
  ]) {
    assert.equal(classifyPath(path), "deny", path);
  }

  assert.equal(classifyPath("/sw.js"), "service-worker");
  assert.equal(classifyPath("/release.json"), "release-manifest");
  assert.equal(classifyPath("/direct"), "navigation");
  assert.equal(classifyPath("/assets/app.01234567.js"), "static-asset");
  assert.equal(classifyPath("not-an-absolute-path"), "deny");
});

test("canonicalization is bounded and produces one routing path", () => {
  assert.deepEqual(decidePath("/missing%2Ejs"), {
    canonicalPath: "/missing.js",
    disposition: "static-asset",
  });
  assert.deepEqual(decidePath("/room/%252e%252e/direct"), {
    canonicalPath: "/direct",
    disposition: "navigation",
  });
  assert.deepEqual(decidePath("/%25252e%252567it%25252fconfig"), {
    canonicalPath: null,
    disposition: "deny",
  });
});
