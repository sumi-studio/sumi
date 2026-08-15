import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import {
  classifyPath,
  decidePath,
  originRoutes,
  runWorkerFirst,
  workspaceIntegrationContract,
} from "../cloudflare/route-policy.ts";
import { handleRequest } from "../cloudflare/worker.ts";

const scriptsDirectory = dirname(fileURLToPath(import.meta.url));
const webDirectory = resolve(scriptsDirectory, "..");
const repositoryRoot = resolve(webDirectory, "../..");
const run = promisify(execFile);

interface DiscoveredRoute {
  file: string;
  line: number;
  pattern: string;
}

interface RouteDiscovery {
  routes: DiscoveredRoute[];
  workspace_package_present: boolean;
  private_dynamic_registries: number;
}

test("the browser API, private transports, service worker, and SPA have distinct dispositions", () => {
  for (const path of [
    "/auth/session",
    "/direct-chat/ws",
    "/messaging/bootstrap",
    "/messaging/ws",
    "/workspaces",
    "/workspaces/0198f3aa-1111-7222-8333-444455556666/members",
    "/workspace-invites/redeem",
    "/apps/catalog",
    "/app-installations",
    "/app-installations/0198f3aa-1111-7222-8333-444455556666/state",
    "/health",
  ]) {
    assert.equal(classifyPath(path), "origin", path);
  }

  for (const path of [
    "/auth",
    "/direct-chat",
    "/messaging",
    "/workspace-invites",
    "/apps",
    "/agent/ws",
    "/agent/ws/",
    "/health/more",
    "/local-control/v1",
    "/local-control/v1/messaging:open",
    "/ready",
    "/ready/more",
    "/mcp-app-sandbox.html",
    "/mcp-app-sandbox.html/child",
    "/release.json/child",
    "/sw.js/child",
    "/src",
    "/src/main.ts",
    "/cloudflare/worker.ts",
    "/contracts/events",
    "/deploy/agent",
    "/docs/adr",
    "/packages/ui",
    "/scripts/cloudflare-edge.test.ts",
    "/e2e/fixtures/mcp-app-sandbox.html",
    "/public/theme-bootstrap.js",
    "/node_modules/wrangler/package.json",
    "/.git",
    "/.git/config",
    "/%2e%67it/config",
    "/%252e%2567it%252fconfig",
    "/local-control%252Fv1%252Fruntime-state%253Apublish",
    "/src%252Fmain%252Ets",
    "/assets%252Fapp.js%252Emap",
    "/mcp-app-sandbox%252Ehtml",
    "/mcp-app-sandbox%252Ehtml%252Fchild",
    "/safe/%252e%252e/src/main.ts",
    "/%25252e%252567it%25252fconfig",
    "/malformed%",
    "/.github/workflows/web-edge.yml",
    "/.env.local",
    "/package.json",
    "/assets/app.js.map",
    "/auth/app.js.map",
    "/messaging/internal.ts",
    "/unexpected.CSS.MAP",
  ]) {
    assert.equal(classifyPath(path), "deny", path);
  }

  assert.equal(classifyPath("/sw.js"), "service-worker");
  assert.equal(classifyPath("/release.json"), "release-manifest");
  for (const path of [
    "/",
    "/direct",
    "/c/0198f3aa-1111-7222-8333-444455556666",
    "/unknown-api",
    "/agent/future-browser-operation",
  ]) {
    assert.equal(classifyPath(path), "navigation", path);
  }
  for (const path of [
    "/index.html",
    "/favicon.svg",
    "/theme-bootstrap.js",
    "/assets/app.01234567.js",
    "/robots.txt",
  ]) {
    assert.equal(classifyPath(path), "static-asset", path);
  }
  assert.equal(classifyPath("not-an-absolute-path"), "deny");
  assert.deepEqual(decidePath("/missing%2Ejs"), {
    canonicalPath: "/missing.js",
    disposition: "static-asset",
  });
  assert.deepEqual(decidePath("/room/%252e%252e/direct"), {
    canonicalPath: "/direct",
    disposition: "navigation",
  });
});

test("origin forwarding preserves the incoming Request and exact Response", async () => {
  const headers = new Headers({
    "Cache-Control": "no-store",
    "Content-Type": "application/json",
  });
  headers.append("Set-Cookie", "sumi_session=one; Secure; HttpOnly");
  headers.append("Set-Cookie", "sumi_flow=two; Secure; HttpOnly");
  const expected = new Response('{"ok":true}', { headers });
  const incoming = new Request(
    "https://workspace.example.com/auth/session?continuation=%2Fdirect",
    {
      method: "POST",
      headers: {
        Cookie: "csrf=opaque",
        Origin: "https://workspace.example.com",
        "X-CSRF-Token": "bound",
      },
      body: '{"token":"opaque"}',
    },
  );
  let observedRequest: Request | undefined;
  let observedInit: unknown;

  const actual = await handleRequest(
    incoming,
    { ASSETS: { fetch: () => assert.fail("asset fallback used") } },
    async (request, init) => {
      observedRequest = request;
      observedInit = init;
      return expected;
    },
  );

  assert.equal(observedRequest, incoming);
  assert.equal((observedInit as RequestInit).signal, incoming.signal);
  assert.deepEqual((observedInit as { cf?: unknown }).cf, {
    cacheTtlByStatus: { "100-599": -1 },
  });
  assert.equal(incoming.bodyUsed, false);
  assert.equal(actual, expected);
  assert.deepEqual(expected.headers.getSetCookie(), [
    "sumi_session=one; Secure; HttpOnly",
    "sumi_flow=two; Secure; HttpOnly",
  ]);
});

test("a WebSocket 101 response is returned by identity", async () => {
  const switchingProtocols = {
    status: 101,
    webSocket: { accepted: true },
  } as unknown as Response;
  const request = new Request("https://workspace.example.com/messaging/ws", {
    headers: {
      Connection: "Upgrade",
      Origin: "https://workspace.example.com",
      Upgrade: "websocket",
    },
  });

  const response = await handleRequest(
    request,
    { ASSETS: { fetch: () => assert.fail("asset fallback used") } },
    async (observed) => {
      assert.equal(observed, request);
      return switchingProtocols;
    },
  );
  assert.equal(response, switchingProtocols);
});

test("origin rejection and Cloudflare gateway failures become a non-cacheable 503", async () => {
  const rejected = await handleRequest(
    new Request("https://workspace.example.com/auth/logout", {
      method: "POST",
    }),
    { ASSETS: { fetch: () => assert.fail("asset fallback used") } },
    async () => {
      throw new Error("named Tunnel has no live connector");
    },
  );
  assert.equal(rejected.status, 503);
  assert.equal(rejected.headers.get("Cache-Control"), "no-store");
  assert.deepEqual(await rejected.json(), { error: "origin_unavailable" });

  for (const status of [502, 504, 520, 521, 522, 523, 524, 525, 526, 530]) {
    const response = await handleRequest(
      new Request("https://workspace.example.com/messaging/bootstrap"),
      { ASSETS: { fetch: () => assert.fail("asset fallback used") } },
      async () => new Response("Cloudflare gateway page", { status }),
    );
    assert.equal(response.status, 503, String(status));
    assert.equal(
      response.headers.get("Cache-Control"),
      "no-store",
      String(status),
    );
  }
});

test("an application 503 remains the application's typed response", async () => {
  const expected = new Response('{"error":"calls_unavailable"}', {
    status: 503,
    headers: {
      "Cache-Control": "no-store",
      "Content-Type": "application/json",
    },
  });
  const actual = await handleRequest(
    new Request("https://workspace.example.com/messaging/calls/token"),
    { ASSETS: { fetch: () => assert.fail("asset fallback used") } },
    async () => expected,
  );
  assert.equal(actual, expected);
});

test("private surfaces are explicit 404s and call neither origin nor assets", async () => {
  for (const path of [
    "/agent/ws",
    "/agent/ws/child",
    "/local-control/v1/runtime-state:publish",
    "/ready",
    "/ready/details",
    "/mcp-app-sandbox.html",
    "/mcp-app-sandbox.html/child",
    "/release.json/child",
    "/sw.js/child",
    "/src/main.ts",
    "/cloudflare/worker.ts",
    "/contracts/events",
    "/deploy/agent",
    "/docs/adr",
    "/packages/ui",
    "/assets/app.js.map",
    "/auth/app.js.map",
    "/messaging/internal.ts",
    "/.git/config",
    "/%2e%67it/config",
    "/%252e%2567it%252fconfig",
    "/local-control%252Fv1%252Fruntime-state%253Apublish",
    "/src%252Fmain%252Ets",
    "/assets%252Fapp.js%252Emap",
    "/mcp-app-sandbox%252Ehtml",
    "/mcp-app-sandbox%252Ehtml%252Fchild",
    "/safe/%252e%252e/src/main.ts",
    "/%25252e%252567it%25252fconfig",
    "/malformed%",
  ]) {
    const response = await handleRequest(
      new Request(`https://workspace.example.com${path}`),
      { ASSETS: { fetch: () => assert.fail("asset fallback used") } },
      async () => assert.fail("origin used"),
    );
    assert.equal(response.status, 404, path);
    assert.equal(response.headers.get("Cache-Control"), "no-store", path);
  }
});

test("a legitimate SPA navigation retains the exact asset binding behavior", async () => {
  const incoming = new Request("https://workspace.example.com/direct");
  const expected = new Response("<!doctype html><title>Sumi</title>", {
    headers: { "Content-Type": "text/html" },
  });
  const actual = await handleRequest(
    incoming,
    {
      ASSETS: {
        fetch(request) {
          assert.equal(request, incoming);
          return Promise.resolve(expected);
        },
      },
    },
    async () => assert.fail("origin used"),
  );
  assert.equal(actual, expected);
});

test("canonical SPA navigation reaches assets directly without exposing binding redirects", async () => {
  const incoming = new Request(
    "https://workspace.example.com/room/%252e%252e/direct",
  );
  const expected = new Response("<!doctype html><title>Sumi</title>", {
    headers: { "Content-Type": "text/html" },
  });
  const actual = await handleRequest(
    incoming,
    {
      ASSETS: {
        fetch(request) {
          assert.equal(new URL(request.url).pathname, "/direct");
          return Promise.resolve(expected);
        },
      },
    },
    async () => assert.fail("origin used"),
  );
  assert.equal(actual, expected);

  const redirected = await handleRequest(
    new Request("https://workspace.example.com/another-navigation"),
    {
      ASSETS: {
        fetch: async () =>
          new Response(null, {
            status: 307,
            headers: { Location: "/index.html" },
          }),
      },
    },
  );
  assert.equal(redirected.status, 404);
  assert.equal(redirected.headers.get("Cache-Control"), "no-store");
  assert.equal(redirected.headers.get("Location"), null);
});

test("real static assets pass unchanged while file-like SPA fallback fails closed", async () => {
  const realRequest = new Request(
    "https://workspace.example.com/assets/app.01234567.js",
  );
  const expected = new Response("export {};", {
    headers: { "Content-Type": "text/javascript" },
  });
  const real = await handleRequest(realRequest, {
    ASSETS: {
      fetch(request) {
        assert.equal(request, realRequest);
        return Promise.resolve(expected);
      },
    },
  });
  assert.equal(real, expected);

  const index = await handleRequest(
    new Request("https://workspace.example.com/index.html"),
    {
      ASSETS: {
        fetch: async (request) => {
          assert.equal(new URL(request.url).pathname, "/");
          return new Response("<!doctype html><title>Sumi</title>", {
            headers: {
              "Cache-Control": "public, max-age=31536000, immutable",
              "Content-Type": "text/html",
            },
          });
        },
      },
    },
  );
  assert.equal(index.status, 200);
  assert.equal(
    index.headers.get("Cache-Control"),
    "public, max-age=0, must-revalidate",
  );

  const missing = await handleRequest(
    new Request("https://workspace.example.com/missing.js"),
    {
      ASSETS: {
        fetch: async () =>
          new Response("<!doctype html><title>Sumi</title>", {
            headers: { "Content-Type": "text/html; charset=utf-8" },
          }),
      },
    },
  );
  assert.equal(missing.status, 404);
  assert.equal(missing.headers.get("Cache-Control"), "no-store");

  const encodedMissing = await handleRequest(
    new Request("https://workspace.example.com/missing%2Ejs"),
    {
      ASSETS: {
        fetch: async (request) => {
          assert.equal(new URL(request.url).pathname, "/missing.js");
          return new Response(null, {
            status: 307,
            headers: { Location: "/missing.js" },
          });
        },
      },
    },
  );
  assert.equal(encodedMissing.status, 404);
  assert.equal(encodedMissing.headers.get("Cache-Control"), "no-store");
  assert.equal(encodedMissing.headers.get("Location"), null);
});

test("sw.js is a 404 while SPA fallback is the only asset at that path", async () => {
  const response = await handleRequest(
    new Request("https://workspace.example.com/sw.js"),
    {
      ASSETS: {
        fetch: async () =>
          new Response("<!doctype html><title>Sumi</title>", {
            headers: { "Content-Type": "text/html; charset=utf-8" },
          }),
      },
    },
  );
  assert.equal(response.status, 404);
  assert.equal(response.headers.get("Cache-Control"), "no-store");
});

test("a real sw.js is served with revalidation without changing its body or validators", async () => {
  const response = await handleRequest(
    new Request("https://workspace.example.com/sw.js"),
    {
      ASSETS: {
        fetch: async () =>
          new Response("self.addEventListener('fetch', () => {});", {
            headers: {
              "Cache-Control": "public, max-age=31536000, immutable",
              "Content-Type": "text/javascript; charset=utf-8",
              ETag: '"exact-worker"',
            },
          }),
      },
    },
  );
  assert.equal(response.status, 200);
  assert.equal(
    response.headers.get("Cache-Control"),
    "no-cache, must-revalidate",
  );
  assert.equal(response.headers.get("ETag"), '"exact-worker"');
  assert.match(await response.text(), /addEventListener/);
});

test("release identity is exact JSON with no-store, never the SPA fallback", async () => {
  const request = new Request("https://workspace.example.com/release.json");
  const missing = await handleRequest(request, {
    ASSETS: {
      fetch: async () =>
        new Response("<!doctype html><title>Sumi</title>", {
          headers: { "Content-Type": "text/html; charset=utf-8" },
        }),
    },
  });
  assert.equal(missing.status, 404);
  assert.equal(missing.headers.get("Cache-Control"), "no-store");

  const releaseSha = "0123456789abcdef0123456789abcdef01234567";
  const present = await handleRequest(request, {
    ASSETS: {
      fetch: async () =>
        new Response(JSON.stringify({ release_sha: releaseSha }), {
          headers: {
            "Cache-Control": "public, max-age=31536000, immutable",
            "Content-Type": "application/json",
            ETag: '"release-manifest"',
          },
        }),
    },
  });
  assert.equal(present.status, 200);
  assert.equal(present.headers.get("Cache-Control"), "no-store");
  assert.equal(present.headers.get("ETag"), '"release-manifest"');
  assert.deepEqual(await present.json(), { release_sha: releaseSha });
});

test("every production API registration has an explicit edge disposition", async () => {
  const discovery = await discoverApiRoutes(
    resolve(repositoryRoot, "apps/api"),
  );
  assert.ok(
    discovery.routes.length >= 30,
    `only ${discovery.routes.length} production routes found`,
  );
  for (const route of discovery.routes) {
    const path = route.pattern.slice(route.pattern.indexOf(" ") + 1);
    const expected =
      path === "/agent/ws" ||
      path === "/ready" ||
      path.startsWith("/local-control/v1/")
        ? "deny"
        : "origin";
    assert.equal(
      classifyPath(path),
      expected,
      `${route.file}:${route.line}: ${route.pattern}`,
    );
  }

  for (const prefix of ["/auth/", "/direct-chat/", "/messaging/"]) {
    assert.ok(
      discovery.routes.some((route) => route.pattern.includes(` ${prefix}`)),
      `policy prefix ${prefix} has no production registrar`,
    );
  }
  assert.ok(
    discovery.routes.some((route) => route.pattern.endsWith(" /health")),
  );
  assert.ok(discovery.private_dynamic_registries >= 1);
  assertWorkspaceIntegrationContract(discovery);

  assert.deepEqual(originRoutes.exact, [
    "/health",
    "/workspaces",
    "/app-installations",
  ]);
});

test("route discovery resolves constants and fails on uninspectable registrations", async () => {
  const temporary = await mkdtemp(resolve(tmpdir(), "sumi-edge-routes-"));
  try {
    const serverDirectory = resolve(temporary, "cmd/server");
    const internalDirectory = resolve(temporary, "internal/sample");
    await mkdir(serverDirectory, { recursive: true });
    await mkdir(internalDirectory, { recursive: true });
    await writeFile(
      resolve(serverDirectory, "main.go"),
      `package main
import "net/http"
const healthPath = "/health"
func register(mux *http.ServeMux) { mux.HandleFunc("GET " + healthPath, nil) }
`,
    );
    await writeFile(
      resolve(internalDirectory, "sample.go"),
      "package sample\n",
    );

    const resolved = await discoverApiRoutes(temporary);
    assert.deepEqual(
      resolved.routes.map((route) => route.pattern),
      ["GET /health"],
    );

    await writeFile(
      resolve(serverDirectory, "main.go"),
      `package main
import "net/http"
func register(mux *http.ServeMux) {
  pattern := "GET /health"
  mux.HandleFunc(pattern, nil)
}
`,
    );
    await assert.rejects(
      discoverApiRoutes(temporary),
      /route parity cannot skip dynamic registrations/,
    );
  } finally {
    await rm(temporary, { force: true, recursive: true });
  }
});

test("a supplied Workspace package cannot pass without parsed control routes", async () => {
  const temporary = await mkdtemp(resolve(tmpdir(), "sumi-edge-workspace-"));
  try {
    await mkdir(resolve(temporary, "cmd/server"), { recursive: true });
    await mkdir(resolve(temporary, "internal/workspace"), { recursive: true });
    await writeFile(
      resolve(temporary, "cmd/server/main.go"),
      'package main\nimport "net/http"\nfunc register(mux *http.ServeMux) { mux.HandleFunc("GET /health", nil) }\n',
    );
    await writeFile(
      resolve(temporary, "internal/workspace/http.go"),
      "package workspace\n",
    );
    const discovery = await discoverApiRoutes(temporary);
    assert.throws(
      () => assertWorkspaceIntegrationContract(discovery),
      /Workspace package is present but no Workspace browser routes were parsed/,
    );
  } finally {
    await rm(temporary, { force: true, recursive: true });
  }
});

test("Wrangler runs policy before every request", async () => {
  const config = JSON.parse(
    await readFile(resolve(webDirectory, "wrangler.jsonc"), "utf8"),
  );
  assert.equal(runWorkerFirst, true);
  assert.equal(config.assets.run_worker_first, runWorkerFirst);
  assert.equal(config.assets.directory, "./dist");
  assert.equal(config.assets.binding, "ASSETS");
  assert.equal(config.assets.not_found_handling, "single-page-application");
  assert.deepEqual(config.compatibility_flags, [
    "global_fetch_private_origin",
    "enable_request_signal",
    "request_signal_passthrough",
  ]);
  assert.equal(config.compatibility_date, "2026-08-11");
  assert.equal(config.workers_dev, false);
  assert.equal(config.preview_urls, false);
  assert.equal(config.routes, undefined);
  assert.equal(config.route, undefined);
  assert.equal(config.account_id, undefined);
  assert.equal(config.zone_id, undefined);
});

test("Node 22 edge tooling and its managed browser stay outside ordinary tests", async () => {
  const rootPackage = JSON.parse(
    await readFile(resolve(repositoryRoot, "package.json"), "utf8"),
  );
  const webPackage = JSON.parse(
    await readFile(resolve(webDirectory, "package.json"), "utf8"),
  );
  assert.equal(rootPackage.engines.node, ">=20.19");
  assert.doesNotMatch(webPackage.scripts.test, /test:edge/);
  assert.match(webPackage.scripts["test:edge"], /test:edge:runtime/);

  const workflow = await readFile(
    resolve(repositoryRoot, ".github/workflows/web-edge.yml"),
    "utf8",
  );
  assert.match(workflow, /node-version: 24/);
  assert.match(
    workflow,
    /playwright install --with-deps chromium/,
    "the dedicated workflow must provision its browser rather than depend on a runner path",
  );
  assert.match(workflow, /@sumi\/web test:edge/);

  const runtimeTest = await readFile(
    resolve(scriptsDirectory, "cloudflare-runtime.test.ts"),
    "utf8",
  );
  assert.doesNotMatch(runtimeTest, /\/usr\/bin\/google-chrome/);
  assert.match(runtimeTest, /SUMI_EDGE_CHROME_PATH/);
});

test("static policy is secure, revalidates HTML and sw.js, and only pins hashed assets", async () => {
  const headers = await readFile(
    resolve(webDirectory, "public/_headers"),
    "utf8",
  );
  const universalBlock = headers.split("\n\n", 1)[0] ?? "";
  assert.match(universalBlock, /X-Content-Type-Options: nosniff/);
  assert.match(universalBlock, /Content-Security-Policy:/);
  assert.match(universalBlock, /camera=\(self\), microphone=\(self\)/);
  assert.match(universalBlock, /frame-src https:\/\/\*\.firebaseapp\.com/);
  assert.doesNotMatch(universalBlock, /livekit\.cloud/);
  assert.doesNotMatch(universalBlock, /script-src[^;]*'unsafe-inline'/);
  assert.doesNotMatch(universalBlock, /frame-src 'none'/);
  assert.doesNotMatch(universalBlock, /Cache-Control:/);
  assert.match(
    headers,
    /\/index\.html\n\s+Cache-Control: public, max-age=0, must-revalidate/,
  );
  assert.match(
    headers,
    /\/assets\/\*\n\s+Cache-Control: public, max-age=31536000, immutable/,
  );
  assert.match(
    headers,
    /\/sw\.js\n\s+Cache-Control: no-cache, must-revalidate/,
  );
  assert.match(
    headers,
    /\/theme-bootstrap\.js\n\s+Cache-Control: no-cache, must-revalidate/,
  );
  assert.match(headers, /\/release\.json\n\s+Cache-Control: no-store/);
  for (const line of headers.split("\n")) {
    assert.ok(line.length <= 2_000, "_headers line exceeds Cloudflare's limit");
  }
});

async function discoverApiRoutes(apiRoot: string): Promise<RouteDiscovery> {
  const { stdout } = await run(
    "go",
    [
      "run",
      resolve(scriptsDirectory, "discover-api-routes.go"),
      "-api-root",
      apiRoot,
    ],
    { cwd: repositoryRoot, maxBuffer: 16 * 1024 * 1024 },
  );
  return JSON.parse(stdout) as RouteDiscovery;
}

function assertWorkspaceIntegrationContract(discovery: RouteDiscovery): void {
  const workspacePaths = discovery.routes
    .map((route) => route.pattern.slice(route.pattern.indexOf(" ") + 1))
    .filter(
      (path) =>
        workspaceIntegrationContract.exact.includes(path) ||
        workspaceIntegrationContract.prefixes.some((prefix) =>
          path.startsWith(prefix),
        ),
    );

  if (!discovery.workspace_package_present) {
    assert.deepEqual(
      workspacePaths,
      [],
      "Workspace routes were registered without the integration package",
    );
    return;
  }
  assert.ok(
    workspacePaths.length > 0,
    "Workspace package is present but no Workspace browser routes were parsed",
  );
  for (const exact of workspaceIntegrationContract.exact) {
    assert.ok(
      workspacePaths.includes(exact),
      `Workspace registrar is missing exact route ${exact}`,
    );
  }
  for (const prefix of workspaceIntegrationContract.prefixes) {
    assert.ok(
      workspacePaths.some((path) => path.startsWith(prefix)),
      `Workspace registrar is missing namespace ${prefix}`,
    );
  }
}
