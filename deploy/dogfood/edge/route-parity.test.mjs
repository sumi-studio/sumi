import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import {
  classifyPath,
  originRoutes,
  workerFirstPatterns,
} from "./route-policy.mjs";

const edgeDirectory = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = resolve(edgeDirectory, "../../..");
const productionRouteSources = [
  "apps/api/cmd/server/main.go",
  "apps/api/cmd/server/human_profile.go",
  "apps/api/internal/agentevents/router.go",
  "apps/api/internal/agentevents/browser_auth.go",
  "apps/api/internal/messaging/http.go",
  "apps/api/internal/messaging/call.go",
];

test("every literal production browser route has an explicit edge disposition", async () => {
  const routes = [];
  for (const relative of productionRouteSources) {
    const source = await readFile(resolve(repositoryRoot, relative), "utf8");
    const matcher = /\b(?:mux)\.Handle(?:Func)?\(\s*("(?:[^"\\]|\\.)*")/g;
    for (const match of source.matchAll(matcher)) {
      const pattern = JSON.parse(match[1]);
      const separator = pattern.indexOf(" ");
      if (separator === -1) continue;
      routes.push({ relative, pattern, path: pattern.slice(separator + 1) });
    }
  }
  assert.ok(
    routes.length >= 40,
    `route extraction unexpectedly found only ${routes.length}`,
  );
  for (const route of routes) {
    const disposition = classifyPath(route.path);
    const expected =
      route.path === "/agent/ws" || route.path === "/ready" ? "deny" : "origin";
    assert.equal(disposition, expected, `${route.relative}: ${route.pattern}`);
  }

  for (const prefix of originRoutes.prefixes) {
    assert.ok(
      routes.some((route) => route.path.startsWith(prefix)),
      `unused origin prefix ${prefix}`,
    );
  }
  for (const exact of originRoutes.exact) {
    assert.ok(
      routes.some((route) => route.path === exact),
      `unused exact origin route ${exact}`,
    );
  }
});

test("wrangler invokes the Worker for the complete policy boundary", async () => {
  const config = JSON.parse(
    await readFile(resolve(edgeDirectory, "wrangler.template.json"), "utf8"),
  );
  assert.deepEqual(config.assets.run_worker_first, workerFirstPatterns);
  assert.equal(config.assets.not_found_handling, "single-page-application");
  assert.deepEqual(config.compatibility_flags, ["global_fetch_private_origin"]);
  assert.equal(config.workers_dev, false);
  assert.equal(config.preview_urls, false);
});
