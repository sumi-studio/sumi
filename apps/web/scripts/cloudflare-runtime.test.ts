import assert from "node:assert/strict";
import { type ChildProcess, execFile, spawn } from "node:child_process";
import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { createServer as createHttpServer } from "node:http";
import { connect, createServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { chromium } from "@playwright/test";

const run = promisify(execFile);
const scriptsDirectory = dirname(fileURLToPath(import.meta.url));
const webDirectory = resolve(scriptsDirectory, "..");
const wranglerEntry = resolve(
  webDirectory,
  "node_modules/wrangler/bin/wrangler.js",
);
const productionWranglerConfig = resolve(webDirectory, "wrangler.jsonc");
const runtimeReleaseSha = "fedcba9876543210fedcba9876543210fedcba98";

test("readiness ignores raw 404s until the Worker denial is canonical", async () => {
  const responses = [
    new Response(null, { status: 404 }),
    new Response(null, {
      status: 404,
      headers: { "Cache-Control": "no-store" },
    }),
    new Response(null, {
      status: 404,
      headers: { "X-Content-Type-Options": "nosniff" },
    }),
    new Response(null, {
      status: 404,
      headers: {
        "Cache-Control": "no-store",
        "X-Content-Type-Options": "nosniff",
      },
    }),
  ];
  let elapsedMilliseconds = 0;
  let probeCount = 0;

  await waitUntilReady(
    "http://127.0.0.1:8787",
    { exitCode: null },
    {
      now: () => elapsedMilliseconds,
      pause: async (milliseconds) => {
        elapsedMilliseconds += milliseconds;
      },
      probe: async (url) => {
        assert.equal(url, "http://127.0.0.1:8787/ready");
        const response = responses[probeCount];
        assert.ok(response, "readiness performed an unexpected extra probe");
        probeCount += 1;
        return response;
      },
    },
  );

  assert.equal(probeCount, responses.length);
  assert.equal(elapsedMilliseconds, 300);
});

test("pinned Wrangler dry-run and local workerd enforce the production artifact", {
  timeout: 120_000,
}, async () => {
  const runtimeDirectory = await mkdtemp(resolve(tmpdir(), "sumi-workerd-"));
  const artifactDirectory = resolve(runtimeDirectory, "dist");
  const wranglerConfig = resolve(runtimeDirectory, "wrangler.jsonc");
  let cancellationOrigin: CancellationOrigin | undefined;
  try {
    await run("pnpm", ["run", "build"], {
      cwd: webDirectory,
      env: {
        ...process.env,
        SUMI_RELEASE_SHA: runtimeReleaseSha,
        SUMI_WEB_DIST_DIR: artifactDirectory,
      },
      maxBuffer: 16 * 1024 * 1024,
    });
    await writeRuntimeWranglerConfig(wranglerConfig, artifactDirectory);

    const releaseManifest = JSON.parse(
      await readFile(resolve(artifactDirectory, "release.json"), "utf8"),
    ) as { release_sha?: unknown };
    assert.deepEqual(releaseManifest, { release_sha: runtimeReleaseSha });

    const version = await run(process.execPath, [wranglerEntry, "--version"], {
      cwd: runtimeDirectory,
      env: { ...process.env, CI: "1" },
    });
    assert.equal(version.stdout.trim(), "4.120.1");

    const dryRun = await run(
      process.execPath,
      [
        wranglerEntry,
        "deploy",
        "--dry-run",
        "--config",
        wranglerConfig,
        "--outdir",
        resolve(runtimeDirectory, "bundle"),
      ],
      {
        cwd: runtimeDirectory,
        env: { ...process.env, CI: "1" },
        maxBuffer: 16 * 1024 * 1024,
      },
    );
    assert.match(dryRun.stdout + dryRun.stderr, /--dry-run: exiting now/);

    const port = await availablePort();
    const inspectorPort = await availablePort();
    const origin = `http://127.0.0.1:${port}`;
    cancellationOrigin = await startCancellationOrigin();
    const server = startWrangler(
      port,
      inspectorPort,
      resolve(runtimeDirectory, "wrangler-state"),
      runtimeDirectory,
      wranglerConfig,
      cancellationOrigin.authority,
    );
    try {
      await waitUntilReady(origin, server);

      const ready = await manualFetch(origin, "/ready");
      assert.equal(ready.status, 404);
      assert.equal(ready.headers.get("Cache-Control"), "no-store");
      assert.equal(ready.headers.get("X-Content-Type-Options"), "nosniff");

      const serviceWorker = await manualFetch(origin, "/sw.js");
      assert.equal(serviceWorker.status, 200);
      assert.equal(
        serviceWorker.headers.get("Cache-Control"),
        "no-cache, must-revalidate",
      );
      assert.match(
        serviceWorker.headers.get("Content-Type") ?? "",
        /^(?:application|text)\/javascript/,
      );
      const serviceWorkerSource = await serviceWorker.text();
      assert.doesNotMatch(
        serviceWorkerSource,
        /\bconsole\s*\./,
        "the Service Worker must not log routing pointers",
      );

      const release = await manualFetch(origin, "/release.json");
      assert.equal(release.status, 200);
      assert.equal(release.headers.get("Cache-Control"), "no-store");
      assert.deepEqual(await release.json(), releaseManifest);

      const deepLink = await manualFetch(origin, "/direct");
      assert.equal(deepLink.status, 200);
      assert.equal(deepLink.headers.get("Location"), null);
      assert.match(deepLink.headers.get("Content-Type") ?? "", /^text\/html/);
      const csp = deepLink.headers.get("Content-Security-Policy") ?? "";
      assert.match(csp, /script-src 'self'/);
      assert.doesNotMatch(csp, /script-src[^;]*'unsafe-inline'/);
      assert.doesNotMatch(csp, /livekit\.cloud/);
      const deepLinkHtml = await deepLink.text();
      assert.match(deepLinkHtml, /<div id="root"><\/div>/);

      const indexAsset = await manualFetch(origin, "/index.html");
      assert.equal(indexAsset.status, 200);
      assert.equal(indexAsset.headers.get("Location"), null);
      assert.match(indexAsset.headers.get("Content-Type") ?? "", /^text\/html/);
      assert.equal(
        indexAsset.headers.get("Cache-Control"),
        "public, max-age=0, must-revalidate",
      );

      const entryAssetPath = deepLinkHtml.match(
        /<script[^>]+src="(\/assets\/[^"]+\.js)"/,
      )?.[1];
      assert.ok(entryAssetPath, "built SPA entry asset was not found");
      const entryAsset = await manualFetch(origin, entryAssetPath);
      assert.equal(entryAsset.status, 200);
      assert.equal(entryAsset.headers.get("Location"), null);
      assert.match(
        entryAsset.headers.get("Content-Type") ?? "",
        /^(?:application|text)\/javascript/,
      );

      const bootstrap = await manualFetch(origin, "/theme-bootstrap.js");
      assert.equal(bootstrap.status, 200);
      assert.equal(bootstrap.headers.get("Location"), null);
      assert.equal(
        bootstrap.headers.get("Cache-Control"),
        "no-cache, must-revalidate",
      );
      assert.match(
        bootstrap.headers.get("Content-Type") ?? "",
        /^(?:application|text)\/javascript/,
      );

      const favicon = await manualFetch(origin, "/favicon.svg");
      assert.equal(favicon.status, 200);
      assert.equal(favicon.headers.get("Location"), null);
      assert.match(favicon.headers.get("Content-Type") ?? "", /^image\/svg/);

      const canonicalNavigation = await manualFetch(
        origin,
        "/room/%252e%252e/direct",
      );
      assert.equal(canonicalNavigation.status, 200);
      assert.equal(canonicalNavigation.headers.get("Location"), null);
      assert.match(
        canonicalNavigation.headers.get("Content-Type") ?? "",
        /^text\/html/,
      );

      for (const protectedPath of [
        "/mcp-app-sandbox.html/child",
        "/release.json/child",
        "/sw.js/child",
        "/src/main.ts",
        "/cloudflare/worker.ts",
        "/contracts/events",
        "/deploy/agent",
        "/docs/adr",
        "/packages/ui",
        "/scripts/cloudflare-edge.test.ts",
        "/.git/config",
        "/%2e%67it/config",
        "/%252e%2567it%252fconfig",
        "/local-control%252Fv1%252Fruntime-state%253Apublish",
        "/src%252Fmain%252Ets",
        "/assets%252Fapp.js%252Emap",
        "/mcp-app-sandbox%252Ehtml",
        "/mcp-app-sandbox%252Ehtml%252Fchild",
        "/safe/%2e%2e/src/main.ts",
        "/safe/%252e%252e/src/main.ts",
        "/%25252e%252567it%25252fconfig",
        "/malformed%",
        "/assets/app.js.map",
        "/auth/app.js.map",
        "/messaging/internal.ts",
        "/missing.js",
        "/missing%2Ejs",
      ]) {
        const protectedResponse = await manualFetch(origin, protectedPath);
        assert.equal(protectedResponse.status, 404, protectedPath);
        assert.equal(
          protectedResponse.headers.get("Cache-Control"),
          "no-store",
          protectedPath,
        );
        assert.equal(
          protectedResponse.headers.get("Location"),
          null,
          protectedPath,
        );
      }

      await verifyOriginCancellation(origin, cancellationOrigin);
      await verifyThemeBootstrapInBrowser(origin);
    } finally {
      await stopWrangler(server);
    }
  } finally {
    await cancellationOrigin?.close();
    await rm(runtimeDirectory, { force: true, recursive: true });
  }
});

interface CancellationOrigin {
  authority: string;
  requestReceived: Promise<void>;
  subrequestCancelled: Promise<void>;
  close(): Promise<void>;
}

async function writeRuntimeWranglerConfig(
  output: string,
  artifactDirectory: string,
): Promise<void> {
  const config = JSON.parse(
    await readFile(productionWranglerConfig, "utf8"),
  ) as {
    main: string;
    assets?: { directory?: string };
  };
  assert.ok(config.assets);
  config.main = resolve(webDirectory, "cloudflare/worker.ts");
  config.assets.directory = artifactDirectory;
  await writeFile(output, `${JSON.stringify(config, null, 2)}\n`, "utf8");
}

async function startCancellationOrigin(): Promise<CancellationOrigin> {
  let resolveRequestReceived!: () => void;
  let resolveSubrequestCancelled!: () => void;
  const requestReceived = new Promise<void>((resolvePromise) => {
    resolveRequestReceived = resolvePromise;
  });
  const subrequestCancelled = new Promise<void>((resolvePromise) => {
    resolveSubrequestCancelled = resolvePromise;
  });
  let observed = false;
  const server = createHttpServer((request, response) => {
    if (request.url !== "/auth/cancellation") {
      response.writeHead(404).end();
      return;
    }
    if (observed) {
      response.writeHead(409).end();
      return;
    }
    observed = true;
    const heartbeat = setInterval(() => {
      response.write("origin stream remains open\n");
    }, 50);
    response.once("close", () => {
      clearInterval(heartbeat);
      if (!response.writableEnded) resolveSubrequestCancelled();
    });
    response.writeHead(200, {
      "Cache-Control": "no-store",
      "Content-Type": "text/plain; charset=utf-8",
    });
    response.write("origin stream is open\n");
    resolveRequestReceived();
    // Deliberately leave the body pending. The only successful completion is
    // workerd closing this origin stream after the browser aborts its body.
  });
  await new Promise<void>((resolveListen, rejectListen) => {
    server.once("error", rejectListen);
    server.listen(0, "127.0.0.1", resolveListen);
  });
  const address = server.address();
  assert.ok(address && typeof address !== "string");

  return {
    authority: `127.0.0.1:${address.port}`,
    requestReceived,
    subrequestCancelled,
    async close() {
      if (!server.listening) return;
      const closed = new Promise<void>((resolveClose) =>
        server.once("close", resolveClose),
      );
      server.close();
      server.closeAllConnections();
      await closed;
    },
  };
}

async function verifyOriginCancellation(
  workerOrigin: string,
  cancellationOrigin: CancellationOrigin,
): Promise<void> {
  const worker = new URL(workerOrigin);
  const socket = connect({
    host: worker.hostname,
    port: Number(worker.port),
  });
  const responseStarted = new Promise<void>((resolveStarted, rejectStarted) => {
    let response = "";
    socket.on("data", (chunk: Buffer) => {
      response += chunk.toString("utf8");
      if (!response.includes("origin stream is open")) return;
      assert.match(response, /^HTTP\/1\.1 200 /);
      resolveStarted();
    });
    socket.once("error", rejectStarted);
    socket.once("close", () => {
      if (!response.includes("origin stream is open")) {
        rejectStarted(
          new Error("workerd closed before returning the origin stream"),
        );
      }
    });
  });
  await new Promise<void>((resolveConnected, rejectConnected) => {
    socket.once("connect", resolveConnected);
    socket.once("error", rejectConnected);
  });
  socket.write(
    [
      "GET /auth/cancellation HTTP/1.1",
      `Host: ${worker.host}`,
      "Connection: close",
      "",
      "",
    ].join("\r\n"),
  );

  await within(
    Promise.all([cancellationOrigin.requestReceived, responseStarted]),
    5_000,
    "workerd did not stream the private-origin response",
  );
  socket.destroy();
  await within(
    cancellationOrigin.subrequestCancelled,
    5_000,
    "workerd did not cancel the private-origin subrequest",
  );
}

async function within<T>(
  operation: Promise<T>,
  timeoutMilliseconds: number,
  message: string,
): Promise<T> {
  let timeout: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      operation,
      new Promise<never>((_resolve, reject) => {
        timeout = setTimeout(
          () => reject(new Error(message)),
          timeoutMilliseconds,
        );
      }),
    ]);
  } finally {
    if (timeout !== undefined) clearTimeout(timeout);
  }
}

function manualFetch(origin: string, path: string): Promise<Response> {
  return fetch(`${origin}${path}`, { redirect: "manual" });
}

function startWrangler(
  port: number,
  inspectorPort: number,
  persistenceDirectory: string,
  workingDirectory: string,
  wranglerConfig: string,
  localUpstream: string,
): ChildProcess {
  const child = spawn(
    process.execPath,
    [
      wranglerEntry,
      "dev",
      "--local",
      "--ip",
      "127.0.0.1",
      "--port",
      String(port),
      "--inspector-port",
      String(inspectorPort),
      "--persist-to",
      persistenceDirectory,
      "--config",
      wranglerConfig,
      "--local-upstream",
      localUpstream,
      "--upstream-protocol",
      "http",
      "--log-level",
      "error",
      "--show-interactive-dev-session=false",
    ],
    {
      cwd: workingDirectory,
      detached: process.platform !== "win32",
      env: { ...process.env, CI: "1" },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  captureBounded(child.stdout);
  captureBounded(child.stderr);
  return child;
}

interface ReadinessDependencies {
  now(): number;
  pause(milliseconds: number): Promise<void>;
  probe(url: string): Promise<Response>;
}

const defaultReadinessDependencies: ReadinessDependencies = {
  now: Date.now,
  pause: (milliseconds) =>
    new Promise((resolveDelay) => setTimeout(resolveDelay, milliseconds)),
  probe: (url) =>
    fetch(url, {
      signal: AbortSignal.timeout(1_000),
    }),
};

function isCanonicalWorkerDenial(response: Response): boolean {
  return (
    response.status === 404 &&
    response.headers.get("Cache-Control") === "no-store" &&
    response.headers.get("X-Content-Type-Options") === "nosniff"
  );
}

async function waitUntilReady(
  origin: string,
  child: Pick<ChildProcess, "exitCode">,
  dependencies: ReadinessDependencies = defaultReadinessDependencies,
): Promise<void> {
  const deadline = dependencies.now() + 30_000;
  while (dependencies.now() < deadline) {
    if (child.exitCode !== null) {
      throw new Error(`wrangler exited before readiness (${child.exitCode})`);
    }
    try {
      const response = await dependencies.probe(`${origin}/ready`);
      if (isCanonicalWorkerDenial(response)) return;
    } catch {}
    await dependencies.pause(100);
  }
  throw new Error("wrangler local runtime did not become ready within 30s");
}

async function verifyThemeBootstrapInBrowser(origin: string): Promise<void> {
  const configuredExecutable = process.env.SUMI_EDGE_CHROME_PATH;
  if (
    configuredExecutable !== undefined &&
    configuredExecutable.trim() !== configuredExecutable
  ) {
    throw new Error("SUMI_EDGE_CHROME_PATH must not contain outer whitespace");
  }
  if (configuredExecutable !== undefined) await stat(configuredExecutable);

  let browser: Awaited<ReturnType<typeof chromium.launch>>;
  try {
    browser = await chromium.launch({
      ...(configuredExecutable === undefined
        ? {}
        : { executablePath: configuredExecutable }),
      headless: true,
    });
  } catch (error) {
    throw new Error(
      "the dedicated edge runtime requires managed Chromium (`pnpm --filter @sumi/web exec playwright install chromium`) or an exact SUMI_EDGE_CHROME_PATH",
      { cause: error },
    );
  }
  try {
    const page = await browser.newPage({ colorScheme: "light" });
    const cspErrors: string[] = [];
    const themeResponses: number[] = [];
    page.on("console", (message) => {
      if (/content security policy/i.test(message.text())) {
        cspErrors.push(message.text());
      }
    });
    page.on("response", (response) => {
      if (new URL(response.url()).pathname === "/theme-bootstrap.js") {
        themeResponses.push(response.status());
      }
    });
    await page.addInitScript(() => {
      localStorage.setItem("sumi:theme", "dark");
    });
    await page.goto(`${origin}/direct`, {
      waitUntil: "domcontentloaded",
    });
    assert.equal(
      await page.evaluate(() => document.documentElement.dataset.theme),
      "dark",
    );
    assert.equal(
      await page.evaluate(
        () => document.documentElement.dataset.themePreference,
      ),
      "dark",
    );
    assert.equal(
      await page.locator("script:not([src])").count(),
      0,
      "production index contains an inline script",
    );
    assert.deepEqual(themeResponses, [200]);
    assert.deepEqual(cspErrors, []);
    await page.close();
    await verifyClosedTabGenericPush(browser, origin);
  } finally {
    await browser.close();
  }
}

async function verifyClosedTabGenericPush(
  browser: Awaited<ReturnType<typeof chromium.launch>>,
  origin: string,
): Promise<void> {
  const context = await browser.newContext();
  // Chrome exposes the ServiceWorker domain on a page target rather than the
  // browser target. Keep one about:blank control target in the same context;
  // the Sumi-origin page itself is still closed before delivery.
  const controlPage = await context.newPage();
  const cdp = await context.newCDPSession(controlPage);
  const registrations = new Map<
    string,
    { registrationId: string; scopeURL: string; isDeleted: boolean }
  >();
  const consoleMessages: string[] = [];
  context.on("console", (message) => consoleMessages.push(message.text()));
  cdp.on(
    "ServiceWorker.workerRegistrationUpdated",
    ({ registrations: next }) => {
      for (const registration of next) {
        registrations.set(registration.registrationId, registration);
      }
    },
  );
  try {
    await context.grantPermissions(["notifications"], { origin });
    await cdp.send("ServiceWorker.enable");
    const page = await context.newPage();
    await page.goto(`${origin}/direct`, { waitUntil: "domcontentloaded" });
    await page.evaluate(async () => {
      await navigator.serviceWorker.register("/sw.js", {
        scope: "/",
        type: "module",
      });
      await navigator.serviceWorker.ready;
    });
    const registration = await waitForServiceWorkerRegistration(
      registrations,
      `${origin}/`,
    );
    await page.close();
    assert.equal(
      context.pages().filter((candidate) => candidate.url().startsWith(origin))
        .length,
      0,
      "the push must be delivered with no Sumi page open",
    );

    const pointer = {
      workspace_id: "workspace-private-pointer",
      place_id: "place-private-pointer",
      place_kind: "channel",
    };
    const delivery = {
      origin,
      registrationId: registration.registrationId,
      data: JSON.stringify(pointer),
    };
    await cdp.send("ServiceWorker.deliverPushMessage", delivery);
    await cdp.send("ServiceWorker.deliverPushMessage", delivery);

    const inspectionPage = await context.newPage();
    await inspectionPage.goto(`${origin}/direct`, {
      waitUntil: "domcontentloaded",
    });
    const notificationHandle = await inspectionPage.waitForFunction(
      async () => {
        const registration = await navigator.serviceWorker.getRegistration("/");
        if (!registration) return false;
        const notifications = await registration.getNotifications();
        if (notifications.length !== 1) return false;
        const [notification] = notifications;
        return {
          title: notification.title,
          body: notification.body,
          tag: notification.tag,
          data: notification.data as unknown,
        };
      },
    );
    const notification = await notificationHandle.jsonValue();
    await notificationHandle.dispose();
    assert.deepEqual(notification, {
      title: "Sumi",
      body: "新しいメッセージがあります",
      tag: "sumi:workspace-private-pointer:channel:place-private-pointer",
      data: {
        url: "/w/workspace-private-pointer/messaging/c/place-private-pointer",
      },
    });
    const visible = `${notification.title}\n${notification.body}`;
    assert.doesNotMatch(
      visible,
      /workspace-private-pointer|place-private-pointer|participant|attachment/i,
    );
    assert.doesNotMatch(
      consoleMessages.join("\n"),
      /workspace-private-pointer|place-private-pointer/,
      "routing pointers must not reach browser console output",
    );
  } finally {
    await cdp.send("ServiceWorker.disable").catch(() => undefined);
    await cdp.detach().catch(() => undefined);
    await context.close();
  }
}

async function waitForServiceWorkerRegistration(
  registrations: ReadonlyMap<
    string,
    { registrationId: string; scopeURL: string; isDeleted: boolean }
  >,
  scopeURL: string,
): Promise<{ registrationId: string; scopeURL: string; isDeleted: boolean }> {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    for (const registration of registrations.values()) {
      if (registration.scopeURL === scopeURL && !registration.isDeleted) {
        return registration;
      }
    }
    await new Promise((resolveDelay) => setTimeout(resolveDelay, 10));
  }
  throw new Error(`Service Worker registration did not appear for ${scopeURL}`);
}

async function availablePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolveListen, rejectListen) => {
    server.once("error", rejectListen);
    server.listen(0, "127.0.0.1", resolveListen);
  });
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  const port = address.port;
  await new Promise<void>((resolveClose, rejectClose) => {
    server.close((error) => (error ? rejectClose(error) : resolveClose()));
  });
  return port;
}

async function stopWrangler(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.pid === undefined) return;
  const exited = new Promise<void>((resolveExit) =>
    child.once("exit", () => resolveExit()),
  );
  try {
    if (process.platform === "win32") {
      child.kill("SIGTERM");
    } else {
      process.kill(-child.pid, "SIGTERM");
    }
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
  }
  await Promise.race([
    exited,
    new Promise<void>((resolveTimeout) => setTimeout(resolveTimeout, 5_000)),
  ]);
  if (child.exitCode === null) {
    if (process.platform === "win32") child.kill("SIGKILL");
    else process.kill(-child.pid, "SIGKILL");
    await Promise.race([
      exited,
      new Promise<void>((resolveTimeout) => setTimeout(resolveTimeout, 2_000)),
    ]);
  }
}

function captureBounded(stream: NodeJS.ReadableStream | null): void {
  let bytes = 0;
  stream?.on("data", (chunk: Buffer) => {
    bytes += chunk.byteLength;
    if (bytes > 256 * 1024) {
      throw new Error("wrangler emitted more than 256 KiB of runtime logs");
    }
  });
}
