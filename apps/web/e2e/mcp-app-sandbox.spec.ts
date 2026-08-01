import { readFile } from "node:fs/promises";
import { expect, test } from "@playwright/test";

const deploymentId = "7".repeat(64);
const hostOrigin = "https://host.example";
const sandboxOrigin = "https://sandbox.example";
const undeclaredOrigin = "https://undeclared.example";
const resourceOrigin = "https://cdn.example";

test("declared resources load and host-open JSON-RPC remains available", async ({
  page,
}) => {
  let resourceRequests = 0;
  const sandboxArtifact = (
    await readFile("public/mcp-app-sandbox.html", "utf8")
  ).replace("__SUMI_MCP_SANDBOX_DEPLOYMENT__", deploymentId);

  await page.route(`${hostOrigin}/**`, async (route) => {
    await route.fulfill({
      contentType: "text/html",
      body: hostDocument(
        `<!doctype html>
          <script src="${resourceOrigin}/app.js"><${"/script"}>`,
        { resourceDomains: [resourceOrigin] },
      ),
    });
  });
  await page.route(`${sandboxOrigin}/**`, async (route) => {
    await route.fulfill({
      contentType: "text/html",
      headers: {
        "cache-control": "no-store",
        "content-security-policy": sandboxDeploymentPolicy(),
        "referrer-policy": "no-referrer",
        "x-content-type-options": "nosniff",
      },
      body: sandboxArtifact,
    });
  });
  await page.route(`${resourceOrigin}/**`, async (route) => {
    resourceRequests += 1;
    await route.fulfill({
      contentType: "text/javascript",
      body: `
        parent.postMessage(
          { jsonrpc: "2.0", method: "view/resource-loaded", params: {} },
          "*",
        );
        parent.postMessage(
          {
            jsonrpc: "2.0",
            id: 1,
            method: "ui/open-link",
            params: { url: "https://example.com/" },
          },
          "*",
        );
      `,
    });
  });

  await page.goto(`${hostOrigin}/test`);
  await expect
    .poll(() =>
      page.evaluate(() => {
        const methods = (window as typeof window & { forwarded: string[] })
          .forwarded;
        return (
          methods.includes("view/resource-loaded") &&
          methods.includes("ui/open-link")
        );
      }),
    )
    .toBe(true);
  expect(resourceRequests).toBe(1);
});

test("an undeclared View self-navigation is blocked and loses bridge trust", async ({
  page,
}) => {
  let undeclaredRequests = 0;
  const sandboxArtifact = (
    await readFile("public/mcp-app-sandbox.html", "utf8")
  ).replace("__SUMI_MCP_SANDBOX_DEPLOYMENT__", deploymentId);

  await page.route(`${hostOrigin}/**`, async (route) => {
    await route.fulfill({
      contentType: "text/html",
      body: hostDocument(viewDocument("https")),
    });
  });
  await page.route(`${sandboxOrigin}/**`, async (route) => {
    await route.fulfill({
      contentType: "text/html",
      headers: {
        "cache-control": "no-store",
        "content-security-policy": sandboxDeploymentPolicy(),
        "referrer-policy": "no-referrer",
        "x-content-type-options": "nosniff",
      },
      body: sandboxArtifact,
    });
  });
  await page.route(`${undeclaredOrigin}/**`, async (route) => {
    undeclaredRequests += 1;
    await route.fulfill({
      contentType: "text/html",
      body: replacementDocument(),
    });
  });

  await page.goto(`${hostOrigin}/test`);
  await expect
    .poll(() =>
      page.evaluate(() =>
        (window as typeof window & { forwarded: string[] }).forwarded.includes(
          "view/initial",
        ),
      ),
    )
    .toBe(true);
  await page.waitForTimeout(300);

  const forwarded = await page.evaluate(
    () => (window as typeof window & { forwarded: string[] }).forwarded,
  );
  expect(undeclaredRequests).toBe(0);
  expect(forwarded).toContain("view/initial");
  expect(forwarded).not.toContain("view/after-navigation");
  expect(forwarded).not.toContain("view/replacement");
  expect(
    page.frames().some((frame) => frame.url().startsWith(undeclaredOrigin)),
  ).toBe(false);
});

for (const scheme of ["data", "blob", "about"] as const) {
  test(`a ${scheme}: View replacement loses bridge trust`, async ({ page }) => {
    const sandboxArtifact = (
      await readFile("public/mcp-app-sandbox.html", "utf8")
    ).replace("__SUMI_MCP_SANDBOX_DEPLOYMENT__", deploymentId);

    await page.route(`${hostOrigin}/**`, async (route) => {
      await route.fulfill({
        contentType: "text/html",
        body: hostDocument(viewDocument(scheme)),
      });
    });
    await page.route(`${sandboxOrigin}/**`, async (route) => {
      await route.fulfill({
        contentType: "text/html",
        headers: {
          "cache-control": "no-store",
          "content-security-policy": sandboxDeploymentPolicy(),
          "referrer-policy": "no-referrer",
          "x-content-type-options": "nosniff",
        },
        body: sandboxArtifact,
      });
    });

    await page.goto(`${hostOrigin}/test`);
    await expect
      .poll(() =>
        page.evaluate(() =>
          (
            window as typeof window & { forwarded: string[] }
          ).forwarded.includes("view/initial"),
        ),
      )
      .toBe(true);
    await page.waitForTimeout(300);

    const forwarded = await page.evaluate(
      () => (window as typeof window & { forwarded: string[] }).forwarded,
    );
    expect(forwarded).not.toContain("view/after-navigation");
    expect(forwarded).not.toContain("view/replacement");
  });
}

function hostDocument(
  viewHtml: string,
  csp: Record<string, unknown> = {},
): string {
  const resource = JSON.stringify(viewHtml).replaceAll("<", "\\u003c");
  const resourceCsp = JSON.stringify(csp).replaceAll("<", "\\u003c");
  const sandboxUrl = `${sandboxOrigin}/mcp-app-sandbox.html?hostOrigin=${encodeURIComponent(
    hostOrigin,
  )}&deploymentId=${deploymentId}`;
  return `<!doctype html><body>
    <script>
      window.forwarded = [];
      const sandbox = document.createElement("iframe");
      sandbox.sandbox = "allow-scripts allow-same-origin";
      sandbox.credentialless = true;
      sandbox.src = ${JSON.stringify(sandboxUrl)};
      window.addEventListener("message", (event) => {
        if (
          event.source !== sandbox.contentWindow ||
          event.origin !== ${JSON.stringify(sandboxOrigin)}
        ) return;
        const method = event.data?.method;
        if (typeof method === "string") window.forwarded.push(method);
        if (method === "ui/notifications/sandbox-proxy-ready") {
          sandbox.contentWindow.postMessage(
            {
              jsonrpc: "2.0",
              method: "ui/notifications/sandbox-resource-ready",
              params: {
                html: ${resource},
                sandbox: "allow-scripts",
                csp: ${resourceCsp},
              },
            },
            ${JSON.stringify(sandboxOrigin)},
          );
        }
      });
      document.body.append(sandbox);
    <${"/script"}></body>`;
}

function viewDocument(scheme: "https" | "data" | "blob" | "about"): string {
  const replacement = replacementDocument().replaceAll("<", "\\u003c");
  const navigation =
    scheme === "https"
      ? `location.href = "${undeclaredOrigin}/replacement";`
      : scheme === "data"
        ? `location.href = "data:text/html;charset=utf-8," + encodeURIComponent(${JSON.stringify(replacement)});`
        : scheme === "blob"
          ? `location.href = URL.createObjectURL(new Blob([${JSON.stringify(replacement)}], { type: "text/html" }));`
          : 'location.href = "about:blank";';
  return `<!doctype html><script>
    parent.postMessage(
      { jsonrpc: "2.0", method: "view/initial", params: {} },
      "*",
    );
    setTimeout(() => {
      ${navigation}
      parent.postMessage(
        { jsonrpc: "2.0", method: "view/after-navigation", params: {} },
        "*",
      );
    }, 100);
  <${"/script"}>`;
}

function replacementDocument(): string {
  return `<!doctype html><script>
    parent.postMessage(
      { jsonrpc: "2.0", method: "view/replacement", params: {} },
      "*",
    );
  <${"/script"}>`;
}

function sandboxDeploymentPolicy(): string {
  return [
    "default-src * data: blob:",
    "script-src * data: blob: 'unsafe-inline'",
    "style-src * data: blob: 'unsafe-inline'",
    "img-src * data: blob:",
    "font-src * data: blob:",
    "media-src * data: blob:",
    "connect-src https: wss:",
    "frame-src *",
    "worker-src * blob:",
    "object-src 'none'",
    "base-uri *",
    "form-action 'none'",
    `frame-ancestors ${hostOrigin}`,
  ].join("; ");
}
