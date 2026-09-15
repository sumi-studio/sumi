import { type ChildProcess, spawn } from "node:child_process";
import { once } from "node:events";
import { createServer as createHttpServer } from "node:http";
import { createServer as createNetServer } from "node:net";
import { resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { expect, test } from "@playwright/test";

// サンドボックスのHTTPプロキシ環境変数をChromeが拾うとローカル宛ても中継へ
// 回って届かない。観測対象はloopbackだけなのでプロキシは外す。
test.use({
  launchOptions: {
    executablePath: "/usr/bin/google-chrome",
    args: ["--no-proxy-server"],
  },
});

/**
 * 実ブラウザでメッセージ描画の外部フェッチを観測する。
 * 「攻撃者」URLはこの spec が立てるローカルHTTPキャプチャだけを指す。
 * 外部ホストには一切触れない。添付画像の取得だけが許される
 * ネットワークアクセスで、それが正のコントロールになる。
 */
test("message renderers never fetch author-controlled image URLs", async ({
  page,
}) => {
  test.setTimeout(120_000);
  const vitePort = await portInOwnedRange();
  const beaconPort = await portInOwnedRange(new Set([vitePort]));
  const origin = `http://127.0.0.1:${vitePort}`;
  const beaconOrigin = `http://127.0.0.1:${beaconPort}`;

  const hits: string[] = [];
  const beacon = createHttpServer((request, response) => {
    hits.push(request.url ?? "");
    response.setHeader("content-type", "image/png");
    response.end(TINY_PNG);
  });
  beacon.listen(beaconPort, "127.0.0.1");
  await once(beacon, "listening");

  const vite = startVite(vitePort);
  try {
    const harnessURL = `${origin}/harness/message-origins.html?beacon=${encodeURIComponent(beaconOrigin)}`;
    await waitFor(harnessURL);

    const pageRequests: string[] = [];
    page.on("request", (request) => pageRequests.push(request.url()));
    await page.addInitScript(() => {
      const w = window as unknown as Record<string, unknown>;
      w.__openedUrls = [];
      w.__clickedHrefs = [];
      window.open = (url?: unknown) => {
        (w.__openedUrls as string[]).push(String(url));
        return null;
      };
      document.addEventListener(
        "click",
        (event) => {
          const anchor = (event.target as Element | null)?.closest?.("a");
          if (!anchor) return;
          event.preventDefault();
          (w.__clickedHrefs as Array<string | null>).push(
            anchor.getAttribute("href"),
          );
        },
        true,
      );
    });
    await page.goto(harnessURL);
    await page.waitForFunction(() => "__originsReady" in window, undefined, {
      timeout: 90_000,
    });

    // 正のコントロール: 許可された添付URLは本当に<img>をfetchする。
    await page.waitForFunction(() => {
      const image = document.querySelector<HTMLImageElement>("#attachment img");
      return image?.complete === true && image.naturalWidth > 0;
    });
    expect(hits).toEqual(["/attachment.png"]);

    // ストリーミング途中の追記も同じポリシーを通る。
    await page.evaluate(
      (chunk) => window.__streamAppend?.(chunk),
      `\n\n![late](${beaconOrigin}/streamed-image.png)\n\n<script>fetch("${beaconOrigin}/streamed-script")</script>\n\nstream-tail`,
    );
    await page.waitForFunction(() =>
      document
        .querySelector("#agent-streaming")
        ?.textContent?.includes("stream-tail"),
    );
    await delay(500);

    const beaconRequests = pageRequests.filter((url) =>
      url.startsWith(beaconOrigin),
    );
    expect(hits).toEqual(["/attachment.png"]);
    expect(beaconRequests).toEqual([`${beaconOrigin}/attachment.png`]);

    for (const section of [
      "#human-message",
      "#agent-static",
      "#agent-streaming",
    ]) {
      const state = await page.locator(section).evaluate((node) => ({
        images: node.querySelectorAll("img").length,
        scripts: node.querySelectorAll("script").length,
        imageLinks: [...node.querySelectorAll("[data-image-link]")].map(
          (link) => link.getAttribute("href"),
        ),
        javascriptHrefs: [...node.querySelectorAll("a[href]")].filter((link) =>
          link.getAttribute("href")?.includes("javascript:"),
        ).length,
        hasDocsText: node.textContent?.includes("docs") === true,
      }));
      expect(state.images).toBe(0);
      expect(state.scripts).toBe(0);
      expect(state.javascriptHrefs).toBe(0);
      if (section !== "#agent-streaming") {
        expect(state.hasDocsText).toBe(true);
        expect(state.imageLinks).toContain(
          `${beaconOrigin}/markdown-image.png`,
        );
      }
    }
    expect(
      await page
        .locator("#agent-static")
        .evaluate((node) =>
          [...node.querySelectorAll("[data-image-link]")].map((link) =>
            link.getAttribute("href"),
          ),
        ),
    ).toContain(`${beaconOrigin}/raw-html-image.png`);
    expect(
      await page
        .locator("#agent-streaming")
        .evaluate((node) =>
          [...node.querySelectorAll("[data-image-link]")].map((link) =>
            link.getAttribute("href"),
          ),
        ),
    ).toContain(`${beaconOrigin}/streamed-image.png`);

    // 明示リンクは開ける。人間側はtarget=_blankの素のリンク、
    // 秘書側はlinkSafetyモーダル経由でwindow.openへ届く。
    const humanDocs = page.locator(
      '#human-message a[href="https://example.com/docs"]',
    );
    await expect(humanDocs).toHaveAttribute("target", "_blank");
    await expect(humanDocs).toHaveAttribute("rel", "noreferrer noopener");
    await humanDocs.click();
    expect(
      await page.evaluate(
        () =>
          (window as unknown as { __clickedHrefs: string[] }).__clickedHrefs,
      ),
    ).toContain("https://example.com/docs");

    await page
      .locator('#agent-static [data-streamdown="link"]:has-text("docs")')
      .click();
    await page
      .locator('[data-streamdown="link-safety-modal"]')
      .getByRole("button", { name: "Open link" })
      .click();
    expect(
      await page.evaluate(
        () => (window as unknown as { __openedUrls: string[] }).__openedUrls,
      ),
    ).toContain("https://example.com/docs");

    expect(hits).toEqual(["/attachment.png"]);
  } finally {
    await stop(vite);
    beacon.close();
  }
});

const TINY_PNG = Buffer.from(
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==",
  "base64",
);

const PORT_RANGE = { first: 12_460, last: 12_479 } as const;

async function portInOwnedRange(
  exclude?: ReadonlySet<number>,
): Promise<number> {
  for (let port = PORT_RANGE.first; port <= PORT_RANGE.last; port += 1) {
    if (exclude?.has(port)) continue;
    const server = createNetServer();
    try {
      server.listen(port, "127.0.0.1");
      await once(server, "listening");
      return port;
    } catch {
    } finally {
      const closed = once(server, "close").catch(() => {});
      server.close();
      await closed;
    }
  }
  throw new Error(
    `no free port in owned range ${PORT_RANGE.first}-${PORT_RANGE.last}`,
  );
}

function startVite(port: number) {
  return spawn(
    process.execPath,
    [
      resolve("node_modules/vite/bin/vite.js"),
      "--host",
      "127.0.0.1",
      "--port",
      String(port),
      "--strictPort",
    ],
    { cwd: ".", stdio: ["ignore", "pipe", "pipe"] },
  );
}

async function waitFor(url: string) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    try {
      if ((await fetch(url)).ok) return;
    } catch {}
    await delay(100);
  }
  throw new Error(`timed out waiting for ${url}`);
}

async function stop(child: ChildProcess) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const gracefulExit = once(child, "exit").then(() => true);
  child.kill("SIGTERM");
  if (await Promise.race([gracefulExit, delay(5_000).then(() => false)])) {
    return;
  }
  child.kill("SIGKILL");
  await once(child, "exit");
}

declare global {
  interface Window {
    __streamAppend?: (chunk: string) => void;
  }
}
