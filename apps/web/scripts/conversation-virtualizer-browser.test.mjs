// Opt-in native geometry regression: node --test apps/web/scripts/conversation-virtualizer-browser.test.mjs
// Uses existing Chromium and web dependencies; starts only a temporary loopback fixture.
import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { chromium } from "@playwright/test";
import react from "@vitejs/plugin-react";
import { createServer } from "vite";

const webRoot = fileURLToPath(new URL("../", import.meta.url));
const component = path.join(
  webRoot,
  "src/components/conversation-virtualizer.tsx",
);

test("lazy replay and resized rows never overlap; reading position survives later updates", {
  timeout: 60000,
}, async (t) => {
  const directory = await mkdtemp(
    path.join(tmpdir(), "sumi-virtualizer-browser-"),
  );
  let server, browser;
  try {
    await symlink(
      path.join(webRoot, "node_modules"),
      path.join(directory, "node_modules"),
      "dir",
    );
    await writeFile(
      path.join(directory, "index.html"),
      '<html><head><meta name="viewport" content="width=device-width,initial-scale=1"></head><body style="margin:0"><div id="root"></div><script type="module" src="/fixture.tsx"></script></body></html>',
    );
    await writeFile(
      path.join(directory, "fixture.tsx"),
      `
import React, { Suspense, lazy, useState, createRef } from "react";
import { createRoot } from "react-dom/client";
import { ConversationVirtualizer } from ${JSON.stringify(`/@fs${component}`)};
const handle = createRef();
let release;
const waiting = new Promise((resolve) => (release = resolve));
const Row = lazy(async () => {
  await waiting;
  return {
    default: ({ item }) => (
      <div
        style={{
          padding: "12px",
          minHeight: item.height,
          boxSizing: "border-box",
          borderBottom: "1px solid #ccc",
          fontSize: 16,
          lineHeight: "24px",
        }}
      >
        {item.id} {item.text}
      </div>
    ),
  };
});
const initial = Array.from({ length: 40 }, (_, i) => ({
  id: "row-" + i,
  height: 64 + (i % 3) * 24,
  text: "A meaningful recorded observation. ".repeat((i % 4) + 1),
}));
function App() {
  const [items, setItems] = useState([]);
  window.fixture = {
    seed: () => setItems(initial),
    release: () => release(),
    grow: () =>
      setItems((old) =>
        old.map((r, i) =>
          i === old.length - 1
            ? {
                ...r,
                height: r.height + 280,
                text: r.text + " Later result.".repeat(50),
              }
            : r,
        ),
      ),
    append: () =>
      setItems((old) => [
        ...old,
        { id: "row-" + old.length, height: 96, text: "A later event." },
      ]),
    prepend: () => setItems((old) => [
      ...Array.from({length: 100}, (_, i) => ({
        id: "older-" + i, height: 64 + (i % 3) * 24,
        text: "An older observation. ".repeat((i % 4) + 1),
      })),
      ...old,
    ]),
    detail: (id) =>
      setItems((old) =>
        old.map((r, i) => (r.id === id ? { ...r, height: r.height + 160 } : r)),
      ),
    end: () => handle.current.scrollToEnd({ behavior: "auto" }),
    offset: () => handle.current.getScrollOffset(),
  };
  return (
    <div style={{ height: "100dvh" }}>
      <ConversationVirtualizer
        ref={handle}
        items={items}
        renderItem={(item) => (
          <Suspense fallback={null}>
            <Row item={item} />
          </Suspense>
        )}
        estimateSize={() => 96}
      />
    </div>
  );
}
createRoot(document.getElementById("root")).render(<App />);

`,
    );
    server = await createServer({
      configFile: false,
      root: directory,
      cacheDir: path.join(directory, "cache"),
      plugins: [react()],
      server: {
        host: "127.0.0.1",
        port: 0,
        fs: { allow: [directory, path.resolve(webRoot, "../..")] },
      },
    });
    await server.listen();
    const origin = server.resolvedUrls.local[0];
    browser = await chromium.launch({
      headless: true,
      ...(existsSync("/usr/bin/google-chrome")
        ? { executablePath: "/usr/bin/google-chrome" }
        : {}),
    });
    for (const width of [1280, 390]) {
      const context = await browser.newContext({
        viewport: { width, height: 640 },
      });
      const page = await context.newPage();
      await page.route("**/*", (route) =>
        new URL(route.request().url()).origin === new URL(origin).origin
          ? route.continue()
          : route.abort(),
      );
      await page.addInitScript(() => {
        window.overlaps = [];
        const sample = () => {
          const rows = [...document.querySelectorAll("[data-message-id]")].map(
            (e) => ({
              id: e.dataset.messageId,
              top: e.getBoundingClientRect().top,
              height: e.getBoundingClientRect().height,
            }),
          );
          for (let i = 1; i < rows.length; i++) {
            if (
              rows[i - 1].height > 0 &&
              rows[i].height > 0 &&
              rows[i].top < rows[i - 1].top + rows[i - 1].height - 1
            ) {
              window.overlaps.push(rows);
              break;
            }
          }
          requestAnimationFrame(sample);
        };
        requestAnimationFrame(sample);
      });
      await page.goto(origin, { waitUntil: "networkidle" });
      await page.evaluate(() => window.fixture.seed());
      await page.waitForTimeout(100);
      await page.evaluate(() => window.fixture.release());
      await page.waitForTimeout(500);
      assert.equal(
        await page.evaluate(() => window.overlaps.length),
        0,
        "lazy hydration must not stack zero-measured rows",
      );
      const viewport = page.locator('[data-slot="conversation-viewport"]');
      const initialPosition = await viewport.evaluate((e) => ({
        gap: e.scrollHeight - e.clientHeight - e.scrollTop,
        offset: e.scrollTop,
      }));
      assert.ok(
        initialPosition.offset > 0 && initialPosition.gap < 3,
        JSON.stringify({ width, initialPosition }),
      );
      await page.evaluate(() => window.fixture.end());
      await page.waitForTimeout(1800);
      await viewport.hover();
      await page.mouse.wheel(0, -950);
      await page.waitForTimeout(400);
      const anchor = await page.evaluate(() => {
        const row = [...document.querySelectorAll("[data-message-id]")].find(
          (e) => e.getBoundingClientRect().top >= 0,
        );
        return {
          id: row.dataset.messageId,
          top: row.getBoundingClientRect().top,
          offset: window.fixture.offset(),
        };
      });
      await page.evaluate(() => {
        window.fixture.grow();
        window.fixture.append();
      });
      await page.waitForTimeout(1800);
      const after = await page.evaluate((id) => {
        const row = document.querySelector(`[data-message-id="${id}"]`);
        return row
          ? {
              top: row.getBoundingClientRect().top,
              offset: window.fixture.offset(),
            }
          : null;
      }, anchor.id);
      assert.ok(after, "reading anchor remains mounted");
      assert.ok(
        Math.abs(after.top - anchor.top) < 3,
        JSON.stringify({ width, anchor, after }),
      );
      await page.evaluate(() => window.fixture.prepend());
      await page.waitForTimeout(600);
      const prependedTop = await page
        .locator(`[data-message-id="${anchor.id}"]`)
        .evaluate((e) => e.getBoundingClientRect().top);
      assert.ok(
        Math.abs(prependedTop - anchor.top) < 3,
        JSON.stringify({ width, anchor, prependedTop }),
      );
      await page.evaluate((id) => window.fixture.detail(id), anchor.id);
      await page.waitForTimeout(500);
      const expandedTop = await page
        .locator(`[data-message-id="${anchor.id}"]`)
        .evaluate((e) => e.getBoundingClientRect().top);
      assert.ok(
        Math.abs(expandedTop - anchor.top) < 3,
        JSON.stringify({ width, anchor, expandedTop }),
      );
      await page.setViewportSize({
        width: width === 1280 ? 720 : 340,
        height: 600,
      });
      await page.waitForTimeout(500);
      assert.equal(
        await page.evaluate(() => window.overlaps.length),
        0,
        "viewport width changes must not overlap adjacent rows",
      );
      await page.evaluate(() => window.fixture.end());
      await page.waitForTimeout(1800);
      await page.evaluate(() => window.fixture.grow());
      await page.waitForTimeout(1800);
      const end = await viewport.evaluate((e) => ({
        gap: e.scrollHeight - e.clientHeight - e.scrollTop,
        offset: e.scrollTop,
      }));
      assert.ok(end.gap < 3, JSON.stringify({ width, end }));
      t.diagnostic(
        JSON.stringify({
          width,
          initialPosition,
          anchor,
          after,
          expandedTop,
          end,
          overlapFrames: await page.evaluate(() => window.overlaps.length),
        }),
      );
      await context.close();
    }
  } finally {
    await browser?.close();
    await server?.close();
    await rm(directory, { recursive: true, force: true });
  }
});
