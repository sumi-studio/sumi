import { type ChildProcess, spawn } from "node:child_process";
import { once } from "node:events";
import { createServer as createNetServer } from "node:net";
import { resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { expect, test } from "@playwright/test";

interface HarnessSnapshot {
  scrollTop: number;
  scrollHeight: number;
  clientHeight: number;
  distanceFromEnd: number;
  atEnd: boolean;
  firstVisibleId: string | null;
}

test("conversation stays anchored to the end while rows land after send", async ({
  page,
}) => {
  const port = await ephemeralPort();
  const harnessURL = `http://127.0.0.1:${port}/harness/scroll-jump.html`;
  const vite = startVite(port);
  try {
    await waitFor(harnessURL);
    await page.goto(harnessURL);
    await page.waitForFunction(() => "__harness" in window);

    // Settle at the end first (the user is reading the latest message).
    await expect
      .poll(async () => {
        await page.evaluate(() => {
          window.__harness.scrollToEnd("auto");
        });
        await delay(120);
        return (await snapshot(page)).distanceFromEnd;
      })
      .toBeLessThanOrEqual(2);
    await page.evaluate(() => {
      (window as unknown as { __scrollLog: unknown[] }).__scrollLog.length = 0;
    });

    // 1. User sends a message: optimistic row + smooth scrollToEnd.
    await page.evaluate(() => {
      window.__harness.append({ id: "user-pending", h: 90 });
      window.__harness.scrollToEnd("smooth");
    });

    // 2. The waiting indicator appears while the run spins up.
    await delay(50);
    await page.evaluate(() => {
      window.__harness.append({ id: "waiting", h: 40 });
    });

    // 3. Canonical log lands: pending row shrinks, waiting row is replaced
    //    by the run row and an approval card.
    await delay(250);
    await page.evaluate(() => {
      window.__harness.remove("waiting");
      window.__harness.resize("user-pending", 70);
      window.__harness.append({ id: "run", h: 30 });
      window.__harness.append({ id: "approval", h: 170 });
    });

    // 4. The run trace streams in and keeps growing the run row.
    for (const height of [80, 160, 260, 380, 520]) {
      await delay(100);
      await page.evaluate((h) => {
        window.__harness.resize("run", h);
      }, height);
    }

    await delay(800);
    const final = await snapshot(page);
    const log = await page.evaluate(
      () => (window as unknown as { __scrollLog: unknown[] }).__scrollLog,
    );
    console.log("final snapshot:", JSON.stringify(final, null, 2));
    console.log("scroll log:", JSON.stringify(log, null, 2));

    expect(final.distanceFromEnd).toBeLessThanOrEqual(80);
  } finally {
    await stop(vite);
  }
});

test("a user wheel gesture wins over streaming follow and is never yanked", async ({
  page,
}) => {
  const port = await ephemeralPort();
  const harnessURL = `http://127.0.0.1:${port}/harness/scroll-jump.html`;
  const vite = startVite(port);
  try {
    await waitFor(harnessURL);
    await page.goto(harnessURL);
    await page.waitForFunction(() => "__harness" in window);
    await expect
      .poll(async () => {
        await page.evaluate(() => {
          window.__harness.scrollToEnd("auto");
        });
        await delay(120);
        return (await snapshot(page)).distanceFromEnd;
      })
      .toBeLessThanOrEqual(2);

    // Streaming growth begins at the end of the conversation.
    await page.evaluate(() => {
      window.__harness.append({ id: "run", h: 40 });
    });
    const grow = (async () => {
      for (let height = 120; height <= 760; height += 80) {
        await delay(90);
        await page.evaluate((h) => {
          window.__harness.resize("run", h);
        }, height);
      }
    })();

    // The user grabs the wheel and scrolls up mid-stream.
    await delay(200);
    const viewport = page.locator('[data-slot="conversation-viewport"]');
    await viewport.hover();
    for (let step = 0; step < 6; step += 1) {
      await page.mouse.wheel(0, -300);
      await delay(40);
    }
    const afterWheel = await snapshot(page);
    expect(afterWheel.distanceFromEnd).toBeGreaterThan(200);

    await grow;
    await delay(500);

    // Reading position holds: no follow re-engagement, no teleport.
    const final = await snapshot(page);
    expect(final.distanceFromEnd).toBeGreaterThan(200);
    expect(Math.abs(final.scrollTop - afterWheel.scrollTop)).toBeLessThan(150);
  } finally {
    await stop(vite);
  }
});

declare global {
  interface Window {
    __harness: {
      append(row: { id: string; h: number }): void;
      remove(id: string): void;
      resize(id: string, h: number): void;
      scrollToEnd(behavior?: "smooth" | "auto"): void;
      isAtEnd(): boolean;
      snapshot(): HarnessSnapshot | null;
    };
  }
}

async function snapshot(page: {
  evaluate: <T>(fn: () => T) => Promise<T>;
}): Promise<HarnessSnapshot> {
  const value = await page.evaluate(() => window.__harness.snapshot());
  if (!value) throw new Error("harness viewport missing");
  return value;
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
    {
      cwd: ".",
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
}

async function ephemeralPort(): Promise<number> {
  const server = createNetServer();
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  if (!address || typeof address === "string") {
    server.close();
    throw new Error("ephemeral port reservation did not expose a TCP address");
  }
  const port = address.port;
  const closed = once(server, "close");
  server.close();
  await closed;
  return port;
}

async function waitFor(url: string) {
  for (let attempt = 0; attempt < 100; attempt++) {
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
