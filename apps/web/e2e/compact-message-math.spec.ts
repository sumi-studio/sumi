import { type ChildProcess, spawn } from "node:child_process";
import { once } from "node:events";
import { createServer as createNetServer } from "node:net";
import { resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { expect, test } from "@playwright/test";

test("math stays locally scrollable and copies one TeX source", async ({
  context,
  page,
}) => {
  const port = await ephemeralPort();
  const origin = `http://127.0.0.1:${port}`;
  const harnessURL = `${origin}/harness/compact-message-math.html`;
  const vite = startVite(port);
  try {
    await waitFor(harnessURL);
    await context.grantPermissions(["clipboard-read", "clipboard-write"], {
      origin,
    });
    await page.goto(harnessURL);
    await page.waitForFunction(() => "__compactMathReady" in window);

    const narrow = await page.locator("#narrow-message").evaluate((message) => {
      const math = message.querySelector<HTMLElement>("[data-math-inline]");
      if (!math) throw new Error("inline math wrapper missing");
      const style = getComputedStyle(math);
      return {
        messageWidth: message.clientWidth,
        clientWidth: math.clientWidth,
        scrollWidth: math.scrollWidth,
        overflowX: style.overflowX,
        display: style.display,
        verticalAlign: style.verticalAlign,
      };
    });
    expect(narrow.clientWidth).toBeLessThanOrEqual(narrow.messageWidth);
    expect(narrow.scrollWidth).toBeGreaterThan(narrow.clientWidth);
    expect(narrow.overflowX).toBe("auto");
    expect(narrow.display).toBe("inline-block");
    expect(narrow.verticalAlign).toBe("baseline");

    await copyFormula(page, "#copy-energy [data-math-inline]");
    await expect
      .poll(() => page.evaluate(() => navigator.clipboard.readText()))
      .toBe("E=mc^2");

    await copyFormula(page, "#copy-fraction [data-math-display]");
    await expect
      .poll(() => page.evaluate(() => navigator.clipboard.readText()))
      .toBe(String.raw`\frac{1}{2}`);

    await copyFormula(page, "#copy-escaped-tex [data-math-inline]");
    await expect
      .poll(() => page.evaluate(() => navigator.clipboard.readText()))
      .toBe(String.raw`a\_b`);

    const adjacentCurrency = await page
      .locator("#currency-adjacent")
      .evaluate((message) => ({
        text: message.textContent ?? "",
        sources: [
          ...message.querySelectorAll(
            'annotation[encoding="application/x-tex"]',
          ),
        ].map((annotation) => annotation.textContent),
      }));
    expect(adjacentCurrency.text).toContain("Price $5, formula:");
    expect(adjacentCurrency.sources).toEqual(["x"]);

    const japaneseCurrency = await mathSources(page, "#currency-japanese");
    expect(japaneseCurrency.text).toContain("価格は$5/個、式は");
    expect(japaneseCurrency.sources).toEqual(["x"]);

    const numericFormula = await mathSources(page, "#numeric-formula-adjacent");
    expect(numericFormula.sources).toEqual(["5 + x", "y"]);

    const macroSource = `\\def\\boom{${"x".repeat(200)}}${"\\boom".repeat(200)}`;
    const macro = await page.locator("#macro-expansion").evaluate((message) => {
      const fallback = message.querySelector<HTMLElement>(
        "[data-math-fallback=macro]",
      );
      return {
        descendants: message.querySelectorAll("*").length,
        fallback: fallback?.textContent ?? null,
        katex: message.querySelector(".katex") !== null,
      };
    });
    expect(macro).toEqual({
      descendants: 4,
      fallback: macroSource,
      katex: false,
    });

    const aggregate = await page
      .locator("#aggregate-math")
      .evaluate((message) => ({
        descendants: message.querySelectorAll("*").length,
        firstFallback:
          message.querySelector<HTMLElement>("[data-math-fallback=aggregate]")
            ?.textContent ?? null,
        fallbacks: message.querySelectorAll("[data-math-fallback=aggregate]")
          .length,
        formulae: message.querySelectorAll(".katex").length,
      }));
    expect(aggregate.formulae).toBe(0);
    expect(aggregate.fallbacks).toBe(4_000);
    expect(aggregate.firstFallback).toBe(String.raw`\frac{1}{2}`);
    expect(aggregate.descendants).toBeLessThan(20_000);

    await copyFormula(page, "#aggregate-math [data-math-fallback=aggregate]");
    await expect
      .poll(() => page.evaluate(() => navigator.clipboard.readText()))
      .toBe(String.raw`\frac{1}{2}`);

    await page.reload();
    await page.waitForFunction(() => "__compactMathReady" in window);
    const aggregateAfterReload = await page
      .locator("#aggregate-math")
      .evaluate((message) => ({
        fallbacks: message.querySelectorAll("[data-math-fallback=aggregate]")
          .length,
        formulae: message.querySelectorAll(".katex").length,
      }));
    expect(aggregateAfterReload).toEqual({ fallbacks: 4_000, formulae: 0 });

    const mixed = String.raw`E=mc^2 tail
next line
Second \frac{1}{2}`;
    await copyMixedFormulae(page, false);
    await expect
      .poll(() => page.evaluate(() => navigator.clipboard.readText()))
      .toBe(mixed);
    await copyMixedFormulae(page, true);
    await expect
      .poll(() => page.evaluate(() => navigator.clipboard.readText()))
      .toBe(mixed);
  } finally {
    await stop(vite);
  }
});

async function mathSources(
  page: {
    locator(selector: string): {
      evaluate<T>(fn: (node: HTMLElement) => T): Promise<T>;
    };
  },
  selector: string,
) {
  return page.locator(selector).evaluate((message) => ({
    text: message.textContent ?? "",
    sources: [
      ...message.querySelectorAll('annotation[encoding="application/x-tex"]'),
    ].map((annotation) => annotation.textContent),
  }));
}

async function copyFormula(
  page: {
    locator(selector: string): {
      evaluate(fn: (node: HTMLElement) => boolean): Promise<boolean>;
    };
  },
  selector: string,
) {
  const copied = await page
    .locator(selector)
    .first()
    .evaluate((node) => {
      const selection = window.getSelection();
      const range = document.createRange();
      if (!selection) return false;
      range.selectNodeContents(node);
      selection.removeAllRanges();
      selection.addRange(range);
      return document.execCommand("copy");
    });
  expect(copied).toBe(true);
}

async function copyMixedFormulae(
  page: {
    locator(selector: string): {
      evaluate(
        fn: (node: HTMLElement, backwards: boolean) => boolean,
        arg: boolean,
      ): Promise<boolean>;
    };
  },
  backwards: boolean,
) {
  const copied = await page.locator("#copy-mixed").evaluate((node, reverse) => {
    const visuals = node.querySelectorAll(".katex-html");
    const selection = window.getSelection();
    const textNode = (element: Element): Text | null => {
      const walker = document.createTreeWalker(element, NodeFilter.SHOW_TEXT);
      let current = walker.nextNode();
      while (current) {
        if (current.textContent) return current as Text;
        current = walker.nextNode();
      }
      return null;
    };
    const first = visuals[0] ? textNode(visuals[0]) : null;
    const second = visuals[1] ? textNode(visuals[1]) : null;
    if (!selection || !first || !second) return false;
    if (reverse) {
      selection.setBaseAndExtent(second, second.length, first, 0);
    } else {
      selection.setBaseAndExtent(first, 0, second, second.length);
    }
    return document.execCommand("copy");
  }, backwards);
  expect(copied).toBe(true);
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
