import { type ChildProcess, spawn } from "node:child_process";
import { once } from "node:events";
import { resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { expect, test } from "@playwright/test";

/**
 * Streaming-performance measurement (not a CI gate — run manually):
 * drives the real ChatScreen + conversation store through a synthetic
 * lifetime history and a live text_delta burst, then reports per-delta
 * processing, React commit work, rAF responsiveness, and composer typing
 * latency for short and long histories.
 *
 *   pnpm --filter @sumi/web exec playwright test e2e/conversation-streaming.spec.ts
 */

const PORT = 10810;

interface StreamStats {
  entries: number;
  runs: number;
}

interface PerfReport {
  deltaEmitMs: number[];
  commits: { actualDuration: number; phase: string }[];
  rafDelays: number[];
  typeLatencies: number[];
}

const scenarios = [
  { name: "short", turns: 24 },
  { name: "long", turns: 1600 },
];

for (const scenario of scenarios) {
  test(`conversation streaming stays responsive — ${scenario.name} history`, async ({
    page,
  }) => {
    test.setTimeout(240_000);
    const vite = startVite(PORT);
    const url = `http://127.0.0.1:${PORT}/harness/conversation-streaming.html?turns=${scenario.turns}`;
    try {
      await waitFor(url);
      const loadStart = Date.now();
      await page.goto(url);
      await page.waitForFunction(() => "__stream" in window);
      await expect
        .poll(
          async () =>
            page.evaluate(
              () =>
                (
                  window as unknown as { __stream: { stats(): StreamStats } }
                ).__stream.stats().entries,
            ),
          { timeout: 120_000 },
        )
        .toBeGreaterThanOrEqual(scenario.turns * 4);
      console.log(
        `[${scenario.name}] history load wall: ${Date.now() - loadStart}ms`,
      );
      const composer = page.getByRole("textbox", { name: "メッセージ" });
      await composer.waitFor({ state: "visible", timeout: 60_000 });
      const stats = await page.evaluate(() =>
        (
          window as unknown as { __stream: { stats(): StreamStats } }
        ).__stream.stats(),
      );
      console.log(`[${scenario.name}] loaded stats:`, JSON.stringify(stats));

      await page.evaluate(() => {
        (
          window as unknown as { __stream: { resetPerf(): void } }
        ).__stream.resetPerf();
        (
          window as unknown as { __stream: { beginReply(): void } }
        ).__stream.beginReply();
      });
      await delay(50);
      await page.evaluate(() => {
        (
          window as unknown as { __stream: { resetPerf(): void } }
        ).__stream.resetPerf();
      });

      const report: PerfReport = {
        deltaEmitMs: [],
        commits: [],
        rafDelays: [],
        typeLatencies: [],
      };

      const composerTyped = "いま";
      await composer.click();

      const deltaCount = 240;
      for (let index = 0; index < deltaCount; index += 1) {
        const emitMs = await page.evaluate((i) => {
          return (
            window as unknown as {
              __stream: { delta(chunk: string): number };
            }
          ).__stream.delta(`delta ${i} `);
        }, index);
        report.deltaEmitMs.push(emitMs);
        if (index % 24 === 23) {
          report.rafDelays.push(
            await page.evaluate(() =>
              (
                window as unknown as {
                  __stream: { rafDelay(): Promise<number> };
                }
              ).__stream.rafDelay(),
            ),
          );
        }
        // Type a few characters mid-stream and measure press→commit latency.
        if (index === Math.floor(deltaCount / 2)) {
          let expected = "";
          for (const char of composerTyped) {
            expected += char;
            const mark = await page.evaluate(() => performance.now());
            await page.keyboard.type(char, { delay: 0 });
            await expect.poll(() => composer.inputValue()).toBe(expected);
            report.typeLatencies.push(
              (await page.evaluate(() => performance.now())) - mark,
            );
          }
        }
      }

      await page.evaluate(() => {
        (
          window as unknown as { __stream: { endReply(): void } }
        ).__stream.endReply();
      });
      await delay(300);

      const perf = await page.evaluate(
        () =>
          (
            window as unknown as {
              __perf: {
                commits: { actualDuration: number; phase: string }[];
                frames: { emitMs: number }[];
              };
            }
          ).__perf,
      );
      report.commits = perf.commits.map((commit) => ({
        actualDuration: commit.actualDuration,
        phase: commit.phase,
      }));

      const summarize = (values: number[]) => {
        const sorted = [...values].sort((a, b) => a - b);
        const pick = (p: number) =>
          sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))];
        return {
          n: sorted.length,
          median: pick(0.5),
          p90: pick(0.9),
          max: sorted.at(-1),
          total: sorted.reduce((a, b) => a + b, 0),
        };
      };
      const summary = {
        scenario: scenario.name,
        turns: scenario.turns,
        entries: stats.entries,
        deltas: summarize(report.deltaEmitMs),
        commitCount: report.commits.length,
        commitActualMs: report.commits.reduce(
          (sum, commit) => sum + commit.actualDuration,
          0,
        ),
        rafDelay: summarize(report.rafDelays),
        typeLatency: summarize(report.typeLatencies),
      };
      console.log(`PERF ${JSON.stringify(summary)}`);

      // Functional guardrail: the full streamed text must be rendered once.
      const expected = Array.from(
        { length: deltaCount },
        (_, i) => `delta ${i} `,
      ).join("");
      await expect(page.getByText(expected.slice(-80))).toBeVisible();
    } finally {
      await stop(vite);
    }
  });
}

// Regression: resetAuthority (session revalidation/authority rebind) replaces
// the session wholesale while ChatScreen stays mounted. The mounted projector
// must rescan the fresh model, not keep the previous transcript.
test("mounted chat screen clears on authority reset and renders the new transcript", async ({
  page,
}) => {
  test.setTimeout(120_000);
  const vite = startVite(PORT);
  const url = `http://127.0.0.1:${PORT}/harness/conversation-streaming.html?turns=24`;
  try {
    await waitFor(url);
    await page.goto(url);
    await page.waitForFunction(() => "__stream" in window);
    const composer = page.getByRole("textbox", { name: "メッセージ" });
    await composer.waitFor({ state: "visible", timeout: 60_000 });
    // The virtualized view anchors at the tail; the last turn's rows are mounted.
    await expect(
      page.getByText("記録の質問 23", { exact: false }).first(),
    ).toBeVisible({ timeout: 60_000 });

    const cleared = await page.evaluate(() =>
      (
        window as unknown as { __stream: { resetAuthority(): boolean } }
      ).__stream.resetAuthority(),
    );
    expect(cleared).toBe(true);

    // The previous authority's transcript is gone; the empty state returns.
    await expect(page.getByText("記録の質問", { exact: false })).toHaveCount(0);
    await expect(page.getByText("Sumiの活動記録")).toBeVisible();

    // New authority events render without appending under stale rows.
    await page.evaluate(() => {
      const stream = (
        window as unknown as {
          __stream: {
            beginReply(): void;
            delta(chunk: string): number;
            endReply(): void;
          };
        }
      ).__stream;
      stream.beginReply();
      stream.delta("新しい権限の応答");
      stream.endReply();
    });
    await expect(
      page.getByText("新しい権限の応答", { exact: false }),
    ).toBeVisible();
    await expect(page.getByText("記録の質問", { exact: false })).toHaveCount(0);
  } finally {
    await stop(vite);
  }
});

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
  for (let attempt = 0; attempt < 150; attempt += 1) {
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
