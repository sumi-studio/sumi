import { type ChildProcess, spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { expect, test } from "@playwright/test";
import { parseDirectChatServerFrame } from "../src/lib/direct-chat-socket";

const webURL = "http://127.0.0.1:4173";
const directChatURL = `${webURL}/direct`;
let fixtureBuild: { directory: string; binary: string } | undefined;

test.beforeAll(async () => {
  fixtureBuild = await buildFixture();
});

test.afterAll(async () => {
  if (fixtureBuild) {
    await rm(fixtureBuild.directory, { recursive: true, force: true });
  }
});

test("real Chrome chat journey uses the browser websocket boundary", async ({
  page,
}) => {
  if (!fixtureBuild) throw new Error("browser E2E fixture was not built");

  const invalidFrames: string[] = [];
  let terminalFrames = 0;
  let toolStartFrames = 0;
  let toolEndFrames = 0;
  let directChatSocketSeen = false;
  let markReplaySettled: (() => void) | undefined;
  const replaySettled = new Promise<void>((resolveReplay) => {
    markReplaySettled = resolveReplay;
  });
  page.on("websocket", (socket) => {
    if (new URL(socket.url()).pathname !== "/direct-chat/ws") return;
    directChatSocketSeen = true;
    socket.on("framereceived", ({ payload }) => {
      if (typeof payload !== "string") return;
      try {
        const frame = JSON.parse(payload) as {
          type?: string;
          envelope?: { event?: { type?: string; message?: unknown } };
        };
        // Validate the actual public payload shape; the socket itself checks
        // ordered sequence continuity across reconnects.
        const sequence = (frame as { envelope?: { seq?: number } }).envelope
          ?.seq;
        if (
          !parseDirectChatServerFrame(
            frame,
            sequence === undefined ? 0 : sequence - 1,
          )
        ) {
          invalidFrames.push(payload);
        }
        const event =
          frame.type === "event" ? frame.envelope?.event : undefined;
        if (
          event?.type === "message_end" &&
          JSON.stringify(event.message).includes("Terminal replay")
        ) {
          terminalFrames++;
        }
        if (event?.type === "tool_execution_start") {
          toolStartFrames++;
        }
        if (event?.type === "tool_execution_end") {
          toolEndFrames++;
        }
        if (event?.type === "agent_end") {
          markReplaySettled?.();
        }
      } catch {
        // Production browser code owns malformed-frame handling. This listener
        // observes only the ordered replay barrier used by this test.
      }
    });
  });

  const fixture = await startFixture(
    fixtureBuild.binary,
    fixtureBuild.directory,
  );
  const vite = startVite(fixture.url);
  try {
    await waitFor(`${webURL}/`);
    expect(
      (await page.request.get(`${fixture.url}/__e2e__/session`)).status(),
    ).toBe(204);
    await page.goto(directChatURL);
    await expect(
      page.getByText("エージェント利用可能", { exact: true }),
    ).toBeVisible();
    await expect.poll(() => directChatSocketSeen).toBe(true);

    const composer = page.getByRole("textbox", {
      name: "メッセージ",
      exact: true,
    });
    await composer.fill("initial user_message");
    await page.getByRole("button", { name: "送信" }).click();
    await expect(
      page.getByText("streamed assistant", { exact: true }),
    ).toBeVisible();
    // The completed operation stays in the conversation as one disclosure.
    const toolSummary = page.getByRole("button", { name: /read_file.*完了/ });
    await expect(toolSummary).toHaveCount(1);
    await expect(toolSummary).toBeVisible();
    await toolSummary.focus();
    await toolSummary.press("Enter");
    await expect(toolSummary).toHaveAttribute("aria-expanded", "true");
    const toolPanel = page.locator(".direct-chat-tool-panel");
    await expect(toolPanel.getByText("ok", { exact: true })).toBeVisible();
    await page.keyboard.press("Tab");
    await expect(toolSummary).not.toBeFocused();
    expect(toolStartFrames).toBe(1);
    expect(toolEndFrames).toBe(1);

    await composer.fill("second message is a steer");
    await page.getByRole("button", { name: "割り込んで送信" }).click();
    await expect(
      page.getByText("応答へ追加の指示を送りました (hard)", { exact: true }),
    ).toBeVisible();
    const approveOnce = page.getByRole("button", {
      name: "今回のみ許可",
    });
    await expect(approveOnce).toBeVisible();
    await approveOnce.click();
    await expect(
      page.getByText("abortable stream", { exact: true }),
    ).toBeVisible();

    const beforeReconnect = await connectionStats(fixture.url);
    expect(beforeReconnect.active).toBe(1);
    await page.getByRole("button", { name: "停止" }).click();
    const disconnect = await fetch(
      `${fixture.url}/__e2e__/disconnect-and-emit-terminal`,
      { method: "POST" },
    );
    expect(disconnect.status).toBe(204);

    // agent_end is appended after the terminal message while no browser
    // connection exists. Seeing that barrier proves the reconnect replay has
    // delivered every earlier frame before duplicate checks run.
    await replaySettled;
    await expect
      .poll(async () => {
        const stats = await connectionStats(fixture.url);
        return stats.active === 1 && stats.accepted > beforeReconnect.accepted;
      })
      .toBe(true);
    await expect(
      page.getByText("エージェント利用可能", { exact: true }),
    ).toBeVisible();
    await expect(
      page.getByText("Terminal replay", { exact: true }),
    ).toHaveCount(1);
    expect(terminalFrames).toBe(1);

    const completedSummary = page.getByText("作業が終了しました", {
      exact: true,
    });
    await expect(completedSummary).toHaveCount(0);
    await expect(toolSummary).toBeVisible();

    const terminalRow = page
      .getByText("Terminal replay", { exact: true })
      .locator("xpath=ancestor::*[@data-message-id][1]");
    await terminalRow.hover();
    await expect(
      terminalRow.getByRole("button", { name: "コピー" }),
    ).toBeVisible();

    const beforeReload = await connectionStats(fixture.url);
    await page.reload();
    await expect(
      page.getByText("エージェント利用可能", { exact: true }),
    ).toBeVisible();
    await expect
      .poll(async () => {
        const stats = await connectionStats(fixture.url);
        return stats.active === 1 && stats.accepted > beforeReload.accepted;
      })
      .toBe(true);
    await expect(
      page.getByText("Terminal replay", { exact: true }),
    ).toHaveCount(1);
    await expect(completedSummary).toHaveCount(0);
    await expect(toolSummary).toBeVisible();
    await toolSummary.focus();
    await toolSummary.press("Space");
    await expect(toolSummary).toHaveAttribute("aria-expanded", "true");
    await expect(toolPanel.getByText("ok", { exact: true })).toBeVisible();
    const panelHandle = await toolPanel.elementHandle();
    await toolSummary.press("Space");
    await expect(toolSummary).toHaveAttribute("aria-expanded", "false");
    await expect(toolPanel).toHaveAttribute("aria-hidden", "true");
    await toolSummary.press("Space");
    await expect(toolSummary).toHaveAttribute("aria-expanded", "true");
    expect(
      await toolPanel.evaluate(
        (panel, original) => panel === original,
        panelHandle,
      ),
    ).toBe(true);
    await panelHandle?.dispose();
    expect(terminalFrames).toBe(2);
    expect(invalidFrames).toEqual([]);
    expect(fixture.stderr).toEqual([]);
  } finally {
    await Promise.all([stop(vite), stop(fixture.process)]);
    await test.info().attach("invalid-websocket-frames", {
      body: JSON.stringify(invalidFrames, null, 2),
      contentType: "application/json",
    });
    await test.info().attach("fixture-stderr", {
      body: fixture.stderr.join(""),
      contentType: "text/plain",
    });
    await rm(fixture.runtimeDirectory, { recursive: true, force: true });
  }
});

async function buildFixture() {
  const directory = await mkdtemp(join(tmpdir(), "sumi-browser-e2e-build-"));
  const binary = join(directory, "browser-e2e-fixture");
  const build = spawn(
    "go",
    ["build", "-o", binary, "./cmd/browser-e2e-fixture"],
    {
      cwd: resolve("../api"),
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  const stderr: Buffer[] = [];
  build.stderr?.on("data", (chunk: Buffer) => stderr.push(chunk));
  try {
    const [code, signal] = (await once(build, "exit")) as [
      number | null,
      NodeJS.Signals | null,
    ];
    if (code !== 0) {
      throw new Error(
        `fixture build failed (${code ?? signal}): ${Buffer.concat(stderr)}`,
      );
    }
    return { directory, binary };
  } catch (error) {
    await rm(directory, { recursive: true, force: true });
    throw error;
  }
}

async function startFixture(binary: string, buildDirectory: string) {
  const runtimeDirectory = await mkdtemp(join(buildDirectory, "runtime-"));
  const child = spawn(binary, [], {
    cwd: resolve("../api"),
    env: {
      ...process.env,
      SUMI_E2E_RUNTIME_DIR: runtimeDirectory,
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const stderr: string[] = [];
  child.stderr?.on("data", (chunk: Buffer) => stderr.push(chunk.toString()));
  try {
    const url = await new Promise<string>((resolveURL, reject) => {
      const timeout = setTimeout(
        () => reject(new Error("fixture did not start")),
        15_000,
      );
      const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
        clearTimeout(timeout);
        reject(new Error(`fixture exited early: ${code ?? signal}`));
      };
      child.once("exit", onExit);
      child.stdout?.on("data", (chunk: Buffer) => {
        const match = chunk.toString().match(/E2E_FIXTURE=(http:\/\/[^\s]+)/);
        if (!match) return;
        clearTimeout(timeout);
        child.off("exit", onExit);
        resolveURL(match[1]);
      });
    });
    return { process: child, runtimeDirectory, url, stderr };
  } catch (error) {
    await stop(child);
    await rm(runtimeDirectory, { recursive: true, force: true });
    throw error;
  }
}

function startVite(apiURL: string) {
  return spawn(
    process.execPath,
    [
      resolve("node_modules/vite/bin/vite.js"),
      "--host",
      "127.0.0.1",
      "--port",
      "4173",
      "--strictPort",
    ],
    {
      cwd: ".",
      env: {
        ...process.env,
        VITE_API_BASE_URL: apiURL,
        VITE_SUMI_AUTH_MODE: "preissued",
        VITE_SUMI_DIRECT_CHAT_INSTALLATION_ID:
          "0198f0f4-9b72-7000-8000-000000000051",
        VITE_SUMI_DIRECT_CHAT_AUTHORITY_EPOCH: "1",
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
}

async function connectionStats(apiURL: string) {
  const response = await fetch(`${apiURL}/__e2e__/connection-stats`);
  if (!response.ok) {
    throw new Error(`connection stats failed: ${response.status}`);
  }
  const stats = (await response.json()) as {
    active?: unknown;
    accepted?: unknown;
  };
  if (
    !Number.isSafeInteger(stats.active) ||
    Number(stats.active) < 0 ||
    !Number.isSafeInteger(stats.accepted) ||
    Number(stats.accepted) < 0
  ) {
    throw new Error(`invalid connection stats: ${JSON.stringify(stats)}`);
  }
  return {
    active: Number(stats.active),
    accepted: Number(stats.accepted),
  };
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
  if (child.exitCode !== null || child.signalCode !== null) return;
  const forcedExit = once(child, "exit");
  child.kill("SIGKILL");
  await forcedExit;
}

function delay(milliseconds: number) {
  return new Promise<void>((resolveDelay) =>
    setTimeout(resolveDelay, milliseconds),
  );
}
