import { type ChildProcessWithoutNullStreams, spawn } from "node:child_process";
import { expect, test } from "@playwright/test";

const webURL = "http://127.0.0.1:4173";

test("real Chrome chat journey uses the browser websocket boundary", async ({
  page,
}) => {
  const fixture = await startFixture();
  const vite = startVite(fixture.url);
  try {
    await waitFor(`${webURL}/`);
    expect(
      (await page.request.get(`${fixture.url}/__e2e__/session`)).status(),
    ).toBe(204);
    await page.goto(webURL);

    const composer = page.getByLabel("メッセージ");
    await composer.fill("initial user_message");
    await page.getByRole("button", { name: "送信" }).click();
    await expect(
      page.getByText("streamed assistant", { exact: true }),
    ).toBeVisible();
    await expect(
      page.getByText("Tool started: read_file", { exact: true }),
    ).toBeVisible();
    await expect(
      page.getByText("Tool finished: call-1", { exact: true }),
    ).toBeVisible();

    await composer.fill("second message is a steer");
    await page.getByRole("button", { name: "Steer" }).click();
    await expect(
      page.getByText("Steered (hard)", { exact: true }),
    ).toBeVisible();
    await expect(
      page.getByText("🔐 承認が必要です", { exact: true }),
    ).toBeVisible();
    await page.getByRole("button", { name: "今回のみ" }).click();
    await expect(
      page.getByText("abortable stream", { exact: true }),
    ).toBeVisible();

    await page.getByRole("button", { name: "停止" }).click();
    expect(
      (await fetch(`${fixture.url}/__e2e__/disconnect`, { method: "POST" }))
        .status,
    ).toBe(204);
    await expect(page.getByText("closed", { exact: true })).toBeVisible();
    const terminal = await fetch(`${fixture.url}/__e2e__/emit-terminal`, {
      method: "POST",
    });
    expect(terminal.status).toBe(204);
    await expect(
      page.getByText("Terminal replay", { exact: true }),
    ).toHaveCount(1);
  } finally {
    stop(vite);
    stop(fixture.process);
  }
});

async function startFixture() {
  const process = spawn("go", ["run", "./cmd/browser-e2e-fixture"], {
    cwd: "../api",
    stdio: ["ignore", "pipe", "pipe"],
  });
  const url = await new Promise<string>((resolve, reject) => {
    const timeout = setTimeout(
      () => reject(new Error("fixture did not start")),
      15_000,
    );
    process.stdout.on("data", (chunk: Buffer) => {
      const match = chunk.toString().match(/E2E_FIXTURE=(http:\/\/[^\s]+)/);
      if (match) {
        clearTimeout(timeout);
        resolve(match[1]);
      }
    });
    process.once("exit", (code) =>
      reject(new Error(`fixture exited early: ${code}`)),
    );
  });
  return { process, url };
}

function startVite(apiURL: string) {
  return spawn(
    "pnpm",
    ["exec", "vite", "--host", "127.0.0.1", "--port", "4173", "--strictPort"],
    {
      cwd: ".",
      env: {
        ...process.env,
        VITE_API_BASE_URL: apiURL,
        VITE_CONVERSATION_ID: "conversation-1",
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
}

async function waitFor(url: string) {
  for (let attempt = 0; attempt < 100; attempt++) {
    try {
      if ((await fetch(url)).ok) return;
    } catch {}
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`timed out waiting for ${url}`);
}

function stop(process: ChildProcessWithoutNullStreams) {
  if (!process.killed) process.kill("SIGTERM");
}
