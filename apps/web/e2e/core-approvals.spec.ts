import { expect, test } from "@playwright/test";
import {
  buildWorkspaceBrowserStack,
  removeWorkspaceBrowserBuild,
  type WorkspaceBrowserBuild,
} from "./support/real-agent-stack";
import { startCoreApprovalStack } from "./support/core-approval-stack";

/**
 * Mounted-browser approval journey: a logged-in human's real session cookie
 * sees their secretary's parked durable operation in the approvals inbox,
 * approves once, and the SAME suspended input resumes through the real
 * TypeScript core — the granted send lands in the durable outbox exactly
 * once. Reload shows the resolved record, not a re-pending one.
 *
 * Fixtures (identified, no real credentials): the e2e-session-cookie issuer
 * mints the Human + secretary binding and signs the session cookie; the model
 * is the scripted MockProvider driven by the input text.
 */

test.describe.configure({ timeout: 300_000 });
test.use({
  actionTimeout: 15_000,
  launchOptions: {
    // SUMI_E2E_CHROME names the system browser when Playwright's own
    // download is absent (e.g. a slim CI image). --no-sandbox is needed in
    // unprivileged containers where Chrome's sandbox has no user namespaces.
    executablePath: process.env.SUMI_E2E_CHROME ?? "/usr/bin/google-chrome",
    args: ["--no-sandbox"],
  },
});

let build: WorkspaceBrowserBuild | undefined;

test.beforeAll(async () => {
  test.setTimeout(180_000);
  build = await buildWorkspaceBrowserStack();
});

test.afterAll(async () => {
  if (build) await removeWorkspaceBrowserBuild(build);
});

test("human sees the parked approval, approves once, and the same operation resumes", async ({
  context,
  page,
}) => {
  if (!build) throw new Error("browser binaries were not built");
  const databaseURL = (
    process.env.SUMI_APPROVAL_E2E_DB_URL ??
    process.env.SUMI_WORKSPACE_E2E_DB_URL ??
    ""
  ).trim();
  if (!databaseURL) {
    throw new Error(
      "SUMI_APPROVAL_E2E_DB_URL must name a disposable empty Postgres database",
    );
  }

  const stack = await startCoreApprovalStack(build, databaseURL);
  try {
    await stack.installSession(context);
    await page.goto(stack.webURL);

    // A real Workspace the human owns — membership is what scopes the live
    // approval nudge to this human's sockets.
    await expect(
      page.getByRole("heading", { name: "どこで一緒に働きますか" }),
    ).toBeVisible();
    await page
      .getByRole("textbox", { name: "新しいWorkspaceの名前" })
      .fill("Approval Studio");
    const createResponse = page.waitForResponse(
      (response) =>
        response.request().method() === "POST" &&
        new URL(response.url()).pathname === "/workspaces",
    );
    await page.getByRole("button", { name: "作成して開く" }).click();
    expect((await createResponse).status()).toBe(201);

    // Messaging installed on the Workspace so the secretary's nudge has a
    // live socket to reach.
    await page.getByRole("button", { name: "アプリ", exact: true }).click();
    const installResponse = page.waitForResponse(
      (response) =>
        response.request().method() === "POST" &&
        new URL(response.url()).pathname === "/app-installations",
    );
    await page.getByRole("button", { name: "インストール" }).click();
    expect((await installResponse).status()).toBe(201);

    // The secretary parks behind consent: the real core plans an elevated
    // send and the operation waits in the durable approvals table.
    const MESSAGE = "e2e browser: 会議を15時に移します";
    await stack.submitInput(`!elevated message.send {"text":"${MESSAGE}"}`);
    stack.runCoreOnce("park");
    const parked = await stack.listApprovals();
    expect(
      parked.filter((a) => a.status === "pending" && a.tool === "message.send"),
    ).toHaveLength(1);

    // The live nudge (or the inbox's own refresh) lands the counted badge —
    // "承認待ちはありません" is the always-present empty state, so wait for
    // the pending count specifically.
    const inboxButton = page.getByRole("button", { name: "承認待ち 1 件" });
    await expect(inboxButton).toBeVisible({ timeout: 45_000 });
    await inboxButton.click();

    // The card explains what will run: which secretary, the tool, the exact
    // request it planned, and where the suspended work came from.
    const popover = page.getByRole("dialog", { name: "承認" });
    await expect(
      popover.getByText(/E2E Secretary: message\.send/),
    ).toBeVisible();
    await expect(popover.locator("pre")).toContainText(MESSAGE.slice(0, 12));
    await page.screenshot({ path: "test-results/core-approval-pending.png" });

    const decisionResponse = page.waitForResponse(
      (response) =>
        response.request().method() === "POST" &&
        /\/me\/approvals\/[^/]+\/decision$/.test(new URL(response.url()).pathname),
    );
    await popover.getByRole("button", { name: "今回のみ許可" }).click();
    const decided = await decisionResponse;
    expect(decided.status()).toBe(200);
    await expect(
      popover.getByText("に今回のみ許可しました"),
    ).toBeVisible();

    // The parked input resumes and the granted send runs exactly once.
    stack.runCoreOnce("resume");
    stack.runCoreOnce("drain");
    const sent = (await stack.outbox()).filter(
      (entry) =>
        entry.kind === "secretary_message" && entry.payload.text === MESSAGE,
    );
    expect(sent).toHaveLength(1);

    // Durable state survives reload: no pending badge, the resolved row
    // stays in the inbox's recent section.
    await page.reload();
    await expect(
      page.getByRole("button", { name: "承認待ちはありません" }),
    ).toBeVisible({ timeout: 45_000 });
    await page.getByRole("button", { name: "承認待ちはありません" }).click();
    await expect(
      page.getByRole("dialog", { name: "承認" }).getByText("承認待ちはありません"),
    ).toBeVisible();
    await expect(
      page.getByRole("dialog", { name: "承認" }).getByText("最近の承認"),
    ).toBeVisible();
  } finally {
    await stack.stop();
  }
});
