import { spawnSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { expect, test } from "@playwright/test";
import { startCoreApprovalStack } from "./support/core-approval-stack";

/**
 * F302 browser lane: a directed Messaging request that fails terminally
 * leaves the requester a visible reply inside the existing conversation.
 *
 * A real Human session in system Chrome watches a shared Messaging channel
 * while the real TypeScript core (mock provider) processes the Human's
 * reply through the production cmd/server binary — real attention intake,
 * real agentstate HTTP, real PostgreSQL. The reply's "!alwayspad" directive
 * makes every decision exceed the agentstate request budget, so the turn
 * resolves to the recorded terminal failure — and the secretary's notice
 * must render inline, reply-associated with the request, durable across a
 * reload, and not duplicated by a later core restart.
 *
 * Env (set by the runner script):
 *   SUMI_NOTICE_E2E_DB_URL   empty disposable database
 *   SUMI_NOTICE_BIN_DIR      sumi-api-server / sumi-e2e-session-cookie / sumi-e2e-seed-member
 *   SUMI_NOTICE_EVIDENCE_DIR where screenshots / frame logs go
 */

test.describe.configure({ timeout: 420_000 });
test.use({
  actionTimeout: 15_000,
  launchOptions: {
    executablePath: process.env.SUMI_E2E_CHROME ?? "/usr/bin/google-chrome",
    args: ["--no-sandbox"],
  },
});

test("a terminally failed directed request leaves an inline notice in the existing conversation", async ({
  context,
  page,
}) => {
  const databaseURL = (process.env.SUMI_NOTICE_E2E_DB_URL ?? "").trim();
  const binDir = (process.env.SUMI_NOTICE_BIN_DIR ?? "").trim();
  const evidenceDir = (process.env.SUMI_NOTICE_EVIDENCE_DIR ?? "test-results").trim();
  if (!databaseURL || !binDir) {
    throw new Error("SUMI_NOTICE_E2E_DB_URL / SUMI_NOTICE_BIN_DIR required");
  }
  mkdirSync(evidenceDir, { recursive: true });
  const build = {
    directory: binDir,
    apiServer: join(binDir, "sumi-api-server"),
    sessionIssuer: join(binDir, "sumi-e2e-session-cookie"),
  };
  const frames: { type: string; event: Record<string, unknown> }[] = [];
  const notes: string[] = [];
  const note = (line: string) => notes.push(`${new Date().toISOString()} ${line}`);
  const stack = await startCoreApprovalStack(build, databaseURL);
  try {
    page.on("websocket", (socket) => {
      if (!socket.url().includes("/messaging/ws")) return;
      socket.on("framereceived", (frame) => {
        if (typeof frame.payload !== "string") return;
        try {
          const payload = asRecord(JSON.parse(frame.payload) as unknown);
          if (payload.type === "event") {
            frames.push({ type: String(asRecord(payload.event).type), event: asRecord(payload.event) });
          }
        } catch {
          // observer only
        }
      });
    });
    await stack.installSession(context);
    await page.goto(stack.webURL);

    // Workspace + seeded secretary membership + Messaging app + channel —
    // the same fixture path the reviewer lane drives.
    await expect(page.getByRole("heading", { name: "どこで一緒に働きますか" })).toBeVisible();
    await page.getByRole("textbox", { name: "新しいWorkspaceの名前" }).fill("Notice Studio");
    const created = page.waitForResponse(
      (r) => r.request().method() === "POST" && new URL(r.url()).pathname === "/workspaces",
    );
    await page.getByRole("button", { name: "作成して開く" }).click();
    const workspace = asRecord(await (await created).json());
    const workspaceID = asString(workspace.workspace_id);
    const seeded = spawnSync(join(binDir, "sumi-e2e-seed-member"), [], {
      env: {
        ...process.env,
        SUMI_E2E_SEED_DATABASE_URL: databaseURL,
        SUMI_E2E_SEED_WORKSPACE_ID: workspaceID,
        SUMI_E2E_SEED_MEMBER_KIND: "personality_agent",
        SUMI_E2E_SEED_MEMBER_ID: stack.personaID,
      },
      encoding: "utf8",
    });
    if (seeded.status !== 0) throw new Error(`seed member: ${seeded.stderr}`);
    note(`workspace=${workspaceID} persona=${stack.personaID} human=${stack.humanID}`);
    await page.reload();
    await expect(page.getByRole("heading", { name: "概要" })).toBeVisible();
    await page.getByRole("button", { name: "アプリ", exact: true }).click();
    const installed = page.waitForResponse(
      (r) => r.request().method() === "POST" && new URL(r.url()).pathname === "/app-installations",
    );
    await page.getByRole("button", { name: "インストール" }).click();
    expect((await installed).status()).toBe(201);
    await page.getByRole("button", { name: "開く", exact: true }).click();
    await expect(page.getByText("場所はまだありません", { exact: true })).toBeVisible();

    await page.getByTitle("チャンネルを作成").click();
    const dialog = page.getByRole("dialog", { name: "チャンネルを作成" });
    await dialog.getByRole("textbox", { name: "名前", exact: true }).fill("notice-general");
    await dialog.getByRole("button", { name: "作成", exact: true }).click();
    const composer = page.getByRole("textbox", { name: "#notice-general へメッセージ" });

    // An ambient message — it never asks the secretary to answer, so even a
    // failing turn on it must stay silent (the no-noise side of F302).
    const AMBIENT = "昨日の議事録を共有します";
    await composer.fill(AMBIENT);
    await composer.press("Enter");
    await expect(page.getByText(AMBIENT, { exact: true })).toBeVisible();
    await expect.poll(() => messageFrame(frames, AMBIENT)).not.toBeNull();
    const channelPlaceID = asString(asRecord(messageFrame(frames, AMBIENT)!.place).channel_id);
    note(`channel place=${channelPlaceID}`);

    // The secretary answers once through its delegated effect — a real
    // message the Human can reply to.
    const SEC_TEXT = "資料を読みました。要約しましょうか？";
    await stack.submitInput(
      `!messaging.send ${JSON.stringify({ place_id: channelPlaceID, content: SEC_TEXT })}`,
    );
    stack.runCoreOnce("secretary-send");
    await expect(page.getByText(SEC_TEXT, { exact: true })).toBeVisible({ timeout: 20_000 });
    note("secretary message rendered");

    // The Human replies to the secretary's message — the reply-to-secretary
    // attention path admits it as a directed input (attention="reply") with
    // real place/message provenance. The directive makes every decision the
    // model emits unrecordable, so the turn fails terminally.
    const secRow = page
      .getByText(SEC_TEXT, { exact: true })
      .locator("xpath=ancestor::div[contains(@class,'group')][1]");
    await secRow.hover();
    await secRow.getByRole("button", { name: "返信", exact: true }).click();
    await composer.fill("!alwayspad 4300000");
    await composer.press("Enter");
    const DIRECTIVE = "!alwayspad 4300000";
    await expect(page.getByText(DIRECTIVE, { exact: true })).toBeVisible();
    await expect.poll(() => messageFrame(frames, DIRECTIVE)).not.toBeNull();
    const requestMessageID = asString(messageFrame(frames, DIRECTIVE)!.message_id);
    note(`directed request message=${requestMessageID}`);

    // The server's attention drain (1s) admits the input; a bounded loop of
    // real core passes covers the admission lag.
    const noticeText = page.getByText(/完了できませんでした/);
    for (let i = 0; i < 6; i++) {
      stack.runCoreOnce(`fail-notice-${i}`);
      if (await noticeText.isVisible().catch(() => false)) break;
      await page.waitForTimeout(1500);
    }
    await expect(noticeText).toBeVisible({ timeout: 30_000 });

    // The notice arrived as an ordinary message_created frame — authored by
    // the secretary, reply-associated with the request, with the classified
    // cause and the safe next action.
    const noticeFrame = frames
      .filter((f) => f.type === "message_created")
      .map((f) => asRecord(f.event.message))
      .find((m) => String(m.content ?? "").includes("完了できませんでした"));
    expect(noticeFrame, "no message_created frame for the notice").toBeTruthy();
    expect(asRecord(noticeFrame!.author).kind).toBe("personality_agent");
    expect(noticeFrame!.reply_to).toBe(requestMessageID);
    expect(String(noticeFrame!.content)).toContain("大きすぎて記録できませんでした");
    expect(String(noticeFrame!.content)).toContain("お尋ねください");
    await page.screenshot({ path: join(evidenceDir, "notice-01-inline.png") });
    note("notice rendered live, reply-associated with the request");

    // A core restart must not post a second notice — the commit replay path
    // and the notice's own nonce both dedupe.
    stack.runCoreOnce("restart-replay");
    await page.waitForTimeout(1500);
    await expect(page.getByText(/完了できませんでした/)).toHaveCount(1);

    // And the ambient message produced no failure noise anywhere.
    await expect(page.getByText(/完了できませんでした/)).toHaveCount(1);
    note("restart produced no duplicate; ambient observation stayed silent");

    // Reconnect durability: a reload reads the same durable history.
    await page.reload();
    await page.getByText("notice-general", { exact: true }).first().click();
    await expect(
      page.getByRole("paragraph").filter({ hasText: "!alwayspad 4300000" }),
    ).toBeVisible();
    await expect(page.getByText(/完了できませんでした/)).toBeVisible();
    await page.screenshot({ path: join(evidenceDir, "notice-02-after-reload.png") });
    note("notice still present after reload — durable in place history");
  } finally {
    writeFileSync(
      join(evidenceDir, "notice-frames.json"),
      JSON.stringify({ notes, eventTypes: frames.map((f) => f.type), frames }, null, 1),
    );
    await stack.stop();
  }
});

function messageFrame(
  frames: { type: string; event: Record<string, unknown> }[],
  content: string,
): Record<string, unknown> | null {
  for (const f of frames) {
    if (f.type !== "message_created") continue;
    const message = asRecord(f.event.message);
    if (message.content === content) return message;
  }
  return null;
}

function asRecord(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error("expected object");
  return value as Record<string, unknown>;
}
function asString(value: unknown): string {
  if (typeof value !== "string" || value.length === 0) throw new Error("expected string");
  return value;
}
