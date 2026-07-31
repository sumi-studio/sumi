import { expect, test } from "@playwright/test";

test.describe("Todo Compose UI", () => {
  test.skip(
    process.env.SUMI_E2E_COMPOSE !== "1",
    "requires the local Docker Compose stack",
  );

  test.afterEach(async ({ page }) => {
    const response = await page.request.get(
      "http://localhost:8080/v1/todos?q=Chrome%20UI%20check&limit=100",
    );
    if (!response.ok()) return;
    const result = (await response.json()) as {
      items: Array<{ id: string; title: string; version: number }>;
    };
    await Promise.all(
      result.items
        .filter((todo) => todo.title.startsWith("Chrome UI check"))
        .map((todo) =>
          page.request.delete(
            `http://localhost:8080/v1/todos/${todo.id}?expected_version=${todo.version}`,
          ),
        ),
    );
  });

  test("creates, edits, completes, and deletes a Todo", async ({ page }) => {
    const title = `Chrome UI check ${Date.now()}`;

    await page.goto("http://localhost:8080/todos");
    await page
      .getByRole("button", { name: "ローカルセッションを開始" })
      .click();
    await expect(page.getByRole("heading", { name: "My tasks" })).toBeVisible();
    await expect(page.getByPlaceholder("Todoを検索")).toBeVisible();
    await expect(page.getByText("Sumiと話す", { exact: true })).toHaveCount(0);

    await page.getByPlaceholder("Todoを追加する…").fill(title);
    await page.getByRole("button", { name: "追加", exact: true }).click();
    await expect(page.getByText(title, { exact: true })).toBeVisible();

    await page
      .getByPlaceholder("メモや詳細を追加…")
      .fill("Chromeから編集しました");
    await page.getByRole("button", { name: "変更を保存" }).click();
    await expect(page.getByRole("status")).toContainText("変更を保存しました");

    await page
      .getByRole("article")
      .filter({ hasText: title })
      .getByRole("button", { name: "完了にする" })
      .click();
    await page.getByRole("button", { name: "Completed" }).click();
    await expect(page.getByText(title, { exact: true })).toBeVisible();

    await page.getByText(title, { exact: true }).click();
    page.once("dialog", (dialog) => dialog.accept());
    await page.getByRole("button", { name: "Todoを削除" }).click();
    await expect(page.getByText(title, { exact: true })).toHaveCount(0);

    await page.screenshot({ path: "/tmp/sumi-todos-ui.png", fullPage: true });
  });

  test("uses the compact navigation on mobile", async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.goto("http://localhost:8080/todos");
    await page
      .getByRole("button", { name: "ローカルセッションを開始" })
      .click();
    await page.getByRole("button", { name: "ナビゲーションを開く" }).click();
    await page.getByRole("button", { name: "Today" }).click();
    await expect(page.getByRole("heading", { name: "Today" })).toBeVisible();
    await page.screenshot({
      path: "/tmp/sumi-todos-mobile.png",
      fullPage: true,
    });
  });
});
