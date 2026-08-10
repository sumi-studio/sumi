import { expect, test } from "@playwright/test";

test("mobile Human can open and resolve an exact request", async ({
  page,
  request,
}, testInfo) => {
  const expiresAt = new Date(Date.now() + 3_600_000).toISOString();
  const created = await request.post("/api/publisher/requests", {
    headers: {
      Authorization: "Bearer local-publisher-token-change-me",
      "Idempotency-Key": `playwright-${Date.now()}`,
    },
    data: {
      title: "Choose today’s integration window",
      body: "The checked release head is ready. Decide whether Codex should integrate it now or wait for the morning review window.",
      source: "Codex · Developer Workspace",
      choices: [
        { id: "now", label: "Integrate now", tone: "positive" },
        { id: "morning", label: "Wait until morning", tone: "neutral" },
        { id: "stop", label: "Stop this change", tone: "destructive" },
      ],
      allowFreeText: true,
      expiresAt,
      callback: { correlationId: "mobile-smoke" },
    },
  });
  expect(created.status()).toBe(201);
  const createdBody = (await created.json()) as { request: { id: string } };

  await page.goto("/#bootstrap=local-human-bootstrap-change-me");
  await expect(
    page.getByRole("heading", { name: "Needs your call" }),
  ).toBeVisible();
  await expect(
    page.getByText("Choose today’s integration window"),
  ).toBeVisible();
  expect(await page.evaluate(() => Notification.permission)).toBe("default");
  await page.screenshot({
    path: testInfo.outputPath("decision-inbox-mobile-list.png"),
    fullPage: true,
  });

  await page.getByText("Choose today’s integration window").click();
  await expect(page).toHaveURL(
    new RegExp(`/requests/${createdBody.request.id}$`),
  );
  await expect(
    page.getByRole("heading", { name: "Choose today’s integration window" }),
  ).toBeVisible();
  await page.screenshot({
    path: testInfo.outputPath("decision-inbox-mobile-detail.png"),
    fullPage: true,
  });

  await page.getByRole("button", { name: "Integrate now" }).click();
  await page
    .getByLabel("Add a short note optional")
    .fill("Use the checked release head.");
  await page.getByRole("button", { name: "Send decision" }).click();
  await expect(
    page.getByRole("heading", { name: "Integrate now" }),
  ).toBeVisible();
  await expect(page.getByText("Use the checked release head.")).toBeVisible();
  await page.screenshot({
    path: testInfo.outputPath("decision-inbox-mobile-resolved.png"),
    fullPage: true,
  });
});
