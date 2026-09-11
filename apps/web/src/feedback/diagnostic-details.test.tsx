// @vitest-environment jsdom
import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from "@testing-library/react";
import { afterEach, expect, it } from "vitest";
import { Diagnostics } from "./diagnostic-details";
import type { FeedbackDiagnostics } from "./diagnostics";

afterEach(cleanup);

const attachment: FeedbackDiagnostics = {
  version: 1,
  captured_at: "2026-09-11T15:10:00.000Z",
  browser: "Chrome/140.0",
  language: "ja-JP",
  time_zone: "Asia/Tokyo",
  utc_offset_minutes: 540,
  online: true,
  visibility: "visible",
  viewport: { width: 1200, height: 800, scale: 1, pixel_ratio: 2 },
  assets: [],
  annotations: [
    {
      number: 1,
      kind: "element",
      tag: "button",
      selector: "main > button:nth-of-type(2)",
      label: "送信",
      rect: { x: 720, y: 660, width: 96, height: 40 },
      captured_at: "2026-09-11T15:09:00.000Z",
      path: "/direct",
      scroll_x: 0,
      scroll_y: 420,
      viewport_width: 1200,
      viewport_height: 800,
    },
    {
      number: 2,
      kind: "region",
      tag: "",
      selector: "",
      label: "",
      rect: { x: 32, y: 64, width: 300, height: 120 },
      captured_at: "2026-09-11T15:09:30.000Z",
      path: "/w/workspace-one/messaging",
      scroll_x: 12,
      scroll_y: 840,
      viewport_width: 1024,
      viewport_height: 768,
    },
  ],
};

it("shows each numbered element and region annotation with its own captured location", () => {
  render(<Diagnostics details={attachment} />);
  fireEvent.click(screen.getByText("添付された診断情報"));
  const references = screen.getByRole("region", { name: "注釈を付けた箇所" });
  const items = within(references).getAllByRole("listitem");
  expect(items).toHaveLength(2);
  expect(items[0].textContent).toContain("#1 · 要素");
  expect(items[0].textContent).toContain("送信");
  expect(items[0].textContent).toContain("/direct");
  expect(items[0].textContent).toContain("位置 (720, 660) · 96 × 40 px");
  expect(items[0].textContent).toContain("main > button:nth-of-type(2)");
  expect(items[1].textContent).toContain("#2 · 範囲");
  expect(items[1].textContent).toContain("選択した範囲");
  expect(items[1].textContent).toContain("/w/workspace-one/messaging");
  expect(items[1].textContent).toContain("位置 (32, 64) · 300 × 120 px");

  fireEvent.click(screen.getByText("すべての診断データ"));
  const raw = screen.getByRole("region", { name: "診断データ JSON" });
  const stored = JSON.parse(raw.textContent ?? "");
  expect(stored.annotations).toEqual(attachment.annotations);
  expect(stored).not.toHaveProperty("selection");
});

it.each([
  undefined,
  [],
])("omits the reference section when annotations are %s", (annotations) => {
  render(<Diagnostics details={{ ...attachment, annotations }} />);
  fireEvent.click(screen.getByText("添付された診断情報"));
  expect(screen.queryByRole("region", { name: "注釈を付けた箇所" })).toBeNull();
});
