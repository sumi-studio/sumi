// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { useEffect, useState } from "react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { feedbackClient } from "./api";
import { resetFeedbackOrigin } from "./diagnostics";
import { FeedbackMode, openFeedbackMode } from "./mode";

const identity = vi.hoisted(() => ({ userId: "mode-human", enabled: true }));
vi.mock("../auth/auth-context", () => ({
  useAuth: () => ({ authenticated: true, user: { id: identity.userId } }),
}));
vi.mock("../participant/app-store", () => ({
  useParticipantApps: () => ({
    owner: {
      kind: "participant",
      participant: { kind: "human", humanId: identity.userId },
    },
    installations: [],
  }),
  participantInstallation: () => ({
    state: identity.enabled ? "enabled" : "disabled",
  }),
}));

beforeEach(() => {
  localStorage.clear();
  resetFeedbackOrigin();
  identity.userId = "mode-human";
  identity.enabled = true;
  vi.spyOn(globalThis, "fetch").mockImplementation(
    async () => new Response("{}", { status: 200 }),
  );
  vi.spyOn(feedbackClient, "bootstrap").mockResolvedValue({
    participant: { kind: "human", human_id: identity.userId },
    recipient_name: "Sumi開発",
    available: true,
    is_recipient: false,
    installed: true,
    enabled: true,
  });
});
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

async function open() {
  act(openFeedbackMode);
  return screen.findByRole("textbox", { name: "件名" });
}

it("keeps the underlying app mounted and preserves both drafts while minimized", async () => {
  const unmounted = vi.fn();
  function Source() {
    const [value, setValue] = useState("");
    useEffect(() => () => unmounted(), []);
    return (
      <input
        aria-label="元の入力"
        value={value}
        onChange={(event) => setValue(event.target.value)}
      />
    );
  }
  render(
    <>
      <Source />
      <FeedbackMode pathname="/direct" />
    </>,
  );
  fireEvent.change(screen.getByLabelText("元の入力"), {
    target: { value: "作業途中" },
  });
  fireEvent.change(await open(), { target: { value: "報告途中" } });
  fireEvent.click(screen.getByRole("button", { name: "小さくして画面を操作" }));
  expect(screen.getByLabelText("元の入力")).toHaveValue("作業途中");
  expect(unmounted).not.toHaveBeenCalled();
  fireEvent.click(
    screen.getByRole("button", { name: "フィードバックの続きを書く" }),
  );
  expect(screen.getByRole("textbox", { name: "件名" })).toHaveValue("報告途中");
});

it("releases target interception when Feedback access disappears", async () => {
  const clicked = vi.fn();
  const content = () => (
    <>
      <button type="button" onClick={clicked}>
        元の操作
      </button>
      <FeedbackMode pathname="/direct" />
    </>
  );
  const view = render(content());
  await open();
  fireEvent.click(screen.getByRole("button", { name: "画面の場所を指定" }));
  identity.enabled = false;
  view.rerender(content());
  expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "元の操作" }));
  expect(clicked).toHaveBeenCalledOnce();
});

it("allows correcting the selected element after an unsuccessful send", async () => {
  vi.spyOn(feedbackClient, "create").mockRejectedValue(
    new TypeError("offline"),
  );
  render(
    <>
      <button type="button">対象A</button>
      <button type="button">対象B</button>
      <FeedbackMode pathname="/direct" />
    </>,
  );
  fireEvent.change(await open(), { target: { value: "操作について" } });
  fireEvent.change(screen.getByRole("textbox", { name: "内容" }), {
    target: { value: "表示が違う" },
  });
  const pick = (label: string) => {
    fireEvent.click(screen.getByRole("button", { name: "画面の場所を指定" }));
    fireEvent.click(screen.getByRole("button", { name: label }));
  };
  pick("対象A");
  fireEvent.click(screen.getByRole("button", { name: "送信する" }));
  await screen.findByRole("alert");
  pick("対象B");
  fireEvent.click(screen.getByRole("button", { name: "送信する" }));
  await waitFor(() => expect(feedbackClient.create).toHaveBeenCalledTimes(2));
  expect(
    vi.mocked(feedbackClient.create).mock.calls[1][3]?.selection?.label,
  ).toBe("対象B");
});

it("restores a written draft with its original screen context and sends only on request", async () => {
  vi.spyOn(feedbackClient, "create").mockRejectedValue(
    new TypeError("offline"),
  );
  const view = render(<FeedbackMode pathname="/direct" />);
  fireEvent.change(await open(), { target: { value: "Directで起きた問題" } });
  fireEvent.change(screen.getByRole("textbox", { name: "内容" }), {
    target: { value: "途中までの報告" },
  });
  fireEvent.click(
    screen.getByRole("button", { name: "閉じる（下書きを保存）" }),
  );
  view.rerender(
    <FeedbackMode pathname="/w/other/messaging" workspaceId="other" />,
  );
  expect(await open()).toHaveValue("Directで起きた問題");
  expect(screen.getByRole("textbox", { name: "内容" })).toHaveValue(
    "途中までの報告",
  );
  expect(feedbackClient.create).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: "送信する" }));
  await screen.findByRole("alert");
  expect(vi.mocked(feedbackClient.create).mock.calls[0][3]?.source?.path).toBe(
    "/direct",
  );
});

it("closes on identity change and keeps each user's draft separate", async () => {
  const view = render(<FeedbackMode pathname="/direct" />);
  fireEvent.change(await open(), {
    target: { value: "最初のユーザーの下書き" },
  });
  identity.userId = "another-human";
  view.rerender(<FeedbackMode pathname="/direct" />);
  expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  expect(await open()).toHaveValue("");
  fireEvent.click(
    screen.getByRole("button", { name: "閉じる（下書きを保存）" }),
  );
  identity.userId = "mode-human";
  view.rerender(<FeedbackMode pathname="/direct" />);
  expect(await open()).toHaveValue("最初のユーザーの下書き");
});

it("keeps IME confirmation local and derives an optional title from the report body", async () => {
  vi.spyOn(feedbackClient, "create").mockRejectedValue(
    new TypeError("offline"),
  );
  render(<FeedbackMode pathname="/direct" />);
  await open();
  const body = screen.getByRole("textbox", { name: "内容" });
  fireEvent.change(body, {
    target: { value: "画面が更新されません\n再読み込みでも同じです" },
  });
  fireEvent.keyDown(body, { key: "Enter", ctrlKey: true, isComposing: true });
  fireEvent.keyDown(body, { key: "Enter", metaKey: true, keyCode: 229 });
  fireEvent.keyDown(body, { key: "Enter" });
  expect(feedbackClient.create).not.toHaveBeenCalled();
  fireEvent.keyDown(body, { key: "Enter", ctrlKey: true });
  await screen.findByRole("alert");
  expect(vi.mocked(feedbackClient.create).mock.calls[0].slice(0, 2)).toEqual([
    "画面が更新されません",
    "画面が更新されません\n再読み込みでも同じです",
  ]);
});
