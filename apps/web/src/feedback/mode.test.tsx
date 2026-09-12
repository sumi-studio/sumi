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

const identity = vi.hoisted(() => ({
  userId: "mode-human",
  authenticated: true,
}));
vi.mock("../auth/auth-context", () => ({
  useAuth: () => ({
    authenticated: identity.authenticated,
    user: { id: identity.userId },
  }),
}));

beforeEach(() => {
  localStorage.clear();
  resetFeedbackOrigin();
  identity.userId = "mode-human";
  identity.authenticated = true;
  vi.spyOn(globalThis, "fetch").mockImplementation(
    async () => new Response("{}", { status: 200 }),
  );
  vi.spyOn(feedbackClient, "bootstrap").mockResolvedValue({
    participant: { kind: "human", human_id: identity.userId },
    recipient_name: "Sumi開発",
    available: true,
    is_recipient: false,
    scope: "builtin",
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
  identity.authenticated = false;
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
    vi.mocked(feedbackClient.create).mock.calls[1][3]?.annotations?.at(-1)
      ?.label,
  ).toBe("対象B");
});

function pointer(
  target: Element,
  type: string,
  x: number,
  y: number,
  pointerType = "mouse",
) {
  const event = new MouseEvent(type, {
    bubbles: true,
    cancelable: true,
    clientX: x,
    clientY: y,
    button: 0,
  });
  Object.defineProperties(event, {
    pointerId: { value: 1 },
    pointerType: { value: pointerType },
  });
  fireEvent(target, event);
}

function savedAnnotations() {
  const key = Object.keys(localStorage).find((key) => key.includes("in-place"));
  return key
    ? JSON.parse(localStorage.getItem(key) || "{}").diagnostics?.annotations
    : undefined;
}

it("appends exactly one element annotation and prevents the underlying pointer action", async () => {
  const action = vi.fn();
  render(
    <>
      <button type="button" onPointerDown={action} onClick={action}>
        選択対象
      </button>
      <FeedbackMode pathname="/direct" />
    </>,
  );
  const target = screen.getByRole("button", { name: "選択対象" });
  vi.spyOn(target, "getBoundingClientRect").mockReturnValue(
    new DOMRect(30, 40, 100, 50),
  );
  await open();
  fireEvent.change(screen.getByRole("textbox", { name: "内容" }), {
    target: { value: "書きかけの説明" },
  });
  fireEvent.click(screen.getByRole("button", { name: "画面の場所を指定" }));
  pointer(target, "pointerdown", 40, 50);
  pointer(target, "pointerup", 42, 51);
  fireEvent.click(target);
  expect(action).not.toHaveBeenCalled();
  expect(savedAnnotations()).toMatchObject([
    {
      number: 1,
      kind: "element",
      path: "/direct",
      label: "選択対象",
      rect: { x: 30, y: 40, width: 100, height: 50 },
    },
  ]);
  expect(savedAnnotations()).toHaveLength(1);
  expect(
    (screen.getByRole("textbox", { name: "内容" }) as HTMLTextAreaElement)
      .value,
  ).toContain("書きかけの説明");
  expect(
    screen.getByRole("button", { name: "指定箇所 1 を確認" }),
  ).toBeInTheDocument();
});

it("captures a reverse touch drag as a region and hides it after a nested scroll or navigation", async () => {
  const content = (path: string) => (
    <>
      <div data-testid="source-region">範囲の対象</div>
      <FeedbackMode pathname={path} />
    </>
  );
  const view = render(content("/direct"));
  await open();
  const target = screen.getByTestId("source-region");
  fireEvent.click(screen.getByRole("button", { name: "画面の場所を指定" }));
  expect(
    document.querySelector("[data-feedback-selection-surface]"),
  ).toBeInTheDocument();
  pointer(target, "pointerdown", 180, 160, "touch");
  pointer(target, "pointermove", 50, 60, "touch");
  pointer(target, "pointerup", 50, 60, "touch");
  fireEvent.click(target);
  expect(savedAnnotations()).toMatchObject([
    {
      number: 1,
      kind: "region",
      rect: { x: 50, y: 60, width: 130, height: 100 },
      scroll_x: 0,
      scroll_y: 0,
      viewport_width: window.innerWidth,
      viewport_height: window.innerHeight,
    },
  ]);
  expect(
    screen.getByRole("button", { name: "指定箇所 1 を確認" }),
  ).toBeInTheDocument();
  target.scrollTop = 20;
  fireEvent.scroll(target);
  await waitFor(() =>
    expect(
      screen.queryByRole("button", { name: "指定箇所 1 を確認" }),
    ).not.toBeInTheDocument(),
  );
  target.scrollTop = 0;
  fireEvent.scroll(target);
  expect(
    await screen.findByRole("button", { name: "指定箇所 1 を確認" }),
  ).toBeInTheDocument();
  view.rerender(content("/w/other/messaging"));
  expect(
    screen.queryByRole("button", { name: "指定箇所 1 を確認" }),
  ).not.toBeInTheDocument();
});

it("uses a valid element for horizontal or vertical drags and bounds regions to the viewport", async () => {
  vi.spyOn(feedbackClient, "create").mockRejectedValue(
    new TypeError("offline"),
  );
  render(
    <>
      <button type="button">ドラッグ対象</button>
      <FeedbackMode pathname="/direct" />
    </>,
  );
  const target = screen.getByRole("button", { name: "ドラッグ対象" });
  vi.spyOn(target, "getBoundingClientRect").mockReturnValue(
    new DOMRect(30, 40, 100, 50),
  );
  await open();
  for (const [x, y] of [
    [100, 20],
    [10, 100],
  ]) {
    fireEvent.click(screen.getByRole("button", { name: "画面の場所を指定" }));
    pointer(target, "pointerdown", 10, 20);
    pointer(target, "pointerup", x, y);
    fireEvent.click(target);
  }
  expect(savedAnnotations()).toMatchObject([
    { kind: "element", rect: { x: 30, y: 40, width: 100, height: 50 } },
    { kind: "element", rect: { x: 30, y: 40, width: 100, height: 50 } },
  ]);
  fireEvent.click(screen.getByRole("button", { name: "画面の場所を指定" }));
  pointer(target, "pointerdown", 10, 20);
  pointer(
    target,
    "pointerup",
    window.innerWidth + 100,
    window.innerHeight + 100,
  );
  fireEvent.click(target);
  expect(savedAnnotations().at(-1)).toMatchObject({
    kind: "region",
    rect: {
      x: 10,
      y: 20,
      width: window.innerWidth - 10,
      height: window.innerHeight - 20,
    },
  });
  fireEvent.click(screen.getByRole("button", { name: "送信する" }));
  await screen.findByRole("alert");
  expect(
    vi.mocked(feedbackClient.create).mock.calls[0][3]?.annotations?.[0],
  ).toMatchObject({
    kind: "element",
    rect: { width: 100, height: 50 },
  });
});

it("cancels unfinished gestures with Escape or pointercancel without adding annotations", async () => {
  render(
    <>
      <button type="button">中断する対象</button>
      <FeedbackMode pathname="/direct" />
    </>,
  );
  await open();
  const target = screen.getByRole("button", { name: "中断する対象" });
  for (const cancel of ["Escape", "pointercancel"]) {
    fireEvent.click(screen.getByRole("button", { name: "画面の場所を指定" }));
    pointer(target, "pointerdown", 10, 10);
    pointer(target, "pointermove", 100, 100);
    if (cancel === "Escape") fireEvent.keyDown(document, { key: "Escape" });
    else pointer(target, "pointercancel", 100, 100);
    expect(
      document.querySelector("[data-feedback-selection-surface]"),
    ).not.toBeInTheDocument();
    expect(savedAnnotations() ?? []).toHaveLength(0);
  }
});

it("caps annotations at ten while retaining the report text", async () => {
  render(
    <>
      <button type="button">繰り返し指す対象</button>
      <FeedbackMode pathname="/direct" />
    </>,
  );
  await open();
  fireEvent.change(screen.getByRole("textbox", { name: "内容" }), {
    target: { value: "説明は保持する" },
  });
  for (let index = 0; index < 10; index++) {
    fireEvent.click(screen.getByRole("button", { name: "画面の場所を指定" }));
    fireEvent.click(screen.getByRole("button", { name: "繰り返し指す対象" }));
  }
  expect(
    screen.getByRole("button", { name: "画面の場所を指定" }),
  ).toBeDisabled();
  expect(savedAnnotations()).toHaveLength(10);
  expect(
    (screen.getByRole("textbox", { name: "内容" }) as HTMLTextAreaElement)
      .value,
  ).toContain("説明は保持する");
});

it("follows the live element on scroll, reveals it, and drops a removed anchor", async () => {
  const content = (show: boolean) => (
    <>
      {show && <button type="button">動く箇所</button>}
      <FeedbackMode pathname="/direct" />
    </>
  );
  const view = render(content(true));
  const target = screen.getByRole("button", { name: "動く箇所" });
  let rect = new DOMRect(40, 100, 100, 50);
  vi.spyOn(target, "getBoundingClientRect").mockImplementation(() => rect);
  const scrollIntoView = vi.fn();
  Object.defineProperty(target, "scrollIntoView", { value: scrollIntoView });
  await open();
  fireEvent.click(screen.getByRole("button", { name: "画面の場所を指定" }));
  fireEvent.click(target);
  rect = new DOMRect(40, 30, 100, 50);
  fireEvent.scroll(document);
  const marker = screen.getByRole("button", { name: "指定箇所 1 を確認" });
  await waitFor(() =>
    expect(marker.parentElement).toHaveStyle({ top: "30px", left: "40px" }),
  );
  fireEvent.click(marker);
  expect(scrollIntoView).toHaveBeenCalledOnce();
  expect(marker.parentElement).toHaveClass("is-revealed");
  expect(
    screen.getByRole("button", { name: "フィードバックの続きを書く" }),
  ).toBeInTheDocument();
  view.rerender(content(false));
  await waitFor(() =>
    expect(
      screen.queryByRole("button", { name: "指定箇所 1 を確認" }),
    ).not.toBeInTheDocument(),
  );
});

it("coalesces source geometry work and ignores activity inside the feedback panel", async () => {
  render(
    <>
      <div data-testid="geometry-source">元の画面</div>
      <FeedbackMode pathname="/direct" />
    </>,
  );
  await open();
  const source = screen.getByTestId("geometry-source");
  for (let index = 0; index < 2; index++) {
    fireEvent.click(screen.getByRole("button", { name: "画面の場所を指定" }));
    pointer(source, "pointerdown", 10, 20);
    pointer(source, "pointerup", 100, 100);
    fireEvent.click(source);
  }
  await act(
    async () =>
      new Promise<void>((resolve) => requestAnimationFrame(() => resolve())),
  );
  const queued = new Map<number, FrameRequestCallback>();
  let frameId = 0;
  vi.spyOn(globalThis, "requestAnimationFrame").mockImplementation(
    (callback) => {
      queued.set(++frameId, callback);
      return frameId;
    },
  );
  vi.spyOn(globalThis, "cancelAnimationFrame").mockImplementation((id) => {
    queued.delete(id);
  });
  const query = vi.spyOn(document, "querySelectorAll");
  const fullScans = () =>
    query.mock.calls.filter(([selector]) => selector === "*").length;
  fireEvent.scroll(source);
  fireEvent.scroll(source);
  await act(async () => {
    source.append(document.createElement("span"));
  });
  expect(queued.size).toBe(1);
  expect(fullScans()).toBe(0);
  act(() => {
    const callbacks = [...queued.values()];
    queued.clear();
    for (const callback of callbacks) callback(0);
  });
  expect(fullScans()).toBe(1);
  query.mockClear();
  const panel = screen.getByRole("dialog");
  fireEvent.scroll(panel);
  fireEvent.change(screen.getByRole("textbox", { name: "内容" }), {
    target: { value: "説明を編集中" },
  });
  await act(async () => {
    panel.append(document.createElement("span"));
  });
  expect(queued.size).toBe(0);
  expect(fullScans()).toBe(0);
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
