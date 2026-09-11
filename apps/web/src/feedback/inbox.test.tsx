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
import { useState } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  type Bootstrap,
  type Detail,
  type FeedbackClient,
  FeedbackError,
  type Thread,
} from "./api";
import { recordFeedbackOrigin, resetFeedbackOrigin } from "./diagnostics";
import { FeedbackInbox, type InboxLocation } from "./inbox";

const author = {
  participant: { kind: "human" as const, human_id: "human-one" },
  display_name: "薄明色",
};
const thread: Thread = {
  id: "thread-one",
  title: "チャンネルの通知について",
  body: "通知設定と届き方が違うようです。",
  status: "resolved",
  author,
  created_at: "2026-09-11T10:00:00Z",
  updated_at: "2026-09-11T10:05:00Z",
  revision: 2,
  latest_message: null,
  unread: true,
};
const bootstrap: Bootstrap = {
  participant: author.participant,
  recipient_name: "Sumi開発",
  available: true,
  is_recipient: false,
  installed: true,
  enabled: true,
};
function setupClient(): FeedbackClient {
  return {
    bootstrap: vi.fn().mockResolvedValue(bootstrap),
    list: vi.fn().mockResolvedValue({ threads: [thread], next_cursor: null }),
    open: vi.fn().mockResolvedValue({
      thread,
      messages: [
        {
          id: "message-one",
          author: { ...author, display_name: "Sumi" },
          body: "[確認ページ](https://example.com)\n再度教えてください。",
          created_at: "2026-09-11T10:05:00Z",
          revision: 2,
        },
      ],
      activities: [],
      next_cursor: null,
    } satisfies Detail),
    read: vi.fn().mockResolvedValue(undefined),
    create: vi.fn().mockResolvedValue({ ...thread, status: "open" }),
    reply: vi.fn().mockResolvedValue({
      id: "new-message",
      author,
      body: "返信",
      created_at: "2026-09-11T10:10:00Z",
      revision: 3,
    }),
    status: vi
      .fn()
      .mockResolvedValue({ ...thread, status: "open", revision: 3 }),
  };
}
let actorNumber = 0;
beforeEach(() => {
  localStorage.clear();
  resetFeedbackOrigin();
  Object.defineProperty(document, "visibilityState", {
    value: "visible",
    configurable: true,
  });
});
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});
function App({
  client,
  initial = {},
  actor = `test-actor-${++actorNumber}`,
}: {
  client: FeedbackClient;
  initial?: InboxLocation;
  actor?: string;
}) {
  const [location, navigate] = useState(initial);
  return (
    <FeedbackInbox
      actor={actor}
      client={client}
      location={location}
      navigate={navigate}
    />
  );
}
describe("Feedback conversations", () => {
  it("keeps resolved follow-up conversations discoverable, uses real links and the shared safe markdown", async () => {
    const client = setupClient();
    render(<App client={client} />);
    const link = await screen.findByRole("link", {
      name: /チャンネルの通知について/,
    });
    expect(link).toHaveAttribute("href", "/feedback?thread=thread-one");
    expect(screen.getByLabelText("未読")).toBeInTheDocument();
    fireEvent.click(link);
    expect(
      await screen.findByRole("link", { name: "確認ページ" }),
    ).toHaveAttribute("href", "https://example.com");
    expect(
      screen.getByRole("button", { name: "再開する" }),
    ).toBeInTheDocument();
    expect(screen.getByRole("textbox", { name: "返信" })).toBeInTheDocument();
    await waitFor(() =>
      expect(client.read).toHaveBeenCalledWith("thread-one", 2),
    );
  });
  it("retains failed replies across back navigation and retries with the same request id", async () => {
    const client = setupClient();
    vi.mocked(client.reply).mockRejectedValueOnce(new TypeError("offline"));
    render(
      <App
        client={client}
        initial={{ thread: thread.id }}
        actor="retry-actor"
      />,
    );
    fireEvent.change(await screen.findByRole("textbox", { name: "返信" }), {
      target: { value: "通知がまた届きません" },
    });
    fireEvent.click(screen.getByRole("button", { name: "返信を送信" }));
    await screen.findByRole("alert");
    const first = vi.mocked(client.reply).mock.calls[0];
    fireEvent.click(screen.getByRole("button", { name: "一覧に戻る" }));
    fireEvent.click(
      await screen.findByRole("link", { name: /チャンネルの通知について/ }),
    );
    expect(await screen.findByRole("textbox", { name: "返信" })).toHaveValue(
      "通知がまた届きません",
    );
    fireEvent.click(screen.getByRole("button", { name: "返信を送信" }));
    await waitFor(() => expect(client.reply).toHaveBeenCalledTimes(2));
    expect(vi.mocked(client.reply).mock.calls[1]).toEqual(first);
    await waitFor(() =>
      expect(screen.getByRole("textbox", { name: "返信" })).toHaveValue(""),
    );
  });
  it("does not send during IME composition or on ordinary Enter", async () => {
    const client = setupClient();
    render(<App client={client} initial={{ thread: thread.id }} />);
    const input = await screen.findByRole("textbox", { name: "返信" });
    fireEvent.change(input, { target: { value: "変換中" } });
    fireEvent.keyDown(input, {
      key: "Enter",
      isComposing: true,
      ctrlKey: true,
    });
    fireEvent.keyDown(input, { key: "Enter", keyCode: 229, metaKey: true });
    fireEvent.keyDown(input, { key: "Enter" });
    expect(client.reply).not.toHaveBeenCalled();
    fireEvent.keyDown(input, { key: "Enter", ctrlKey: true });
    await waitFor(() => expect(client.reply).toHaveBeenCalledTimes(1));
  });
  it("keeps drafts isolated when another participant opens the same thread", async () => {
    const client = setupClient();
    const first = render(
      <App client={client} initial={{ thread: thread.id }} actor="alice" />,
    );
    fireEvent.change(await screen.findByRole("textbox", { name: "返信" }), {
      target: { value: "Aliceの下書き" },
    });
    first.unmount();
    render(<App client={client} initial={{ thread: thread.id }} actor="bob" />);
    expect(await screen.findByRole("textbox", { name: "返信" })).toHaveValue(
      "",
    );
  });
  it("preserves a reply when a concurrent status change conflicts", async () => {
    const client = setupClient();
    vi.mocked(client.status).mockRejectedValue(
      new FeedbackError(409, "revision_conflict"),
    );
    render(<App client={client} initial={{ thread: thread.id }} />);
    fireEvent.change(await screen.findByRole("textbox", { name: "返信" }), {
      target: { value: "まだ確認しています" },
    });
    fireEvent.click(screen.getByRole("button", { name: "再開する" }));
    expect(await screen.findByRole("alert")).toHaveTextContent(
      "会話が更新されています",
    );
    expect(screen.getByRole("textbox", { name: "返信" })).toHaveValue(
      "まだ確認しています",
    );
  });
  it("does not mark newly loaded events read while the document is hidden", async () => {
    Object.defineProperty(document, "visibilityState", {
      value: "hidden",
      configurable: true,
    });
    const client = setupClient();
    render(<App client={client} initial={{ thread: thread.id }} />);
    await screen.findByRole("textbox", { name: "返信" });
    await act(async () => {});
    expect(client.read).not.toHaveBeenCalled();
  });
  it("refreshes an abandoned empty draft's source when reporting from another app", async () => {
    const client = setupClient();
    recordFeedbackOrigin("/direct");
    const first = render(
      <App client={client} initial={{ compose: true }} actor="empty-source" />,
    );
    expect(
      document.querySelector(".feedback-diagnostics pre")?.textContent,
    ).toContain('"path": "/direct"');
    first.unmount();
    recordFeedbackOrigin("/w/workspace-one/messaging", "workspace-one");
    render(
      <App client={client} initial={{ compose: true }} actor="empty-source" />,
    );
    expect(
      document.querySelector(".feedback-diagnostics pre")?.textContent,
    ).toContain('"path": "/w/workspace-one/messaging"');
    await act(async () => {});
  });
  it("creates a discussion without forced choices and retains the title/body after failure", async () => {
    let manifest!: (response: Response) => void;
    vi.spyOn(globalThis, "fetch").mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          manifest = resolve;
        }),
    );
    const client = setupClient();
    vi.mocked(client.create).mockRejectedValueOnce(new TypeError("offline"));
    const rendered = render(
      <App
        client={client}
        initial={{ compose: true }}
        actor="diagnostic-retry"
      />,
    );
    fireEvent.change(screen.getByRole("textbox", { name: "件名" }), {
      target: { value: "こんな使い方はどう？" },
    });
    fireEvent.change(screen.getByRole("textbox", { name: "内容" }), {
      target: { value: "毎日の相談をここでしたいです。" },
    });
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "送信する" })).toBeEnabled(),
    );
    fireEvent.click(screen.getByRole("button", { name: "送信する" }));
    await screen.findByRole("alert");
    expect(screen.getByRole("textbox", { name: "件名" })).toHaveValue(
      "こんな使い方はどう？",
    );
    const first = vi.mocked(client.create).mock.calls[0];
    expect(first[3]).toMatchObject({
      version: 1,
      captured_at: expect.any(String),
    });
    await act(async () =>
      manifest(
        new Response(JSON.stringify({ release_sha: "a".repeat(40) }), {
          status: 200,
        }),
      ),
    );
    rendered.unmount();
    render(
      <App
        client={client}
        initial={{ compose: true }}
        actor="diagnostic-retry"
      />,
    );
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "送信する" })).toBeEnabled(),
    );
    fireEvent.click(screen.getByRole("button", { name: "送信する" }));
    await waitFor(() => expect(client.create).toHaveBeenCalledTimes(2));
    expect(vi.mocked(client.create).mock.calls[1]).toEqual(first);
  });
  it("does not let an old successful request erase a newer draft after returning to the conversation", async () => {
    const client = setupClient();
    let finish!: (value: Awaited<ReturnType<FeedbackClient["reply"]>>) => void;
    vi.mocked(client.reply).mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    );
    render(
      <App
        client={client}
        initial={{ thread: thread.id }}
        actor="stale-response-actor"
      />,
    );
    fireEvent.change(await screen.findByRole("textbox", { name: "返信" }), {
      target: { value: "最初の返信" },
    });
    fireEvent.click(screen.getByRole("button", { name: "返信を送信" }));
    fireEvent.click(screen.getByRole("button", { name: "一覧に戻る" }));
    fireEvent.click(
      await screen.findByRole("link", { name: /チャンネルの通知について/ }),
    );
    fireEvent.change(await screen.findByRole("textbox", { name: "返信" }), {
      target: { value: "次の下書き" },
    });
    await act(async () => {
      finish({
        id: "late-message",
        author,
        body: "最初の返信",
        created_at: thread.updated_at,
        revision: 3,
      });
    });
    fireEvent.click(screen.getByRole("button", { name: "一覧に戻る" }));
    fireEvent.click(
      await screen.findByRole("link", { name: /チャンネルの通知について/ }),
    );
    expect(await screen.findByRole("textbox", { name: "返信" })).toHaveValue(
      "次の下書き",
    );
  });
  it("exposes and fills a missing middle page after a long background interval", async () => {
    const client = setupClient();
    render(<App client={client} initial={{ thread: thread.id }} />);
    await screen.findByRole("textbox", { name: "返信" });
    const latest = { ...thread, revision: 6 };
    vi.mocked(client.open).mockResolvedValue({
      thread: latest,
      messages: [
        {
          id: "m6",
          author,
          body: "新しい返信",
          created_at: thread.updated_at,
          revision: 6,
        },
      ],
      activities: [],
      next_cursor: "before-6",
    });
    fireEvent(window, new Event("focus"));
    const gap = await screen.findByRole("button", {
      name: "間のやりとりを読む",
    });
    vi.mocked(client.open).mockResolvedValue({
      thread: latest,
      messages: [3, 4, 5].map((revision) => ({
        id: `m${revision}`,
        author,
        body: `途中の返信${revision}`,
        created_at: thread.updated_at,
        revision,
      })),
      activities: [],
      next_cursor: null,
    });
    fireEvent.click(gap);
    await screen.findByText("途中の返信4");
    expect(client.open).toHaveBeenCalledWith(thread.id, "before-6");
    expect(
      screen.queryByRole("button", { name: "間のやりとりを読む" }),
    ).not.toBeInTheDocument();
    expect(screen.getByText("新しい返信")).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "確認ページ" }),
    ).toBeInTheDocument();
  });
  it("retains a loadable list cursor when more than a page of new threads arrives", async () => {
    const client = setupClient();
    render(<App client={client} />);
    await screen.findByRole("link", { name: /チャンネルの通知について/ });
    vi.mocked(client.list).mockResolvedValue({
      threads: [
        {
          ...thread,
          revision: 3,
          title: "更新で浮上した相談",
          updated_at: "2026-09-12T12:00:00Z",
        },
        {
          ...thread,
          revision: 3,
          title: "更新で浮上した相談",
          updated_at: "2026-09-12T12:00:00Z",
        },
        {
          ...thread,
          id: "newest",
          title: "新しい相談",
          updated_at: "2026-09-12T11:00:00Z",
        },
      ],
      next_cursor: "middle-threads",
    });
    fireEvent(window, new Event("focus"));
    const load = await screen.findByRole("button", { name: "さらに読み込む" });
    vi.mocked(client.list).mockResolvedValue({
      threads: [{ ...thread, id: "middle", title: "間に届いた相談" }],
      next_cursor: null,
    });
    fireEvent.click(load);
    await screen.findByRole("link", { name: /間に届いた相談/ });
    expect(client.list).toHaveBeenCalledWith("all", "middle-threads");
  });
  it("keeps the newest draft when storage reads work but writes exceed quota", async () => {
    const client = setupClient();
    render(
      <App
        client={client}
        initial={{ thread: thread.id }}
        actor="quota-actor"
      />,
    );
    fireEvent.change(await screen.findByRole("textbox", { name: "返信" }), {
      target: { value: "古い下書き" },
    });
    vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new DOMException("full", "QuotaExceededError");
    });
    fireEvent.change(screen.getByRole("textbox", { name: "返信" }), {
      target: { value: "保存領域がなくても残す下書き" },
    });
    fireEvent.click(screen.getByRole("button", { name: "一覧に戻る" }));
    fireEvent.click(
      await screen.findByRole("link", { name: /チャンネルの通知について/ }),
    );
    expect(await screen.findByRole("textbox", { name: "返信" })).toHaveValue(
      "保存領域がなくても残す下書き",
    );
  });
  it("keeps a reader's scroll position when a new reply arrives", async () => {
    const client = setupClient();
    const { container } = render(
      <App client={client} initial={{ thread: thread.id }} />,
    );
    await screen.findByRole("textbox", { name: "返信" });
    const scroll = container.querySelector(
      ".feedback-conversation",
    ) as HTMLElement;
    Object.defineProperties(scroll, {
      scrollHeight: { configurable: true, value: 1000 },
      clientHeight: { configurable: true, value: 200 },
    });
    scroll.scrollTop = 150;
    fireEvent.scroll(scroll);
    vi.mocked(client.open).mockResolvedValue({
      thread: { ...thread, revision: 3 },
      messages: [
        {
          id: "arrived",
          author,
          body: "届いた新しい返信",
          created_at: thread.updated_at,
          revision: 3,
        },
      ],
      activities: [],
      next_cursor: null,
    });
    fireEvent(window, new Event("focus"));
    await screen.findByText("届いた新しい返信");
    expect(scroll.scrollTop).toBe(150);
    expect(screen.getByRole("button", { name: "最新へ" })).toBeInTheDocument();
    expect(client.read).not.toHaveBeenCalledWith(thread.id, 3);
  });
  it("lets a slow response finish instead of superseding it on every polling tick", async () => {
    vi.useFakeTimers();
    try {
      const client = setupClient();
      let finish!: (value: Detail) => void;
      vi.mocked(client.open).mockImplementation(
        () =>
          new Promise((resolve) => {
            finish = resolve;
          }),
      );
      render(<App client={client} initial={{ thread: thread.id }} />);
      await act(async () => {
        await vi.advanceTimersByTimeAsync(30_000);
      });
      expect(client.open).toHaveBeenCalledTimes(1);
      await act(async () => {
        finish({ thread, messages: [], activities: [], next_cursor: null });
      });
      expect(screen.getByRole("textbox", { name: "返信" })).toBeInTheDocument();
    } finally {
      cleanup();
      vi.useRealTimers();
    }
  });
});
