// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  lockedMessageId,
  lockMessageActions,
  resetMessageActionLock,
} from "../message-action-lock";
import type {
  MemberProfile,
  Message,
  ParticipantKey,
  ParticipantRef,
} from "../model";
import { participantKey } from "../model";
import {
  noteEmojiUsed,
  recentEmojis,
  resetRecentEmojis,
} from "../recent-emoji";
import { MessageItem } from "./message-item";

const human: ParticipantRef = { kind: "human", humanId: "h1" };
const agent: ParticipantRef = {
  kind: "personality_agent",
  personalityAgentId: "a1",
};
const selfKey: ParticipantKey = participantKey(human);

const membersByKey: Record<ParticipantKey, MemberProfile> = {
  [participantKey(human)]: {
    participant: human,
    displayName: "余白",
    tagline: "創業・デザイン",
  },
  [participantKey(agent)]: {
    participant: agent,
    displayName: "墨",
    tagline: "秘書",
  },
};

function makeMessage(overrides: Partial<Message> = {}): Message {
  return {
    messageId: "m1",
    place: { kind: "channel", channelId: "c1" },
    seq: 1,
    author: human,
    content: "本文です",
    mentions: [],
    urgency: "normal",
    reactions: [],
    attachments: [],
    replyTo: null,
    createdAt: Date.UTC(2026, 0, 1, 3, 0),
    editedAt: null,
    deleted: false,
    ...overrides,
  };
}

function renderItem(
  message: Message,
  props: Partial<React.ComponentProps<typeof MessageItem>> = {},
) {
  const noop = () => undefined;
  const view = render(
    <MessageItem
      message={message}
      grouped={false}
      pending={false}
      failed={false}
      selfKey={selfKey}
      membersByKey={membersByKey}
      replyLaterBy={[]}
      allowReactions
      allowReplyLater
      findMessage={() => undefined}
      onReply={noop}
      onReplyLater={noop}
      onToggleReaction={noop}
      onCopyLink={() => Promise.resolve(true)}
      onEdit={noop}
      onDelete={noop}
      onJumpTo={noop}
      onRetry={noop}
      editing={false}
      editDraft=""
      editConflict={null}
      onEditDraftChange={noop}
      onSubmitEdit={noop}
      onCancelEdit={noop}
      onReloadEditConflict={noop}
      revealedAttachmentIds={new Set()}
      onRevealAttachment={noop}
      onOpenImage={noop}
      {...props}
    />,
  );
  return { ...view, row: view.container.firstElementChild as HTMLElement };
}

beforeEach(() => {
  resetRecentEmojis();
  resetMessageActionLock();
});

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

describe("MessageItem の行の見せ方", () => {
  it("操作チップは対象メッセージの要素の内側に置かれる", () => {
    const { row } = renderItem(makeMessage());
    const toolbar = screen.getByLabelText("返信").closest("div");
    expect(toolbar).not.toBeNull();
    expect(row.contains(toolbar)).toBe(true);
    // 行の外へはみ出さない（translateで上へ逃がさない）。
    expect(toolbar?.className).not.toContain("-translate-y-1/2");
  });

  it("行にはホバー時のハイライトと左端の目印がある", () => {
    const { row } = renderItem(makeMessage());
    expect(row.className).toContain("hover:bg-accent");
    expect(row.className).not.toContain("hover:bg-accent/55");
    const marker = row.querySelector("span[aria-hidden]");
    expect(marker?.className).toContain("group-hover:bg-primary/50");
  });

  it("返信引用は本文と同じ左端に揃い、投稿者のミニアバターを伴う", () => {
    const target = makeMessage({
      messageId: "m0",
      seq: 0,
      author: agent,
      content: "元のメッセージ",
    });
    renderItem(makeMessage({ messageId: "m1", replyTo: "m0" }), {
      findMessage: (id) => (id === "m0" ? target : undefined),
    });
    const quote = screen.getByTitle("墨 の返信元へ移動");
    expect(quote).toHaveTextContent("墨");
    expect(quote).toHaveTextContent("元のメッセージ");
    // カギ線 → ミニアバター → 名前 → 抜粋 の順で一つの階層に並ぶ。
    const connector = quote.querySelector("span[aria-hidden]");
    expect(connector?.className).toContain("border-l-2");
    expect(quote.querySelectorAll("span[aria-hidden]").length).toBeGreaterThan(
      1,
    );
  });

  it("本文のない返信元は「添付ファイル」と示す", () => {
    const target = makeMessage({ messageId: "m0", seq: 0, content: "" });
    renderItem(makeMessage({ messageId: "m1", replyTo: "m0" }), {
      findMessage: () => target,
    });
    expect(screen.getByTitle("余白 の返信元へ移動")).toHaveTextContent(
      "添付ファイル",
    );
  });
});

describe("リンクのコピー", () => {
  it("成功したらチェック表示へ一時的に変わる", async () => {
    const onCopyLink = vi.fn().mockResolvedValue(true);
    renderItem(makeMessage(), { onCopyLink });
    fireEvent.click(screen.getByLabelText("リンクをコピー"));
    expect(onCopyLink).toHaveBeenCalledTimes(1);
    await waitFor(() =>
      expect(screen.getByLabelText("リンクをコピーしました")).toBeVisible(),
    );
  });

  it("失敗したら成功と偽らず、失敗として示す", async () => {
    const onCopyLink = vi.fn().mockResolvedValue(false);
    renderItem(makeMessage(), { onCopyLink });
    fireEvent.click(screen.getByLabelText("リンクをコピー"));
    await waitFor(() =>
      expect(
        screen.getByLabelText("リンクをコピーできませんでした"),
      ).toBeVisible(),
    );
    expect(screen.queryByLabelText("リンクをコピーしました")).toBeNull();
  });
});

describe("インライン編集", () => {
  it("編集中は本文の位置に入力欄とヒントが出て、操作チップは引っ込む", () => {
    renderItem(makeMessage({ content: "編集前" }), {
      editing: true,
      editDraft: "編集前",
    });
    const textarea = screen.getByLabelText("メッセージを編集");
    expect(textarea).toHaveValue("編集前");
    expect(screen.getByText("Escでキャンセル・Enterで保存")).toBeVisible();
    expect(screen.queryByLabelText("返信")).toBeNull();
  });

  it("外部編集との衝突は書きかけを残して保存を止め、新しい本文を読み込める", () => {
    const reload = vi.fn();
    renderItem(makeMessage({ content: "別の場所の本文" }), {
      editing: true,
      editDraft: "自分の書きかけ",
      editConflict: { content: "別の場所の本文", revision: 2 },
      onReloadEditConflict: reload,
    });

    expect(screen.getByRole("alert")).toHaveTextContent(
      "別の場所で編集されました",
    );
    expect(screen.getByRole("button", { name: "保存" })).toBeDisabled();
    fireEvent.click(
      screen.getByRole("button", { name: "新しい本文を読み込む" }),
    );
    expect(reload).toHaveBeenCalledOnce();
  });

  it("保存中は保存操作を無効化する", () => {
    renderItem(makeMessage({ content: "編集前" }), {
      editing: true,
      editDraft: "保存中の本文",
      editSaving: true,
    });

    expect(screen.getByRole("button", { name: "保存" })).toBeDisabled();
    expect(screen.getByRole("status")).toHaveTextContent("保存中");
  });

  it("競合でも対象消滅でもない保存失敗を表示する", () => {
    renderItem(makeMessage(), {
      editing: true,
      editFailure: "保存できませんでした。もう一度お試しください。",
    });

    expect(screen.getByRole("alert")).toHaveTextContent(
      "保存できませんでした。もう一度お試しください。",
    );
  });

  it("書きかけは行ではなく渡されたドラフトが正本になる", () => {
    // 行が一度アンマウントされて作り直されても、描くのは本文ではなく
    // 編集セッションのドラフト。
    const { unmount } = renderItem(makeMessage({ content: "元の本文" }), {
      editing: true,
      editDraft: "書きかけ",
    });
    expect(screen.getByLabelText("メッセージを編集")).toHaveValue("書きかけ");
    unmount();
    cleanup();
    renderItem(makeMessage({ content: "元の本文" }), {
      editing: true,
      editDraft: "書きかけ",
    });
    expect(screen.getByLabelText("メッセージを編集")).toHaveValue("書きかけ");
  });

  it("入力はドラフトの持ち主へ渡り、Enterで保存・Escで取り消す", () => {
    const onEditDraftChange = vi.fn();
    const onSubmitEdit = vi.fn();
    const onCancelEdit = vi.fn();
    renderItem(makeMessage({ content: "編集前" }), {
      editing: true,
      editDraft: "編集前",
      onEditDraftChange,
      onSubmitEdit,
      onCancelEdit,
    });
    const textarea = screen.getByLabelText("メッセージを編集");
    fireEvent.change(textarea, { target: { value: "編集後" } });
    expect(onEditDraftChange).toHaveBeenCalledWith("編集後");
    fireEvent.keyDown(textarea, { key: "Enter" });
    expect(onSubmitEdit).toHaveBeenCalledTimes(1);
    fireEvent.keyDown(textarea, { key: "Escape" });
    expect(onCancelEdit).toHaveBeenCalledTimes(1);
  });

  it("IME変換中のEnter・Escは編集の操作として奪わない", () => {
    const onSubmitEdit = vi.fn();
    const onCancelEdit = vi.fn();
    renderItem(makeMessage(), {
      editing: true,
      editDraft: "本文です",
      onSubmitEdit,
      onCancelEdit,
    });
    const textarea = screen.getByLabelText("メッセージを編集");
    fireEvent.keyDown(textarea, { key: "Enter", isComposing: true });
    fireEvent.keyDown(textarea, { key: "Escape", isComposing: true });
    // isComposing が立たないブラウザ向けの keyCode 229 も同じく奪わない。
    fireEvent.keyDown(textarea, { key: "Enter", keyCode: 229 });
    expect(onSubmitEdit).not.toHaveBeenCalled();
    expect(onCancelEdit).not.toHaveBeenCalled();
  });

  it("IME変換中に保存ボタンを押しても、compositionendの確定値を保存する", () => {
    let draft = "編集前";
    const saved = vi.fn();
    const onEditDraftChange = vi.fn((next: string) => {
      draft = next;
    });
    renderItem(makeMessage({ content: draft }), {
      editing: true,
      editDraft: draft,
      onEditDraftChange,
      onSubmitEdit: () => saved(draft),
    });
    const textarea = screen.getByLabelText(
      "メッセージを編集",
    ) as HTMLTextAreaElement;

    fireEvent.compositionStart(textarea);
    // Safari/ソフトキーボードではReact stateより先にDOM値だけが確定し得る。
    const valueSetter = Object.getOwnPropertyDescriptor(
      HTMLTextAreaElement.prototype,
      "value",
    )?.set;
    if (!valueSetter) throw new Error("textarea value setter was not found");
    valueSetter.call(textarea, "変換を確定した本文");
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    fireEvent.compositionEnd(textarea);

    expect(onEditDraftChange).toHaveBeenCalledWith("変換を確定した本文");
    expect(saved).toHaveBeenCalledWith("変換を確定した本文");
  });

  it("Shift+Enterと未閉鎖のコードフェンス内では保存しない", () => {
    const onSubmitEdit = vi.fn();
    renderItem(makeMessage({ content: "```ts" }), {
      editing: true,
      editDraft: "```ts",
      onSubmitEdit,
    });
    const textarea = screen.getByLabelText("メッセージを編集");
    fireEvent.keyDown(textarea, { key: "Enter", shiftKey: true });
    expect(onSubmitEdit).not.toHaveBeenCalled();
    fireEvent.keyDown(textarea, { key: "Enter" });
    expect(onSubmitEdit).not.toHaveBeenCalled();
  });

  it("空にして保存しようとしたら削除ではなく取消として扱う", () => {
    const onSubmitEdit = vi.fn();
    const onCancelEdit = vi.fn();
    renderItem(makeMessage({ content: "編集前" }), {
      editing: true,
      editDraft: "   ",
      onSubmitEdit,
      onCancelEdit,
    });
    fireEvent.click(screen.getByText("保存"));
    expect(onSubmitEdit).not.toHaveBeenCalled();
    expect(onCancelEdit).toHaveBeenCalledTimes(1);
  });
});

describe("リアクションの選択", () => {
  it("直近使用の3つがピッカー無しでチップに並ぶ", () => {
    noteEmojiUsed("🔥");
    noteEmojiUsed("🚀");
    const onToggleReaction = vi.fn();
    renderItem(makeMessage(), { onToggleReaction });
    // 直近が先頭。足りない分は既定で埋める。
    expect(screen.getByLabelText("進める でリアクション")).toBeVisible();
    expect(screen.getByLabelText("熱い でリアクション")).toBeVisible();
    fireEvent.click(screen.getByLabelText("進める でリアクション"));
    expect(onToggleReaction).toHaveBeenCalledWith(
      expect.objectContaining({ messageId: "m1" }),
      "🚀",
    );
  });

  it("使った絵文字が直近の先頭へ来る", () => {
    const { unmount } = renderItem(makeMessage());
    fireEvent.click(screen.getByLabelText("完了 でリアクション"));
    unmount();
    cleanup();
    renderItem(makeMessage());
    const quick = screen
      .getAllByRole("button")
      .filter((node) =>
        node.getAttribute("aria-label")?.endsWith("でリアクション"),
      );
    expect(quick[0]).toHaveAccessibleName("完了 でリアクション");
  });

  it("自分のリアクションを外すときは直近の並びを変えない", () => {
    noteEmojiUsed("✅");
    noteEmojiUsed("🔥");
    const onToggleReaction = vi.fn();
    renderItem(
      makeMessage({
        reactions: [{ emoji: "✅", participants: [human] }],
      }),
      { onToggleReaction },
    );

    fireEvent.click(screen.getByLabelText("完了 でリアクション"));

    expect(recentEmojis()).toEqual(["🔥", "✅"]);
    expect(onToggleReaction).toHaveBeenCalledWith(
      expect.objectContaining({ messageId: "m1" }),
      "✅",
    );
  });

  it("ピッカーは検索・カテゴリ・最近から選べる", async () => {
    noteEmojiUsed("🐛");
    const onToggleReaction = vi.fn();
    renderItem(makeMessage(), { onToggleReaction });
    fireEvent.click(screen.getByLabelText("絵文字を追加"));
    const search = await screen.findByLabelText("絵文字を検索");
    expect(screen.getByText("最近使った絵文字")).toBeVisible();
    expect(screen.getByTitle("顔・気持ち")).toBeVisible();
    fireEvent.change(search, { target: { value: "祝" } });
    const found = await screen.findByTitle("祝う");
    fireEvent.click(found);
    expect(onToggleReaction).toHaveBeenCalledWith(
      expect.objectContaining({ messageId: "m1" }),
      "🎉",
    );
  });

  it("見つからない検索語では素直に見つからないと言う", async () => {
    renderItem(makeMessage());
    fireEvent.click(screen.getByLabelText("絵文字を追加"));
    const search = await screen.findByLabelText("絵文字を検索");
    fireEvent.change(search, { target: { value: "zzzznotfound" } });
    expect(await screen.findByText("見つかりませんでした")).toBeVisible();
  });
});

describe("操作対象の固定", () => {
  it("ピッカーを開いている間、その行が対象を握り続ける", async () => {
    const { row } = renderItem(makeMessage());
    fireEvent.click(screen.getByLabelText("絵文字を追加"));
    await screen.findByLabelText("絵文字を検索");
    await waitFor(() => expect(lockedMessageId()).toBe("m1"));
    // 握っている行はホバーを待たずにハイライトと操作チップを出し続ける。
    expect(row.className).toContain("bg-accent");
    expect(screen.getByLabelText("返信").closest("div")?.className).toContain(
      "opacity-100",
    );
  });

  it("他の行がパネルを開いている間は自分のチップを出さない", () => {
    lockMessageActions("other");
    const { row } = renderItem(makeMessage({ messageId: "m1" }));
    expect(screen.queryByLabelText("返信")).toBeNull();
    expect(row.className).not.toContain("hover:bg-accent");
  });

  it("行が消えるときは対象の固定を手放す", async () => {
    const view = renderItem(makeMessage());
    fireEvent.click(screen.getByLabelText("絵文字を追加"));
    await screen.findByLabelText("絵文字を検索");
    await waitFor(() => expect(lockedMessageId()).toBe("m1"));
    view.unmount();
    expect(lockedMessageId()).toBeNull();
  });
});
