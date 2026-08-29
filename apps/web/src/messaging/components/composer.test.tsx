// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type {
  ChannelSummary,
  MemberProfile,
  Message,
  ParticipantRef,
  PollInput,
  Urgency,
} from "../model";
import { participantKey } from "../model";
import { useMessaging } from "../store";
import { Composer } from "./composer";

const mocks = vi.hoisted(() => ({
  send: vi.fn(),
  sendTyping: vi.fn(),
  submitEdit: vi.fn(),
}));

const human: ParticipantRef = { kind: "human", humanId: "h1" };
const agent: ParticipantRef = {
  kind: "personality_agent",
  personalityAgentId: "a1",
};
const secondAgent: ParticipantRef = {
  kind: "personality_agent",
  personalityAgentId: "a2",
};
const thirdAgent: ParticipantRef = {
  kind: "personality_agent",
  personalityAgentId: "a3",
};
const humanKey = participantKey(human);

const members: MemberProfile[] = [
  { participant: human, displayName: "余白", tagline: "創業・デザイン" },
  { participant: agent, displayName: "墨", tagline: "秘書" },
];

const channel: ChannelSummary = {
  channelId: "c1",
  workspaceId: "w1",
  revision: 1,
  name: "general",
  topic: "",
  visibility: "public",
  voice: false,
};

const placeKey = `channel:${channel.channelId}` as const;

function composer() {
  return screen.getByRole<HTMLTextAreaElement>("textbox", {
    name: "#general へメッセージ",
  });
}

function openPlusMenu() {
  fireEvent.click(screen.getByRole("button", { name: "作成メニューを開く" }));
}

function fillPoll(question = "いつにしますか？") {
  fireEvent.change(screen.getByLabelText("質問"), {
    target: { value: question },
  });
  fireEvent.change(screen.getByLabelText("選択肢 1"), {
    target: { value: "今日" },
  });
  fireEvent.change(screen.getByLabelText("選択肢 2"), {
    target: { value: "明日" },
  });
}

function ownMessage(content: string): Message {
  return {
    messageId: "m1",
    place: { kind: "channel", channelId: channel.channelId },
    seq: 1,
    author: human,
    content,
    mentions: [],
    urgency: "normal",
    reactions: [],
    attachments: [],
    poll: null,
    replyTo: null,
    createdAt: 0,
    editedAt: null,
    deleted: false,
  };
}

beforeEach(() => {
  useMessaging.setState({
    self: human,
    selfKey: humanKey,
    activePlaceKey: placeKey,
    channels: [channel],
    dms: [],
    draftByPlace: {},
    messagesByPlace: {},
    draftAttachmentsByPlace: {},
    draftAttachmentOverflowByPlace: {},
    capabilities: {
      ...useMessaging.getState().capabilities,
      polls: true,
    },
    pollVoteByMessage: {},
    editingMessageId: null,
    replyTargetId: null,
    membersByKey: Object.fromEntries(
      members.map((member) => [participantKey(member.participant), member]),
    ),
    // 本物のsendはdraftを空にする（store.ts）。送信直後にボタンがdisabledへ
    // 変わる経路まで再現しないと、フォーカスの行き先を検査できない。
    send: (content: string, urgency: Urgency, poll?: PollInput | null) => {
      if (poll) mocks.send(content, urgency, poll);
      else mocks.send(content, urgency);
      useMessaging.setState((state) => ({
        draftByPlace: { ...state.draftByPlace, [placeKey]: "" },
        replyTargetId: null,
      }));
      return true;
    },
    sendTyping: mocks.sendTyping,
    submitEdit: mocks.submitEdit,
  });
});

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

describe("Composer 送信ボタン", () => {
  it("ArrowDownを2回押してkeyupを通っても3番目の候補をEnterで選ぶ", () => {
    useMessaging.setState({
      membersByKey: Object.fromEntries(
        [
          ...members,
          { participant: secondAgent, displayName: "綾", tagline: "編集" },
          { participant: thirdAgent, displayName: "凛", tagline: "分析" },
        ].map((member) => [participantKey(member.participant), member]),
      ),
    });
    render(<Composer />);
    const input = composer();

    fireEvent.change(input, { target: { value: "@" } });
    fireEvent.keyDown(input, { key: "ArrowDown" });
    fireEvent.keyUp(input, { key: "ArrowDown" });
    fireEvent.keyDown(input, { key: "ArrowDown" });
    fireEvent.keyUp(input, { key: "ArrowDown" });
    fireEvent.keyDown(input, { key: "Enter" });
    fireEvent.keyUp(input, { key: "Enter" });

    expect(input).toHaveValue("@凛 ");
  });

  it("空入力では無効で、文字を入れるとクリックだけで送れる", () => {
    render(<Composer />);
    const button = screen.getByRole("button", { name: "送信" });
    expect(button).toBeDisabled();

    fireEvent.change(composer(), { target: { value: "こんにちは" } });

    expect(button).toBeEnabled();
    fireEvent.click(button);
    expect(mocks.send).toHaveBeenCalledWith("こんにちは", "normal");
  });

  it("空白だけの入力では送れない", () => {
    render(<Composer />);
    fireEvent.change(composer(), { target: { value: "   " } });

    const button = screen.getByRole("button", { name: "送信" });
    expect(button).toBeDisabled();
    fireEvent.click(button);
    expect(mocks.send).not.toHaveBeenCalled();
  });

  it("キーボードで押しても送信後のフォーカスは入力欄に戻る", () => {
    render(<Composer />);
    fireEvent.change(composer(), { target: { value: "やあ" } });
    const button = screen.getByRole("button", { name: "送信" });
    // Tabで辿り着いてSpace/Enterで押す経路。mousedownは発火しない。
    button.focus();
    expect(button).toHaveFocus();

    fireEvent.click(button);

    expect(mocks.send).toHaveBeenCalledWith("やあ", "normal");
    // 送信でdraftが空になりボタンはdisabledへ変わる。焦点が消えた要素に
    // 残らないよう、submitが入力欄へ戻す。
    expect(button).toBeDisabled();
    expect(composer()).toHaveFocus();
  });

  it("変換中に押されたら、いま見えている文字を確定して送る", () => {
    // ボタンには「変換を確定する」という仕事が無い。ここで止めると、画面が何も
    // 変わらず理由も出ないまま enabled で居続ける死んだボタンになる。
    const blur = vi.spyOn(HTMLTextAreaElement.prototype, "blur");
    render(<Composer />);
    const input = composer();

    // 変換中もブラウザはinputを発火するので、いま見えている読みがdraftに載る。
    fireEvent.compositionStart(input);
    fireEvent.change(input, { target: { value: "かんじ" } });
    expect(screen.getByRole("button", { name: "送信" })).toBeEnabled();

    fireEvent.click(screen.getByRole("button", { name: "送信" }));

    expect(mocks.send).toHaveBeenCalledWith("かんじ", "normal");
    // 変換は終わらせる。残したままだと確定分が空になった入力欄へ戻ってくる。
    expect(blur).toHaveBeenCalled();
    blur.mockRestore();
  });

  it("blurが変換を確定させる環境では、確定後の値が送られる", () => {
    // Chromeデスクトップの順序（blur → 変換確定 → compositionend）をblurの中で
    // 再現する。このときReactの値はまだ確定前の読みなので、入力欄の中身を正と
    // しないと未変換の読みが送られてしまう。
    render(<Composer />);
    const input = composer();
    const blur = vi
      .spyOn(HTMLTextAreaElement.prototype, "blur")
      .mockImplementation(function commit(this: HTMLTextAreaElement) {
        fireEvent.compositionEnd(this, { target: { value: "漢字" } });
      });

    fireEvent.compositionStart(input);
    fireEvent.change(input, { target: { value: "かんじ" } });
    fireEvent.click(screen.getByRole("button", { name: "送信" }));

    expect(mocks.send).toHaveBeenCalledWith("漢字", "normal");
    expect(mocks.send).not.toHaveBeenCalledWith("かんじ", "normal");
    blur.mockRestore();
  });

  it("変換が確定した後は確定後の値を送る", () => {
    render(<Composer />);
    const input = composer();
    fireEvent.compositionStart(input);
    fireEvent.change(input, { target: { value: "かんじ" } });
    fireEvent.compositionEnd(input, { target: { value: "漢字" } });

    fireEvent.click(screen.getByRole("button", { name: "送信" }));

    expect(mocks.send).toHaveBeenCalledWith("漢字", "normal");
  });

  it("変換中のEnterはIMEのものなので送らない", () => {
    // Enterはブラウザ側で変換確定に使われる。そこに送信を重ねない。
    render(<Composer />);
    const input = composer();
    fireEvent.compositionStart(input);
    fireEvent.change(input, { target: { value: "かんじ" } });

    fireEvent.keyDown(input, { key: "Enter", isComposing: true, keyCode: 229 });

    expect(mocks.send).not.toHaveBeenCalled();
  });

  it("変換中でないEnterは既定を止めて送る", () => {
    render(<Composer />);
    const input = composer();
    fireEvent.change(input, { target: { value: "こんばんは" } });

    // preventDefaultされていれば改行は入らない。fireEventはfalseを返す。
    expect(fireEvent.keyDown(input, { key: "Enter" })).toBe(false);
    expect(mocks.send).toHaveBeenCalledWith("こんばんは", "normal");
  });

  it("Shift+Enterは改行なので送らない", () => {
    render(<Composer />);
    const input = composer();
    fireEvent.change(input, { target: { value: "つづき" } });

    expect(fireEvent.keyDown(input, { key: "Enter", shiftKey: true })).toBe(
      true,
    );
    expect(mocks.send).not.toHaveBeenCalled();
  });

  it("未閉鎖のコードブロックの中ではEnterを改行に譲る", () => {
    render(<Composer />);
    const input = composer();
    fireEvent.change(input, { target: { value: "```ts\nconst a = 1" } });
    input.setSelectionRange(input.value.length, input.value.length);

    // 既定を止めないので改行が入る。送信もしない。
    expect(fireEvent.keyDown(input, { key: "Enter" })).toBe(true);
    expect(mocks.send).not.toHaveBeenCalled();
  });

  it("空欄の↑は削除済みを飛ばして、本文の残る自分の直前の発言を編集する", () => {
    const startEdit = vi.fn();
    const older = { ...ownMessage("まだ残っている発言"), messageId: "m0" };
    const tombstone: Message = {
      ...ownMessage(""),
      messageId: "m1",
      seq: 2,
      deleted: true,
    };
    useMessaging.setState({
      messagesByPlace: { [placeKey]: [older, tombstone] },
      startEdit,
    });
    render(<Composer />);

    fireEvent.keyDown(composer(), { key: "ArrowUp" });

    expect(startEdit).toHaveBeenCalledWith("m0");
  });

  it("行内編集中でもcomposerは下書きの送信経路を保つ", () => {
    useMessaging.setState({
      editingMessageId: "m1",
      messagesByPlace: { [placeKey]: [ownMessage("もとの本文")] },
    });
    render(<Composer />);
    fireEvent.change(composer(), { target: { value: "別の投稿" } });

    expect(fireEvent.keyDown(composer(), { key: "Enter" })).toBe(false);
    expect(mocks.send).toHaveBeenCalledWith("別の投稿", "normal");
    expect(mocks.submitEdit).not.toHaveBeenCalled();
  });

  it("添付の準備が終わるまで押せない（Enter送信と同じ判定）", () => {
    useMessaging.setState({
      draftAttachmentsByPlace: {
        [placeKey]: [
          {
            clientNonce: "n1",
            filename: "a.png",
            contentType: "image/png",
            sizeBytes: 10,
            status: "uploading",
          },
        ],
      },
    });
    render(<Composer />);
    fireEvent.change(composer(), { target: { value: "どうぞ" } });

    expect(screen.getByRole("button", { name: "送信" })).toBeDisabled();
  });

  it("行内編集中でもcomposerは送信ボタンのまま", () => {
    useMessaging.setState({
      editingMessageId: "m1",
      messagesByPlace: { [placeKey]: [ownMessage("もとの本文")] },
    });
    render(<Composer />);

    fireEvent.change(composer(), { target: { value: "別の投稿" } });
    fireEvent.click(screen.getByRole("button", { name: "送信" }));

    expect(mocks.send).toHaveBeenCalledWith("別の投稿", "normal");
  });

  it("行内編集中でも空のcomposerは送信しない", () => {
    useMessaging.setState({
      editingMessageId: "m1",
      messagesByPlace: { [placeKey]: [ownMessage("もとの本文")] },
    });
    render(<Composer />);
    fireEvent.change(composer(), { target: { value: "  " } });

    expect(screen.getByRole("button", { name: "送信" })).toBeDisabled();
    fireEvent.keyDown(composer(), { key: "Enter" });
    expect(mocks.submitEdit).not.toHaveBeenCalled();
    expect(mocks.send).not.toHaveBeenCalled();
    expect(useMessaging.getState().editingMessageId).toBe("m1");
  });
});

describe("Composer ＋メニュー", () => {
  it("添付は＋メニューの中にあり、選ぶとファイル選択が開く", () => {
    const click = vi.spyOn(HTMLInputElement.prototype, "click");
    render(<Composer />);
    // クリップ単独のボタンは無くなり、入口は＋に集約されている。
    expect(
      screen.queryByRole("button", { name: "ファイルを添付" }),
    ).not.toBeInTheDocument();

    openPlusMenu();
    fireEvent.click(screen.getByRole("button", { name: /ファイルを添付/ }));

    expect(click).toHaveBeenCalled();
    click.mockRestore();
  });

  it("添付が上限に達していると添付の項目を選べない", () => {
    useMessaging.setState({
      draftAttachmentsByPlace: {
        [placeKey]: Array.from({ length: 10 }, (_unused, index) => ({
          clientNonce: `n${index}`,
          filename: `a${index}.png`,
          contentType: "image/png",
          sizeBytes: 10,
          status: "ready" as const,
        })),
      },
    });
    render(<Composer />);
    openPlusMenu();

    const attach = screen.getByRole("button", { name: /ファイルを添付/ });
    expect(attach).toBeDisabled();
    // 押しても何も起きず、メニューも閉じない（選べなかったことが見える）。
    // jsdomにはレイアウトが無くBase UIが位置決めまでvisibilityを伏せるため、
    // 可視判定ではなく在否で見る。
    fireEvent.click(attach);
    expect(
      screen.getByRole("button", { name: /メンション/ }),
    ).toBeInTheDocument();
  });

  it("メンションを選ぶと @ が入り、候補から選んで入力欄に挿入できる", async () => {
    render(<Composer />);
    fireEvent.change(composer(), { target: { value: "おはよう" } });
    composer().setSelectionRange(4, 4);

    openPlusMenu();
    fireEvent.click(screen.getByRole("button", { name: /メンション/ }));

    await vi.waitFor(() => {
      expect(composer()).toHaveValue("おはよう @");
    });
    // 自分は候補に出さない。
    expect(
      screen.queryByRole("button", { name: /余白/ }),
    ).not.toBeInTheDocument();

    fireEvent.mouseDown(screen.getByRole("button", { name: /墨/ }));

    expect(composer()).toHaveValue("おはよう @墨 ");
  });

  it("文の途中で選ぶとカーソル位置に差し込み、キャレットも@の後に置く", async () => {
    render(<Composer />);
    fireEvent.change(composer(), { target: { value: "おはよう、よろしく" } });
    // 「おはよう、」の直後。直前が文字なので " @" と空白を補う。
    composer().setSelectionRange(5, 5);

    openPlusMenu();
    fireEvent.click(screen.getByRole("button", { name: /メンション/ }));

    await vi.waitFor(() => {
      expect(composer()).toHaveValue("おはよう、 @よろしく");
    });
    // rAFでフォーカスとキャレットを戻すところまで見る（値だけでは通ってしまう）。
    await vi.waitFor(() => {
      expect(composer()).toHaveFocus();
      expect(composer().selectionStart).toBe(7);
    });

    // 挿入した @ が語頭として拾われ、候補が開いている。
    fireEvent.mouseDown(screen.getByRole("button", { name: /墨/ }));
    expect(composer()).toHaveValue("おはよう、 @墨 よろしく");
  });

  it("空の入力から選ぶと先頭に @ だけを入れる", async () => {
    render(<Composer />);
    openPlusMenu();
    fireEvent.click(screen.getByRole("button", { name: /メンション/ }));

    await vi.waitFor(() => {
      expect(composer()).toHaveValue("@");
    });
    await vi.waitFor(() => {
      expect(composer().selectionStart).toBe(1);
    });
  });

  it("Escapeで閉じるとフォーカスは入力欄に戻る", async () => {
    render(<Composer />);
    openPlusMenu();
    const attach = await screen.findByRole("button", {
      name: /ファイルを添付/,
    });
    // jsdomにはレイアウトが無くinitialFocusが働かないので、キーボードで項目まで
    // 進んだ状態を作る。ここを作らないと入力欄が焦点を持ったままになり、
    // finalFocusが無くてもテストが通ってしまう（既定は開いたトリガーへ戻す）。
    attach.focus();
    expect(attach).toHaveFocus();

    fireEvent.keyDown(attach, { key: "Escape" });

    await vi.waitFor(() => {
      expect(
        screen.queryByRole("button", { name: /ファイルを添付/ }),
      ).not.toBeInTheDocument();
    });
    // finalFocusで入力欄へ返す。入力の続きを妨げない。
    await vi.waitFor(() => {
      expect(composer()).toHaveFocus();
    });
  });

  it("行内編集中でもcomposerの＋メニューは使える", () => {
    useMessaging.setState({
      editingMessageId: "m1",
      messagesByPlace: { [placeKey]: [ownMessage("もとの本文")] },
    });
    render(<Composer />);

    expect(
      screen.getByRole("button", { name: "作成メニューを開く" }),
    ).toBeInTheDocument();
  });
});

describe("Composer 投票", () => {
  it("＋メニューから投票を開き、本文・緊急度・返信先と一緒に送る", async () => {
    const replied = {
      ...ownMessage(""),
      messageId: "reply-parent",
      poll: {
        question: "親投票の質問",
        allowMulti: false,
        closesAt: null,
        revision: 4,
        options: [
          { optionId: "yes", text: "はい", voters: [] },
          { optionId: "no", text: "いいえ", voters: [] },
        ],
      },
    };
    useMessaging.setState({
      messagesByPlace: { [placeKey]: [replied] },
      replyTargetId: replied.messageId,
      draftByPlace: { [placeKey]: "日程を決めます" },
    });
    render(<Composer />);

    expect(screen.getByText("親投票の質問")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "急ぎ" }));
    openPlusMenu();
    fireEvent.click(screen.getByRole("button", { name: /投票を作成/ }));
    await vi.waitFor(() => expect(screen.getByLabelText("質問")).toHaveFocus());
    fillPoll();
    fireEvent.click(
      screen.getByRole("button", { name: "複数選べるようにする" }),
    );
    fireEvent.click(screen.getByRole("button", { name: "投票を送信" }));

    expect(mocks.send).toHaveBeenCalledWith("日程を決めます", "urgent", {
      question: "いつにしますか？",
      options: ["今日", "明日"],
      allowMulti: true,
      closesAt: null,
    });
    expect(screen.getByRole("button", { name: "普通" })).toHaveAttribute(
      "aria-pressed",
      "true",
    );
    expect(useMessaging.getState().replyTargetId).toBeNull();
  });

  it("capability または添付がある間は理由を示して投票下書きを開かない", () => {
    useMessaging.setState({
      draftByPlace: { [placeKey]: "残す本文" },
      draftAttachmentsByPlace: {
        [placeKey]: [
          {
            clientNonce: "attachment-1",
            filename: "agenda.pdf",
            contentType: "application/pdf",
            sizeBytes: 1,
            status: "ready",
          },
        ],
      },
    });
    const { unmount } = render(<Composer />);
    openPlusMenu();
    const attached = screen.getByRole("button", { name: /投票を作成/ });
    expect(attached).toBeDisabled();
    expect(attached).toHaveTextContent("添付を外すと作成できます");
    fireEvent.click(attached);
    expect(screen.queryByRole("dialog", { name: "投票を作成" })).toBeNull();
    expect(useMessaging.getState().draftByPlace[placeKey]).toBe("残す本文");

    unmount();
    useMessaging.setState((state) => ({
      capabilities: { ...state.capabilities, polls: false },
      draftAttachmentsByPlace: {},
    }));
    render(<Composer />);
    openPlusMenu();
    const unavailable = screen.getByRole("button", { name: /投票を作成/ });
    expect(unavailable).toBeDisabled();
    expect(unavailable).toHaveTextContent("この接続では利用できません");
  });

  it("enqueue が拒否したときは緊急度と投票ダイアログを保つ", () => {
    useMessaging.setState({
      send: () => false,
      draftByPlace: { [placeKey]: "本文" },
    });
    render(<Composer />);
    fireEvent.click(screen.getByRole("button", { name: "急ぎ" }));
    openPlusMenu();
    fireEvent.click(screen.getByRole("button", { name: /投票を作成/ }));
    fillPoll();
    fireEvent.click(screen.getByRole("button", { name: "投票を送信" }));

    expect(screen.getByRole("dialog", { name: "投票を作成" })).toBeVisible();
    expect(screen.getByRole("button", { name: "急ぎ" })).toHaveAttribute(
      "aria-pressed",
      "true",
    );
    expect(useMessaging.getState().draftByPlace[placeKey]).toBe("本文");
  });
});
