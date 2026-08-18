// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type {
  ChannelSummary,
  MemberProfile,
  Message,
  ParticipantRef,
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
const humanKey = participantKey(human);

const members: MemberProfile[] = [
  { participant: human, displayName: "余白", tagline: "創業・デザイン" },
  { participant: agent, displayName: "墨", tagline: "秘書" },
];

const channel: ChannelSummary = {
  channelId: "c1",
  workspaceId: "w1",
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
    editingMessageId: null,
    replyTargetId: null,
    membersByKey: Object.fromEntries(
      members.map((member) => [participantKey(member.participant), member]),
    ),
    // 本物のsendはdraftを空にする（store.ts）。送信直後にボタンがdisabledへ
    // 変わる経路まで再現しないと、フォーカスの行き先を検査できない。
    send: (content: string, urgency: Urgency) => {
      mocks.send(content, urgency);
      useMessaging.setState((state) => ({
        draftByPlace: { ...state.draftByPlace, [placeKey]: "" },
      }));
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

  it("IME変換中はボタンでも送らない（Enter経路と同じ規律）", () => {
    render(<Composer />);
    const input = composer();

    // 変換中もChromeはinputを発火するので、未変換の読みがdraftに載る。
    fireEvent.compositionStart(input);
    fireEvent.change(input, { target: { value: "かんじ" } });
    fireEvent.click(screen.getByRole("button", { name: "送信" }));
    expect(mocks.send).not.toHaveBeenCalled();

    // 確定すれば同じボタンで送れる。送るのは確定後の値。
    fireEvent.compositionEnd(input, { target: { value: "漢字" } });
    fireEvent.click(screen.getByRole("button", { name: "送信" }));
    expect(mocks.send).toHaveBeenCalledWith("漢字", "normal");
  });

  it("IME変換中はEnterでも送らない", () => {
    render(<Composer />);
    const input = composer();
    fireEvent.compositionStart(input);
    fireEvent.change(input, { target: { value: "かんじ" } });

    fireEvent.keyDown(input, { key: "Enter", isComposing: true, keyCode: 229 });

    expect(mocks.send).not.toHaveBeenCalled();
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

  it("編集中は同じ場所が編集の保存になる", () => {
    useMessaging.setState({
      editingMessageId: "m1",
      messagesByPlace: { [placeKey]: [ownMessage("もとの本文")] },
    });
    render(<Composer />);

    expect(
      screen.queryByRole("button", { name: "送信" }),
    ).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "編集を保存" }));

    expect(mocks.submitEdit).toHaveBeenCalledWith("もとの本文");
  });

  it("編集中に本文を空にするとEnterもボタンも保存せず、理由を出す", () => {
    useMessaging.setState({
      editingMessageId: "m1",
      messagesByPlace: { [placeKey]: [ownMessage("もとの本文")] },
    });
    render(<Composer />);
    fireEvent.change(composer(), { target: { value: "  " } });

    // ボタンは押せず、Enterも同じく何もしない（片方だけ編集を破棄しない）。
    expect(screen.getByRole("button", { name: "編集を保存" })).toBeDisabled();
    fireEvent.keyDown(composer(), { key: "Enter" });
    expect(mocks.submitEdit).not.toHaveBeenCalled();
    expect(useMessaging.getState().editingMessageId).toBe("m1");
    // 押せない理由と、取り消しの明示の口を出す。
    expect(screen.getByText(/Escで取り消し/)).toBeInTheDocument();
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

  it("編集中は＋メニューを出さない", () => {
    useMessaging.setState({
      editingMessageId: "m1",
      messagesByPlace: { [placeKey]: [ownMessage("もとの本文")] },
    });
    render(<Composer />);

    expect(
      screen.queryByRole("button", { name: "作成メニューを開く" }),
    ).not.toBeInTheDocument();
  });
});
