// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type {
  ChannelSummary,
  MemberProfile,
  Message,
  ParticipantRef,
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
    send: mocks.send,
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

  it("押しても入力欄からキャレットを奪わない", () => {
    render(<Composer />);
    fireEvent.change(composer(), { target: { value: "やあ" } });

    // mousedownの既定が止まっていれば、フォーカスは入力欄に残る。
    expect(
      fireEvent.mouseDown(screen.getByRole("button", { name: "送信" })),
    ).toBe(false);
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

    expect(
      screen.getByRole("button", { name: /ファイルを添付/ }),
    ).toBeDisabled();
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
