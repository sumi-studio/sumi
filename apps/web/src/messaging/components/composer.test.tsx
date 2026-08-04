// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ChannelSummary, MemberProfile, ParticipantRef } from "../model";
import { participantKey } from "../model";
import { useMessaging } from "../store";
import { Composer } from "./composer";

const mocks = vi.hoisted(() => ({
  send: vi.fn(),
  sendTyping: vi.fn(),
  uploadAttachment: vi.fn(),
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

function composer() {
  return screen.getByRole<HTMLTextAreaElement>("textbox", {
    name: "#general へメッセージ",
  });
}

function openPlusMenu() {
  fireEvent.click(screen.getByRole("button", { name: "作成メニューを開く" }));
}

beforeEach(() => {
  useMessaging.setState({
    ready: true,
    self: human,
    selfKey: humanKey,
    activePlaceKey: `channel:${channel.channelId}`,
    channels: [channel],
    dms: [],
    draftByPlace: {},
    messagesByPlace: {},
    editingMessageId: null,
    replyTargetId: null,
    membersByKey: Object.fromEntries(
      members.map((member) => [participantKey(member.participant), member]),
    ),
    send: mocks.send,
    sendTyping: mocks.sendTyping,
    uploadAttachment: mocks.uploadAttachment,
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
    expect(mocks.send).toHaveBeenCalledWith("こんにちは", "normal", []);
  });

  it("空白だけの入力では送れない", () => {
    render(<Composer />);
    fireEvent.change(composer(), { target: { value: "   " } });

    expect(screen.getByRole("button", { name: "送信" })).toBeDisabled();
    fireEvent.click(screen.getByRole("button", { name: "送信" }));
    expect(mocks.send).not.toHaveBeenCalled();
  });

  it("Enterで送信する説明は残す", () => {
    render(<Composer />);
    expect(screen.getByText(/Enterで送信/)).toBeInTheDocument();
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

  it("メンションを選ぶと @ が入り、候補から選んで入力欄に挿入できる", async () => {
    render(<Composer />);
    fireEvent.change(composer(), { target: { value: "おはよう" } });
    // カーソルは末尾にある想定（changeの後の既定位置）。
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

  it("未実装の作成導線は席だけ用意して選べない状態で置く", () => {
    render(<Composer />);
    openPlusMenu();

    expect(
      screen.getByRole("button", { name: /スレッドを作成/ }),
    ).toBeDisabled();
    expect(screen.getByRole("button", { name: /投票を作成/ })).toBeDisabled();
  });
});
