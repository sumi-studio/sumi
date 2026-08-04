// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type {
  MemberProfile,
  Message,
  ParticipantKey,
  ParticipantRef,
} from "../model";
import { participantKey } from "../model";
import { MessageItem } from "./message-item";

vi.mock("@tanstack/react-router", () => ({
  useNavigate: () => vi.fn(),
}));

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
  return render(
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
      onCopyLink={noop}
      onEdit={noop}
      onDelete={noop}
      onJumpTo={noop}
      onRetry={noop}
      {...props}
    />,
  );
}

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

describe("MessageItem の行の見せ方", () => {
  it("操作チップは対象メッセージの要素の内側に置かれる", () => {
    const { container } = renderItem(makeMessage());
    const row = container.querySelector("[data-message-id='m1']");
    const toolbar = screen.getByLabelText("返信").closest("div");
    expect(row).not.toBeNull();
    expect(row?.contains(toolbar ?? null)).toBe(true);
    // 行の外へはみ出さない（translateで上へ逃がさない）。
    expect(toolbar?.className).not.toContain("-translate-y-1/2");
  });

  it("行にはホバー時のハイライトと左端の目印がある", () => {
    const { container } = renderItem(makeMessage());
    const row = container.querySelector("[data-message-id='m1']");
    expect(row?.className).toContain("hover:bg-accent");
    const marker = row?.querySelector("span[aria-hidden]");
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
