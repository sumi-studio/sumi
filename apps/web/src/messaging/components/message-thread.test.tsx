// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Message } from "../model";
import { bindMessagingSessionIdentity, useMessaging } from "../store";
import { MessageThreadAction } from "./message-thread";

vi.mock("../place-route", () => ({ usePlaceNavigate: () => vi.fn() }));

const message: Message = {
  messageId: "message-1",
  place: { kind: "channel", channelId: "channel-1" },
  seq: 1,
  author: { kind: "human", humanId: "human-a" },
  content: "スレッドにします",
  mentions: [],
  urgency: "normal",
  reactions: [],
  attachments: [],
  replyTo: null,
  createdAt: 1,
  editedAt: null,
  deleted: false,
};

describe("MessageThreadAction", () => {
  beforeEach(() => {
    bindMessagingSessionIdentity("thread-action-test");
    useMessaging.setState({
      capabilities: {
        status: false,
        replyLater: false,
        reactions: false,
        notifications: false,
        threads: false,
      },
      activePlaceKey: "channel:channel-1",
      threadsById: {},
    });
  });

  afterEach(() => {
    cleanup();
    bindMessagingSessionIdentity(null);
  });

  it("does not expose thread creation when threads are unavailable", () => {
    render(<MessageThreadAction message={message} />);

    expect(screen.queryByLabelText("スレッドを作成")).not.toBeInTheDocument();
  });
});
