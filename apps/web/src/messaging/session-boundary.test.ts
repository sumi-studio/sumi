import { afterEach, describe, expect, it } from "vitest";
import {
  bindMessagingSessionIdentity,
  getMessagingSessionIdentity,
  useMessaging,
} from "./store";

describe("messaging session boundary", () => {
  afterEach(() => bindMessagingSessionIdentity(null));

  it("disposes private state before a different signed-in human can render", () => {
    bindMessagingSessionIdentity("human-a");
    useMessaging.setState({
      ready: true,
      self: { kind: "human", humanId: "human-a" },
      selfKey: "human:human-a",
      channels: [
        {
          channelId: "private-a",
          workspaceId: "workspace",
          name: "A",
          topic: "",
          visibility: "private",
        },
      ],
      messagesByPlace: {
        "channel:private-a": [
          {
            messageId: "message-a",
            place: { kind: "channel", channelId: "private-a" },
            seq: 1,
            author: { kind: "human", humanId: "human-a" },
            content: "A only",
            mentions: [],
            urgency: "normal",
            reactions: [],
            replyTo: null,
            createdAt: 1,
            editedAt: null,
            deleted: false,
          },
        ],
      },
    });

    bindMessagingSessionIdentity(null);
    bindMessagingSessionIdentity("human-b");

    expect(getMessagingSessionIdentity()).toBe("human-b");
    expect(useMessaging.getState()).toMatchObject({
      ready: false,
      self: null,
      selfKey: "",
      channels: [],
      messagesByPlace: {},
      activePlaceKey: null,
      connection: "disconnected",
    });
  });
});
