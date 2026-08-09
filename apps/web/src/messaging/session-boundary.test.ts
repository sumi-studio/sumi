import { afterEach, describe, expect, it, vi } from "vitest";
import { MockMessagingServer } from "./mock-server";
import {
  bindMessagingSessionIdentity,
  getMessagingSessionIdentity,
  installMessagingBackend,
  refreshMessagingMemberProfiles,
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
          voice: false,
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
            attachments: [],
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

  it("ignores a bootstrap that completes after the session changes", async () => {
    const serverA = new MockMessagingServer();
    const serverB = new MockMessagingServer();
    const baseA = await serverA.bootstrap();
    const baseB = await serverB.bootstrap();
    let resolveA: (snapshot: typeof baseA) => void = () => {};
    const pendingA = new Promise<typeof baseA>((resolve) => {
      resolveA = resolve;
    });
    vi.spyOn(serverA, "bootstrap").mockReturnValue(pendingA);
    vi.spyOn(serverB, "bootstrap").mockResolvedValue({
      ...baseB,
      self: { kind: "human", humanId: "human-b" },
    });
    const subscribeB = vi.spyOn(serverB, "subscribe");

    bindMessagingSessionIdentity("human-a");
    installMessagingBackend(serverA);
    useMessaging.getState().init();

    bindMessagingSessionIdentity("human-b");
    installMessagingBackend(serverB);
    useMessaging.getState().init();
    await vi.waitFor(() =>
      expect(useMessaging.getState()).toMatchObject({
        ready: true,
        selfKey: "human:human-b",
      }),
    );

    resolveA({
      ...baseA,
      self: { kind: "human", humanId: "human-a" },
    });
    await pendingA;
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(useMessaging.getState()).toMatchObject({
      ready: true,
      selfKey: "human:human-b",
    });
    expect(subscribeB).toHaveBeenCalledTimes(1);
  });

  it("atomically refreshes Human and contextual agent presentation profiles", async () => {
    bindMessagingSessionIdentity("human-a");
    const server = new MockMessagingServer();
    const snapshot = await server.bootstrap();
    vi.spyOn(server, "bootstrap").mockResolvedValue({
      ...snapshot,
      self: { kind: "human", humanId: "human-a" },
      members: [
        {
          participant: { kind: "human", humanId: "human-a" },
          displayName: "After",
          tagline: "",
        },
        {
          participant: {
            kind: "personality_agent",
            personalityAgentId: "agent-a",
          },
          displayName: "Sumi（After）",
          tagline: "",
        },
      ],
    });
    installMessagingBackend(server);
    const messagesByPlace = {
      "channel:private-a": [
        {
          messageId: "message-a",
          place: { kind: "channel" as const, channelId: "private-a" },
          seq: 1,
          author: { kind: "human" as const, humanId: "human-a" },
          content: "A only",
          mentions: [],
          urgency: "normal" as const,
          reactions: [],
          attachments: [],
          replyTo: null,
          createdAt: 1,
          editedAt: null,
          deleted: false,
        },
      ],
    };
    useMessaging.setState({
      ready: true,
      self: { kind: "human", humanId: "human-a" },
      selfKey: "human:human-a",
      membersByKey: {
        "human:human-a": {
          participant: { kind: "human", humanId: "human-a" },
          displayName: "Before",
          tagline: "",
        },
        "personality_agent:agent-a": {
          participant: {
            kind: "personality_agent",
            personalityAgentId: "agent-a",
          },
          displayName: "Sumi（Before）",
          tagline: "",
        },
      },
      messagesByPlace,
    });

    await refreshMessagingMemberProfiles();

    expect(useMessaging.getState().membersByKey).toMatchObject({
      "human:human-a": { displayName: "After" },
      "personality_agent:agent-a": { displayName: "Sumi（After）" },
    });
    expect(useMessaging.getState().messagesByPlace).toBe(messagesByPlace);
  });
});
