import { afterEach, describe, expect, it, vi } from "vitest";
import { MockMessagingServer } from "./mock-server";
import type { ChannelSummary, DmSummary } from "./model";
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

  it("rejects a deferred DM result after the messaging identity changes", async () => {
    bindMessagingSessionIdentity("human-a");
    const server = new MockMessagingServer();
    let resolveDM!: (dm: DmSummary) => void;
    const deferredDM = new Promise<DmSummary>((resolve) => {
      resolveDM = resolve;
    });
    vi.spyOn(server, "ensureDM").mockReturnValue(deferredDM);
    installMessagingBackend(server);
    useMessaging.setState({
      ready: true,
      self: { kind: "human", humanId: "human-a" },
      selfKey: "human:human-a",
      dms: [],
    });

    const operation = useMessaging
      .getState()
      .startDM([{ kind: "human", humanId: "human-b" }]);
    bindMessagingSessionIdentity(null);
    bindMessagingSessionIdentity("human-b");
    resolveDM({
      dmId: "stale-dm",
      kind: "dm",
      participants: [
        { kind: "human", humanId: "human-a" },
        { kind: "human", humanId: "human-b" },
      ],
    });

    await expect(operation).rejects.toThrow(
      "Messaging session changed during DM start",
    );
    expect(useMessaging.getState().dms).toEqual([]);
  });

  it("creates a channel only in the explicitly named Workspace", async () => {
    bindMessagingSessionIdentity("human-a");
    const server = new MockMessagingServer();
    const create = vi.spyOn(server, "createChannel");
    installMessagingBackend(server);

    await useMessaging
      .getState()
      .createChannel("workspace-explicit", "dev", "開発");

    expect(create).toHaveBeenCalledWith("workspace-explicit", "dev", "開発");
  });

  it("rejects a deferred channel result after the messaging identity changes", async () => {
    bindMessagingSessionIdentity("human-a");
    const server = new MockMessagingServer();
    let resolveChannel!: (channel: ChannelSummary) => void;
    const deferredChannel = new Promise<ChannelSummary>((resolve) => {
      resolveChannel = resolve;
    });
    vi.spyOn(server, "createChannel").mockReturnValue(deferredChannel);
    installMessagingBackend(server);
    useMessaging.setState({
      ready: true,
      self: { kind: "human", humanId: "human-a" },
      selfKey: "human:human-a",
      workspaces: [{ workspaceId: "workspace-a", name: "A" }],
      channels: [],
    });

    const operation = useMessaging
      .getState()
      .createChannel("workspace-a", "private-a", "A only");
    bindMessagingSessionIdentity(null);
    bindMessagingSessionIdentity("human-b");
    resolveChannel({
      channelId: "stale-channel",
      workspaceId: "workspace-a",
      name: "private-a",
      topic: "A only",
      visibility: "private",
    });

    await expect(operation).rejects.toThrow(
      "Messaging session changed during channel creation",
    );
    expect(useMessaging.getState().workspaces).toEqual([]);
    expect(useMessaging.getState().channels).toEqual([]);
  });
});
