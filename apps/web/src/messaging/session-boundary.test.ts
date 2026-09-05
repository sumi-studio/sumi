import { afterEach, describe, expect, it, vi } from "vitest";
import { MockMessagingServer } from "./mock-server";
import type { ChannelSummary, DmSummary, ThreadSummary } from "./model";
import {
  bindMessagingScope,
  bindMessagingSessionIdentity,
  getMessagingSessionIdentity,
  installMessagingBackend,
  refreshMessagingMemberProfiles,
  useMessaging,
} from "./store";

const MESSAGING_SCOPE = {
  workspaceId: "workspace-1",
  installationId: "installation-1",
  authorityEpoch: "1",
} as const;

describe("messaging session boundary", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    bindMessagingScope(null);
    bindMessagingSessionIdentity(null);
  });

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
          revision: 1,
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
            poll: null,
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

  // Every store action that awaits the backend is fenced the same way: the
  // answer is applied only if it comes back into the session that asked. A
  // place action that outlives a Workspace switch would otherwise put the old
  // Workspace's channel into the new one's sidebar — and, for a duplicate,
  // navigate there.
  it.each([
    {
      name: "duplicateChannel",
      method: "duplicateChannel" as const,
      run: () => useMessaging.getState().duplicateChannel("ch-general"),
    },
    {
      name: "updateChannel",
      method: "updateChannel" as const,
      run: () =>
        useMessaging.getState().updateChannel("ch-general", { name: "設計" }),
    },
  ])("discards the answer to $name after a Workspace switch", async ({
    method,
    run,
  }) => {
    bindMessagingSessionIdentity("human-self");
    bindMessagingScope(MESSAGING_SCOPE);
    const server = new MockMessagingServer();
    let release!: (channel: ChannelSummary) => void;
    const answer = new Promise<ChannelSummary>((resolve) => {
      release = resolve;
    });
    vi.spyOn(server, method).mockImplementation(() => answer);
    installMessagingBackend(server);
    const original: ChannelSummary = {
      channelId: "ch-general",
      workspaceId: "workspace-1",
      revision: 1,
      name: "general",
      topic: "",
      visibility: "public",
      voice: false,
    };
    useMessaging.setState({
      ready: true,
      self: { kind: "human", humanId: "self" },
      selfKey: "human:self",
      channels: [original],
    });

    const pending = run();
    // 別のWorkspaceへ移る。ここで前のsessionのstateは捨てられる。
    bindMessagingScope({ ...MESSAGING_SCOPE, authorityEpoch: "2" });
    release({ ...original, channelId: "ch-copy", name: "general のコピー" });

    await expect(pending).rejects.toThrow(/session changed/i);
    expect(useMessaging.getState().channels).toEqual([]);
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
          poll: null,
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
      .createChannel("workspace-explicit", "dev", "開発", true);

    expect(create).toHaveBeenCalledWith(
      "workspace-explicit",
      "dev",
      "開発",
      true,
      expect.any(String),
    );
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
      .createChannel("workspace-a", "private-a", "A only", false);
    bindMessagingSessionIdentity(null);
    bindMessagingSessionIdentity("human-b");
    resolveChannel({
      channelId: "stale-channel",
      workspaceId: "workspace-a",
      revision: 1,
      name: "private-a",
      topic: "A only",
      visibility: "private",
      voice: false,
    });

    await expect(operation).rejects.toThrow(
      "Messaging session changed during channel creation",
    );
    expect(useMessaging.getState().workspaces).toEqual([]);
    expect(useMessaging.getState().channels).toEqual([]);
  });

  it("rejects a deferred thread result after the messaging identity changes", async () => {
    bindMessagingSessionIdentity("human-a");
    const server = new MockMessagingServer();
    let resolveThread!: (thread: ThreadSummary) => void;
    const deferredThread = new Promise<ThreadSummary>((resolve) => {
      resolveThread = resolve;
    });
    vi.spyOn(server, "createThread").mockReturnValue(deferredThread);
    installMessagingBackend(server);
    useMessaging.setState({
      ready: true,
      self: { kind: "human", humanId: "human-a" },
      selfKey: "human:human-a",
      channels: [
        {
          channelId: "channel-a",
          revision: 1,
          workspaceId: "workspace-a",
          name: "private-a",
          topic: "",
          visibility: "private",
          voice: false,
        },
      ],
      threadsById: {},
    });

    const operation = useMessaging
      .getState()
      .createThread("channel:channel-a", "stale thread", null);
    bindMessagingSessionIdentity(null);
    bindMessagingSessionIdentity("human-b");
    resolveThread({
      threadId: "stale-thread",
      revision: 1,
      workspaceId: "workspace-a",
      parentPlace: { kind: "channel", channelId: "channel-a" },
      parentMessageId: null,
      name: "stale thread",
      messageCount: 0,
      lastMessageAt: null,
      lastMessage: "",
      participants: [{ kind: "human", humanId: "human-a" }],
      latestSeq: 0,
    });

    await expect(operation).rejects.toThrow(
      "Messaging session changed during thread creation",
    );
    expect(useMessaging.getState().threadsById).toEqual({});
    expect(useMessaging.getState().channels).toEqual([]);
  });
});
