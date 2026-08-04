import type {
  ChannelSummary,
  ConnectionState,
  DmSummary,
  MemberProfile,
  Message,
  MessagingBackend,
  ParticipantRef,
  Place,
  PlaceKey,
  ReactionMutationResult,
  ReactionSummary,
  ReadMarker,
  SendMessageInput,
  SendReceipt,
  ServerEvent,
  UnreadSummary,
} from "./model";
import { MAX_SEQ, parsePlaceKey } from "./model";

const REQUEST_TIMEOUT_MS = 15_000;

export class MessagingAPIError extends Error {
  readonly code: string;
  readonly status: number;

  constructor(code: string, status: number) {
    super(code);
    this.name = "MessagingAPIError";
    this.code = code;
    this.status = status;
  }
}

/** Same-origin browser-session client for the shared messaging surface. */
export class ApiMessagingBackend implements MessagingBackend {
  readonly capabilities = {
    status: false,
    replyLater: false,
    reactions: true,
  } as const;
  private readonly listeners = new Set<(event: ServerEvent) => void>();
  private readonly connectionListeners = new Set<
    (state: ConnectionState) => void
  >();
  private readonly cursors = new Map<string, number>();
  private readonly places = new Map<string, Place>();
  private socket: WebSocket | null = null;
  private reconnectTimer: number | null = null;
  private reconnectDelay = 250;
  private stopped = false;

  async bootstrap(): ReturnType<MessagingBackend["bootstrap"]> {
    const body = asRecord(await this.request("/messaging/bootstrap"));
    const workspaces = asArray(body.workspaces).map((entry) => {
      const value = asRecord(entry);
      return {
        workspaceId: asString(value.workspace_id),
        name: asString(value.name),
      };
    });
    const channels = asArray(body.channels).map((entry) =>
      this.registerChannel(entry),
    );
    const dms: DmSummary[] = asArray(body.dms).map((entry) =>
      this.registerDm(entry),
    );
    const members: MemberProfile[] = asArray(body.members).map((entry) => {
      const value = asRecord(entry);
      return {
        participant: parseParticipant(value.participant),
        displayName: asString(value.display_name),
        tagline: typeof value.tagline === "string" ? value.tagline : "",
      };
    });
    const readMarkers: ReadMarker[] = asArray(body.read_markers).map(
      (entry) => {
        const value = asRecord(entry);
        return {
          place: parsePlace(value.place),
          lastReadSeq: asSeq(value.last_read_seq),
        };
      },
    );
    const unreadSummaries: UnreadSummary[] = asArray(body.unread_summaries).map(
      (entry) => {
        const value = asRecord(entry);
        return {
          place: parsePlace(value.place),
          latestSeq: asSeq(value.latest_seq),
          unreadCount: asSeq(value.unread_count),
          mentionCount: asSeq(value.mention_count),
        };
      },
    );
    return {
      self: parseParticipant(body.self),
      workspaces,
      channels,
      dms,
      members,
      statuses: [],
      readMarkers,
      unreadSummaries,
      replyLaterMarkers: [],
      employedAgents: [],
    };
  }

  async fetchMessages(
    place: Place,
    options: { beforeSeq?: number; limit?: number } = {},
  ): Promise<Message[]> {
    const query = new URLSearchParams();
    if (options.beforeSeq !== undefined) {
      query.set("before_seq", String(options.beforeSeq));
    }
    if (options.limit !== undefined) query.set("limit", String(options.limit));
    const suffix = query.size > 0 ? `?${query}` : "";
    const body = asRecord(
      await this.request(
        `/messaging/places/${encodeURIComponent(placeID(place))}/messages${suffix}`,
      ),
    );
    return asArray(body.messages).map(parseMessage);
  }

  async createChannel(
    workspaceId: string,
    name: string,
    topic: string,
  ): Promise<ChannelSummary> {
    const body = await this.request("/messaging/channels", {
      method: "POST",
      body: { workspace_id: workspaceId, name, topic },
    });
    return this.registerChannel(body);
  }

  async ensureDM(participant: ParticipantRef): Promise<DmSummary> {
    const body = await this.request("/messaging/dms", {
      method: "POST",
      body: { participant: participantToWire(participant) },
    });
    return this.registerDm(body);
  }

  async createGroupDM(participants: ParticipantRef[]): Promise<DmSummary> {
    const body = await this.request("/messaging/group-dms", {
      method: "POST",
      body: { participants: participants.map(participantToWire) },
    });
    return this.registerDm(body);
  }

  async updateChannelTopic(
    channelId: string,
    topic: string,
  ): Promise<ChannelSummary> {
    const body = await this.request(
      `/messaging/places/${encodeURIComponent(channelId)}`,
      { method: "PATCH", body: { topic } },
    );
    return this.registerChannel(body);
  }

  async sendMessage(input: SendMessageInput): Promise<SendReceipt> {
    const body = asRecord(
      await this.request(
        `/messaging/places/${encodeURIComponent(placeID(input.place))}/messages`,
        {
          method: "POST",
          body: {
            content: input.content,
            urgency: input.urgency,
            reply_to: input.replyTo ?? "",
            client_nonce: input.clientNonce,
          },
        },
      ),
    );
    return { messageId: asString(body.message_id), seq: asSeq(body.seq) };
  }

  async editMessage(
    place: Place,
    messageId: string,
    content: string,
  ): Promise<void> {
    await this.request(
      `/messaging/places/${encodeURIComponent(placeID(place))}/messages/${encodeURIComponent(messageId)}`,
      { method: "PATCH", body: { content } },
    );
  }

  async deleteMessage(place: Place, messageId: string): Promise<void> {
    await this.request(
      `/messaging/places/${encodeURIComponent(placeID(place))}/messages/${encodeURIComponent(messageId)}`,
      { method: "DELETE" },
    );
  }

  async markRead(place: Place, lastReadSeq: number): Promise<void> {
    await this.request(
      `/messaging/places/${encodeURIComponent(placeID(place))}/read-through`,
      { method: "PUT", body: { seq: lastReadSeq } },
    );
  }

  setStatus(): Promise<void> {
    return unsupported();
  }
  createReplyLater(): Promise<void> {
    return unsupported();
  }
  resolveReplyLater(): Promise<void> {
    return unsupported();
  }

  async toggleReaction(
    place: Place,
    messageId: string,
    emoji: string,
    clientNonce: string,
  ): Promise<ReactionMutationResult> {
    const body = asRecord(
      await this.request(
        `/messaging/places/${encodeURIComponent(placeID(place))}/messages/${encodeURIComponent(messageId)}/reactions`,
        { method: "POST", body: { emoji, client_nonce: clientNonce } },
      ),
    );
    const message = asRecord(body.message);
    return {
      messageId: asString(message.message_id),
      reactions: asArray(message.reactions).map(parseReaction),
    };
  }

  sendTyping(place: Place): void {
    if (this.socket?.readyState !== WebSocket.OPEN) return;
    this.socket.send(
      JSON.stringify({ type: "typing", place_id: placeID(place) }),
    );
  }

  subscribe(
    listener: (event: ServerEvent) => void,
    options: { sinceByPlace?: Record<PlaceKey, number> } = {},
  ): () => void {
    this.listeners.add(listener);
    for (const [key, seq] of Object.entries(options.sinceByPlace ?? {})) {
      const place = parsePlaceKey(key);
      if (place) this.cursors.set(placeID(place), seq);
    }
    this.stopped = false;
    this.connect();
    return () => {
      this.listeners.delete(listener);
      if (this.listeners.size === 0) this.stopSocket();
    };
  }

  subscribeConnection(listener: (state: ConnectionState) => void): () => void {
    this.connectionListeners.add(listener);
    listener(
      this.socket?.readyState === WebSocket.OPEN ? "connected" : "reconnecting",
    );
    return () => this.connectionListeners.delete(listener);
  }

  dispose(): void {
    this.listeners.clear();
    this.connectionListeners.clear();
    this.stopSocket();
  }

  private connect(): void {
    if (
      this.stopped ||
      this.listeners.size === 0 ||
      this.socket?.readyState === WebSocket.OPEN ||
      this.socket?.readyState === WebSocket.CONNECTING
    ) {
      return;
    }
    this.emitConnection("reconnecting");
    const url = new URL("/messaging/ws", window.location.href);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    const socket = new WebSocket(url);
    this.socket = socket;
    socket.addEventListener("open", () => {
      if (this.socket !== socket) return;
      socket.send(
        JSON.stringify({
          type: "hello",
          cursors: Object.fromEntries(this.cursors),
        }),
      );
    });
    socket.addEventListener("message", (event) => {
      if (typeof event.data !== "string") return;
      try {
        this.handleFrame(asRecord(JSON.parse(event.data) as unknown));
      } catch {
        socket.close(1002, "invalid messaging frame");
      }
    });
    socket.addEventListener("close", () => {
      if (this.socket !== socket) return;
      this.socket = null;
      if (this.stopped || this.listeners.size === 0) {
        this.emitConnection("disconnected");
        return;
      }
      this.emitConnection("reconnecting");
      const delay = this.reconnectDelay;
      this.reconnectDelay = Math.min(this.reconnectDelay * 2, 5_000);
      this.reconnectTimer = window.setTimeout(() => this.connect(), delay);
    });
  }

  private handleFrame(frame: Record<string, unknown>): void {
    const type = asString(frame.type);
    if (type === "hello_ack") {
      this.reconnectDelay = 250;
      this.emitConnection("connected");
      return;
    }
    if (type === "caught_up") {
      // Catch-up replays only messages after the cursor, so reactions that
      // landed on already-read messages while the socket was down are not in
      // it. Surface the boundary so the subscriber can re-read what it holds.
      const place = this.places.get(asString(frame.place_id));
      if (place) this.emit({ type: "caught_up", place });
      return;
    }
    if (type === "receipt") return;
    if (type === "error") throw new Error("messaging socket error");
    if (type !== "event") throw new Error("unknown messaging frame");
    const wire = asRecord(frame.event);
    const eventType = asString(wire.type);
    let parsed: ServerEvent;
    if (
      eventType === "message_created" ||
      eventType === "message_edited" ||
      eventType === "message_deleted"
    ) {
      const message = parseMessage(wire.message);
      this.cursors.set(placeID(message.place), message.seq);
      parsed = { type: eventType, message };
    } else if (eventType === "reaction_updated") {
      // A reaction can target a message older than the replay cursor, so it
      // must never move the cursor (backwards or at all). It is also a partial
      // update: applying it as a whole message would roll back an edit that
      // committed while this event was in flight.
      const place = this.places.get(asString(wire.place_id));
      if (!place) return;
      const update = asRecord(wire.reaction);
      parsed = {
        type: eventType,
        place,
        messageId: asString(update.message_id),
        reactions: asArray(update.reactions).map(parseReaction),
      };
    } else if (eventType === "typing") {
      const id = asString(wire.place_id);
      const place = this.places.get(id);
      if (!place) return;
      parsed = {
        type: "typing",
        place,
        participant: parseParticipant(wire.actor),
      };
    } else if (eventType === "place_created") {
      parsed =
        wire.channel === undefined || wire.channel === null
          ? { type: "place_created", dm: this.registerDm(wire.dm) }
          : {
              type: "place_created",
              channel: this.registerChannel(wire.channel),
            };
    } else if (eventType === "place_updated") {
      parsed = {
        type: "place_updated",
        channel: this.registerChannel(wire.channel),
      };
    } else {
      return;
    }
    this.emit(parsed);
  }

  private emit(event: ServerEvent): void {
    for (const listener of this.listeners) listener(event);
  }

  /** Parses a channel wire shape and remembers the place for event routing. */
  private registerChannel(value: unknown): ChannelSummary {
    const wire = asRecord(value);
    const channel: ChannelSummary = {
      channelId: asString(wire.channel_id),
      workspaceId: asString(wire.workspace_id),
      name: asString(wire.name),
      topic: asString(wire.topic),
      visibility: asVisibility(wire.visibility),
    };
    this.places.set(channel.channelId, {
      kind: "channel",
      channelId: channel.channelId,
    });
    return channel;
  }

  /** Parses a dm wire shape and remembers the place for event routing. */
  private registerDm(value: unknown): DmSummary {
    const wire = asRecord(value);
    const dm: DmSummary = {
      dmId: asString(wire.dm_id),
      kind: asDMKind(wire.kind),
      participants: asArray(wire.participants).map(parseParticipant),
    };
    this.places.set(dm.dmId, { kind: dm.kind, dmId: dm.dmId });
    return dm;
  }

  private stopSocket(): void {
    this.stopped = true;
    if (this.reconnectTimer !== null) window.clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    this.socket?.close(1000, "unsubscribed");
    this.socket = null;
    this.emitConnection("disconnected");
  }

  private emitConnection(state: ConnectionState): void {
    for (const listener of this.connectionListeners) listener(state);
  }

  private async request(
    path: string,
    options: { method?: string; body?: unknown } = {},
  ): Promise<unknown> {
    const response = await fetch(path, {
      method: options.method ?? "GET",
      credentials: "include",
      cache: "no-store",
      headers: {
        Accept: "application/json",
        ...(options.body === undefined
          ? {}
          : { "Content-Type": "application/json" }),
      },
      body:
        options.body === undefined ? undefined : JSON.stringify(options.body),
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });
    if (!response.ok) {
      let code = "messaging_request_failed";
      try {
        const body = asRecord(await response.json());
        if (typeof body.error === "string") code = body.error;
      } catch {
        // Status remains the authoritative non-sensitive signal.
      }
      throw new MessagingAPIError(code, response.status);
    }
    if (response.status === 204) return null;
    return response.json() as Promise<unknown>;
  }
}

function unsupported(): Promise<void> {
  return Promise.reject(new MessagingAPIError("not_implemented", 501));
}

function placeID(place: Place): string {
  return place.kind === "channel" ? place.channelId : place.dmId;
}

function participantToWire(ref: ParticipantRef): Record<string, string> {
  return ref.kind === "human"
    ? { kind: "human", human_id: ref.humanId }
    : {
        kind: "personality_agent",
        personality_agent_id: ref.personalityAgentId,
      };
}

function parseParticipant(value: unknown): ParticipantRef {
  const wire = asRecord(value);
  const kind = asString(wire.kind);
  if (kind === "human") return { kind, humanId: asString(wire.human_id) };
  if (kind === "personality_agent") {
    return {
      kind,
      personalityAgentId: asString(wire.personality_agent_id),
    };
  }
  throw new Error("invalid participant");
}

function parseReaction(value: unknown): ReactionSummary {
  const wire = asRecord(value);
  return {
    emoji: asString(wire.emoji),
    participants: asArray(wire.participants).map(parseParticipant),
  };
}

function parsePlace(value: unknown): Place {
  const wire = asRecord(value);
  const kind = asString(wire.kind);
  if (kind === "channel") {
    return { kind, channelId: asString(wire.channel_id) };
  }
  if (kind === "dm" || kind === "group_dm") {
    return { kind, dmId: asString(wire.dm_id) };
  }
  throw new Error("invalid place");
}

function parseMessage(value: unknown): Message {
  const wire = asRecord(value);
  return {
    messageId: asString(wire.message_id),
    place: parsePlace(wire.place),
    seq: asSeq(wire.seq),
    author: parseParticipant(wire.author),
    content: asString(wire.content),
    mentions: asArray(wire.mentions).map(parseParticipant),
    urgency: asUrgency(wire.urgency),
    reactions: asArray(wire.reactions).map(parseReaction),
    replyTo: wire.reply_to === null ? null : asString(wire.reply_to),
    clientNonce:
      typeof wire.client_nonce === "string" ? wire.client_nonce : undefined,
    createdAt: asTimestamp(wire.created_at),
    editedAt: wire.edited_at === null ? null : asTimestamp(wire.edited_at),
    deleted: asBoolean(wire.deleted),
  };
}

function asRecord(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("invalid messaging response");
  }
  return value as Record<string, unknown>;
}
function asArray(value: unknown): unknown[] {
  if (!Array.isArray(value)) throw new Error("invalid messaging response");
  return value;
}
function asString(value: unknown): string {
  if (typeof value !== "string") throw new Error("invalid messaging response");
  return value;
}
function asBoolean(value: unknown): boolean {
  if (typeof value !== "boolean") throw new Error("invalid messaging response");
  return value;
}
function asSeq(value: unknown): number {
  if (
    !Number.isSafeInteger(value) ||
    Number(value) < 0 ||
    Number(value) > MAX_SEQ
  ) {
    throw new Error("invalid messaging sequence");
  }
  return Number(value);
}
function asTimestamp(value: unknown): number {
  const parsed = Date.parse(asString(value));
  if (!Number.isFinite(parsed)) throw new Error("invalid messaging timestamp");
  return parsed;
}
function asVisibility(value: unknown): "public" | "private" {
  if (value === "public" || value === "private") return value;
  throw new Error("invalid channel visibility");
}
function asDMKind(value: unknown): "dm" | "group_dm" {
  if (value === "dm" || value === "group_dm") return value;
  throw new Error("invalid dm kind");
}
function asUrgency(value: unknown): Message["urgency"] {
  if (value === "urgent" || value === "normal" || value === "fyi") return value;
  if (value === "") return "normal";
  throw new Error("invalid urgency");
}
