import { parseCallState } from "./call/call-api";
import type {
  Attachment,
  AttachmentDraftPatch,
  ChannelSummary,
  ConnectionState,
  DmSummary,
  MemberProfile,
  Message,
  MessageSearchResult,
  MessagingBackend,
  NotificationLevel,
  NotificationSetting,
  NotificationSettingInput,
  NotifyReason,
  ParticipantRef,
  ParticipantStatus,
  Place,
  PlaceKey,
  ReactionMutationResult,
  ReactionSummary,
  ReadMarker,
  ReplyLaterMarker,
  SendMessageInput,
  SendReceipt,
  ServerEvent,
  StatusKind,
  UnreadSummary,
  UploadAttachmentInput,
  UploadAttachmentReceipt,
} from "./model";
import { MAX_ATTACHMENT_BYTES, MAX_SEQ, parsePlaceKey } from "./model";
import {
  bindMessagingScopeToURL,
  type MessagingScope,
  scopedMessagingPath,
  validateMessagingScope,
} from "./scope";

const REQUEST_TIMEOUT_MS = 15_000;
/** 20 MiBを遅い回線で送り切る猶予。サーバーの130秒より短く保つ。 */
const UPLOAD_TIMEOUT_MS = 120_000;

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
    status: true,
    replyLater: true,
    reactions: true,
    notifications: true,
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
  private readonly abortController = new AbortController();

  readonly scope: MessagingScope;

  constructor(scope: MessagingScope) {
    this.scope = validateMessagingScope(scope);
  }

  async bootstrap(): ReturnType<MessagingBackend["bootstrap"]> {
    const body = asRecord(await this.request("/messaging/bootstrap"));
    const workspaces = asArray(body.workspaces).map((entry) => {
      const value = asRecord(entry);
      return {
        workspaceId: asString(value.workspace_id),
        name: asString(value.name),
      };
    });
    if (
      workspaces.length !== 1 ||
      workspaces[0]?.workspaceId !== this.scope.workspaceId
    ) {
      throw new Error("Messaging bootstrap crossed Workspace scope");
    }
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
    // status_updated は replay されないvolatile eventなので、現在値はここでしか
    // 手に入らない。ReplyLaterのmarkerも同じく開いていないplaceの分まで届く。
    const presence = parsePresence(body);
    return {
      self: parseParticipant(body.self),
      workspaces,
      channels,
      dms,
      members,
      statuses: presence.statuses,
      readMarkers,
      unreadSummaries,
      replyLaterMarkers: presence.replyLaterMarkers,
      notificationSetting: parseNotificationSetting(body.notification_setting),
      employedAgents: [],
    };
  }

  /**
   * 再接続後の再同期。cursorが戻せるのはplaceのdurableな並びだけなので、
   * statusと開いているmarkerはserverの現在値で置き換えるほかない。
   */
  async fetchPresence(): ReturnType<MessagingBackend["fetchPresence"]> {
    return parsePresence(asRecord(await this.request("/messaging/bootstrap")));
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

  async searchMessages(
    query: string,
    options: { place?: Place; limit?: number } = {},
  ): Promise<MessageSearchResult[]> {
    const params = new URLSearchParams({ q: query });
    if (options.place) params.set("place_id", placeID(options.place));
    if (options.limit !== undefined) params.set("limit", String(options.limit));
    const body = asRecord(await this.request(`/messaging/search?${params}`));
    return asArray(body.results).map(parseSearchResult);
  }

  async createChannel(
    workspaceId: string,
    name: string,
    topic: string,
    voice: boolean,
  ): Promise<ChannelSummary> {
    const body = await this.request("/messaging/channels", {
      method: "POST",
      body: { workspace_id: workspaceId, name, topic, voice },
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
            attachments: input.attachments,
          },
        },
      ),
    );
    return {
      clientNonce: asString(body.client_nonce),
      messageId: asString(body.message_id),
      seq: asSeq(body.seq),
      created: asBoolean(body.created),
    };
  }

  async uploadAttachment(
    input: UploadAttachmentInput,
  ): Promise<UploadAttachmentReceipt> {
    if (input.body.size <= 0 || input.body.size > MAX_ATTACHMENT_BYTES) {
      throw new MessagingAPIError("attachment_too_large", 413);
    }
    const signals = [
      this.abortController.signal,
      AbortSignal.timeout(UPLOAD_TIMEOUT_MS),
    ];
    if (input.signal) signals.push(input.signal);
    // 生バイトを本文に、メタデータをheaderに。Content-Lengthはブラウザが
    // Blobから確定するので、サーバーはbodyを読む前に宣言サイズでquotaを予約できる。
    const response = await fetch(
      scopedMessagingPath(
        `/messaging/places/${encodeURIComponent(placeID(input.place))}/attachments`,
        this.scope,
      ),
      {
        method: "POST",
        credentials: "include",
        cache: "no-store",
        headers: {
          Accept: "application/json",
          "Content-Type": input.contentType || "application/octet-stream",
          "Idempotency-Key": input.clientNonce,
          "X-Sumi-Attachment-Filename": encodeURIComponent(input.filename),
        },
        body: input.body,
        signal: AbortSignal.any(signals),
      },
    );
    if (!response.ok) {
      let code = "attachment_upload_failed";
      try {
        const body = asRecord(await response.json());
        if (typeof body.error === "string") code = body.error;
      } catch {
        // Status remains the authoritative non-sensitive signal.
      }
      throw new MessagingAPIError(code, response.status);
    }
    const body = asRecord(await response.json());
    return {
      attachment: parseAttachment(body.attachment),
      created: asBoolean(body.created),
    };
  }

  /** 送信前の添付の編集。省略した項目はサーバー側でも「触らない」。 */
  async updateDraftAttachment(
    attachmentId: string,
    patch: AttachmentDraftPatch,
  ): Promise<Attachment> {
    const body: Record<string, unknown> = {};
    if (patch.filename !== undefined) body.filename = patch.filename;
    if (patch.alt !== undefined) body.alt = patch.alt;
    if (patch.spoiler !== undefined) body.spoiler = patch.spoiler;
    return parseAttachment(
      await this.request(
        `/messaging/attachments/${encodeURIComponent(attachmentId)}`,
        { method: "PATCH", body },
      ),
    );
  }

  attachmentURL(attachmentId: string): string {
    return scopedMessagingPath(
      `/messaging/attachments/${encodeURIComponent(attachmentId)}`,
      this.scope,
    );
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

  /** 自分のstatusだけを置き換える。参加者はsessionが決め、bodyには載せない。 */
  async setStatus(
    status: StatusKind,
    note: string,
  ): Promise<ParticipantStatus> {
    return parseStatus(
      await this.request("/messaging/status", {
        method: "PUT",
        body: { status, note },
      }),
    );
  }

  async createReplyLater(
    place: Place,
    messageId: string,
    remindAt: number,
  ): Promise<ReplyLaterMarker> {
    const body = asRecord(
      await this.request(
        `/messaging/places/${encodeURIComponent(placeID(place))}/messages/${encodeURIComponent(messageId)}/reply-later`,
        {
          method: "POST",
          body: { remind_at: new Date(remindAt).toISOString() },
        },
      ),
    );
    return parseReplyLater(body.marker);
  }

  async resolveReplyLater(markerId: string): Promise<ReplyLaterMarker> {
    const body = asRecord(
      await this.request(
        `/messaging/reply-later/${encodeURIComponent(markerId)}/resolve`,
        { method: "POST", body: {} },
      ),
    );
    return parseReplyLater(body.marker);
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

  /** PUTは全置換。クライアントは常に現在値を持っているので差分は要らない。 */
  async setNotificationSetting(
    input: NotificationSettingInput,
  ): Promise<NotificationSetting> {
    const body = await this.request("/messaging/notification-settings", {
      method: "PUT",
      body: {
        defaults: { level: input.defaults.level },
        per_place: input.perPlace.map((entry) => ({
          place: placeToWire(entry.place),
          level: entry.level,
        })),
        keywords: input.keywords,
      },
    });
    return parseNotificationSetting(body);
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
    this.abortController.abort();
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
    const url = bindMessagingScopeToURL(
      new URL("/messaging/ws", window.location.href),
      this.scope,
    );
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
    if (eventType === "message_created") {
      const message = parseMessage(wire.message);
      this.cursors.set(placeID(message.place), message.seq);
      // notifyが無いことは欠損ではなく「呼んでいない」という答え。
      parsed = { type: eventType, message, notify: parseNotify(wire.notify) };
    } else if (
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
    } else if (eventType === "status_updated") {
      // 自己申告のattention。placeを持たず、seqも進めない。
      parsed = { type: eventType, status: parseStatus(wire.status) };
    } else if (eventType === "reply_later_created") {
      parsed = { type: eventType, marker: parseReplyLater(wire.marker) };
    } else if (eventType === "reply_later_resolved") {
      parsed = { type: eventType, markerId: asString(wire.marker_id) };
    } else if (eventType === "typing") {
      const id = asString(wire.place_id);
      const place = this.places.get(id);
      if (!place) return;
      parsed = {
        type: "typing",
        place,
        participant: parseParticipant(wire.actor),
      };
    } else if (eventType === "call_state") {
      parsed = { type: "call_state", call: parseCallState(wire.call) };
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
      voice: asBoolean(wire.voice),
    };
    if (!this.cursors.has(channel.channelId)) {
      this.cursors.set(channel.channelId, 0);
    }
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
    if (!this.cursors.has(dm.dmId)) this.cursors.set(dm.dmId, 0);
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
    const response = await fetch(scopedMessagingPath(path, this.scope), {
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
      signal: AbortSignal.any([
        this.abortController.signal,
        AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      ]),
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

function placeToWire(place: Place): Record<string, string> {
  return place.kind === "channel"
    ? { kind: place.kind, channel_id: place.channelId }
    : { kind: place.kind, dm_id: place.dmId };
}

function parseNotify(value: unknown): { reason: NotifyReason } | null {
  if (value == null) return null;
  const wire = asRecord(value);
  const reason = asString(wire.reason);
  if (
    reason === "dm" ||
    reason === "mention" ||
    reason === "keyword" ||
    reason === "all"
  ) {
    return { reason };
  }
  // 未知のreasonはfail-closedに無視する（呼ばれなかったのと同じ扱い）。
  return null;
}

function asNotificationLevel(value: unknown): NotificationLevel {
  if (value === "all" || value === "mentions" || value === "mute") return value;
  throw new Error("invalid notification level");
}

function parseNotificationSetting(value: unknown): NotificationSetting {
  const wire = asRecord(value);
  return {
    owner: parseParticipant(wire.owner),
    defaults: {
      level: asNotificationLevel(asRecord(wire.defaults).level),
    },
    perPlace: asArray(wire.per_place).map((entry) => {
      const item = asRecord(entry);
      return {
        place: parsePlace(item.place),
        level: asNotificationLevel(item.level),
      };
    }),
    keywords: asArray(wire.keywords).map(asString),
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

function parsePresence(body: Record<string, unknown>): {
  statuses: ParticipantStatus[];
  replyLaterMarkers: ReplyLaterMarker[];
} {
  return {
    statuses: asArray(body.statuses).map(parseStatus),
    replyLaterMarkers: asArray(body.reply_later_markers).map(parseReplyLater),
  };
}

function parseStatus(value: unknown): ParticipantStatus {
  const wire = asRecord(value);
  return {
    participant: parseParticipant(wire.participant),
    status: asStatusKind(wire.status),
    note: asString(wire.note),
    expiresAt: wire.expires_at == null ? null : asTimestamp(wire.expires_at),
  };
}

function parseReplyLater(value: unknown): ReplyLaterMarker {
  const wire = asRecord(value);
  return {
    markerId: asString(wire.marker_id),
    participant: parseParticipant(wire.participant),
    place: parsePlace(wire.place),
    messageId: asString(wire.message_id),
    note: asString(wire.note),
    // remind_atは本人のwireにしか載らない。無いことは欠損ではなく、
    // 「自分の約束ではない」という正しい答え。
    remindAt: wire.remind_at == null ? null : asTimestamp(wire.remind_at),
    resolved: asBoolean(wire.resolved),
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
    attachments: asArray(wire.attachments ?? []).map(parseAttachment),
    replyTo: wire.reply_to === null ? null : asString(wire.reply_to),
    clientNonce:
      typeof wire.client_nonce === "string" ? wire.client_nonce : undefined,
    createdAt: asTimestamp(wire.created_at),
    editedAt: wire.edited_at === null ? null : asTimestamp(wire.edited_at),
    deleted: asBoolean(wire.deleted),
  };
}

function parseAttachment(value: unknown): Attachment {
  const wire = asRecord(value);
  const sizeBytes = asSeq(wire.size_bytes);
  const position = asSeq(wire.position);
  if (sizeBytes <= 0 || sizeBytes > MAX_ATTACHMENT_BYTES) {
    throw new Error("invalid attachment size");
  }
  return {
    attachmentId: asString(wire.attachment_id),
    filename: asString(wire.filename),
    mime: asString(wire.mime),
    sizeBytes,
    sha256: asString(wire.sha256),
    position,
    // These declarations are mandatory on every attachment wire. In
    // particular, inventing `false` for a missing spoiler would reveal an
    // image the sender asked to keep covered.
    spoiler: asBoolean(wire.spoiler),
    alt: asString(wire.alt),
  };
}

function parseSearchResult(value: unknown): MessageSearchResult {
  const wire = asRecord(value);
  return {
    messageId: asString(wire.message_id),
    place: parsePlace(wire.place),
    seq: asSeq(wire.seq),
    author: parseParticipant(wire.author),
    snippet: asString(wire.snippet),
    createdAt: asTimestamp(wire.created_at),
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
function asStatusKind(value: unknown): StatusKind {
  if (value === "available" || value === "busy" || value === "away") {
    return value;
  }
  throw new Error("invalid participant status");
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
