import { create } from "zustand";
import { secureRandomUUID } from "../lib/random-uuid";
import { ApiMessagingBackend } from "./api-backend";
import { hasDisplayMention } from "./mention";
import type {
  ChannelSummary,
  ConnectionState,
  DmSummary,
  MemberProfile,
  Message,
  MessagingBackend,
  MessagingCapabilities,
  ParticipantKey,
  ParticipantRef,
  ParticipantStatus,
  Place,
  PlaceKey,
  ReplyLaterMarker,
  StatusKind,
  Urgency,
  WorkspaceSummary,
} from "./model";
import { parsePlaceKey, participantKey, placeKey } from "./model";
import type { PendingMessage } from "./timeline";
import { mergeMessages, upsertMessage } from "./timeline";

const TYPING_TTL_MS = 4_500;
const DEFAULT_REPLY_LATER_REMIND_MS = 30 * 60_000;

let backend: MessagingBackend = new ApiMessagingBackend();

/** Tests and explicit development harnesses may replace the transport before init. */
export function installMessagingBackend(override: MessagingBackend): void {
  if (initialized) throw new Error("Messaging backend is already initialized");
  backend = override;
}

interface MessagingState {
  capabilities: MessagingCapabilities;
  ready: boolean;
  self: ParticipantRef | null;
  selfKey: ParticipantKey;
  workspaces: WorkspaceSummary[];
  channels: ChannelSummary[];
  dms: DmSummary[];
  membersByKey: Record<ParticipantKey, MemberProfile>;
  statusByKey: Record<ParticipantKey, ParticipantStatus>;
  messagesByPlace: Record<PlaceKey, Message[]>;
  pendingByPlace: Record<PlaceKey, PendingMessage[]>;
  lastReadByPlace: Record<PlaceKey, number>;
  unreadCountByPlace: Record<PlaceKey, number>;
  mentionCountByPlace: Record<PlaceKey, number>;
  /** placeへ入った時点のlastReadのスナップショット。離れるまで動かさない。 */
  unreadLineByPlace: Record<PlaceKey, number | null>;
  draftByPlace: Record<PlaceKey, string>;
  typingByPlace: Record<PlaceKey, Record<ParticipantKey, number>>;
  replyLaterById: Record<string, ReplyLaterMarker>;
  employedAgents: ParticipantRef[];
  hasMoreByPlace: Record<PlaceKey, boolean>;
  loadingOlderByPlace: Record<PlaceKey, boolean>;
  activePlaceKey: PlaceKey | null;
  editingMessageId: string | null;
  replyTargetId: string | null;
  connection: ConnectionState;

  init(): void;
  selectPlace(key: PlaceKey): void;
  loadPlaceAround(key: PlaceKey, seq: number): Promise<boolean>;
  setDraft(key: PlaceKey, draft: string): void;
  send(content: string, urgency: Urgency): void;
  retrySend(clientNonce: string): void;
  startEdit(messageId: string): void;
  cancelEdit(): void;
  submitEdit(content: string): void;
  deleteMessage(messageId: string): void;
  setReplyTarget(messageId: string | null): void;
  noteReadUpTo(key: PlaceKey, seq: number): void;
  setStatus(status: StatusKind, note: string): void;
  createReplyLater(message: Message, delayMs?: number): void;
  toggleReaction(message: Message, emoji: string): void;
  loadOlder(key: PlaceKey): Promise<void>;
  resolveReplyLater(markerId: string): void;
  sendTyping(): void;
}

function resolveMentions(
  content: string,
  members: Record<ParticipantKey, MemberProfile>,
  selfKey: ParticipantKey,
): ParticipantRef[] {
  const mentions: ParticipantRef[] = [];
  for (const member of Object.values(members)) {
    const key = participantKey(member.participant);
    if (key === selfKey) continue;
    if (hasDisplayMention(content, member.displayName)) {
      mentions.push(member.participant);
    }
  }
  return mentions;
}

function unreadContribution(
  message: Message,
  lastReadSeq: number,
  selfKey: ParticipantKey,
): { unread: number; mentions: number } {
  if (
    message.deleted ||
    message.seq <= lastReadSeq ||
    participantKey(message.author) === selfKey
  ) {
    return { unread: 0, mentions: 0 };
  }
  return {
    unread: 1,
    mentions:
      message.urgency !== "fyi" &&
      message.mentions.some((ref) => participantKey(ref) === selfKey)
        ? 1
        : 0,
  };
}

let initialized = false;

export const useMessaging = create<MessagingState>((set, get) => {
  if (import.meta.env.DEV) {
    // 開発時のデバッグ・E2E検証用のstate参照口。
    (globalThis as Record<string, unknown>).__sumiMessaging = () => get();
  }
  const applyEvent = (
    event: Parameters<Parameters<MessagingBackend["subscribe"]>[0]>[0],
  ) => {
    if (
      event.type === "message_created" ||
      event.type === "message_edited" ||
      event.type === "reaction_updated"
    ) {
      const key = placeKey(event.message.place);
      set((state) => {
        const existing = (state.messagesByPlace[key] ?? []).find(
          (message) => message.messageId === event.message.messageId,
        );
        const messages = upsertMessage(
          state.messagesByPlace[key] ?? [],
          event.message,
        );
        const nonce = event.message.clientNonce;
        const pending = nonce
          ? (state.pendingByPlace[key] ?? []).filter(
              (entry) => entry.clientNonce !== nonce,
            )
          : (state.pendingByPlace[key] ?? []);
        const authorKey = participantKey(event.message.author);
        const typing = { ...(state.typingByPlace[key] ?? {}) };
        delete typing[authorKey];
        const lastRead =
          authorKey === state.selfKey
            ? Math.max(state.lastReadByPlace[key] ?? 0, event.message.seq)
            : (state.lastReadByPlace[key] ?? 0);
        const previousContribution = existing
          ? unreadContribution(existing, lastRead, state.selfKey)
          : { unread: 0, mentions: 0 };
        const nextContribution = unreadContribution(
          event.message,
          lastRead,
          state.selfKey,
        );
        return {
          messagesByPlace: { ...state.messagesByPlace, [key]: messages },
          pendingByPlace: { ...state.pendingByPlace, [key]: pending },
          typingByPlace: { ...state.typingByPlace, [key]: typing },
          lastReadByPlace: { ...state.lastReadByPlace, [key]: lastRead },
          unreadCountByPlace: {
            ...state.unreadCountByPlace,
            [key]: Math.max(
              0,
              (state.unreadCountByPlace[key] ?? 0) -
                previousContribution.unread +
                nextContribution.unread,
            ),
          },
          mentionCountByPlace: {
            ...state.mentionCountByPlace,
            [key]: Math.max(
              0,
              (state.mentionCountByPlace[key] ?? 0) -
                previousContribution.mentions +
                nextContribution.mentions,
            ),
          },
        };
      });
      return;
    }
    if (event.type === "message_deleted") {
      const key = placeKey(event.message.place);
      set((state) => {
        const existing = (state.messagesByPlace[key] ?? []).find(
          (message) => message.messageId === event.message.messageId,
        );
        const previous = existing ?? { ...event.message, deleted: false };
        const contribution = unreadContribution(
          previous,
          state.lastReadByPlace[key] ?? 0,
          state.selfKey,
        );
        return {
          messagesByPlace: {
            ...state.messagesByPlace,
            [key]: upsertMessage(
              state.messagesByPlace[key] ?? [],
              event.message,
            ),
          },
          unreadCountByPlace: {
            ...state.unreadCountByPlace,
            [key]: Math.max(
              0,
              (state.unreadCountByPlace[key] ?? 0) - contribution.unread,
            ),
          },
          mentionCountByPlace: {
            ...state.mentionCountByPlace,
            [key]: Math.max(
              0,
              (state.mentionCountByPlace[key] ?? 0) - contribution.mentions,
            ),
          },
        };
      });
      return;
    }
    if (event.type === "typing") {
      const key = placeKey(event.place);
      const typerKey = participantKey(event.participant);
      if (typerKey === get().selfKey) return;
      set((state) => ({
        typingByPlace: {
          ...state.typingByPlace,
          [key]: {
            ...(state.typingByPlace[key] ?? {}),
            [typerKey]: Date.now() + TYPING_TTL_MS,
          },
        },
      }));
      return;
    }
    if (event.type === "status_updated") {
      set((state) => ({
        statusByKey: {
          ...state.statusByKey,
          [participantKey(event.status.participant)]: event.status,
        },
      }));
      return;
    }
    if (event.type === "reply_later_created") {
      set((state) => ({
        replyLaterById: {
          ...state.replyLaterById,
          [event.marker.markerId]: event.marker,
        },
      }));
      return;
    }
    if (event.type === "reply_later_resolved") {
      set((state) => {
        const marker = state.replyLaterById[event.markerId];
        if (!marker) return {};
        return {
          replyLaterById: {
            ...state.replyLaterById,
            [event.markerId]: { ...marker, resolved: true },
          },
        };
      });
    }
  };

  const PAGE_SIZE = 50;

  const loadPlace = async (place: Place) => {
    const key = placeKey(place);
    if (get().messagesByPlace[key]) return;
    const messages = await backend.fetchMessages(place, { limit: PAGE_SIZE });
    set((state) => ({
      messagesByPlace: {
        ...state.messagesByPlace,
        [key]: mergeMessages(state.messagesByPlace[key] ?? [], messages),
      },
      hasMoreByPlace: {
        ...state.hasMoreByPlace,
        [key]: messages.length >= PAGE_SIZE,
      },
    }));
  };

  // 送信・再送の共通経路。ACK(receipt)はecho eventで照合されるため、
  // ここでは失敗時にpendingへfailedを立てて再送UIへ委ねるだけで良い。
  const dispatchSend = (key: PlaceKey, pending: PendingMessage) => {
    const place = parsePlaceKey(key);
    if (!place) return;
    backend
      .sendMessage({
        place,
        content: pending.content,
        urgency: pending.urgency,
        replyTo: pending.replyTo,
        clientNonce: pending.clientNonce,
      })
      .then(async (receipt) => {
        let confirmed = (get().messagesByPlace[key] ?? []).some(
          (message) =>
            message.messageId === receipt.messageId ||
            message.clientNonce === pending.clientNonce,
        );
        if (!confirmed) {
          // ACKだけ届き、live echoを取りこぼした再送もreceiptのseqから確定する。
          const messages = await backend.fetchMessages(place, {
            beforeSeq: receipt.seq + 1,
            limit: 1,
          });
          const committed = messages.find(
            (message) => message.messageId === receipt.messageId,
          );
          if (committed) {
            set((state) => ({
              messagesByPlace: {
                ...state.messagesByPlace,
                [key]: upsertMessage(
                  state.messagesByPlace[key] ?? [],
                  committed,
                ),
              },
            }));
            confirmed = true;
          }
        }
        if (!confirmed) throw new Error("Committed message was not found");
        set((state) => ({
          pendingByPlace: {
            ...state.pendingByPlace,
            [key]: (state.pendingByPlace[key] ?? []).filter(
              (entry) => entry.clientNonce !== pending.clientNonce,
            ),
          },
        }));
      })
      .catch(() => {
        set((state) => ({
          pendingByPlace: {
            ...state.pendingByPlace,
            [key]: (state.pendingByPlace[key] ?? []).map((entry) =>
              entry.clientNonce === pending.clientNonce
                ? { ...entry, failed: true }
                : entry,
            ),
          },
        }));
      });
  };

  return {
    capabilities: backend.capabilities,
    ready: false,
    self: null,
    selfKey: "",
    workspaces: [],
    channels: [],
    dms: [],
    membersByKey: {},
    statusByKey: {},
    messagesByPlace: {},
    pendingByPlace: {},
    lastReadByPlace: {},
    unreadCountByPlace: {},
    mentionCountByPlace: {},
    unreadLineByPlace: {},
    draftByPlace: {},
    typingByPlace: {},
    replyLaterById: {},
    employedAgents: [],
    hasMoreByPlace: {},
    loadingOlderByPlace: {},
    activePlaceKey: null,
    editingMessageId: null,
    replyTargetId: null,
    connection: "connected",

    init() {
      if (initialized) return;
      initialized = true;
      void backend
        .bootstrap()
        .then((snapshot) => {
          const membersByKey: Record<ParticipantKey, MemberProfile> = {};
          for (const member of snapshot.members) {
            membersByKey[participantKey(member.participant)] = member;
          }
          const statusByKey: Record<ParticipantKey, ParticipantStatus> = {};
          for (const status of snapshot.statuses) {
            statusByKey[participantKey(status.participant)] = status;
          }
          const lastReadByPlace: Record<PlaceKey, number> = {};
          for (const marker of snapshot.readMarkers) {
            lastReadByPlace[placeKey(marker.place)] = marker.lastReadSeq;
          }
          const replyLaterById: Record<string, ReplyLaterMarker> = {};
          for (const marker of snapshot.replyLaterMarkers) {
            replyLaterById[marker.markerId] = marker;
          }
          const unreadCountByPlace: Record<PlaceKey, number> = {};
          const mentionCountByPlace: Record<PlaceKey, number> = {};
          const sinceByPlace: Record<PlaceKey, number> = {};
          for (const summary of snapshot.unreadSummaries) {
            const key = placeKey(summary.place);
            unreadCountByPlace[key] = summary.unreadCount;
            mentionCountByPlace[key] = summary.mentionCount;
            sinceByPlace[key] = summary.latestSeq;
          }
          set({
            ready: true,
            capabilities: backend.capabilities,
            self: snapshot.self,
            selfKey: participantKey(snapshot.self),
            workspaces: snapshot.workspaces,
            channels: snapshot.channels,
            dms: snapshot.dms,
            membersByKey,
            statusByKey,
            lastReadByPlace,
            unreadCountByPlace,
            mentionCountByPlace,
            replyLaterById,
            employedAgents: snapshot.employedAgents,
          });
          backend.subscribe(applyEvent, { sinceByPlace });
          backend.subscribeConnection((state) => set({ connection: state }));
        })
        .catch(() => {
          initialized = false;
          set({ connection: "disconnected" });
        });
    },

    selectPlace(key) {
      const place = parsePlaceKey(key);
      if (!place) return;
      const state = get();
      const known =
        place.kind === "channel"
          ? state.channels.some(
              (channel) => channel.channelId === place.channelId,
            )
          : state.dms.some(
              (dm) => dm.kind === place.kind && dm.dmId === place.dmId,
            );
      if (!known) return;
      set((state) => ({
        activePlaceKey: key,
        editingMessageId: null,
        replyTargetId: null,
        unreadLineByPlace: {
          ...state.unreadLineByPlace,
          [key]: state.lastReadByPlace[key] ?? 0,
        },
      }));
      void loadPlace(place);
    },

    async loadPlaceAround(key, seq) {
      const place = parsePlaceKey(key);
      if (!place || !Number.isSafeInteger(seq) || seq < 1) return false;
      if (
        (get().messagesByPlace[key] ?? []).some(
          (message) => message.seq === seq,
        )
      ) {
        return true;
      }
      const messages = await backend.fetchMessages(place, {
        beforeSeq: seq + 1,
        limit: 50,
      });
      set((state) => ({
        messagesByPlace: {
          ...state.messagesByPlace,
          [key]: mergeMessages(state.messagesByPlace[key] ?? [], messages),
        },
      }));
      return messages.some((message) => message.seq === seq);
    },

    setDraft(key, draft) {
      set((state) => ({
        draftByPlace: { ...state.draftByPlace, [key]: draft },
      }));
    },

    send(content, urgency) {
      const state = get();
      const key = state.activePlaceKey;
      const place = key ? parsePlaceKey(key) : null;
      const trimmed = content.trim();
      if (!key || !place || !trimmed || !state.self) return;
      const pending: PendingMessage = {
        clientNonce: secureRandomUUID(),
        content: trimmed,
        mentions: resolveMentions(trimmed, state.membersByKey, state.selfKey),
        urgency,
        replyTo: state.replyTargetId,
        createdAt: Date.now(),
      };
      set((current) => ({
        pendingByPlace: {
          ...current.pendingByPlace,
          [key]: [...(current.pendingByPlace[key] ?? []), pending],
        },
        draftByPlace: { ...current.draftByPlace, [key]: "" },
        replyTargetId: null,
      }));
      dispatchSend(key, pending);
    },

    retrySend(clientNonce) {
      const key = get().activePlaceKey;
      if (!key) return;
      const pending = (get().pendingByPlace[key] ?? []).find(
        (entry) => entry.clientNonce === clientNonce,
      );
      if (!pending) return;
      set((current) => ({
        pendingByPlace: {
          ...current.pendingByPlace,
          [key]: (current.pendingByPlace[key] ?? []).map((entry) =>
            entry.clientNonce === clientNonce
              ? { ...entry, failed: false }
              : entry,
          ),
        },
      }));
      dispatchSend(key, pending);
    },

    startEdit(messageId) {
      set({ editingMessageId: messageId, replyTargetId: null });
    },

    cancelEdit() {
      set({ editingMessageId: null });
    },

    submitEdit(content) {
      const state = get();
      const key = state.activePlaceKey;
      const place = key ? parsePlaceKey(key) : null;
      const messageId = state.editingMessageId;
      const trimmed = content.trim();
      if (!key || !place || !messageId) return;
      if (trimmed) void backend.editMessage(place, messageId, trimmed);
      set({ editingMessageId: null });
    },

    deleteMessage(messageId) {
      const state = get();
      const place = state.activePlaceKey
        ? parsePlaceKey(state.activePlaceKey)
        : null;
      if (!place) return;
      void backend.deleteMessage(place, messageId);
    },

    setReplyTarget(messageId) {
      set({ replyTargetId: messageId, editingMessageId: null });
    },

    noteReadUpTo(key, seq) {
      const state = get();
      const current = state.lastReadByPlace[key] ?? 0;
      if (seq <= current) return;
      const place = parsePlaceKey(key);
      if (!place) return;
      set((entry) => ({
        lastReadByPlace: { ...entry.lastReadByPlace, [key]: seq },
        unreadCountByPlace: {
          ...entry.unreadCountByPlace,
          [key]: (entry.messagesByPlace[key] ?? []).filter(
            (message) =>
              unreadContribution(message, seq, entry.selfKey).unread > 0,
          ).length,
        },
        mentionCountByPlace: {
          ...entry.mentionCountByPlace,
          [key]: (entry.messagesByPlace[key] ?? []).filter(
            (message) =>
              unreadContribution(message, seq, entry.selfKey).mentions > 0,
          ).length,
        },
      }));
      void backend.markRead(place, seq);
    },

    setStatus(status, note) {
      void backend.setStatus(status, note).catch(() => undefined);
    },

    createReplyLater(message, delayMs = DEFAULT_REPLY_LATER_REMIND_MS) {
      void backend
        .createReplyLater(
          message.place,
          message.messageId,
          Date.now() + delayMs,
        )
        .catch(() => undefined);
    },

    toggleReaction(message, emoji) {
      void backend
        .toggleReaction(message.place, message.messageId, emoji)
        .catch(() => undefined);
    },

    async loadOlder(key) {
      const state = get();
      const place = parsePlaceKey(key);
      const current = state.messagesByPlace[key];
      if (!place || !current || current.length === 0) return;
      if (state.loadingOlderByPlace[key] || !state.hasMoreByPlace[key]) return;
      set((entry) => ({
        loadingOlderByPlace: { ...entry.loadingOlderByPlace, [key]: true },
      }));
      const older = await backend.fetchMessages(place, {
        beforeSeq: current[0].seq,
        limit: PAGE_SIZE,
      });
      set((entry) => {
        const existing = entry.messagesByPlace[key] ?? [];
        const known = new Set(existing.map((m) => m.messageId));
        const fresh = older.filter((m) => !known.has(m.messageId));
        return {
          messagesByPlace: {
            ...entry.messagesByPlace,
            [key]: [...fresh, ...existing],
          },
          hasMoreByPlace: {
            ...entry.hasMoreByPlace,
            [key]: older.length >= PAGE_SIZE,
          },
          loadingOlderByPlace: { ...entry.loadingOlderByPlace, [key]: false },
        };
      });
    },

    resolveReplyLater(markerId) {
      void backend.resolveReplyLater(markerId).catch(() => undefined);
    },

    sendTyping() {
      const key = get().activePlaceKey;
      const place = key ? parsePlaceKey(key) : null;
      if (place) backend.sendTyping(place);
    },
  };
});

let messagingSessionIdentity: string | null = null;

export function getMessagingSessionIdentity(): string | null {
  return messagingSessionIdentity;
}

export function bindMessagingSessionIdentity(identity: string | null): void {
  if (identity === messagingSessionIdentity) return;
  messagingSessionIdentity = identity;
  backend.dispose();
  backend = new ApiMessagingBackend();
  initialized = false;
  useMessaging.setState({
    capabilities: backend.capabilities,
    ready: false,
    self: null,
    selfKey: "",
    workspaces: [],
    channels: [],
    dms: [],
    membersByKey: {},
    statusByKey: {},
    messagesByPlace: {},
    pendingByPlace: {},
    lastReadByPlace: {},
    unreadCountByPlace: {},
    mentionCountByPlace: {},
    unreadLineByPlace: {},
    draftByPlace: {},
    typingByPlace: {},
    replyLaterById: {},
    employedAgents: [],
    hasMoreByPlace: {},
    loadingOlderByPlace: {},
    activePlaceKey: null,
    editingMessageId: null,
    replyTargetId: null,
    connection: "disconnected",
  });
}
