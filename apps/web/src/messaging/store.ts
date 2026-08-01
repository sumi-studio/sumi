import { create } from "zustand";
import { secureRandomUUID } from "../lib/random-uuid";
import { MockMessagingServer } from "./mock-server";
import type {
  ChannelSummary,
  DmSummary,
  MemberProfile,
  Message,
  MessagingBackend,
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
import { removeMessage, upsertMessage } from "./timeline";

const TYPING_TTL_MS = 4_500;
const DEFAULT_REPLY_LATER_REMIND_MS = 30 * 60_000;

/**
 * 実装差し替え点。実API接続時はここをWS+RESTクライアントに置き換える。
 */
const backend: MessagingBackend = new MockMessagingServer();

interface MessagingState {
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
  /** placeへ入った時点のlastReadのスナップショット。離れるまで動かさない。 */
  unreadLineByPlace: Record<PlaceKey, number | null>;
  draftByPlace: Record<PlaceKey, string>;
  typingByPlace: Record<PlaceKey, Record<ParticipantKey, number>>;
  replyLaterById: Record<string, ReplyLaterMarker>;
  activePlaceKey: PlaceKey | null;
  editingMessageId: string | null;
  replyTargetId: string | null;

  init(): void;
  selectPlace(key: PlaceKey): void;
  setDraft(key: PlaceKey, draft: string): void;
  send(content: string, urgency: Urgency): void;
  startEdit(messageId: string): void;
  cancelEdit(): void;
  submitEdit(content: string): void;
  deleteMessage(messageId: string): void;
  setReplyTarget(messageId: string | null): void;
  noteReadUpTo(key: PlaceKey, seq: number): void;
  setStatus(status: StatusKind, note: string): void;
  createReplyLater(message: Message): void;
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
    if (content.includes(`@${member.displayName}`)) {
      mentions.push(member.participant);
    }
  }
  return mentions;
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
    if (event.type === "message_created" || event.type === "message_edited") {
      const key = placeKey(event.message.place);
      set((state) => {
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
        return {
          messagesByPlace: { ...state.messagesByPlace, [key]: messages },
          pendingByPlace: { ...state.pendingByPlace, [key]: pending },
          typingByPlace: { ...state.typingByPlace, [key]: typing },
          lastReadByPlace: { ...state.lastReadByPlace, [key]: lastRead },
        };
      });
      return;
    }
    if (event.type === "message_deleted") {
      const key = placeKey(event.place);
      set((state) => ({
        messagesByPlace: {
          ...state.messagesByPlace,
          [key]: removeMessage(
            state.messagesByPlace[key] ?? [],
            event.messageId,
          ),
        },
      }));
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

  const loadPlace = async (place: Place) => {
    const key = placeKey(place);
    if (get().messagesByPlace[key]) return;
    const messages = await backend.fetchMessages(place);
    set((state) => ({
      messagesByPlace: { ...state.messagesByPlace, [key]: messages },
    }));
  };

  return {
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
    unreadLineByPlace: {},
    draftByPlace: {},
    typingByPlace: {},
    replyLaterById: {},
    activePlaceKey: null,
    editingMessageId: null,
    replyTargetId: null,

    init() {
      if (initialized) return;
      initialized = true;
      backend.subscribe(applyEvent);
      void backend.bootstrap().then((snapshot) => {
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
        set({
          ready: true,
          self: snapshot.self,
          selfKey: participantKey(snapshot.self),
          workspaces: snapshot.workspaces,
          channels: snapshot.channels,
          dms: snapshot.dms,
          membersByKey,
          statusByKey,
          lastReadByPlace,
          replyLaterById,
        });
        const first = snapshot.channels[0];
        if (first) {
          get().selectPlace(
            placeKey({ kind: "channel", channelId: first.channelId }),
          );
        }
      });
    },

    selectPlace(key) {
      const place = parsePlaceKey(key);
      if (!place) return;
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
      backend.sendMessage({
        place,
        content: pending.content,
        mentions: pending.mentions,
        urgency: pending.urgency,
        replyTo: pending.replyTo,
        clientNonce: pending.clientNonce,
      });
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
      if (trimmed) backend.editMessage(place, messageId, trimmed);
      set({ editingMessageId: null });
    },

    deleteMessage(messageId) {
      const state = get();
      const place = state.activePlaceKey
        ? parsePlaceKey(state.activePlaceKey)
        : null;
      if (!place) return;
      backend.deleteMessage(place, messageId);
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
      }));
      backend.markRead(place, seq);
    },

    setStatus(status, note) {
      backend.setStatus(status, note);
    },

    createReplyLater(message) {
      backend.createReplyLater(
        message.place,
        message.messageId,
        Date.now() + DEFAULT_REPLY_LATER_REMIND_MS,
      );
    },

    resolveReplyLater(markerId) {
      backend.resolveReplyLater(markerId);
    },

    sendTyping() {
      const key = get().activePlaceKey;
      const place = key ? parsePlaceKey(key) : null;
      if (place) backend.sendTyping(place);
    },
  };
});
