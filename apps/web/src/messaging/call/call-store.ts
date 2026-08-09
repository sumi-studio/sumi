/**
 * 通話の状態。ADR 0012の境界をそのまま持つ:
 *
 * - 「どのplaceで誰が通話しているか」の正本はサーバー。call_stateイベントで
 *   届き、再接続時は GET /messaging/calls で読み直す。ここはその写し。
 * - 「自分が今どこにいるか」（接続段階・マイク・カメラ・画面共有・届いている
 *   映像）はこの端末の事実で、サーバーには持たせない。
 *
 * 通話はテキストの上に乗る追加の層で、通話が動かなくてもメッセージングは
 * そのまま動く。したがってここでの失敗はすべて局所的に畳む。
 */

import { create } from "zustand";
import type { ParticipantKey, ParticipantRef, Place, PlaceKey } from "../model";
import { participantKey, placeKey } from "../model";
import { CallAPIError, fetchCallStates, fetchCallTicket } from "./call-api";
import {
  type CallTransport,
  type CallTransportFactory,
  createLiveKitTransport,
} from "./call-transport";
import type {
  CallLocalState,
  CallMediaTrack,
  CallPhase,
  CallState,
} from "./model";

/** 発話中の表示を消すまでの猶予。喋るたびに点滅させない。 */
const SPEAKING_TTL_MS = 1_200;

let transportFactory: CallTransportFactory = createLiveKitTransport;
let transport: CallTransport | null = null;
let joinGeneration = 0;
let snapshotGeneration = 0;
let reconciliation: {
  generation: number;
  liveEvents: CallState[];
} | null = null;

/** テストと開発ハーネスはメディア層を差し替えられる。 */
export function installCallTransportFactory(
  factory: CallTransportFactory,
): void {
  transportFactory = factory;
}

/** 着信の一件。相手が始めた通話のうち、まだ応答も拒否もしていないもの。 */
export interface IncomingCall {
  placeKey: PlaceKey;
  place: Place;
  /** 通話を始めた（=最初に入った）参加者。 */
  from: ParticipantRef;
}

interface CallStoreState {
  /** placeごとの通話状態。サーバーの写し。 */
  stateByPlace: Record<PlaceKey, CallState>;
  /** 自分が入っているplace。同時に一つ——人は二つの部屋で同時に話せない。 */
  activePlaceKey: PlaceKey | null;
  phase: CallPhase;
  /** 通話に入れなかった理由。SFU未設定と権限falseを言い分ける。 */
  failure: "unavailable" | "failed" | null;
  local: CallLocalState;
  tracks: CallMediaTrack[];
  speakingUntil: Record<ParticipantKey, number>;
  /** 拒否・応答済みで、もう鳴らさない通話（place単位）。 */
  dismissedPlaces: Record<PlaceKey, boolean>;

  /** 接続直後の現在値を読み込む。SFUが無い環境では黙って何もしない。 */
  hydrate(): Promise<void>;
  applyCallState(state: CallState): void;
  join(key: PlaceKey): Promise<void>;
  leave(): Promise<void>;
  toggleMicrophone(): void;
  toggleCamera(): void;
  toggleScreenShare(): void;
  dismissIncoming(key: PlaceKey): void;
  /** セッションが変わったら通話も終える。 */
  reset(): void;
}

const IDLE_LOCAL: CallLocalState = {
  micEnabled: true,
  cameraEnabled: false,
  screenShareEnabled: false,
};

export const useCall = create<CallStoreState>((set, get) => {
  const eventsFor = (
    generation: number,
    ownedTransport: () => CallTransport | null,
  ) => {
    const ownsCurrentCall = () =>
      joinGeneration === generation && transport === ownedTransport();
    return {
      onTracks(tracks: CallMediaTrack[]) {
        if (ownsCurrentCall()) set({ tracks });
      },
      onSpeaking(participants: ParticipantKey[]) {
        if (!ownsCurrentCall()) return;
        const until = Date.now() + SPEAKING_TTL_MS;
        set((state) => {
          const speakingUntil = { ...state.speakingUntil };
          for (const key of participants) speakingUntil[key] = until;
          return { speakingUntil };
        });
      },
      onParticipants(_participants: ParticipantKey[]) {
        // 在室者の正本はサーバーのcall_state。ここでは受け取るだけにして、
        // 二つの真実を画面に並べない。
      },
      onDisconnected() {
        if (!ownsCurrentCall()) return;
        joinGeneration += 1;
        transport = null;
        set({
          activePlaceKey: null,
          phase: "idle",
          tracks: [],
          local: IDLE_LOCAL,
        });
      },
    };
  };

  return {
    stateByPlace: {},
    activePlaceKey: null,
    phase: "idle",
    failure: null,
    local: IDLE_LOCAL,
    tracks: [],
    speakingUntil: {},
    dismissedPlaces: {},

    async hydrate() {
      const generation = ++snapshotGeneration;
      const pending = { generation, liveEvents: [] };
      reconciliation = pending;
      try {
        const states = await fetchCallStates();
        if (snapshotGeneration !== generation || reconciliation !== pending) {
          return;
        }
        set((current) => {
          let stateByPlace: Record<PlaceKey, CallState> = {};
          let dismissedPlaces: Record<PlaceKey, boolean> = {};
          for (const state of states) {
            const key = placeKey(state.place);
            if (
              current.dismissedPlaces[key] &&
              current.stateByPlace[key]?.startedAt === state.startedAt
            ) {
              dismissedPlaces[key] = true;
            }
            ({ stateByPlace, dismissedPlaces } = reduceCallState(
              stateByPlace,
              dismissedPlaces,
              state,
            ));
          }
          for (const event of pending.liveEvents) {
            ({ stateByPlace, dismissedPlaces } = reduceCallState(
              stateByPlace,
              dismissedPlaces,
              event,
            ));
          }
          return { stateByPlace, dismissedPlaces };
        });
      } catch {
        // 通話が使えないことはメッセージングの失敗ではない。
      } finally {
        if (reconciliation === pending) reconciliation = null;
      }
    },

    applyCallState(state) {
      reconciliation?.liveEvents.push(state);
      set((current) =>
        reduceCallState(current.stateByPlace, current.dismissedPlaces, state),
      );
    },

    async join(key) {
      const state = get();
      if (state.activePlaceKey === key && state.phase !== "idle") return;
      if (state.activePlaceKey && state.activePlaceKey !== key) {
        await get().leave();
      }
      const place = parsePlaceKeyStrict(key);
      if (!place) return;
      const generation = ++joinGeneration;
      set({
        activePlaceKey: key,
        phase: "connecting",
        failure: null,
        local: IDLE_LOCAL,
        tracks: [],
        dismissedPlaces: { ...get().dismissedPlaces, [key]: true },
      });
      let created: CallTransport | null = null;
      try {
        const ticket = await fetchCallTicket(place);
        if (joinGeneration !== generation) return;
        const events = eventsFor(generation, () => created);
        created = transportFactory(events);
        if (joinGeneration !== generation) {
          await disconnectQuietly(created);
          return;
        }
        transport = created;
        await created.connect(ticket);
        if (joinGeneration !== generation || transport !== created) {
          await disconnectQuietly(created);
          return;
        }
        set({ phase: "connected" });
      } catch (error) {
        if (joinGeneration === generation) {
          joinGeneration += 1;
          if (transport === created) transport = null;
          set({
            activePlaceKey: null,
            phase: "failed",
            failure:
              error instanceof CallAPIError && error.unavailable
                ? "unavailable"
                : "failed",
            tracks: [],
          });
        }
        await disconnectQuietly(created);
      }
    },

    async leave() {
      joinGeneration += 1;
      const current = transport;
      transport = null;
      set({
        activePlaceKey: null,
        phase: "idle",
        failure: null,
        tracks: [],
        local: IDLE_LOCAL,
      });
      try {
        await current?.disconnect();
      } catch {
        // 切れたことの方が大事で、切り方の失敗は握り潰してよい。
      }
    },

    toggleMicrophone() {
      const next = !get().local.micEnabled;
      set((state) => ({ local: { ...state.local, micEnabled: next } }));
      void transport?.setMicrophoneEnabled(next).catch(() => {
        set((state) => ({ local: { ...state.local, micEnabled: !next } }));
      });
    },

    toggleCamera() {
      const next = !get().local.cameraEnabled;
      set((state) => ({ local: { ...state.local, cameraEnabled: next } }));
      void transport?.setCameraEnabled(next).catch(() => {
        set((state) => ({ local: { ...state.local, cameraEnabled: !next } }));
      });
    },

    toggleScreenShare() {
      const next = !get().local.screenShareEnabled;
      set((state) => ({ local: { ...state.local, screenShareEnabled: next } }));
      void transport?.setScreenShareEnabled(next).catch(() => {
        set((state) => ({
          local: { ...state.local, screenShareEnabled: !next },
        }));
      });
    },

    dismissIncoming(key) {
      set((state) => ({
        dismissedPlaces: { ...state.dismissedPlaces, [key]: true },
      }));
    },

    reset() {
      joinGeneration += 1;
      snapshotGeneration += 1;
      reconciliation = null;
      const current = transport;
      transport = null;
      void current?.disconnect().catch(() => undefined);
      set({
        stateByPlace: {},
        activePlaceKey: null,
        phase: "idle",
        failure: null,
        local: IDLE_LOCAL,
        tracks: [],
        speakingUntil: {},
        dismissedPlaces: {},
      });
    },
  };
});

function reduceCallState(
  currentStates: Record<PlaceKey, CallState>,
  currentDismissed: Record<PlaceKey, boolean>,
  state: CallState,
): Pick<CallStoreState, "stateByPlace" | "dismissedPlaces"> {
  const key = placeKey(state.place);
  const stateByPlace = { ...currentStates };
  const dismissedPlaces = { ...currentDismissed };
  const previous = stateByPlace[key];
  if (!state.active && state.participants.length === 0) {
    delete stateByPlace[key];
    delete dismissedPlaces[key];
  } else {
    stateByPlace[key] = state;
    // 終了eventを取り逃して同じplaceで次の通話が始まっていても、前回の拒否を
    // 引き継がない。startedAtは一つの通話を識別するserver-owned generation。
    if (previous && previous.startedAt !== state.startedAt) {
      delete dismissedPlaces[key];
    }
  }
  return { stateByPlace, dismissedPlaces };
}

async function disconnectQuietly(current: CallTransport | null): Promise<void> {
  try {
    await current?.disconnect();
  } catch {
    // 部分接続を畳むbest effort。storeの所有権は既に外している。
  }
}

function parsePlaceKeyStrict(key: PlaceKey): Place | null {
  const separator = key.indexOf(":");
  if (separator < 0) return null;
  const kind = key.slice(0, separator);
  const id = key.slice(separator + 1);
  if (!id) return null;
  if (kind === "channel") return { kind, channelId: id };
  if (kind === "dm" || kind === "group_dm") return { kind, dmId: id };
  return null;
}

/**
 * 今このplaceの通話に入っている参加者。placeを見ているUIはこれだけを読む。
 */
export function callParticipantsFor(
  state: Pick<CallStoreState, "stateByPlace">,
  key: PlaceKey,
): ParticipantRef[] {
  return (state.stateByPlace[key]?.participants ?? []).map(
    (entry) => entry.participant,
  );
}

/** そのplaceで通話が続いているか。 */
export function isCallActive(
  state: Pick<CallStoreState, "stateByPlace">,
  key: PlaceKey,
): boolean {
  const call = state.stateByPlace[key];
  return call !== undefined && (call.active || call.participants.length > 0);
}

/**
 * 今鳴らすべき着信。DM・グループDMで、自分は入っておらず、まだ応答も拒否も
 * していないもの。channelの通話は着信にしない——チャンネルは「入れる場所」で
 * あって、そこにいる誰かが自分を呼んだわけではない。
 */
export function incomingCallFor(
  state: Pick<
    CallStoreState,
    "stateByPlace" | "activePlaceKey" | "dismissedPlaces"
  >,
  selfKey: ParticipantKey,
): IncomingCall | null {
  for (const [key, call] of Object.entries(state.stateByPlace)) {
    if (call.place.kind === "channel") continue;
    if (state.activePlaceKey === key) continue;
    if (state.dismissedPlaces[key]) continue;
    if (call.participants.length === 0) continue;
    const others = call.participants.filter(
      (entry) => participantKey(entry.participant) !== selfKey,
    );
    if (others.length === 0) continue;
    const inCall = call.participants.some(
      (entry) => participantKey(entry.participant) === selfKey,
    );
    if (inCall) continue;
    return { placeKey: key, place: call.place, from: others[0].participant };
  }
  return null;
}
