import { create } from "zustand";
import type { ParticipantKey, ParticipantRef, Place, PlaceKey } from "../model";
import { parsePlaceKey, participantKey, placeKey } from "../model";
import { CallAPIError, fetchCallStates, fetchCallTicket } from "./call-api";
import {
  type CallTransport,
  type CallTransportFactory,
  createLiveKitTransport,
} from "./call-transport";
import type {
  CallFailure,
  CallLocalState,
  CallMediaTrack,
  CallPhase,
  CallState,
  CallTicket,
} from "./model";

const SPEAKING_TTL_MS = 1_200;

let transportFactory: CallTransportFactory = createLiveKitTransport;
let transport: CallTransport | null = null;
let joinGeneration = 0;
let snapshotGeneration = 0;
let reconciliation: { generation: number; liveEvents: CallState[] } | null =
  null;

export function installCallTransportFactory(
  factory: CallTransportFactory,
): void {
  transportFactory = factory;
}

export interface IncomingCall {
  placeKey: PlaceKey;
  place: Place;
  from: ParticipantRef;
}

interface CallStoreState {
  stateByPlace: Record<PlaceKey, CallState>;
  activePlaceKey: PlaceKey | null;
  phase: CallPhase;
  failure: CallFailure | null;
  failurePlaceKey: PlaceKey | null;
  local: CallLocalState;
  tracks: CallMediaTrack[];
  audioPlaybackBlocked: boolean;
  speakingUntil: Record<ParticipantKey, number>;
  dismissedPlaces: Record<PlaceKey, boolean>;

  hydrate(): Promise<void>;
  applyCallState(state: CallState): void;
  join(key: PlaceKey): Promise<void>;
  leave(): Promise<void>;
  toggleMicrophone(): void;
  toggleCamera(): void;
  toggleScreenShare(): void;
  resumeAudio(): Promise<void>;
  dismissIncoming(key: PlaceKey): void;
  dismissFailure(): void;
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
        // Server call_state remains the one participant-presence projection.
      },
      onAudioPlaybackBlocked(blocked: boolean) {
        if (ownsCurrentCall()) set({ audioPlaybackBlocked: blocked });
      },
      onDisconnected() {
        if (!ownsCurrentCall()) return;
        joinGeneration += 1;
        transport = null;
        set({
          activePlaceKey: null,
          phase: "failed",
          failure: "connection_failed",
          failurePlaceKey: get().activePlaceKey,
          tracks: [],
          audioPlaybackBlocked: false,
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
    failurePlaceKey: null,
    local: IDLE_LOCAL,
    tracks: [],
    audioPlaybackBlocked: false,
    speakingUntil: {},
    dismissedPlaces: {},

    async hydrate() {
      const generation = ++snapshotGeneration;
      const pending = { generation, liveEvents: [] as CallState[] };
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
        // Calls are optional; text bootstrap owns messaging availability.
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
      const currentState = get();
      if (
        currentState.activePlaceKey === key &&
        currentState.phase !== "idle"
      ) {
        return;
      }
      if (currentState.activePlaceKey && currentState.activePlaceKey !== key) {
        await get().leave();
      }
      const place = parsePlaceKey(key);
      if (!place) return;
      const environmentFailure = callEnvironmentFailure();
      if (environmentFailure) {
        set({
          activePlaceKey: null,
          phase: "failed",
          failure: environmentFailure,
          failurePlaceKey: key,
        });
        return;
      }

      const generation = ++joinGeneration;
      set({
        activePlaceKey: key,
        phase: "connecting",
        failure: null,
        failurePlaceKey: null,
        local: IDLE_LOCAL,
        tracks: [],
        audioPlaybackBlocked: false,
        dismissedPlaces: { ...get().dismissedPlaces, [key]: true },
      });
      let created: CallTransport | null = null;
      try {
        await confirmMicrophonePermission();
        if (joinGeneration !== generation) return;
        const ticket = await fetchCallTicket(place);
        assertSafeSignallingURL(ticket);
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
            failure: classifyCallFailure(error),
            failurePlaceKey: key,
            tracks: [],
            audioPlaybackBlocked: false,
            local: IDLE_LOCAL,
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
        failurePlaceKey: null,
        tracks: [],
        audioPlaybackBlocked: false,
        local: IDLE_LOCAL,
      });
      try {
        await current?.disconnect();
      } catch {
        // Local ownership is already released; disconnect is best effort.
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
      set((state) => ({
        local: { ...state.local, screenShareEnabled: next },
      }));
      void transport?.setScreenShareEnabled(next).catch(() => {
        set((state) => ({
          local: { ...state.local, screenShareEnabled: !next },
        }));
      });
    },

    async resumeAudio() {
      const current = transport;
      const generation = joinGeneration;
      if (!current) return;
      await current.resumeAudio();
      if (joinGeneration === generation && transport === current) {
        set({ audioPlaybackBlocked: false });
      }
    },

    dismissIncoming(key) {
      set((state) => ({
        dismissedPlaces: { ...state.dismissedPlaces, [key]: true },
      }));
    },

    dismissFailure() {
      set({ failure: null, failurePlaceKey: null, phase: "idle" });
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
        failurePlaceKey: null,
        local: IDLE_LOCAL,
        tracks: [],
        audioPlaybackBlocked: false,
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
    if (previous && previous.startedAt !== state.startedAt) {
      delete dismissedPlaces[key];
    }
  }
  return { stateByPlace, dismissedPlaces };
}

function callEnvironmentFailure(): CallFailure | null {
  if (globalThis.isSecureContext === false) return "insecure_context";
  if (!navigator.mediaDevices?.getUserMedia) return "insecure_context";
  return null;
}

async function confirmMicrophonePermission(): Promise<void> {
  const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
  for (const track of stream.getTracks()) track.stop();
}

function assertSafeSignallingURL(ticket: CallTicket): void {
  let signalling: URL;
  try {
    signalling = new URL(
      ticket.url,
      globalThis.location?.href ?? "https://invalid.local/",
    );
  } catch {
    throw new CallConnectionError("connection_failed");
  }
  if (signalling.protocol !== "ws:" && signalling.protocol !== "wss:") {
    throw new CallConnectionError("connection_failed");
  }
  const pageHost = globalThis.location?.hostname.toLowerCase();
  const isLoopbackPage = pageHost === "localhost" || pageHost === "127.0.0.1";
  if (
    (globalThis.location?.protocol === "https:" ||
      globalThis.isSecureContext === true) &&
    signalling.protocol !== "wss:" &&
    !isLoopbackPage
  ) {
    throw new CallConnectionError("mixed_content");
  }
}

class CallConnectionError extends Error {
  readonly failure: CallFailure;

  constructor(failure: CallFailure) {
    super(failure);
    this.failure = failure;
  }
}

function classifyCallFailure(error: unknown): CallFailure {
  if (error instanceof CallConnectionError) return error.failure;
  if (error instanceof CallAPIError) {
    if (error.unavailable) return "unavailable";
    if ([401, 403, 404].includes(error.status)) return "not_allowed";
  }
  if (
    error instanceof DOMException &&
    ["NotAllowedError", "NotFoundError", "NotReadableError"].includes(
      error.name,
    )
  ) {
    return "microphone_denied";
  }
  return "connection_failed";
}

async function disconnectQuietly(current: CallTransport | null): Promise<void> {
  try {
    await current?.disconnect();
  } catch {
    // Partial connections are already detached from store ownership.
  }
}

export function callParticipantsFor(
  state: Pick<CallStoreState, "stateByPlace">,
  key: PlaceKey,
): ParticipantRef[] {
  return (state.stateByPlace[key]?.participants ?? []).map(
    (entry) => entry.participant,
  );
}

export function isCallActive(
  state: Pick<CallStoreState, "stateByPlace">,
  key: PlaceKey,
): boolean {
  const call = state.stateByPlace[key];
  return call !== undefined && (call.active || call.participants.length > 0);
}

export function incomingCallFor(
  state: Pick<
    CallStoreState,
    "stateByPlace" | "activePlaceKey" | "dismissedPlaces"
  >,
  selfKey: ParticipantKey,
): IncomingCall | null {
  for (const [key, call] of Object.entries(state.stateByPlace)) {
    if (call.place.kind === "channel") continue;
    if (state.activePlaceKey === key || state.dismissedPlaces[key]) continue;
    if (call.participants.length === 0) continue;
    const others = call.participants.filter(
      (entry) => participantKey(entry.participant) !== selfKey,
    );
    if (others.length === 0) continue;
    if (
      call.participants.some(
        (entry) => participantKey(entry.participant) === selfKey,
      )
    ) {
      continue;
    }
    return {
      placeKey: key,
      place: call.place,
      from: others[0].participant,
    };
  }
  return null;
}
