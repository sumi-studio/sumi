import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ParticipantRef, Place } from "../model";
import {
  callParticipantsFor,
  incomingCallFor,
  installCallTransportFactory,
  isCallActive,
  useCall,
} from "./call-store";
import type { CallTransport, CallTransportEvents } from "./call-transport";
import type { CallState } from "./model";

const CHANNEL: Place = { kind: "channel", channelId: "c1" };
const DM: Place = { kind: "dm", dmId: "d1" };

const ALICE = { kind: "human", humanId: "alice" } as const;
const BOB = { kind: "human", humanId: "bob" } as const;
const KURO = {
  kind: "personality_agent",
  personalityAgentId: "kuro",
} as const;

function callState(place: Place, participants: ParticipantRef[]): CallState {
  return {
    place,
    active: participants.length > 0,
    startedAt: 1_000,
    participants: participants.map((participant, index) => ({
      participant,
      joinedAt: 1_000 + index,
      screenShare: false,
    })),
  };
}

/** メディアを一切持たないtransport。状態遷移だけを見る。 */
class FakeTransport implements CallTransport {
  readonly log: string[] = [];
  readonly events: CallTransportEvents;
  connectRejects = false;

  constructor(events: CallTransportEvents) {
    this.events = events;
  }

  async connect(): Promise<void> {
    if (this.connectRejects) throw new Error("no media");
    this.log.push("connect");
  }
  async setMicrophoneEnabled(enabled: boolean): Promise<void> {
    this.log.push(`mic:${enabled}`);
  }
  async setCameraEnabled(enabled: boolean): Promise<void> {
    this.log.push(`camera:${enabled}`);
  }
  async setScreenShareEnabled(enabled: boolean): Promise<void> {
    this.log.push(`screen:${enabled}`);
  }
  async disconnect(): Promise<void> {
    this.log.push("disconnect");
  }
}

let transports: FakeTransport[] = [];

beforeEach(() => {
  transports = [];
  installCallTransportFactory((events) => {
    const transport = new FakeTransport(events);
    transports.push(transport);
    return transport;
  });
  useCall.getState().reset();
  vi.restoreAllMocks();
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: string) => {
      if (String(input).endsWith("/call/token")) {
        return new Response(
          JSON.stringify({
            url: "ws://livekit.test",
            token: "t",
            room: "c1",
            identity: "human:alice",
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        );
      }
      return new Response(JSON.stringify({ calls: [] }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }),
  );
});

describe("サーバーが配る通話状態", () => {
  it("placeごとに保持し、終わったplaceは消える", () => {
    const store = useCall.getState();
    store.applyCallState(callState(CHANNEL, [ALICE, KURO]));
    expect(isCallActive(useCall.getState(), "channel:c1")).toBe(true);
    expect(callParticipantsFor(useCall.getState(), "channel:c1")).toEqual([
      ALICE,
      KURO,
    ]);

    store.applyCallState({
      place: CHANNEL,
      active: false,
      startedAt: null,
      participants: [],
    });
    expect(isCallActive(useCall.getState(), "channel:c1")).toBe(false);
    expect(useCall.getState().stateByPlace["channel:c1"]).toBeUndefined();
  });

  it("人格agentも人間と同じ参加者として並ぶ", () => {
    useCall.getState().applyCallState(callState(DM, [KURO]));
    expect(callParticipantsFor(useCall.getState(), "dm:d1")).toEqual([KURO]);
  });
});

describe("着信", () => {
  it("自分のいないDMの通話だけを鳴らす", () => {
    useCall.getState().applyCallState(callState(DM, [BOB]));
    const incoming = incomingCallFor(useCall.getState(), "human:alice");
    expect(incoming?.placeKey).toBe("dm:d1");
    expect(incoming?.from).toEqual(BOB);
  });

  it("自分が既に入っている通話は着信にしない", () => {
    useCall.getState().applyCallState(callState(DM, [BOB, ALICE]));
    expect(incomingCallFor(useCall.getState(), "human:alice")).toBeNull();
  });

  it("チャンネルの通話は着信にしない（呼ばれたわけではない）", () => {
    useCall.getState().applyCallState(callState(CHANNEL, [BOB]));
    expect(incomingCallFor(useCall.getState(), "human:alice")).toBeNull();
  });

  it("拒否したら鳴らないが、通話が終われば次はまた鳴る", () => {
    const store = useCall.getState();
    store.applyCallState(callState(DM, [BOB]));
    store.dismissIncoming("dm:d1");
    expect(incomingCallFor(useCall.getState(), "human:alice")).toBeNull();

    store.applyCallState({
      place: DM,
      active: false,
      startedAt: null,
      participants: [],
    });
    store.applyCallState(callState(DM, [BOB]));
    expect(incomingCallFor(useCall.getState(), "human:alice")?.placeKey).toBe(
      "dm:d1",
    );
  });
});

describe("自分の通話セッション", () => {
  it("参加すると接続し、離れると切る", async () => {
    await useCall.getState().join("channel:c1");
    expect(useCall.getState().phase).toBe("connected");
    expect(useCall.getState().activePlaceKey).toBe("channel:c1");
    expect(transports[0].log).toEqual(["connect"]);

    await useCall.getState().leave();
    expect(useCall.getState().phase).toBe("idle");
    expect(useCall.getState().activePlaceKey).toBeNull();
    expect(transports[0].log).toContain("disconnect");
  });

  it("別のplaceへ参加すると前の通話から抜ける（人は一つの部屋にしかいない）", async () => {
    await useCall.getState().join("channel:c1");
    await useCall.getState().join("dm:d1");
    expect(transports[0].log).toContain("disconnect");
    expect(useCall.getState().activePlaceKey).toBe("dm:d1");
    expect(transports).toHaveLength(2);
  });

  it("マイク・カメラ・画面共有は手元を先に動かし、失敗したら戻す", async () => {
    await useCall.getState().join("channel:c1");
    const store = useCall.getState();

    store.toggleCamera();
    expect(useCall.getState().local.cameraEnabled).toBe(true);
    store.toggleMicrophone();
    expect(useCall.getState().local.micEnabled).toBe(false);
    store.toggleScreenShare();
    expect(useCall.getState().local.screenShareEnabled).toBe(true);
    await Promise.resolve();
    expect(transports[0].log).toEqual([
      "connect",
      "camera:true",
      "mic:false",
      "screen:true",
    ]);

    transports[0].setCameraEnabled = async () => {
      throw new Error("camera busy");
    };
    useCall.getState().toggleCamera();
    expect(useCall.getState().local.cameraEnabled).toBe(false);
    await Promise.resolve();
    await Promise.resolve();
    expect(useCall.getState().local.cameraEnabled).toBe(true);
  });

  it("SFUが未設定なら失敗理由をunavailableとして残し、通話に入らない", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(JSON.stringify({ error: "calls_unavailable" }), {
            status: 503,
            headers: { "Content-Type": "application/json" },
          }),
      ),
    );
    await useCall.getState().join("channel:c1");
    expect(useCall.getState().phase).toBe("failed");
    expect(useCall.getState().failure).toBe("unavailable");
    expect(useCall.getState().activePlaceKey).toBeNull();
  });

  it("回線側で切れたら自分の状態だけを畳む", async () => {
    await useCall.getState().join("channel:c1");
    transports[0].events.onDisconnected();
    expect(useCall.getState().phase).toBe("idle");
    expect(useCall.getState().activePlaceKey).toBeNull();
  });

  it("発話は届いた分だけ点り、他人の状態を消さない", async () => {
    await useCall.getState().join("channel:c1");
    transports[0].events.onSpeaking(["human:bob"]);
    expect(useCall.getState().speakingUntil["human:bob"]).toBeGreaterThan(
      Date.now(),
    );
    transports[0].events.onSpeaking(["personality_agent:kuro"]);
    expect(useCall.getState().speakingUntil["human:bob"]).toBeDefined();
  });
});
