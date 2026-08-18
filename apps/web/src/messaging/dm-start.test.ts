import { afterEach, describe, expect, it, vi } from "vitest";
import { MockMessagingServer } from "./mock-server";
import type { DmSummary, ParticipantRef } from "./model";
import {
  bindMessagingSessionIdentity,
  installMessagingBackend,
  useMessaging,
} from "./store";

/**
 * 「DM開始の保留は同時に一つ」はstoreが持つ契約で、その一つが解放されないと
 * member listの全行・全プロフィールカードの「DMを送る」・グループDMダイアログが
 * まとめて押せないまま残る。解放と拒否をstore越しに直接測る。
 */

const SELF: ParticipantRef = { kind: "human", humanId: "human-a" };
const BOB: ParticipantRef = { kind: "human", humanId: "human-b" };
const CAROL: ParticipantRef = { kind: "human", humanId: "human-c" };

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function signIn() {
  bindMessagingSessionIdentity(null);
  bindMessagingSessionIdentity("human-a");
  const server = new MockMessagingServer();
  installMessagingBackend(server);
  useMessaging.setState({
    ready: true,
    self: SELF,
    selfKey: "human:human-a",
    dms: [],
  });
  return server;
}

describe("DM開始の保留", () => {
  afterEach(() => {
    bindMessagingSessionIdentity(null);
    vi.restoreAllMocks();
  });

  it("開始中は保留が立ち、成功で解放される", async () => {
    const server = signIn();
    const pending = deferred<DmSummary>();
    vi.spyOn(server, "ensureDM").mockReturnValue(pending.promise);

    const attempt = useMessaging.getState().startDM([BOB]);
    expect(useMessaging.getState().startingDM?.participants).toEqual([BOB]);

    pending.resolve({ dmId: "dm-bob", kind: "dm", participants: [SELF, BOB] });

    await expect(attempt).resolves.toBe("dm:dm-bob");
    expect(useMessaging.getState().startingDM).toBeNull();
  });

  it("失敗しても保留は解放され、次のDM開始が通る", async () => {
    const server = signIn();
    const failing = deferred<DmSummary>();
    const ensureDM = vi
      .spyOn(server, "ensureDM")
      .mockReturnValueOnce(failing.promise);

    const attempt = useMessaging.getState().startDM([BOB]);
    expect(useMessaging.getState().startingDM?.participants).toEqual([BOB]);

    failing.reject(new Error("offline"));

    await expect(attempt).rejects.toThrow("offline");
    expect(useMessaging.getState().startingDM).toBeNull();

    ensureDM.mockResolvedValueOnce({
      dmId: "dm-bob",
      kind: "dm",
      participants: [SELF, BOB],
    });
    await expect(useMessaging.getState().startDM([BOB])).resolves.toBe(
      "dm:dm-bob",
    );
    expect(useMessaging.getState().startingDM).toBeNull();
  });

  it("保留中の2本目はstoreが拒み、1本目の相手を上書きしない", async () => {
    const server = signIn();
    const pending = deferred<DmSummary>();
    vi.spyOn(server, "ensureDM").mockReturnValue(pending.promise);

    const first = useMessaging.getState().startDM([BOB]);

    await expect(useMessaging.getState().startDM([CAROL])).rejects.toThrow(
      "A DM start is already pending",
    );
    expect(server.ensureDM).toHaveBeenCalledTimes(1);
    expect(server.ensureDM).toHaveBeenCalledWith(BOB);
    expect(useMessaging.getState().startingDM?.participants).toEqual([BOB]);

    pending.resolve({ dmId: "dm-bob", kind: "dm", participants: [SELF, BOB] });
    await expect(first).resolves.toBe("dm:dm-bob");
    expect(useMessaging.getState().startingDM).toBeNull();
  });

  it("古い開始の完了は、新しい保留を解放しない", async () => {
    const participants = [BOB];
    const firstServer = signIn();
    const firstPending = deferred<DmSummary>();
    vi.spyOn(firstServer, "ensureDM").mockReturnValue(firstPending.promise);
    const first = useMessaging.getState().startDM(participants);

    // session reset後に同じ配列を再利用して次の開始ができる形を再現する。
    const secondServer = signIn();
    const secondPending = deferred<DmSummary>();
    vi.spyOn(secondServer, "ensureDM").mockReturnValue(secondPending.promise);
    const second = useMessaging.getState().startDM(participants);

    firstPending.resolve({
      dmId: "dm-first",
      kind: "dm",
      participants: [SELF, BOB],
    });
    await expect(first).rejects.toThrow(
      "Messaging session changed during DM start",
    );
    expect(useMessaging.getState().startingDM?.participants).toBe(participants);

    secondPending.resolve({
      dmId: "dm-second",
      kind: "dm",
      participants: [SELF, BOB],
    });
    await expect(second).resolves.toBe("dm:dm-second");
    expect(useMessaging.getState().startingDM).toBeNull();
  });
});
