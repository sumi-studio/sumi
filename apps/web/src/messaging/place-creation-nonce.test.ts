// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { MockMessagingServer } from "./mock-server";
import { type ParticipantRef, participantKey } from "./model";
import {
  PlaceCreationAttemptCapacityError,
  PlaceCreationAttemptLedger,
} from "./place-creation-attempt-ledger";
import {
  bindMessagingSessionIdentity,
  installMessagingBackend,
  useMessaging,
} from "./store";

const SELF: ParticipantRef = { kind: "human", humanId: "human-a" };
const BOB: ParticipantRef = { kind: "human", humanId: "human-b" };
const CAROL: ParticipantRef = { kind: "human", humanId: "human-c" };

function signIn(): MockMessagingServer {
  bindMessagingSessionIdentity(null);
  bindMessagingSessionIdentity("human-a");
  const server = new MockMessagingServer();
  installMessagingBackend(server);
  useMessaging.setState({
    ready: true,
    self: SELF,
    selfKey: "human:human-a",
    channels: [],
    dms: [],
  });
  return server;
}

describe("place creation logical-attempt nonces", () => {
  afterEach(() => {
    bindMessagingSessionIdentity(null);
    sessionStorage.clear();
    vi.restoreAllMocks();
  });

  it("retains a channel nonce for manual retry and renews it after success", async () => {
    const server = signIn();
    const create = server.createChannel.bind(server);
    const nonces: string[] = [];
    vi.spyOn(server, "createChannel").mockImplementation(async (...args) => {
      nonces.push(args[4]);
      if (nonces.length === 1) throw new TypeError("ambiguous response loss");
      return create(...args);
    });

    const gesture = () =>
      useMessaging
        .getState()
        .createChannel("workspace-1", "incident", "coordination", false);
    await expect(gesture()).rejects.toThrow("ambiguous response loss");
    const reconciled = await gesture();
    const [laterGesture, concurrent] = await Promise.all([
      gesture(),
      gesture(),
    ]);

    expect(nonces[0]).toBeTruthy();
    expect(nonces[1]).toBe(nonces[0]);
    expect(nonces[2]).not.toBe(nonces[1]);
    expect(nonces[3]).toBe(nonces[2]);
    expect(laterGesture).not.toBe(reconciled);
    expect(concurrent).toBe(laterGesture);
  });

  it("retains a group-DM nonce for manual retry and renews it after success", async () => {
    const server = signIn();
    const create = server.createGroupDM.bind(server);
    const nonces: string[] = [];
    const participantSets: string[][] = [];
    vi.spyOn(server, "createGroupDM").mockImplementation(async (...args) => {
      nonces.push(args[1]);
      participantSets.push(args[0].map(participantKey));
      if (nonces.length === 1) throw new TypeError("ambiguous response loss");
      return create(...args);
    });

    await expect(
      useMessaging.getState().startDM([CAROL, BOB, CAROL]),
    ).rejects.toThrow("ambiguous response loss");
    const gesture = () => useMessaging.getState().startDM([BOB, CAROL]);
    const reconciled = await gesture();
    const laterGesture = await gesture();

    expect(participantSets).toEqual([
      ["human:human-b", "human:human-c"],
      ["human:human-b", "human:human-c"],
      ["human:human-b", "human:human-c"],
    ]);
    expect(nonces[0]).toBeTruthy();
    expect(nonces[1]).toBe(nonces[0]);
    expect(nonces[2]).not.toBe(nonces[1]);
    expect(laterGesture).not.toBe(reconciled);
  });

  it("converges concurrent duplicate invocations and renews after success", async () => {
    const server = signIn();
    const duplicate = server.duplicateChannel.bind(server);
    const nonces: string[] = [];
    vi.spyOn(server, "duplicateChannel").mockImplementation(async (...args) => {
      nonces.push(args[1]);
      return duplicate(...args);
    });

    const gesture = () =>
      useMessaging.getState().duplicateChannel("ch-general");
    const [first, concurrent] = await Promise.all([gesture(), gesture()]);
    const laterGesture = await gesture();

    expect(first).toBe(concurrent);
    expect(nonces[0]).toBeTruthy();
    expect(nonces[1]).toBe(nonces[0]);
    expect(nonces[2]).not.toBe(nonces[1]);
    expect(laterGesture).not.toBe(first);
  });

  it("reuses the unresolved duplicate nonce after exhausted reconciliation", async () => {
    const server = signIn();
    const duplicate = server.duplicateChannel.bind(server);
    const nonces: string[] = [];
    vi.spyOn(server, "duplicateChannel").mockImplementation(async (...args) => {
      nonces.push(args[1]);
      if (nonces.length === 1) {
        throw new TypeError("ambiguous reconciliation exhausted");
      }
      return duplicate(...args);
    });

    const gesture = () =>
      useMessaging.getState().duplicateChannel("ch-general");
    await expect(gesture()).rejects.toThrow(
      "ambiguous reconciliation exhausted",
    );
    await expect(gesture()).resolves.toMatch(/^channel:/);

    expect(nonces).toHaveLength(2);
    expect(nonces[0]).toBeTruthy();
    expect(nonces[1]).toBe(nonces[0]);
  });

  it("does not carry an unresolved nonce into a replacement session", async () => {
    const firstServer = signIn();
    let staleNonce = "";
    vi.spyOn(firstServer, "createChannel").mockImplementation(
      async (_workspaceId, _name, _topic, _voice, clientNonce) => {
        staleNonce = clientNonce;
        throw new TypeError("ambiguous response loss");
      },
    );
    const gesture = () =>
      useMessaging
        .getState()
        .createChannel("workspace-1", "session-fenced", "", false);
    await expect(gesture()).rejects.toThrow("ambiguous response loss");

    const replacement = signIn();
    const create = replacement.createChannel.bind(replacement);
    let replacementNonce = "";
    vi.spyOn(replacement, "createChannel").mockImplementation(
      async (...args) => {
        replacementNonce = args[4];
        return create(...args);
      },
    );
    await expect(gesture()).resolves.toMatch(/^channel:/);

    expect(staleNonce).toBeTruthy();
    expect(replacementNonce).toBeTruthy();
    expect(replacementNonce).not.toBe(staleNonce);
  });

  it("fails closed at capacity without evicting the oldest attempt", async () => {
    const server = signIn();
    const calls: Array<{ name: string; nonce: string }> = [];
    vi.spyOn(server, "createChannel").mockImplementation(
      async (_workspaceId, name, _topic, _voice, clientNonce) => {
        calls.push({ name, nonce: clientNonce });
        throw new TypeError("ambiguous response loss");
      },
    );
    const create = (name: string) =>
      useMessaging.getState().createChannel("workspace-1", name, "", false);

    for (let index = 0; index < 32; index += 1) {
      await expect(create(`unresolved-${index}`)).rejects.toThrow(
        "ambiguous response loss",
      );
    }
    const oldestNonce = calls[0]?.nonce;
    await expect(create("unresolved-32")).rejects.toBeInstanceOf(
      PlaceCreationAttemptCapacityError,
    );
    expect(calls).toHaveLength(32);

    await expect(create("unresolved-0")).rejects.toThrow(
      "ambiguous response loss",
    );
    expect(calls).toHaveLength(33);
    expect(calls.at(-1)).toEqual({
      name: "unresolved-0",
      nonce: oldestNonce,
    });
  });

  it("survives a hard reload until authoritative acknowledgement", () => {
    const owner = JSON.stringify([
      "human-a",
      "workspace-1",
      "installation-1",
      "7",
    ]);
    const declaration = JSON.stringify([
      "create_channel",
      "workspace-1",
      "reload-safe",
      "",
      false,
    ]);
    let generated = 0;
    const nonceFactory = () => `reload-nonce-${++generated}`;

    const beforeReload = new PlaceCreationAttemptLedger(
      sessionStorage,
      32,
      nonceFactory,
    );
    beforeReload.activate(owner, true);
    const committedButAmbiguous = beforeReload.nonceFor(declaration);

    // A new ledger instance is the relevant hard-page-reload boundary. No
    // in-memory state is shared, but the exact authority loads the same nonce.
    const afterReload = new PlaceCreationAttemptLedger(
      sessionStorage,
      32,
      nonceFactory,
    );
    afterReload.activate(owner, true);
    expect(afterReload.nonceFor(declaration)).toBe(committedButAmbiguous);

    afterReload.complete(declaration, committedButAmbiguous);
    const afterAcknowledgement = new PlaceCreationAttemptLedger(
      sessionStorage,
      32,
      nonceFactory,
    );
    afterAcknowledgement.activate(owner, true);
    expect(afterAcknowledgement.nonceFor(declaration)).not.toBe(
      committedButAmbiguous,
    );
  });

  it("does not carry persisted reconciliation state across authority replacement", () => {
    let generated = 0;
    const nonceFactory = () => `authority-nonce-${++generated}`;
    const declaration = JSON.stringify(["duplicate_channel", "channel-1"]);
    const firstOwner = JSON.stringify([
      "human-a",
      "workspace-1",
      "installation-1",
      "7",
    ]);
    const replacementOwner = JSON.stringify([
      "human-a",
      "workspace-1",
      "installation-1",
      "8",
    ]);
    const first = new PlaceCreationAttemptLedger(
      sessionStorage,
      32,
      nonceFactory,
    );
    first.activate(firstOwner, true);
    const obsolete = first.nonceFor(declaration);

    const replacement = new PlaceCreationAttemptLedger(
      sessionStorage,
      32,
      nonceFactory,
    );
    replacement.activate(replacementOwner, true);
    const afterReplacement = new PlaceCreationAttemptLedger(
      sessionStorage,
      32,
      nonceFactory,
    );
    afterReplacement.activate(firstOwner, true);

    expect(afterReplacement.nonceFor(declaration)).not.toBe(obsolete);
  });
});
