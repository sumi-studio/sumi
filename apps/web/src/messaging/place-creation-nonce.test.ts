import { afterEach, describe, expect, it, vi } from "vitest";
import { MockMessagingServer } from "./mock-server";
import type { ParticipantRef } from "./model";
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
    vi.spyOn(server, "createGroupDM").mockImplementation(async (...args) => {
      nonces.push(args[1]);
      if (nonces.length === 1) throw new TypeError("ambiguous response loss");
      return create(...args);
    });

    const gesture = () => useMessaging.getState().startDM([BOB, CAROL]);
    await expect(gesture()).rejects.toThrow("ambiguous response loss");
    const reconciled = await gesture();
    const laterGesture = await gesture();

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
});
