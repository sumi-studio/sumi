// @vitest-environment jsdom

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  resolveTerminalWsURL,
  TerminalAttach,
  type TerminalAttachEvents,
  type TerminalSocketLike,
} from "./attach";
import { encodeBase64 } from "./model";

const SCOPE = {
  installationId: "0198f0f4-9b72-7000-8000-000000000051",
  authorityEpoch: "3",
};
const SESSION_ID = "0198f0f4-9b72-7000-8000-0000000000aa";

class FakeSocket implements TerminalSocketLike {
  static instances: FakeSocket[] = [];

  readyState = 0;
  onopen: (() => void) | null = null;
  onclose: ((event: { code: number; reason?: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  onmessage: ((event: { data: unknown }) => void) | null = null;
  readonly sent: string[] = [];
  closeCalls: Array<{ code?: number; reason?: string }> = [];
  readonly url: string;

  constructor(url: string) {
    this.url = url;
    FakeSocket.instances.push(this);
  }

  send(payload: string): void {
    this.sent.push(payload);
  }

  close(code?: number, reason?: string): void {
    this.readyState = 3;
    this.closeCalls.push({ code, reason });
  }

  serverOpen(): void {
    this.readyState = 1;
    this.onopen?.();
  }

  serverFrame(frame: Record<string, unknown>): void {
    this.onmessage?.({ data: JSON.stringify(frame) });
  }

  serverClose(code = 1006, reason = ""): void {
    this.readyState = 3;
    this.onclose?.({ code, reason });
  }
}

const sessionWire = {
  session_id: SESSION_ID,
  name: "main",
  mode: "interactive",
  backend: "container",
  status: "active",
  requested_by: "human",
  output_bytes: 0,
  output_base: 0,
  control_holder: "",
  created_at: "2026-09-19T00:00:00Z",
  updated_at: "2026-09-19T00:00:00Z",
};

interface Recorded {
  sessions: unknown[];
  outputs: Array<{ base: number; text: string }>;
  gaps: Array<{ base: number; to: number | null }>;
  acks: Array<{ inputId: string; seq: number; status: string }>;
  ended: Array<{ status: string; reason: string }>;
  errors: Array<{ code: string; message: string }>;
  connections: Array<{ state: string; info?: unknown }>;
}

function recorder(): { events: TerminalAttachEvents; seen: Recorded } {
  const seen: Recorded = {
    sessions: [],
    outputs: [],
    gaps: [],
    acks: [],
    ended: [],
    errors: [],
    connections: [],
  };
  const decoder = new TextDecoder();
  return {
    seen,
    events: {
      onSession: (session) => seen.sessions.push(session),
      onOutput: (base, data) =>
        seen.outputs.push({ base, text: decoder.decode(data) }),
      onGap: (base, to) => seen.gaps.push({ base, to }),
      onInputAck: (ack) => seen.acks.push(ack),
      onEnded: (info) => seen.ended.push(info),
      onServerError: (code, message) => seen.errors.push({ code, message }),
      onConnection: (state, info) => seen.connections.push({ state, info }),
    },
  };
}

function attach(
  events: TerminalAttachEvents,
  options: { maxReconnectAttempts?: number } = {},
): TerminalAttach {
  return new TerminalAttach({
    scope: SCOPE,
    sessionId: SESSION_ID,
    events,
    socketFactory: (url) => new FakeSocket(url),
    ...options,
  });
}

beforeEach(() => {
  FakeSocket.instances = [];
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

describe("resolveTerminalWsURL", () => {
  it("builds a same-origin ws URL with scope and cursor", () => {
    const url = resolveTerminalWsURL({
      scope: SCOPE,
      sessionId: SESSION_ID,
      cursor: 512,
      pageOrigin: "https://sumi.example",
    });
    expect(url.protocol).toBe("wss:");
    expect(url.host).toBe("sumi.example");
    expect(url.pathname).toBe("/terminal/ws");
    expect(url.searchParams.get("installation_id")).toBe(SCOPE.installationId);
    expect(url.searchParams.get("authority_epoch")).toBe("3");
    expect(url.searchParams.get("session_id")).toBe(SESSION_ID);
    expect(url.searchParams.get("cursor")).toBe("512");
  });

  it("omits the cursor at the stream base", () => {
    const url = resolveTerminalWsURL({
      scope: SCOPE,
      sessionId: SESSION_ID,
      cursor: 0,
      pageOrigin: "https://sumi.example",
    });
    expect(url.searchParams.has("cursor")).toBe(false);
  });

  it("refuses cross-origin outside explicit preissued mode", () => {
    expect(() =>
      resolveTerminalWsURL({
        apiBaseURL: "https://fixture.example",
        scope: SCOPE,
        sessionId: SESSION_ID,
        cursor: 0,
        pageOrigin: "https://sumi.example",
      }),
    ).toThrow(/preissued/);
    expect(() =>
      resolveTerminalWsURL({
        apiBaseURL: "https://fixture.example",
        authMode: "preissued",
        scope: SCOPE,
        sessionId: SESSION_ID,
        cursor: 0,
        pageOrigin: "https://sumi.example",
      }),
    ).not.toThrow();
  });
});

describe("TerminalAttach", () => {
  it("replays session, output and gap frames in order", () => {
    const { events, seen } = recorder();
    attach(events).open();
    const socket = FakeSocket.instances[0];
    socket.serverOpen();
    socket.serverFrame({ type: "session", session: sessionWire });
    socket.serverFrame({ type: "gap", base: 0, to: 100 });
    socket.serverFrame({
      type: "output",
      base: 100,
      data: encodeBase64(new TextEncoder().encode("hello ")),
    });
    socket.serverFrame({
      type: "output",
      base: 106,
      data: encodeBase64(new TextEncoder().encode("world")),
    });

    expect(seen.sessions).toHaveLength(1);
    expect(seen.gaps).toEqual([{ base: 0, to: 100 }]);
    expect(seen.outputs).toEqual([
      { base: 100, text: "hello " },
      { base: 106, text: "world" },
    ]);
    expect(seen.connections.at(-1)?.state).toBe("open");
  });

  it("trims replayed overlap so nothing renders twice after resume", () => {
    const { events, seen } = recorder();
    attach(events).open();
    const socket = FakeSocket.instances[0];
    socket.serverOpen();
    socket.serverFrame({
      type: "output",
      base: 100,
      data: encodeBase64(new TextEncoder().encode("hello ")),
    });
    socket.serverClose(1006);
    vi.advanceTimersByTime(400);

    const resumed = FakeSocket.instances[1];
    expect(new URL(resumed.url).searchParams.get("cursor")).toBe("106");
    resumed.serverOpen();
    // The server may re-send from an earlier base; the tail must not dup.
    resumed.serverFrame({
      type: "output",
      base: 100,
      data: encodeBase64(new TextEncoder().encode("hello world")),
    });
    expect(seen.outputs).toEqual([
      { base: 100, text: "hello " },
      { base: 106, text: "world" },
    ]);
  });

  it("reconnects with backoff and reports the retry", () => {
    const { events, seen } = recorder();
    attach(events).open();
    FakeSocket.instances[0].serverOpen();
    FakeSocket.instances[0].serverClose(1006);
    expect(seen.connections.at(-1)).toEqual({
      state: "closed",
      info: { willRetry: true },
    });
    vi.advanceTimersByTime(400);
    expect(FakeSocket.instances).toHaveLength(2);
    FakeSocket.instances[1].serverOpen();
    expect(seen.connections.at(-1)?.state).toBe("open");
  });

  it("stops reconnecting when retries are exhausted", () => {
    const { events, seen } = recorder();
    attach(events, { maxReconnectAttempts: 1 }).open();
    FakeSocket.instances[0].serverClose(1006);
    vi.advanceTimersByTime(400);
    FakeSocket.instances[1].serverClose(1006);
    vi.advanceTimersByTime(60_000);
    expect(FakeSocket.instances).toHaveLength(2);
    expect(seen.connections.at(-1)).toEqual({
      state: "closed",
      info: { willRetry: false, reason: "retries" },
    });
  });

  it("does not retry after a policy-violation close and reports authorization", () => {
    const { events, seen } = recorder();
    attach(events).open();
    FakeSocket.instances[0].serverClose(1008, "authorization expired");
    vi.advanceTimersByTime(60_000);
    expect(FakeSocket.instances).toHaveLength(1);
    expect(seen.connections.at(-1)).toEqual({
      state: "closed",
      info: { willRetry: false, reason: "authorization" },
    });
  });

  it("does not reconnect once the session ended", () => {
    const { events, seen } = recorder();
    attach(events).open();
    const socket = FakeSocket.instances[0];
    socket.serverOpen();
    socket.serverFrame({
      type: "ended",
      status: "ended",
      reason: "closed by the person",
      exit_code: 0,
    });
    socket.serverClose(1000);
    vi.advanceTimersByTime(60_000);
    expect(FakeSocket.instances).toHaveLength(1);
    expect(seen.ended).toEqual([
      {
        status: "ended",
        reason: "closed by the person",
        exitCode: 0,
        exitSignal: null,
      },
    ]);
  });

  it("detaches the viewer without ending the session or reconnecting", () => {
    const { events, seen } = recorder();
    const a = attach(events);
    a.open();
    const socket = FakeSocket.instances[0];
    socket.serverOpen();
    a.detach();
    vi.advanceTimersByTime(60_000);
    expect(socket.closeCalls).toEqual([
      { code: 1000, reason: "viewer detached" },
    ]);
    expect(FakeSocket.instances).toHaveLength(1);
    // detach is not a session close: no {"type":"close"} was ever sent.
    expect(socket.sent.some((m) => m.includes('"close"'))).toBe(false);
    expect(seen.ended).toHaveLength(0);
  });

  it("sends input frames only while open, encoded on the wire", () => {
    const { events } = recorder();
    const a = attach(events);
    a.open();
    const socket = FakeSocket.instances[0];
    // Not open yet: the caller must be told the send did not happen.
    expect(a.sendStdin("ls\n")).toBe(false);
    socket.serverOpen();
    expect(a.sendStdin("ls\n")).toBe(true);
    expect(a.sendResize(120, 40)).toBe(true);
    expect(a.sendSignal("INT")).toBe(true);
    expect(a.sendEof()).toBe(true);
    expect(a.setControl(true)).toBe(true);
    const frames = socket.sent.map((line) => JSON.parse(line));
    expect(frames[0]).toEqual({
      type: "stdin",
      data: encodeBase64(new TextEncoder().encode("ls\n")),
    });
    expect(frames[1]).toEqual({ type: "resize", cols: 120, rows: 40 });
    expect(frames[2]).toEqual({ type: "signal", signal: "INT" });
    expect(frames[3]).toEqual({ type: "eof" });
    expect(frames[4]).toEqual({ type: "control", hold: true });
  });

  it("rejects invalid resize geometry", () => {
    const { events } = recorder();
    const a = attach(events);
    a.open();
    FakeSocket.instances[0].serverOpen();
    expect(a.sendResize(0, 40)).toBe(false);
    expect(a.sendResize(120, -1)).toBe(false);
    expect(a.sendResize(120.5, 40)).toBe(false);
  });

  it("relays input acknowledgements honestly", () => {
    const { events, seen } = recorder();
    attach(events).open();
    const socket = FakeSocket.instances[0];
    socket.serverOpen();
    socket.serverFrame({
      type: "input_ack",
      input_id: "in-1",
      seq: 3,
      status: "intended",
    });
    expect(seen.acks).toEqual([
      { inputId: "in-1", seq: 3, status: "intended" },
    ]);
  });

  it("surfaces server error frames", () => {
    const { events, seen } = recorder();
    attach(events).open();
    FakeSocket.instances[0].serverOpen();
    FakeSocket.instances[0].serverFrame({
      type: "error",
      code: "not_live",
      message: "input rejected",
    });
    expect(seen.errors).toEqual([
      { code: "not_live", message: "input rejected" },
    ]);
  });

  it("reopens cleanly after detach when asked", () => {
    const { events, seen } = recorder();
    const a = attach(events);
    a.open();
    FakeSocket.instances[0].serverOpen();
    a.detach();
    a.open();
    expect(FakeSocket.instances).toHaveLength(2);
    FakeSocket.instances[1].serverOpen();
    expect(seen.connections.at(-1)?.state).toBe("open");
  });
});
