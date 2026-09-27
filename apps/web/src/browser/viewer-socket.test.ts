import { afterEach, describe, expect, it, vi } from "vitest";
import type { CloudBrowserAPI } from "./api";
import { VIEWER_PROTOCOL, ViewerSocket } from "./viewer-socket";

class FakeSocket {
  readyState = 0;
  sent: string[] = [];
  onopen: (() => void) | null = null;
  onmessage: ((e: { data: unknown }) => void) | null = null;
  onclose: ((e: { code: number }) => void) | null = null;
  readonly url: string;
  readonly protocols: string[];
  constructor(url: string, protocols: string[]) {
    this.url = url;
    this.protocols = protocols;
  }
  send(data: string) {
    this.sent.push(data);
  }
  close() {
    this.readyState = 3;
  }
  open() {
    this.readyState = 1;
    this.onopen?.();
  }
}

function api(): CloudBrowserAPI & { tickets: number } {
  const value = {
    tickets: 0,
    async viewerTicket() {
      value.tickets++;
      return { ticket: `sbt1.t${value.tickets}.s`, expiresAt: "", path: "/browser-cloud/viewer" };
    },
  };
  return value as unknown as CloudBrowserAPI & { tickets: number };
}

afterEach(() => vi.useRealTimers());

describe("viewer socket", () => {
  it("offers the ticket only as a subprotocol and renews it in-band", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const a = api();
    const frames: unknown[] = [];
    const viewer = new ViewerSocket({
      api: a,
      profileId: "b1",
      origin: "https://sumi.example",
      onMessage: () => {},
      onFrame: (f) => frames.push(f),
      onStatus: () => {},
      createSocket: (url, protocols) => {
        const s = new FakeSocket(url, protocols);
        sockets.push(s);
        return s as unknown as WebSocket;
      },
    });
    viewer.start();
    await vi.advanceTimersByTimeAsync(0);
    const s = sockets[0] as FakeSocket;
    expect(s.url).toBe("wss://sumi.example/browser-cloud/viewer");
    expect(s.protocols).toEqual([VIEWER_PROTOCOL, "sbt1.t1.s"]);
    s.open();
    s.onmessage?.({ data: JSON.stringify({ type: "frame", data: "AAA", width: 1280, height: 800 }) });
    expect(frames).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(4 * 60_000);
    expect(JSON.parse(s.sent[0] as string)).toEqual({ type: "reauth", ticket: "sbt1.t2.s" });
    viewer.stop();
  });

  it("reconnects after a drop but not after a reset or lost ownership", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const statuses: string[] = [];
    const viewer = new ViewerSocket({
      api: api(),
      profileId: "b1",
      origin: "http://127.0.0.1:5173",
      onMessage: () => {},
      onFrame: () => {},
      onStatus: (s, reason) => statuses.push(reason ? `${s}:${reason}` : s),
      createSocket: (url, protocols) => {
        const s = new FakeSocket(url, protocols);
        sockets.push(s);
        return s as unknown as WebSocket;
      },
    });
    viewer.start();
    await vi.advanceTimersByTimeAsync(0);
    sockets[0]?.open();
    sockets[0]?.onclose?.({ code: 1006 });
    await vi.advanceTimersByTimeAsync(1_000);
    expect(sockets).toHaveLength(2);
    sockets[1]?.open();
    sockets[1]?.onclose?.({ code: 4410 });
    await vi.advanceTimersByTimeAsync(30_000);
    expect(sockets).toHaveLength(2);
    expect(statuses.at(-1)).toBe("closed:reset");
  });
});
