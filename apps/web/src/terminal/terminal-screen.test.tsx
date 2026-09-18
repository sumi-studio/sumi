// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  TerminalAPIError,
  TerminalAPIUncertainError,
  type TerminalApiClient,
} from "./api";
import type { TerminalAttachEvents } from "./attach";
import type { TerminalSession } from "./model";
import { TerminalSessionView } from "./terminal-screen";

const fakes = vi.hoisted(() => {
  class FakeTerminal {
    static instances: FakeTerminal[] = [];
    cols = 80;
    rows = 24;
    options: Record<string, unknown> = { disableStdin: false };
    written: string[] = [];
    disposed = false;
    private dataCb: ((data: string) => void) | null = null;
    private resizeCb: ((event: { cols: number; rows: number }) => void) | null =
      null;

    constructor() {
      FakeTerminal.instances.push(this);
    }

    loadAddon(): void {}
    open(): void {}
    write(data: Uint8Array): void {
      this.written.push(new TextDecoder().decode(data));
    }
    writeln(text: string): void {
      this.written.push(text);
    }
    dispose(): void {
      this.disposed = true;
    }
    onData(callback: (data: string) => void): { dispose(): void } {
      this.dataCb = callback;
      return { dispose: () => (this.dataCb = null) };
    }
    onResize(callback: (event: { cols: number; rows: number }) => void): {
      dispose(): void;
    } {
      this.resizeCb = callback;
      return { dispose: () => (this.resizeCb = null) };
    }
    emitData(data: string): void {
      this.dataCb?.(data);
    }
    emitResize(cols: number, rows: number): void {
      this.cols = cols;
      this.rows = rows;
      this.resizeCb?.({ cols, rows });
    }
  }

  class FakeFitAddon {
    fitCalls = 0;
    fit(): void {
      this.fitCalls += 1;
    }
  }

  class FakeAttach {
    static instances: FakeAttach[] = [];
    readonly events: TerminalAttachEvents;
    resizeCalls: Array<[number, number]> = [];
    stdinData: string[] = [];
    signalCalls: string[] = [];
    eofCalls = 0;
    openCalls = 0;
    detachCalls = 0;
    socketOpen = true;

    constructor(options: { events: TerminalAttachEvents }) {
      this.events = options.events;
      FakeAttach.instances.push(this);
    }

    get cursor(): number {
      return 0;
    }
    get isEnded(): boolean {
      return false;
    }
    open(): void {
      this.openCalls += 1;
    }
    detach(): void {
      this.detachCalls += 1;
    }
    sendStdin(data: string): boolean {
      if (!this.socketOpen) return false;
      this.stdinData.push(data);
      return true;
    }
    sendResize(cols: number, rows: number): boolean {
      if (!this.socketOpen) return false;
      this.resizeCalls.push([cols, rows]);
      return true;
    }
    sendSignal(signal: string): boolean {
      if (!this.socketOpen) return false;
      this.signalCalls.push(signal);
      return true;
    }
    sendEof(): boolean {
      if (!this.socketOpen) return false;
      this.eofCalls += 1;
      return true;
    }
    setControl(): boolean {
      return this.socketOpen;
    }
    requestClose(): boolean {
      return this.socketOpen;
    }
  }

  return { FakeTerminal, FakeFitAddon, FakeAttach };
});

vi.mock("@xterm/xterm", () => ({ Terminal: fakes.FakeTerminal }));
vi.mock("@xterm/addon-fit", () => ({ FitAddon: fakes.FakeFitAddon }));

vi.mock("./attach", async (importOriginal) => {
  const mod = await importOriginal<typeof import("./attach")>();
  return { ...mod, TerminalAttach: fakes.FakeAttach };
});

vi.mock("../participant/app-store", () => ({
  useParticipantApps: (
    selector: (state: { refresh: () => Promise<void> }) => unknown,
  ) => selector({ refresh: async () => {} }),
}));

class FakeResizeObserver {
  observe(): void {}
  unobserve(): void {}
  disconnect(): void {}
}

function session(
  status: string,
  overrides: Partial<TerminalSession> = {},
): TerminalSession {
  return {
    sessionId: "sess-1",
    name: "テスト",
    mode: "pty",
    backend: "fixture",
    status,
    requestedBy: "human",
    outputBytes: 0,
    outputBase: 0,
    controlHolder: "",
    exitCode: null,
    exitSignal: null,
    endReason: null,
    createdAt: "2026-09-19T00:00:00Z",
    updatedAt: "2026-09-19T00:00:00Z",
    endedAt: null,
    ...overrides,
  };
}

function client(overrides: Record<string, unknown> = {}): TerminalApiClient {
  return {
    getSession: vi.fn(async () => session("active")),
    setControl: vi.fn(async () =>
      session("active", { controlHolder: "human" }),
    ),
    closeSession: vi.fn(async () => session("ending")),
    ...overrides,
  } as unknown as TerminalApiClient;
}

function mount(api: TerminalApiClient, sess?: TerminalSession) {
  render(
    <TerminalSessionView
      client={api}
      scope={{ installationId: "inst-1", authorityEpoch: "1" }}
      sessionId="sess-1"
      session={sess}
      onSession={() => {}}
      onDeselect={() => {}}
    />,
  );
  const attach = fakes.FakeAttach.instances.at(-1);
  const term = fakes.FakeTerminal.instances.at(-1);
  if (!attach || !term) throw new Error("view did not mount xterm/attach");
  return { attach, term };
}

const endedInfo = {
  status: "ended",
  reason: "closed",
  exitCode: 0,
  exitSignal: null,
};

beforeEach(() => {
  vi.stubGlobal("ResizeObserver", FakeResizeObserver);
  fakes.FakeTerminal.instances.length = 0;
  fakes.FakeAttach.instances.length = 0;
});

afterEach(() => {
  vi.useRealTimers();
  cleanup();
});

describe("TerminalSessionView lifecycle admission", () => {
  it("sends no resize on socket open; syncs when the session reports active", () => {
    vi.useFakeTimers();
    const { attach, term } = mount(client(), session("requested"));

    act(() => attach.events.onConnection("open"));
    expect(attach.resizeCalls).toEqual([]);

    act(() => attach.events.onSession(session("active")));
    expect(attach.resizeCalls).toEqual([[80, 24]]);

    act(() => term.emitResize(100, 30));
    act(() => {
      vi.advanceTimersByTime(149);
    });
    expect(attach.resizeCalls).toHaveLength(1);
    act(() => {
      vi.advanceTimersByTime(2);
    });
    expect(attach.resizeCalls).toEqual([
      [80, 24],
      [100, 30],
    ]);

    act(() => term.emitData("ls\n"));
    expect(attach.stdinData).toEqual(["ls\n"]);
  });

  it("declines typed input before the session accepts it, with a notice", () => {
    const { attach, term } = mount(client(), session("requested"));

    act(() => attach.events.onConnection("open"));
    act(() => term.emitData("x"));

    expect(attach.stdinData).toEqual([]);
    expect(
      screen.getByText("セッションはまだ入力を受け付けていません"),
    ).toBeInTheDocument();
  });

  it("cancels a debounced resize that straddles the session end", () => {
    vi.useFakeTimers();
    const { attach, term } = mount(client(), session("active"));

    act(() => attach.events.onConnection("open"));
    act(() => attach.events.onSession(session("active")));
    expect(attach.resizeCalls).toEqual([[80, 24]]);

    act(() => term.emitResize(90, 27));
    act(() => attach.events.onEnded(endedInfo));
    act(() => {
      vi.advanceTimersByTime(500);
    });

    expect(attach.resizeCalls).toEqual([[80, 24]]);
    expect(screen.getByText(/セッションは終了しました/)).toBeInTheDocument();
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    expect(term.options.disableStdin).toBe(true);
  });

  it("sends no housekeeping input when attaching to an ended session", () => {
    const { attach, term } = mount(client(), session("ended"));

    act(() => attach.events.onConnection("open"));
    act(() => attach.events.onSession(session("ended")));
    act(() => attach.events.onEnded(endedInfo));

    expect(attach.resizeCalls).toEqual([]);
    expect(attach.stdinData).toEqual([]);
    expect(term.options.disableStdin).toBe(true);
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    expect(screen.getByText(/セッションは終了しました/)).toBeInTheDocument();
  });

  it("gates housekeeping on REST/poll lifecycle, not only socket frames", async () => {
    vi.useFakeTimers();
    const api = client({
      getSession: vi.fn(async () => session("ending")),
    });
    const { attach, term } = mount(api, session("active"));

    act(() => attach.events.onConnection("open"));
    act(() => attach.events.onSession(session("active")));
    expect(attach.resizeCalls).toEqual([[80, 24]]);

    // The 15s session poll learns the session is ending before the socket
    // reports it; that authoritative state must gate later housekeeping.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(15_000);
    });
    expect(api.getSession).toHaveBeenCalled();

    act(() => term.emitResize(90, 27));
    act(() => {
      vi.advanceTimersByTime(500);
    });
    expect(attach.resizeCalls).toEqual([[80, 24]]);
    expect(term.options.disableStdin).toBe(true);
  });
});

describe("TerminalSessionView uncertain mutation (TUI-02)", () => {
  function uncertainClient(getSession: () => Promise<TerminalSession>) {
    return client({
      closeSession: vi.fn(async () => {
        throw new TerminalAPIUncertainError(new Error("response lost"));
      }),
      getSession: vi.fn(getSession),
    });
  }

  async function requestClose() {
    fireEvent.click(screen.getByRole("button", { name: "終了" }));
    fireEvent.click(screen.getByRole("button", { name: "終了する" }));
  }

  it("claims a refreshed state only after the reconciliation read succeeds", async () => {
    const api = uncertainClient(async () => session("ended"));
    mount(api, session("active"));

    await requestClose();

    await waitFor(() => {
      expect(
        screen.getByText(
          "操作の結果は不明のままです。最新の状態に更新しました",
        ),
      ).toBeInTheDocument();
    });
    // The mutation was not blindly resubmitted; reconciliation did the read.
    expect(api.closeSession).toHaveBeenCalledTimes(1);
    expect(api.getSession).toHaveBeenCalledTimes(1);
  });

  it("stays honest when the reconciliation read also fails", async () => {
    const api = uncertainClient(async () => {
      throw new TerminalAPIError("unavailable", 503);
    });
    mount(api, session("active"));

    await requestClose();

    await waitFor(() => {
      expect(
        screen.getByText(
          "操作の結果を確認できず、状態の再読み込みにも失敗しました",
        ),
      ).toBeInTheDocument();
    });
    expect(
      screen.queryByText(/最新の状態に更新しました/),
    ).not.toBeInTheDocument();
    expect(api.closeSession).toHaveBeenCalledTimes(1);
    expect(api.getSession).toHaveBeenCalledTimes(1);
  });
});
