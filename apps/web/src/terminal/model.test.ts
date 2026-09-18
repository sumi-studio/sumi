import { describe, expect, it } from "vitest";
import {
  decodeBase64,
  encodeBase64,
  isLiveTerminalStatus,
  parseTerminalRead,
  parseTerminalSession,
  terminalStatusLabel,
} from "./model";

const sessionWire = {
  session_id: "0198f0f4-9b72-7000-8000-0000000000aa",
  name: "build",
  mode: "interactive",
  backend: "container",
  status: "active",
  requested_by: "human",
  output_bytes: 4096,
  output_base: 128,
  control_holder: "human",
  exit_code: 0,
  exit_signal: "TERM",
  end_reason: "closed",
  created_at: "2026-09-19T00:00:00Z",
  updated_at: "2026-09-19T00:01:00Z",
  ended_at: "2026-09-19T00:02:00Z",
};

describe("parseTerminalSession", () => {
  it("parses the full wire shape", () => {
    const session = parseTerminalSession(sessionWire);
    expect(session.sessionId).toBe(sessionWire.session_id);
    expect(session.status).toBe("active");
    expect(session.controlHolder).toBe("human");
    expect(session.outputBase).toBe(128);
    expect(session.exitCode).toBe(0);
    expect(session.exitSignal).toBe("TERM");
    expect(session.endReason).toBe("closed");
    expect(session.endedAt).toBe("2026-09-19T00:02:00Z");
  });

  it("tolerates absent optional fields", () => {
    const { exit_code, exit_signal, end_reason, ended_at, ...rest } =
      sessionWire;
    const session = parseTerminalSession(rest);
    expect(session.exitCode).toBeNull();
    expect(session.exitSignal).toBeNull();
    expect(session.endReason).toBeNull();
    expect(session.endedAt).toBeNull();
  });

  it("rejects non-session payloads", () => {
    expect(() => parseTerminalSession(null)).toThrow();
    expect(() => parseTerminalSession({})).toThrow();
    expect(() =>
      parseTerminalSession({ ...sessionWire, status: 42 }),
    ).toThrow();
  });
});

describe("parseTerminalRead", () => {
  it("decodes data chunks and resumes past them", () => {
    const data = encodeBase64(new TextEncoder().encode("hello"));
    const read = parseTerminalRead({
      session: sessionWire,
      cursor: 10,
      chunks: [{ kind: "data", base: 10, data }],
    });
    expect(read.chunks).toHaveLength(1);
    const chunk = read.chunks[0];
    expect(chunk.kind).toBe("data");
    if (chunk.kind === "data") {
      expect(new TextDecoder().decode(chunk.data)).toBe("hello");
    }
    expect(read.nextCursor).toBe(15);
  });

  it("derives the resume cursor from the furthest chunk, not the echo", () => {
    const read = parseTerminalRead({
      session: sessionWire,
      cursor: 0,
      chunks: [
        { kind: "gap", base: 0, gap_to: 512 },
        { kind: "data", base: 512, data: encodeBase64(new Uint8Array(8)) },
      ],
    });
    expect(read.nextCursor).toBe(520);
    expect(read.chunks[0]).toEqual({ kind: "gap", base: 0, gapTo: 512 });
  });

  it("keeps the requested cursor when nothing was retained", () => {
    const read = parseTerminalRead({
      session: sessionWire,
      cursor: 77,
      chunks: [],
    });
    expect(read.nextCursor).toBe(77);
  });
});

describe("base64", () => {
  it("round-trips arbitrary bytes including UTF-8", () => {
    const bytes = new TextEncoder().encode("こんにちは\x00\x03\xff");
    expect(decodeBase64(encodeBase64(bytes))).toEqual(bytes);
  });
});

describe("status helpers", () => {
  it("treats only recoverable statuses as live", () => {
    for (const status of [
      "requested",
      "claimed",
      "active",
      "ending",
      "interrupted",
    ]) {
      expect(isLiveTerminalStatus(status)).toBe(true);
    }
    for (const status of ["ended", "lost", "unknown-future"]) {
      expect(isLiveTerminalStatus(status)).toBe(false);
    }
  });

  it("labels known statuses and passes unknown ones through honestly", () => {
    expect(terminalStatusLabel("active")).toBe("実行中");
    expect(terminalStatusLabel("ended")).toBe("終了");
    expect(terminalStatusLabel("lost")).toBe("見失い");
    expect(terminalStatusLabel("some-future-status")).toBe(
      "some-future-status",
    );
  });
});
