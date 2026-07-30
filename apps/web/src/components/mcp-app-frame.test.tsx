// @vitest-environment jsdom

import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import type { JSONRPCMessage } from "@modelcontextprotocol/sdk/types.js";
import { describe, expect, it, vi } from "vitest";
import type { VerifiedMcpAppProjection } from "../agent/mcp-app";
import {
  clampMcpAppHeight,
  createMcpAppHostSession,
  ExactOriginPostMessageTransport,
} from "./mcp-app-frame";

function verifiedProjection(): VerifiedMcpAppProjection {
  return {
    kind: "trusted_mcp_app_projection",
    provenance: {
      serverId: "calendar-server",
      toolName: "show-calendar",
      resourceUri: "ui://calendar/view",
      resourceSha256: `sha256:${"a".repeat(64)}`,
    },
    resource: {
      uri: "ui://calendar/view",
      mimeType: "text/html;profile=mcp-app",
      text: "<!doctype html><html></html>",
      csp: { resourceDomains: ["https://cdn.example"] },
    },
    toolInput: { month: "2026-07" },
    toolResult: { content: [{ type: "text", text: "done" }] },
    html: "<!doctype html><html></html>",
  };
}

function fakeBridge(log: string[]) {
  const listeners = new Map<string, Set<(value: unknown) => void>>();
  const add = (type: string, listener: (value: unknown) => void) => {
    const set = listeners.get(type) ?? new Set();
    set.add(listener);
    listeners.set(type, set);
  };
  const remove = (type: string, listener: (value: unknown) => void) => {
    listeners.get(type)?.delete(listener);
  };
  return {
    emit(type: string, value: unknown = {}) {
      for (const listener of listeners.get(type) ?? []) {
        listener(value);
      }
    },
    addEventListener: vi.fn(add),
    removeEventListener: vi.fn(remove),
    connect: vi.fn(async () => {
      log.push("connect");
    }),
    sendSandboxResourceReady: vi.fn(async () => {
      log.push("resource");
    }),
    sendToolInput: vi.fn(async () => {
      log.push("input");
    }),
    sendToolResult: vi.fn(async () => {
      log.push("result");
    }),
    teardownResource: vi.fn(async () => {
      log.push("teardown");
      return {};
    }),
    close: vi.fn(async () => {
      log.push("close");
    }),
  };
}

describe("MCP App host lifecycle", () => {
  it("sends resource, input, and result once and only after initialization", async () => {
    const log: string[] = [];
    const bridge = fakeBridge(log);
    const session = createMcpAppHostSession(
      bridge,
      {} as Transport,
      verifiedProjection(),
      vi.fn(),
    );

    await session.start();
    bridge.emit("initialized");
    await Promise.resolve();
    expect(log).toEqual(["connect"]);

    bridge.emit("sandboxready");
    bridge.emit("sandboxready");
    await vi.waitFor(() => {
      expect(log).toEqual(["connect", "resource", "input", "result"]);
    });

    bridge.emit("initialized");
    bridge.emit("sandboxready");
    await Promise.resolve();
    expect(log).toEqual(["connect", "resource", "input", "result"]);
    expect(bridge.sendSandboxResourceReady).toHaveBeenCalledWith(
      expect.not.objectContaining({ permissions: expect.anything() }),
    );
    expect(bridge.sendToolInput).toHaveBeenCalledTimes(1);
    expect(bridge.sendToolResult).toHaveBeenCalledTimes(1);
  });

  it("tears down an initialized view once and removes all listeners", async () => {
    const log: string[] = [];
    const bridge = fakeBridge(log);
    const session = createMcpAppHostSession(
      bridge,
      {} as Transport,
      verifiedProjection(),
      vi.fn(),
    );
    await session.start();
    bridge.emit("initialized");

    await session.close();
    await session.close();

    expect(log).toEqual(["connect", "teardown", "close"]);
    expect(bridge.teardownResource).toHaveBeenCalledTimes(1);
    expect(bridge.close).toHaveBeenCalledTimes(1);
    expect(bridge.removeEventListener).toHaveBeenCalledTimes(3);
  });

  it("accepts only finite bounded heights", () => {
    expect(clampMcpAppHeight(-100)).toBe(180);
    expect(clampMcpAppHeight(450)).toBe(450);
    expect(clampMcpAppHeight(1_000)).toBe(720);
    expect(clampMcpAppHeight(Number.NaN)).toBe(320);
  });
});

describe("exact-origin postMessage transport", () => {
  it("ignores the wrong source or origin and accepts only a bounded JSON-RPC message", async () => {
    const source = {} as MessageEventSource;
    const otherSource = {} as MessageEventSource;
    const target = { postMessage: vi.fn() } as unknown as Window;
    const transport = new ExactOriginPostMessageTransport(
      target,
      source,
      "https://sandbox.example",
    );
    const onmessage = vi.fn();
    const onerror = vi.fn();
    transport.onmessage = onmessage;
    transport.onerror = onerror;
    await transport.start();

    const data: JSONRPCMessage = { jsonrpc: "2.0", id: 1, result: {} };
    window.dispatchEvent(
      new MessageEvent("message", {
        data,
        origin: "https://evil.example",
        source,
      }),
    );
    window.dispatchEvent(
      new MessageEvent("message", {
        data,
        origin: "https://sandbox.example",
        source: otherSource,
      }),
    );
    expect(onmessage).not.toHaveBeenCalled();

    window.dispatchEvent(
      new MessageEvent("message", {
        data,
        origin: "https://sandbox.example",
        source,
      }),
    );
    expect(onmessage).toHaveBeenCalledOnce();

    window.dispatchEvent(
      new MessageEvent("message", {
        data: {
          jsonrpc: "2.0",
          method: "x".repeat(256 * 1024),
        },
        origin: "https://sandbox.example",
        source,
      }),
    );
    expect(onmessage).toHaveBeenCalledOnce();
    expect(onerror).toHaveBeenCalledOnce();

    await transport.send(data);
    expect(target.postMessage).toHaveBeenCalledWith(
      data,
      "https://sandbox.example",
    );
    await transport.close();
  });
});
