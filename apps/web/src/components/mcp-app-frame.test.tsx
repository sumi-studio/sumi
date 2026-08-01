// @vitest-environment jsdom

import { readFileSync } from "node:fs";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import type { JSONRPCMessage } from "@modelcontextprotocol/sdk/types.js";
import { render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type {
  IntegrityCheckedMcpAppProjection,
  ProvenanceBoundMcpAppActivation,
} from "../agent/mcp-app";
import {
  clampMcpAppHeight,
  createMcpAppHostSession,
  ExactOriginPostMessageTransport,
  MAX_MCP_APP_MESSAGES_PER_WINDOW,
  McpAppFrame,
} from "./mcp-app-frame";

function testOnlyProvenanceBoundActivation(): ProvenanceBoundMcpAppActivation {
  const integrityChecked: IntegrityCheckedMcpAppProjection = {
    kind: "mcp_app_projection_candidate",
    claimedSource: {
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
  // Unit tests exercise the dormant host lifecycle directly. Production code
  // has no cast or factory that can create the provenance brand.
  return integrityChecked as ProvenanceBoundMcpAppActivation;
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

afterEach(() => {
  vi.useRealTimers();
});

describe("MCP App renderer gate", () => {
  it("stays explicitly dormant without a backend-authenticated projection", () => {
    render(<McpAppFrame />);

    expect(
      screen
        .getByTestId("mcp-app-unavailable")
        .getAttribute("data-mcp-app-state"),
    ).toBe("dormant");
    expect(screen.queryByTitle("MCP App")).toBeNull();
  });
});

describe("MCP App host lifecycle", () => {
  it("sends resource, input, and result once and only after initialization", async () => {
    const log: string[] = [];
    const bridge = fakeBridge(log);
    const session = createMcpAppHostSession(
      bridge,
      {} as Transport,
      testOnlyProvenanceBoundActivation(),
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
      testOnlyProvenanceBoundActivation(),
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

  it("fails closed when the proxy-ready or initialized handshake times out", async () => {
    vi.useFakeTimers();
    const proxyFailure = vi.fn();
    const proxyBridge = fakeBridge([]);
    const proxySession = createMcpAppHostSession(
      proxyBridge,
      {} as Transport,
      testOnlyProvenanceBoundActivation(),
      vi.fn(),
      proxyFailure,
      { proxyReadyTimeoutMs: 10 },
    );
    await proxySession.start();
    await vi.advanceTimersByTimeAsync(11);
    expect(proxyFailure).toHaveBeenCalledWith(
      expect.objectContaining({
        message: "MCP App sandbox proxy did not become ready in time.",
      }),
    );
    expect(proxyBridge.close).toHaveBeenCalledOnce();

    const initializedFailure = vi.fn();
    const initializedBridge = fakeBridge([]);
    const initializedSession = createMcpAppHostSession(
      initializedBridge,
      {} as Transport,
      testOnlyProvenanceBoundActivation(),
      vi.fn(),
      initializedFailure,
      { proxyReadyTimeoutMs: 100, initializedTimeoutMs: 10 },
    );
    await initializedSession.start();
    initializedBridge.emit("sandboxready");
    await Promise.resolve();
    await vi.advanceTimersByTimeAsync(11);
    expect(initializedFailure).toHaveBeenCalledWith(
      expect.objectContaining({
        message: "MCP App view did not initialize in time.",
      }),
    );
    expect(initializedBridge.close).toHaveBeenCalledOnce();
  });

  it("bounds teardown when an initialized view does not respond", async () => {
    vi.useFakeTimers();
    const bridge = fakeBridge([]);
    bridge.teardownResource.mockImplementation(
      () => new Promise<Record<string, unknown>>(() => undefined),
    );
    const session = createMcpAppHostSession(
      bridge,
      {} as Transport,
      testOnlyProvenanceBoundActivation(),
      vi.fn(),
      vi.fn(),
      { teardownTimeoutMs: 10 },
    );
    await session.start();
    bridge.emit("initialized");

    const closing = session.close();
    await vi.advanceTimersByTimeAsync(11);
    await closing;

    expect(bridge.teardownResource).toHaveBeenCalledWith({}, { timeout: 10 });
    expect(bridge.close).toHaveBeenCalledOnce();
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

  it("rate-limits inbound and outbound traffic independently", async () => {
    const source = {} as MessageEventSource;
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
    for (let index = 0; index <= MAX_MCP_APP_MESSAGES_PER_WINDOW; index += 1) {
      window.dispatchEvent(
        new MessageEvent("message", {
          data,
          origin: "https://sandbox.example",
          source,
        }),
      );
    }
    expect(onmessage).toHaveBeenCalledTimes(MAX_MCP_APP_MESSAGES_PER_WINDOW);
    expect(onerror).toHaveBeenCalledWith(
      expect.objectContaining({ message: "MCP App message rate exceeded." }),
    );

    for (let index = 0; index < MAX_MCP_APP_MESSAGES_PER_WINDOW; index += 1) {
      await transport.send(data);
    }
    await expect(transport.send(data)).rejects.toThrow(
      "MCP App message rate exceeded.",
    );
    await transport.close();
  });
});

describe("MCP App sandbox artifact", () => {
  const sandboxHtml = readFileSync("public/mcp-app-sandbox.html", "utf8");

  it("places the deployment CSP before executable content and preserves View HTML bytes around the injected policy", () => {
    expect(
      sandboxHtml.indexOf('http-equiv="Content-Security-Policy"'),
    ).toBeLessThan(sandboxHtml.indexOf("<style>"));
    expect(sandboxHtml).not.toContain("DOMParser");
    expect(sandboxHtml).toContain("html.slice(0, insertionIndex)");
    expect(sandboxHtml).toContain("html.slice(insertionIndex)");
    expect(sandboxHtml).toContain("view.srcdoc = html");
  });

  it("keeps the deployment policy broad and installs a replacement-resistant resource boundary", () => {
    expect(sandboxHtml).toContain("connect-src https: wss:");
    expect(sandboxHtml).not.toContain("document.referrer");
    expect(sandboxHtml).toContain("event.origin === parentOrigin");
    expect(sandboxHtml).toContain("\"default-src 'none'\"");
    expect(sandboxHtml).toContain("createBoundaryDocument(params.csp)");
    expect(sandboxHtml).toContain('["\'self\'", ...frames].join(" ")');
    expect(sandboxHtml).toContain('"securitypolicyviolation"');
    expect(sandboxHtml).toContain("viewLoads > 1");
    expect(sandboxHtml).toContain("trustEpoch === epoch");
    expect(sandboxHtml).toContain(
      "`base-uri $" + `{bases.length ? bases.join(" ") : "'self'"}`,
    );
  });
});
