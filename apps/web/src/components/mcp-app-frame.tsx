import type { McpUiResourceCsp } from "@modelcontextprotocol/ext-apps/app-bridge";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import {
  type JSONRPCMessage,
  JSONRPCMessageSchema,
  type MessageExtraInfo,
} from "@modelcontextprotocol/sdk/types.js";
import {
  boundedJsonByteLength,
  type IntegrityCheckedMcpAppProjection,
  MAX_MCP_APP_MESSAGE_BYTES,
  type ProvenanceBoundMcpAppActivation,
} from "../agent/mcp-app";

const MIN_HEIGHT = 180;
const MAX_HEIGHT = 720;
const DEFAULT_HEIGHT = 320;
export const MCP_APP_PROXY_READY_TIMEOUT_MS = 5_000;
export const MCP_APP_INITIALIZED_TIMEOUT_MS = 5_000;
export const MCP_APP_TEARDOWN_TIMEOUT_MS = 500;
export const MAX_MCP_APP_MESSAGES_PER_WINDOW = 120;
export const MCP_APP_MESSAGE_RATE_WINDOW_MS = 1_000;

interface BridgePort {
  addEventListener(
    type: "sandboxready" | "initialized" | "sizechange",
    listener: (event: unknown) => void,
  ): void;
  removeEventListener(
    type: "sandboxready" | "initialized" | "sizechange",
    listener: (event: unknown) => void,
  ): void;
  connect(transport: Transport): Promise<void>;
  sendSandboxResourceReady(params: {
    html: string;
    sandbox: string;
    csp?: McpUiResourceCsp;
  }): Promise<void>;
  sendToolInput(params: { arguments: Record<string, unknown> }): Promise<void>;
  sendToolResult(
    params: IntegrityCheckedMcpAppProjection["toolResult"],
  ): Promise<void>;
  teardownResource(
    params: Record<string, never>,
    options?: { timeout?: number },
  ): Promise<Record<string, unknown>>;
  close(): Promise<void>;
}

export interface McpAppHostSession {
  start(): Promise<void>;
  close(): Promise<void>;
}

/**
 * Rendering remains intentionally dormant until a same-origin backend binds a
 * tool result and ui:// resource read to the same authenticated MCP server
 * connection. A browser-side shape check or digest cannot provide that proof.
 */
export function McpAppFrame() {
  return (
    <div
      className="rounded-xl bg-card p-4 text-sm text-muted-foreground"
      data-mcp-app-state="dormant"
      data-testid="mcp-app-unavailable"
    >
      This MCP App is unavailable because authenticated MCP resource delivery is
      not configured.
    </div>
  );
}

export interface McpAppHostSessionOptions {
  proxyReadyTimeoutMs?: number;
  initializedTimeoutMs?: number;
  teardownTimeoutMs?: number;
}

export function createMcpAppHostSession(
  bridge: BridgePort,
  transport: Transport,
  projection: ProvenanceBoundMcpAppActivation,
  onHeight: (height: number) => void,
  onFailure: (error: Error) => void = () => undefined,
  options: McpAppHostSessionOptions = {},
): McpAppHostSession {
  const proxyReadyTimeoutMs = boundedTimeout(
    options.proxyReadyTimeoutMs,
    MCP_APP_PROXY_READY_TIMEOUT_MS,
  );
  const initializedTimeoutMs = boundedTimeout(
    options.initializedTimeoutMs,
    MCP_APP_INITIALIZED_TIMEOUT_MS,
  );
  const teardownTimeoutMs = boundedTimeout(
    options.teardownTimeoutMs,
    MCP_APP_TEARDOWN_TIMEOUT_MS,
  );
  let resourceState: "pending" | "sending" | "sent" = "pending";
  let initialized = false;
  let dataState: "pending" | "sending" | "sent" = "pending";
  let started = false;
  let closed = false;
  let failureReported = false;
  let closePromise: Promise<void> | null = null;
  let proxyReadyTimer: ReturnType<typeof setTimeout> | undefined;
  let initializedTimer: ReturnType<typeof setTimeout> | undefined;

  const clearLifecycleTimers = () => {
    if (proxyReadyTimer !== undefined) {
      clearTimeout(proxyReadyTimer);
      proxyReadyTimer = undefined;
    }
    if (initializedTimer !== undefined) {
      clearTimeout(initializedTimer);
      initializedTimer = undefined;
    }
  };

  const removeListeners = () => {
    bridge.removeEventListener("sandboxready", onSandboxReady);
    bridge.removeEventListener("initialized", onInitialized);
    bridge.removeEventListener("sizechange", onSizeChange);
  };

  const closeInternal = (): Promise<void> => {
    if (closePromise) {
      return closePromise;
    }
    closed = true;
    clearLifecycleTimers();
    removeListeners();
    closePromise = (async () => {
      if (initialized) {
        await Promise.race([
          bridge
            .teardownResource({}, { timeout: teardownTimeoutMs })
            .catch(() => ({})),
          new Promise<Record<string, unknown>>((resolve) =>
            setTimeout(() => resolve({}), teardownTimeoutMs),
          ),
        ]);
      }
      await bridge.close().catch(() => undefined);
    })();
    return closePromise;
  };

  const fail = (message: string, cause?: unknown) => {
    if (closed || failureReported) {
      return;
    }
    failureReported = true;
    const error = cause instanceof Error ? cause : new Error(message);
    try {
      onFailure(error);
    } catch {
      // A reporting callback must not keep an untrusted session alive.
    } finally {
      void closeInternal();
    }
  };

  const armProxyReadyTimeout = () => {
    proxyReadyTimer = setTimeout(
      () => fail("MCP App sandbox proxy did not become ready in time."),
      proxyReadyTimeoutMs,
    );
  };

  const armInitializedTimeout = () => {
    if (initialized || closed) {
      return;
    }
    initializedTimer = setTimeout(
      () => fail("MCP App view did not initialize in time."),
      initializedTimeoutMs,
    );
  };

  const maybeSendData = async () => {
    if (
      closed ||
      !initialized ||
      resourceState !== "sent" ||
      dataState !== "pending"
    ) {
      return;
    }
    dataState = "sending";
    await bridge.sendToolInput({ arguments: projection.toolInput });
    if (closed) {
      return;
    }
    await bridge.sendToolResult(projection.toolResult);
    if (closed) {
      return;
    }
    dataState = "sent";
  };

  function onSandboxReady() {
    if (closed || resourceState !== "pending") {
      return;
    }
    if (proxyReadyTimer !== undefined) {
      clearTimeout(proxyReadyTimer);
      proxyReadyTimer = undefined;
    }
    resourceState = "sending";
    void bridge
      .sendSandboxResourceReady({
        html: projection.html,
        sandbox: "allow-scripts",
        ...(projection.resource.csp ? { csp: projection.resource.csp } : {}),
      })
      .then(() => {
        if (closed) {
          return;
        }
        resourceState = "sent";
        armInitializedTimeout();
        return maybeSendData().catch((error) =>
          fail("MCP App tool data delivery failed.", error),
        );
      })
      .catch((error) => fail("MCP App resource delivery failed.", error));
  }
  function onInitialized() {
    if (closed) {
      return;
    }
    initialized = true;
    if (initializedTimer !== undefined) {
      clearTimeout(initializedTimer);
      initializedTimer = undefined;
    }
    void maybeSendData().catch((error) =>
      fail("MCP App tool data delivery failed.", error),
    );
  }
  function onSizeChange(value: unknown) {
    if (
      !closed &&
      isRecord(value) &&
      typeof value.height === "number" &&
      Number.isFinite(value.height)
    ) {
      onHeight(value.height);
    }
  }

  bridge.addEventListener("sandboxready", onSandboxReady);
  bridge.addEventListener("initialized", onInitialized);
  bridge.addEventListener("sizechange", onSizeChange);

  return {
    async start() {
      if (started || closed) {
        return;
      }
      started = true;
      armProxyReadyTimeout();
      try {
        await bridge.connect(transport);
      } catch (error) {
        fail("MCP App bridge connection failed.", error);
        await closeInternal();
        throw error;
      }
    },
    async close() {
      await closeInternal();
    },
  };
}

export class ExactOriginPostMessageTransport implements Transport {
  private started = false;
  private readonly eventTarget: Window;
  private readonly eventSource: MessageEventSource;
  private readonly targetOrigin: string;
  private incomingWindowStartedAt = -1;
  private incomingWindowCount = 0;
  private outgoingWindowStartedAt = -1;
  private outgoingWindowCount = 0;
  private readonly onWindowMessage = (event: MessageEvent<unknown>) => {
    if (
      event.source !== this.eventSource ||
      event.origin !== this.targetOrigin
    ) {
      return;
    }
    if (!this.consumeRateLimit("incoming")) {
      this.onerror?.(new Error("MCP App message rate exceeded."));
      return;
    }
    const size = boundedJsonByteLength(event.data);
    if (size === null || size > MAX_MCP_APP_MESSAGE_BYTES) {
      this.onerror?.(new Error("MCP App message exceeds the allowed size."));
      return;
    }
    const parsed = JSONRPCMessageSchema.safeParse(event.data);
    if (!parsed.success) {
      this.onerror?.(new Error("Invalid MCP App JSON-RPC message."));
      return;
    }
    this.onmessage?.(parsed.data);
  };

  constructor(
    eventTarget: Window,
    eventSource: MessageEventSource,
    targetOrigin: string,
  ) {
    if (exactHttpsOrigin(targetOrigin) !== targetOrigin) {
      throw new Error("MCP App target origin must be an exact HTTPS origin.");
    }
    this.eventTarget = eventTarget;
    this.eventSource = eventSource;
    this.targetOrigin = targetOrigin;
  }

  async start(): Promise<void> {
    if (this.started) {
      return;
    }
    this.started = true;
    window.addEventListener("message", this.onWindowMessage);
  }

  async send(message: JSONRPCMessage): Promise<void> {
    if (!this.started) {
      throw new Error("MCP App transport is not started.");
    }
    if (!this.consumeRateLimit("outgoing")) {
      throw new Error("MCP App message rate exceeded.");
    }
    const size = boundedJsonByteLength(message);
    if (size === null || size > MAX_MCP_APP_MESSAGE_BYTES) {
      throw new Error("MCP App message exceeds the allowed size.");
    }
    this.eventTarget.postMessage(message, this.targetOrigin);
  }

  async close(): Promise<void> {
    if (!this.started) {
      return;
    }
    this.started = false;
    window.removeEventListener("message", this.onWindowMessage);
    this.onclose?.();
  }

  onclose?: () => void;
  onerror?: (error: Error) => void;
  onmessage?: (message: JSONRPCMessage, extra?: MessageExtraInfo) => void;
  sessionId?: string;
  setProtocolVersion?: (version: string) => void;

  private consumeRateLimit(direction: "incoming" | "outgoing"): boolean {
    const now = Date.now();
    const windowStartedAt =
      direction === "incoming"
        ? this.incomingWindowStartedAt
        : this.outgoingWindowStartedAt;
    if (
      windowStartedAt < 0 ||
      now - windowStartedAt >= MCP_APP_MESSAGE_RATE_WINDOW_MS
    ) {
      if (direction === "incoming") {
        this.incomingWindowStartedAt = now;
        this.incomingWindowCount = 1;
      } else {
        this.outgoingWindowStartedAt = now;
        this.outgoingWindowCount = 1;
      }
      return true;
    }
    if (direction === "incoming") {
      this.incomingWindowCount += 1;
      return this.incomingWindowCount <= MAX_MCP_APP_MESSAGES_PER_WINDOW;
    }
    this.outgoingWindowCount += 1;
    return this.outgoingWindowCount <= MAX_MCP_APP_MESSAGES_PER_WINDOW;
  }
}

export function clampMcpAppHeight(height: number): number {
  if (!Number.isFinite(height)) {
    return DEFAULT_HEIGHT;
  }
  return Math.min(MAX_HEIGHT, Math.max(MIN_HEIGHT, height));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function boundedTimeout(value: number | undefined, fallback: number): number {
  return typeof value === "number" &&
    Number.isFinite(value) &&
    value > 0 &&
    value <= 60_000
    ? value
    : fallback;
}

function exactHttpsOrigin(value: string): string | null {
  try {
    const url = new URL(value);
    return url.protocol === "https:" &&
      url.username === "" &&
      url.password === "" &&
      url.pathname === "/" &&
      url.search === "" &&
      url.hash === ""
      ? url.origin
      : null;
  } catch {
    return null;
  }
}
