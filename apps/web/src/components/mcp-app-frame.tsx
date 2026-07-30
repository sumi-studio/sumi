import {
  AppBridge,
  type McpUiResourceCsp,
} from "@modelcontextprotocol/ext-apps/app-bridge";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import {
  type JSONRPCMessage,
  JSONRPCMessageSchema,
  type MessageExtraInfo,
} from "@modelcontextprotocol/sdk/types.js";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  boundedJsonByteLength,
  MAX_MCP_APP_MESSAGE_BYTES,
  parseTrustedMcpAppProjection,
  resolveMcpAppSandboxConfig,
  type TrustedMcpAppProjection,
  type VerifiedMcpAppProjection,
  verifyTrustedMcpAppProjection,
} from "../agent/mcp-app";

const MIN_HEIGHT = 180;
const MAX_HEIGHT = 720;
const DEFAULT_HEIGHT = 320;
const TEARDOWN_TIMEOUT_MS = 500;
const DENIED_PERMISSIONS =
  "camera 'none'; microphone 'none'; geolocation 'none'; clipboard-write 'none'";

interface McpAppFrameProps {
  projection: TrustedMcpAppProjection;
}

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
  sendToolResult(params: VerifiedMcpAppProjection["toolResult"]): Promise<void>;
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

export function McpAppFrame({ projection }: McpAppFrameProps) {
  const frameRef = useRef<HTMLIFrameElement>(null);
  const [verified, setVerified] = useState<VerifiedMcpAppProjection | null>(
    null,
  );
  const [height, setHeight] = useState(DEFAULT_HEIGHT);
  const [unavailable, setUnavailable] = useState(false);

  const sandboxConfig = useMemo(
    () =>
      resolveMcpAppSandboxConfig(
        import.meta.env.VITE_MCP_APP_SANDBOX_URL,
        import.meta.env.VITE_MCP_APP_SANDBOX_DEPLOYMENT_ID,
        window.location.href,
      ),
    [],
  );

  useEffect(() => {
    let cancelled = false;
    setVerified(null);
    setUnavailable(false);
    if (!sandboxConfig) {
      setUnavailable(true);
      return;
    }
    const parsed = parseTrustedMcpAppProjection(projection);
    if (!parsed) {
      setUnavailable(true);
      return;
    }
    void verifyTrustedMcpAppProjection(parsed)
      .then((value) => {
        if (cancelled) {
          return;
        }
        if (value) {
          setVerified(value);
        } else {
          setUnavailable(true);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setUnavailable(true);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [projection, sandboxConfig]);

  useEffect(() => {
    const frame = frameRef.current;
    if (!frame || !verified || !sandboxConfig) {
      return;
    }

    let cancelled = false;
    const target = frame.contentWindow;
    if (!target || !("credentialless" in frame)) {
      setUnavailable(true);
      return;
    }

    (
      frame as HTMLIFrameElement & {
        credentialless: boolean;
      }
    ).credentialless = true;
    const transport = new ExactOriginPostMessageTransport(
      target,
      target,
      sandboxConfig.origin,
    );
    const bridge = new AppBridge(
      null,
      { name: "Sumi", version: "0.1.0" },
      {
        sandbox: {
          csp: verified.resource.csp ?? {},
          permissions: {},
        },
      },
      {
        hostContext: {
          theme: document.documentElement.classList.contains("dark")
            ? "dark"
            : "light",
          displayMode: "inline",
          availableDisplayModes: ["inline"],
          containerDimensions: { maxHeight: MAX_HEIGHT },
        },
      },
    );
    const session = createMcpAppHostSession(
      bridge,
      transport,
      verified,
      (nextHeight) => setHeight(clampMcpAppHeight(nextHeight)),
      () => setUnavailable(true),
    );

    void session
      .start()
      .then(() => {
        if (cancelled) {
          return session.close();
        }
        frame.src = sandboxConfig.url;
      })
      .catch(() => {
        if (!cancelled) {
          setUnavailable(true);
        }
      });

    return () => {
      cancelled = true;
      void session.close();
    };
  }, [sandboxConfig, verified]);

  if (unavailable) {
    return (
      <div
        className="rounded-xl bg-card p-4 text-sm text-muted-foreground"
        data-testid="mcp-app-unavailable"
      >
        This MCP App is unavailable.
      </div>
    );
  }

  if (!verified || !sandboxConfig) {
    return (
      <div
        className="rounded-xl bg-card p-4 text-sm text-muted-foreground"
        data-testid="mcp-app-verifying"
      >
        Verifying MCP App…
      </div>
    );
  }

  return (
    <div
      className={
        verified.resource.prefersBorder === false
          ? "overflow-hidden"
          : "overflow-hidden rounded-xl bg-card"
      }
      data-mcp-app-uri={verified.resource.uri}
      data-mcp-app-server={verified.provenance.serverId}
      data-mcp-app-tool={verified.provenance.toolName}
    >
      <iframe
        ref={frameRef}
        title="MCP App"
        sandbox="allow-scripts allow-same-origin"
        allow={DENIED_PERMISSIONS}
        referrerPolicy="origin"
        style={{ height }}
        className="block w-full border-0 bg-transparent"
      />
    </div>
  );
}

export function createMcpAppHostSession(
  bridge: BridgePort,
  transport: Transport,
  projection: VerifiedMcpAppProjection,
  onHeight: (height: number) => void,
  onFailure: () => void = () => undefined,
): McpAppHostSession {
  let resourceState: "pending" | "sending" | "sent" = "pending";
  let initialized = false;
  let dataState: "pending" | "sending" | "sent" = "pending";
  let started = false;
  let closed = false;

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
    await bridge.sendToolResult(projection.toolResult);
    dataState = "sent";
  };

  const onSandboxReady = () => {
    if (closed || resourceState !== "pending") {
      return;
    }
    resourceState = "sending";
    void bridge
      .sendSandboxResourceReady({
        html: projection.html,
        sandbox: "allow-scripts",
        ...(projection.resource.csp ? { csp: projection.resource.csp } : {}),
      })
      .then(() => {
        resourceState = "sent";
        return maybeSendData().catch(onFailure);
      })
      .catch(onFailure);
  };
  const onInitialized = () => {
    if (closed) {
      return;
    }
    initialized = true;
    void maybeSendData().catch(onFailure);
  };
  const onSizeChange = (value: unknown) => {
    if (
      !closed &&
      isRecord(value) &&
      typeof value.height === "number" &&
      Number.isFinite(value.height)
    ) {
      onHeight(value.height);
    }
  };

  bridge.addEventListener("sandboxready", onSandboxReady);
  bridge.addEventListener("initialized", onInitialized);
  bridge.addEventListener("sizechange", onSizeChange);

  return {
    async start() {
      if (started || closed) {
        return;
      }
      started = true;
      await bridge.connect(transport);
    },
    async close() {
      if (closed) {
        return;
      }
      closed = true;
      bridge.removeEventListener("sandboxready", onSandboxReady);
      bridge.removeEventListener("initialized", onInitialized);
      bridge.removeEventListener("sizechange", onSizeChange);
      if (initialized) {
        await Promise.race([
          bridge
            .teardownResource({}, { timeout: TEARDOWN_TIMEOUT_MS })
            .catch(() => ({})),
          new Promise<Record<string, unknown>>((resolve) =>
            setTimeout(() => resolve({}), TEARDOWN_TIMEOUT_MS),
          ),
        ]);
      }
      await bridge.close().catch(() => undefined);
    },
  };
}

export class ExactOriginPostMessageTransport implements Transport {
  private started = false;
  private readonly eventTarget: Window;
  private readonly eventSource: MessageEventSource;
  private readonly targetOrigin: string;
  private readonly onWindowMessage = (event: MessageEvent<unknown>) => {
    if (
      event.source !== this.eventSource ||
      event.origin !== this.targetOrigin
    ) {
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
