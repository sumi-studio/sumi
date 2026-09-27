import { Cdp, type CdpEvent, type SocketLike } from "./cdp.ts";

/** The subset of the Cloudflare Browser Run binding this host uses. */
export interface BrowserRunBinding {
  acquire(options: {
    keepAlive?: number;
    outboundByHost?: Record<string, unknown>;
  }): Promise<{ sessionId: string }>;
  getSession(sessionId: string): Promise<unknown>;
  connectSession(sessionId: string): Promise<{
    webSocket: {
      fetch(url: string, init: RequestInit): Promise<Response & { webSocket?: SocketLike & { accept(): void } | null }>;
    };
  }>;
  closeSession(sessionId: string): Promise<unknown>;
}

/** Browser Run rejects keepAlive above 1,200,000 ms (measured 2026-09-27). */
export const MAX_KEEP_ALIVE_MS = 1_200_000;

/** `getSession` is documented to return null for a missing session; on
 * Cloud (2026-09-27) it threw "Session not found (status=404)" instead.
 * Both mean the browser is gone. */
export async function sessionAlive(
  browser: BrowserRunBinding,
  sessionId: string,
): Promise<boolean> {
  try {
    return !!(await browser.getSession(sessionId));
  } catch (error) {
    if (/status=404|not found/i.test(error instanceof Error ? error.message : ""))
      return false;
    throw error;
  }
}

export async function openCdp(
  browser: BrowserRunBinding,
  sessionId: string,
  handlers: { onEvent?: (event: CdpEvent) => void; onClose?: (why: string) => void },
): Promise<Cdp> {
  const connection = await browser.connectSession(sessionId);
  const response = await connection.webSocket.fetch(
    "https://browser-binding.invalid",
    { headers: { Upgrade: "websocket" } },
  );
  const socket = response.webSocket;
  if (!socket)
    throw new Error(
      `Browser Run did not return a WebSocket (HTTP ${response.status})`,
    );
  socket.accept();
  return new Cdp(socket, handlers);
}

/** Local development stand-in, enabled only when LOCAL_POOL is set in
 * `.dev.vars` (wrangler dev's Browser Run simulator lacks acquire()). It is
 * never part of a deployed configuration and is not Cloud evidence. */
export function localPool(pool: string): BrowserRunBinding {
  const get = async (id: string) =>
    (await (await fetch(`${pool}/session/${encodeURIComponent(id)}`)).json()) as {
      ws: string;
    } | null;
  return {
    async acquire() {
      return (await (await fetch(`${pool}/acquire`, { method: "POST" })).json()) as {
        sessionId: string;
      };
    },
    getSession: get,
    async connectSession(id) {
      const session = await get(id);
      if (!session) throw new Error("Session not found (status=404)");
      return {
        webSocket: {
          fetch: (_url, init) => fetch(session.ws, init) as never,
        },
      };
    },
    async closeSession(id) {
      return (await fetch(`${pool}/session/${encodeURIComponent(id)}`, { method: "DELETE" })).json();
    },
  };
}
