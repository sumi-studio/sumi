import type { CloudBrowserAPI } from "./api";
import type { ServerMessage } from "./model";

export const VIEWER_PROTOCOL = "sumi.browser.v1";
/** Tickets live 60 s; the viewer session 10 min. Renew well before. */
const REAUTH_MS = 4 * 60_000;
const BACKOFF_MS = [1_000, 2_000, 5_000, 10_000];
/** A hidden page stops keeping the remote browser alive after this. */
export const HIDDEN_RELEASE_MS = 5 * 60_000;

export type SocketStatus = "connecting" | "open" | "retrying" | "paused" | "closed";

export interface FrameMessage {
  /** The remote tab this frame shows; the person's input names it. */
  tab?: string;
  data: string;
  width: number;
  height: number;
}

export interface ViewerSocketOptions {
  api: CloudBrowserAPI;
  profileId: string;
  onMessage(message: ServerMessage): void;
  onFrame(frame: FrameMessage): void;
  onStatus(status: SocketStatus, reason?: "forbidden" | "reset"): void;
  /** Test seam. */
  createSocket?: (url: string, protocols: string[]) => WebSocket;
  origin?: string;
}

/**
 * One viewer connection to a profile's shared screen. The ticket travels in
 * Sec-WebSocket-Protocol (never in the URL) and is renewed in-band. The
 * connection retries with bounded backoff while the page is visible and is
 * released (letting the remote browser save and sleep) while hidden.
 */
export class ViewerSocket {
  private readonly options: ViewerSocketOptions;
  private socket?: WebSocket;
  private attempt = 0;
  private refusals = 0;
  private stopped = false;
  private paused = false;
  private retryTimer?: ReturnType<typeof setTimeout>;
  private reauthTimer?: ReturnType<typeof setInterval>;

  constructor(options: ViewerSocketOptions) {
    this.options = options;
  }

  start(): void {
    this.stopped = false;
    void this.connect();
  }

  stop(): void {
    this.stopped = true;
    this.teardown();
    this.options.onStatus("closed");
  }

  /** Release the viewer (page hidden) without forgetting the profile. */
  pause(): void {
    if (this.stopped || this.paused) return;
    this.paused = true;
    this.teardown();
    this.options.onStatus("paused");
  }

  resume(): void {
    if (this.stopped || !this.paused) return;
    this.paused = false;
    this.attempt = 0;
    void this.connect();
  }

  send(message: Record<string, unknown>): boolean {
    if (this.socket?.readyState !== 1) return false;
    this.socket.send(JSON.stringify(message));
    return true;
  }

  private teardown(): void {
    clearTimeout(this.retryTimer);
    clearInterval(this.reauthTimer);
    this.retryTimer = undefined;
    this.reauthTimer = undefined;
    const socket = this.socket;
    this.socket = undefined;
    if (socket && socket.readyState <= 1) socket.close(1000, "viewer_closed");
  }

  private url(path: string): string {
    const origin = this.options.origin ?? window.location.origin;
    const url = new URL(path, origin);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    return url.href;
  }

  private async connect(): Promise<void> {
    if (this.stopped || this.paused) return;
    this.options.onStatus(this.attempt ? "retrying" : "connecting");
    let ticket: Awaited<ReturnType<CloudBrowserAPI["viewerTicket"]>>;
    try {
      ticket = await this.options.api.viewerTicket(this.options.profileId);
    } catch (error) {
      if (this.stopped || this.paused) return;
      const code = (error as { code?: string }).code;
      // A 403 may be a CSRF token rotated by a concurrent request; only a
      // repeated refusal (or a missing profile) means this person lost it.
      if (code === "not_found" || (code === "not_authorized" && this.refusals++ >= 2)) {
        this.stopped = true;
        this.options.onStatus("closed", "forbidden");
        return;
      }
      this.retry();
      return;
    }
    this.refusals = 0;
    if (this.stopped || this.paused) return;
    const create = this.options.createSocket ?? ((url, protocols) => new WebSocket(url, protocols));
    const socket = create(this.url(ticket.path), [VIEWER_PROTOCOL, ticket.ticket]);
    this.socket = socket;
    socket.onopen = () => {
      if (socket !== this.socket) return;
      this.attempt = 0;
      this.options.onStatus("open");
      clearInterval(this.reauthTimer);
      this.reauthTimer = setInterval(() => void this.reauth(), REAUTH_MS);
    };
    socket.onmessage = (event) => {
      if (socket !== this.socket || typeof event.data !== "string") return;
      let message: ServerMessage;
      try {
        message = JSON.parse(event.data);
      } catch {
        return;
      }
      if (message.type === "frame") {
        const { tab, data, width, height } = message as unknown as FrameMessage;
        if (typeof data === "string") this.options.onFrame({ tab: typeof tab === "string" ? tab : undefined, data, width, height });
        return;
      }
      this.options.onMessage(message);
    };
    socket.onclose = (event) => {
      if (socket !== this.socket) return;
      this.socket = undefined;
      clearInterval(this.reauthTimer);
      // 4403: this person no longer owns the profile. 4410: it was reset.
      if (event.code === 4403 || event.code === 4410) {
        this.stopped = true;
        this.options.onStatus("closed", event.code === 4403 ? "forbidden" : "reset");
        return;
      }
      this.retry();
    };
  }

  private async reauth(): Promise<void> {
    try {
      const ticket = await this.options.api.viewerTicket(this.options.profileId);
      this.send({ type: "reauth", ticket: ticket.ticket });
    } catch {
      /* the server closes with 4401 at expiry; reconnect handles it */
    }
  }

  private retry(): void {
    if (this.stopped || this.paused) return;
    const delay = BACKOFF_MS[Math.min(this.attempt, BACKOFF_MS.length - 1)] ?? 10_000;
    this.attempt++;
    this.options.onStatus("retrying");
    this.retryTimer = setTimeout(() => void this.connect(), delay);
  }
}
