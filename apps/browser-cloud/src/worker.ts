/**
 * Sumi Cloud browser Worker: one Durable Object per secretary browser
 * profile, a Browser Run session per live profile, and the shared screen.
 *
 * Routes:
 *   GET  /health
 *   POST /profiles/:id/wake        bearer SUMI_BROWSER_CLOUD_TOKEN (the Sumi
 *                                  API, when Cloud tab work is queued or a
 *                                  live host must learn new grants)
 *   GET  /browser-cloud/viewer     WebSocket for the person's viewer. The
 *                                  API-issued ticket travels in
 *                                  Sec-WebSocket-Protocol, never in the URL.
 * Nothing else is served: there is no evaluation or automation endpoint.
 *
 * Bindings: BROWSER (Browser Run), PROFILE (Durable Objects), SUMI_STATE
 * (Workers VPC Service to the Sumi API in Cloud), vars SUMI_STATE_URL,
 * SUMI_BROWSER_IDLE_GRACE_MS, SUMI_BROWSER_KEEPALIVE_MS; secret
 * SUMI_BROWSER_CLOUD_TOKEN (the API's value).
 */
import { Buffer } from "node:buffer";
import type { Env } from "./profile.ts";
import { verifyTicket } from "./ticket.ts";

// The shared host code reads response bodies with Buffer.
(globalThis as { Buffer?: unknown }).Buffer ??= Buffer;

export { ProfileBrowser } from "./profile.ts";

interface WorkerEnv extends Env {
  PROFILE: {
    idFromName(name: string): unknown;
    get(id: unknown): { fetch(request: Request): Promise<Response> };
  };
}

const PROTOCOL = "sumi.browser.v1";
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const encoder = new TextEncoder();

function sameSecret(a: string, b: string): boolean {
  const x = encoder.encode(a);
  const y = encoder.encode(b);
  if (x.length !== y.length || !x.length) return false;
  let diff = 0;
  for (let i = 0; i < x.length; i++) diff |= (x[i] as number) ^ (y[i] as number);
  return diff === 0;
}

function profileStub(env: WorkerEnv, profile: string) {
  return env.PROFILE.get(env.PROFILE.idFromName(profile));
}

export async function handle(request: Request, env: WorkerEnv): Promise<Response> {
  const url = new URL(request.url);
  if (url.pathname === "/health") return new Response("ok");
  const token = env.SUMI_BROWSER_CLOUD_TOKEN ?? "";
  if (!token) return new Response("unavailable", { status: 503 });

  const wake = url.pathname.match(/^\/profiles\/([0-9a-f-]{36})\/wake$/);
  if (wake && request.method === "POST") {
    const auth = request.headers.get("authorization") ?? "";
    if (!sameSecret(auth, `Bearer ${token}`) || !UUID.test(wake[1] as string))
      return new Response("unauthorized", { status: 401 });
    const body = (await request.json().catch(() => ({}))) as { work?: boolean };
    return profileStub(env, wake[1] as string).fetch(
      new Request("https://profile.invalid/wake", {
        method: "POST",
        body: JSON.stringify({ profile: wake[1], work: body.work === true }),
      }),
    );
  }

  if (url.pathname === "/browser-cloud/viewer") {
    if (request.headers.get("upgrade")?.toLowerCase() !== "websocket")
      return new Response("websocket required", { status: 426 });
    const offered = (request.headers.get("sec-websocket-protocol") ?? "").split(",").map((p) => p.trim());
    const ticket = offered.find((p) => p.startsWith("sbt1."));
    if (!offered.includes(PROTOCOL) || !ticket) return new Response("forbidden", { status: 403 });
    const claims = await verifyTicket(ticket, token);
    if (!claims) return new Response("forbidden", { status: 403 });
    const response = await profileStub(env, claims.b).fetch(
      new Request("https://profile.invalid/viewer", {
        headers: { upgrade: "websocket", "x-sumi-viewer": JSON.stringify(claims) },
      }),
    );
    if (response.status !== 101) return response;
    const headers = new Headers(response.headers);
    headers.set("sec-websocket-protocol", PROTOCOL);
    return new Response(null, { status: 101, headers, webSocket: (response as { webSocket?: unknown }).webSocket } as ResponseInit);
  }
  return new Response("not found", { status: 404 });
}

export default {
  fetch(request: Request, env: WorkerEnv): Promise<Response> {
    return handle(request, env);
  },
};
