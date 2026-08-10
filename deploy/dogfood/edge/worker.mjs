import { classifyPath } from "./route-policy.mjs";

function unavailable() {
  return new Response(JSON.stringify({ error: "origin_unavailable" }), {
    status: 503,
    headers: {
      "Cache-Control": "no-store",
      "Content-Type": "application/json; charset=utf-8",
    },
  });
}

function denied() {
  return new Response(null, {
    status: 404,
    headers: { "Cache-Control": "no-store" },
  });
}

const unavailableOriginStatuses = new Set([502, 504, 521, 522, 523, 524, 530]);

export async function handleRequest(request, environment, originFetch = fetch) {
  const route = classifyPath(new URL(request.url).pathname);
  if (route === "deny") return denied();
  if (route === "asset") return environment.ASSETS.fetch(request);

  try {
    // cache=no-store bypasses the Cloudflare cache without rebuilding the
    // origin Response. The Go API sets Cache-Control:no-store itself, so
    // Set-Cookie multiplicity, streaming bodies, and WebSocket 101 stay intact.
    const originRequest = new Request(request, { cache: "no-store" });
    const response = await originFetch(originRequest);
    // A live Tunnel connector with a stopped/unreachable origin yields an HTTP
    // Cloudflare gateway response rather than rejecting fetch(). Normalize
    // known gateway statuses, but preserve application 503 responses: Sumi
    // uses them for honest capability/dependency failures with typed bodies.
    if (unavailableOriginStatuses.has(response.status)) return unavailable();
    return response;
  } catch {
    return unavailable();
  }
}

export default {
  fetch(request, environment) {
    return handleRequest(request, environment);
  },
};
