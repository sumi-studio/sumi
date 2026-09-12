import { decidePath } from "./route-policy.ts";

interface AssetFetcher {
  fetch(request: Request): Promise<Response>;
}

export interface CloudflareEnvironment {
  ASSETS: AssetFetcher;
  /** A fixed-target VPC Service binding, never a VPC Network binding. */
  SUMI_ORIGIN?: {
    fetch(request: Request, init?: RequestInit): Promise<Response>;
  };
  /** Transport to the registered private service; public Origin stays intact. */
  SUMI_ORIGIN_PROTOCOL?: "http" | "https";
}

interface CloudflareRequestInit extends RequestInit {
  cf?: {
    cacheTtlByStatus?: Record<string, number>;
  };
}

export type OriginFetch = (
  request: Request,
  init?: CloudflareRequestInit,
) => Promise<Response>;

const unavailableOriginStatuses = new Set([
  502, 504, 520, 521, 522, 523, 524, 525, 526, 530,
]);

const cacheBypass: CloudflareRequestInit = Object.freeze({
  cf: Object.freeze({
    // A negative TTL is Cloudflare's per-subrequest cache bypass. Passing the
    // incoming Request itself keeps URL, Host, Origin, Cookie, body, Upgrade,
    // and cancellation bound to the browser request.
    cacheTtlByStatus: Object.freeze({ "100-599": -1 }),
  }),
});

function originRequestInit(request: Request): CloudflareRequestInit {
  return {
    ...cacheBypass,
    // enable_request_signal makes the incoming signal observable and
    // request_signal_passthrough gives forwarded fetches the same lifetime.
    // Binding it explicitly also keeps cancellation intact when the cf cache
    // override is supplied as a separate RequestInit object.
    signal: request.signal,
  };
}

async function fetchPrivateService(
  request: Request,
  environment: CloudflareEnvironment,
): Promise<Response> {
  const binding = environment.SUMI_ORIGIN;
  if (!binding) throw new Error("Private service is not configured");
  const publicUrl = new URL(request.url);
  const protocol = environment.SUMI_ORIGIN_PROTOCOL ?? "http";
  if (protocol !== "http" && protocol !== "https")
    throw new Error("Invalid private transport");
  const privateUrl = new URL(publicUrl);
  // VPC Service configuration pins the actual host/port. The fetch URL host
  // supplies Host/SNI, while its scheme selects plaintext or TLS transport.
  privateUrl.protocol = `${protocol}:`;
  const forwarded = new Request(privateUrl, request);
  // Caller-supplied proxy metadata must never describe our trusted hop.
  for (const key of [...forwarded.headers.keys()]) {
    if (
      key.toLowerCase() === "forwarded" ||
      key.toLowerCase().startsWith("x-forwarded-") ||
      key.toLowerCase() === "x-real-ip"
    )
      forwarded.headers.delete(key);
  }
  forwarded.headers.set("Host", publicUrl.host);
  forwarded.headers.set("X-Forwarded-Host", publicUrl.host);
  forwarded.headers.set("X-Forwarded-Proto", publicUrl.protocol.slice(0, -1));
  forwarded.headers.set(
    "X-Forwarded-Port",
    publicUrl.port || (publicUrl.protocol === "https:" ? "443" : "80"),
  );
  // Return redirects to the browser, without replaying cookies/body to a new
  // destination. WebSocket upgrades retain their exact response identity.
  const response = await binding.fetch(forwarded, {
    signal: request.signal,
    redirect: "manual",
  });
  return publicUrl.protocol === "https:"
    ? secureOriginCookies(response)
    : response;
}

function secureOriginCookies(response: Response): Response {
  if (response.status === 101 || !response.headers.has("Set-Cookie"))
    return response;
  const cookies = response.headers.getSetCookie();
  const secured = cookies.map((cookie) =>
    cookie
      .split(";")
      .slice(1)
      .some((attribute) => attribute.trim().toLowerCase() === "secure")
      ? cookie
      : `${cookie}; Secure`,
  );
  if (secured.every((cookie, index) => cookie === cookies[index]))
    return response;
  const headers = new Headers(response.headers);
  headers.delete("Set-Cookie");
  for (const cookie of secured) headers.append("Set-Cookie", cookie);
  // HTTPS termination is here; the same API still serves local HTTP clients.
  // Reuse the untouched stream, without cloning, reading, or buffering it.
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

function unavailable(): Response {
  return new Response(JSON.stringify({ error: "origin_unavailable" }), {
    status: 503,
    headers: {
      "Cache-Control": "no-store",
      "Content-Type": "application/json; charset=utf-8",
      "X-Content-Type-Options": "nosniff",
    },
  });
}

function denied(): Response {
  return new Response(null, {
    status: 404,
    headers: {
      "Cache-Control": "no-store",
      "X-Content-Type-Options": "nosniff",
    },
  });
}

function isJavaScript(response: Response): boolean {
  const contentType = response.headers.get("Content-Type") ?? "";
  return /^(?:application|text)\/(?:java|ecma)script(?:;|$)/i.test(contentType);
}

function isJson(response: Response): boolean {
  const contentType = response.headers.get("Content-Type") ?? "";
  return /^application\/json(?:;|$)/i.test(contentType);
}

function isHtml(response: Response): boolean {
  const contentType = response.headers.get("Content-Type") ?? "";
  return /^text\/html(?:;|$)/i.test(contentType);
}

function isUnexpectedAssetResponse(response: Response): boolean {
  return (
    response.status >= 400 ||
    (response.status >= 300 && response.status < 400 && response.status !== 304)
  );
}

function canonicalAssetRequest(
  request: Request,
  canonicalPath: string,
): Request {
  const url = new URL(request.url);
  // Cloudflare Static Assets canonically redirects /index.html to /. Fetching
  // the equivalent root artifact internally keeps the browser response direct
  // and prevents a general redirect pass-through exception.
  const assetPath = canonicalPath === "/index.html" ? "/" : canonicalPath;
  if (url.pathname === assetPath) return request;
  url.pathname = assetPath;
  return new Request(url, request);
}

async function serveServiceWorker(
  request: Request,
  canonicalPath: string,
  environment: CloudflareEnvironment,
): Promise<Response> {
  const asset = await environment.ASSETS.fetch(
    canonicalAssetRequest(request, canonicalPath),
  );

  // Static Assets' SPA fallback returns index.html with 200 for a missing
  // path. A service-worker URL must never install that HTML as script. Wrangler
  // assigns a JavaScript MIME type to a real sw.js, so MIME is the exact-asset
  // discriminator while the file is optional.
  if (isUnexpectedAssetResponse(asset) || !isJavaScript(asset)) return denied();

  const headers = new Headers(asset.headers);
  headers.set("Cache-Control", "no-cache, must-revalidate");
  headers.set("X-Content-Type-Options", "nosniff");
  return new Response(asset.body, {
    status: asset.status,
    statusText: asset.statusText,
    headers,
  });
}

async function serveReleaseManifest(
  request: Request,
  canonicalPath: string,
  environment: CloudflareEnvironment,
): Promise<Response> {
  const asset = await environment.ASSETS.fetch(
    canonicalAssetRequest(request, canonicalPath),
  );
  if (isUnexpectedAssetResponse(asset) || !isJson(asset)) return denied();

  const headers = new Headers(asset.headers);
  headers.set("Cache-Control", "no-store");
  headers.set("X-Content-Type-Options", "nosniff");
  return new Response(asset.body, {
    status: asset.status,
    statusText: asset.statusText,
    headers,
  });
}

async function serveStaticAsset(
  request: Request,
  canonicalPath: string,
  environment: CloudflareEnvironment,
): Promise<Response> {
  const asset = await environment.ASSETS.fetch(
    canonicalAssetRequest(request, canonicalPath),
  );

  // With SPA fallback enabled, a missing file-like path is returned as HTML
  // with 200. Only index.html itself may be an HTML static asset; every other
  // file-like request must resolve to a real artifact or fail closed.
  if (
    isUnexpectedAssetResponse(asset) ||
    (canonicalPath !== "/index.html" && isHtml(asset))
  ) {
    return denied();
  }
  if (canonicalPath === "/index.html") {
    const headers = new Headers(asset.headers);
    headers.set("Cache-Control", "public, max-age=0, must-revalidate");
    return new Response(asset.body, {
      status: asset.status,
      statusText: asset.statusText,
      headers,
    });
  }
  return asset;
}

async function serveNavigation(
  request: Request,
  canonicalPath: string,
  environment: CloudflareEnvironment,
): Promise<Response> {
  const asset = await environment.ASSETS.fetch(
    canonicalAssetRequest(request, canonicalPath),
  );
  return isUnexpectedAssetResponse(asset) ? denied() : asset;
}

export async function handleRequest(
  request: Request,
  environment: CloudflareEnvironment,
  originFetch: OriginFetch = fetch,
): Promise<Response> {
  const route = decidePath(new URL(request.url).pathname);
  if (route.disposition === "deny" || route.canonicalPath === null) {
    return denied();
  }
  if (route.disposition === "service-worker") {
    return serveServiceWorker(request, route.canonicalPath, environment);
  }
  if (route.disposition === "release-manifest") {
    return serveReleaseManifest(request, route.canonicalPath, environment);
  }
  if (route.disposition === "static-asset") {
    return serveStaticAsset(request, route.canonicalPath, environment);
  }
  if (route.disposition === "navigation") {
    return serveNavigation(request, route.canonicalPath, environment);
  }

  try {
    // Do not clone either side of this fetch. On a Worker Route,
    // global_fetch_private_origin sends the incoming Request to the DNS origin
    // behind the named Tunnel. Returning the exact Response preserves multiple
    // Set-Cookie fields, streaming bodies, and WebSocket 101 state.
    const response = environment.SUMI_ORIGIN
      ? await fetchPrivateService(request, environment)
      : await originFetch(request, originRequestInit(request));
    if (unavailableOriginStatuses.has(response.status)) return unavailable();
    return response;
  } catch {
    return unavailable();
  }
}

export default {
  fetch(request: Request, environment: CloudflareEnvironment) {
    return handleRequest(request, environment);
  },
};
