export type RouteDisposition =
  | "origin"
  | "firebase-auth"
  | "deny"
  | "service-worker"
  | "release-manifest"
  | "static-asset"
  | "navigation";

export interface RouteDecision {
  canonicalPath: string | null;
  disposition: RouteDisposition;
}

export const originRoutes = Object.freeze({
  exact: Object.freeze([
    "/health",
    "/api/model-connections",
    "/api/mcp-connections",
    "/api/browser-tabs",
    "/api/usage",
    "/workspaces",
    "/app-installations",
    "/feedback/bootstrap",
    "/feedback/threads",
    "/feedback/attachments",
    "/feedback/diagnostics",
    "/me/approvals",
  ]),
  prefixes: Object.freeze([
    "/auth/",
    "/api/model-connections/",
    // Shared tool grants use browser session auth; connected browser hosts
    // poll/complete with their tab-scoped bearer. The API authorizes both.
    "/api/mcp-connections/",
    "/api/browser-tabs/",
    "/api/browser-host/tabs/",
    "/api/usage/",
    "/direct-chat/",
    "/messaging/",
    // Shared terminal API + WS. Only the subpath prefix proxies: bare
    // `/terminal` is the SPA page and must fall through to navigation.
    "/terminal/",
    "/feedback/threads/",
    "/feedback/attachments/",
    // Keep transfer API requests out of the SPA fallback. The API origin
    // returns 404 until the registrar is mounted with production auth.
    "/api/secretary-transfer/",
    // The return namespace is mounted while the shared-file policy stays
    // undecided: begun-session status/cancel/recovery must reach the API,
    // and the origin itself refuses new admission. Routing is transport,
    // not the policy decision.
    "/api/secretary-return/",
    // The scoped storage proxy for a returned secretary's Cloud working
    // store (cloud file mode). Same posture as the return namespace:
    // routing is transport; the origin authorizes the storage credential.
    "/api/secretary-files/",
    // Person file workspace: the API proxies these ops to the private
    // filesvc with session-derived scope; filesvc itself is never on the
    // public edge.
    "/files/",
    "/workspaces/",
    "/workspace-invites/",
    "/apps/",
    "/app-installations/",
    "/me/approvals/",
  ]),
});

// The Workspace control registrar is stacked above the current base. Keeping
// its expected namespace as an explicit integration contract lets this edge
// ship first without pretending those API routes exist on main. The parity
// gate changes from "pending" to mandatory as soon as the workspace package is
// present in an integration head.
export const workspaceIntegrationContract = Object.freeze({
  packageName: "workspace",
  exact: Object.freeze(["/workspaces", "/app-installations"]),
  prefixes: Object.freeze([
    "/workspaces/",
    "/workspace-invites/",
    "/apps/",
    "/app-installations/",
  ]),
});

// Bare dynamic namespaces are not application pages. Keeping them out of the
// SPA fallback makes a missing API route fail visibly instead of rendering the
// signed-in shell. The other entries are private transports or dormant
// artifacts that must never reach the canonical browser origin. `/internal/*`
// is the server-only namespace: provider transports live on the private
// local-control registry, and the persona state service is reached by the
// secretary core through its own configured service URL — never through this
// public front door.
export const deniedRoutes = Object.freeze({
  exact: Object.freeze([
    "/.env",
    "/.git",
    "/.github",
    "/.hg",
    "/.svn",
    "/.wrangler",
    "/assets",
    "/auth",
    "/cloudflare",
    "/contracts",
    "/coverage",
    "/deploy",
    "/direct-chat",
    "/docs",
    "/e2e",
    "/files",
    // Persona-capability endpoints belong to the Local host, not Cloud.
    "/fm",
    "/jenkinsfile",
    "/makefile",
    "/messaging",
    "/node_modules",
    "/package.json",
    "/packages",
    "/public",
    "/scripts",
    "/src",
    "/tsconfig.json",
    "/turbo.json",
    "/vite.config.ts",
    "/wrangler.jsonc",
    "/workspace-invites",
    "/apps",
    "/api/browser-host",
    "/api/browser-host/tabs",
    "/api/secretary-transfer",
    "/api/secretary-return",
    "/api/secretary-files",
    "/agent/ws",
    "/internal",
    "/local-control/v1",
    "/ready",
    "/mcp-app-sandbox.html",
  ]),
  prefixes: Object.freeze([
    "/.env.",
    "/.git/",
    "/.github/",
    "/.hg/",
    "/.svn/",
    "/.wrangler/",
    "/agent/ws/",
    "/cloudflare/",
    "/contracts/",
    "/coverage/",
    "/deploy/",
    "/docs/",
    "/e2e/",
    "/favicon.svg/",
    "/fm/",
    "/health/",
    "/index.html/",
    "/internal/",
    "/local-control/v1/",
    "/mcp-app-sandbox.html/",
    "/node_modules/",
    "/packages/",
    "/public/",
    "/ready/",
    "/release.json/",
    "/scripts/",
    "/src/",
    "/sw.js/",
    "/theme-bootstrap.js/",
  ]),
});

const staticAssetRoutes = Object.freeze({
  exact: Object.freeze(["/favicon.svg", "/index.html", "/theme-bootstrap.js"]),
  prefixes: Object.freeze(["/assets/"]),
});

const sourceLikeSuffixes = Object.freeze([
  ".c",
  ".cc",
  ".cpp",
  ".env",
  ".go",
  ".h",
  ".hpp",
  ".java",
  ".jsx",
  ".lock",
  ".md",
  ".mdx",
  ".proto",
  ".py",
  ".rs",
  ".sql",
  ".toml",
  ".ts",
  ".tsx",
  ".yaml",
  ".yml",
]);

function matches(
  pathname: string,
  routes: Readonly<{
    exact: readonly string[];
    prefixes: readonly string[];
  }>,
): boolean {
  return (
    routes.exact.includes(pathname) ||
    routes.prefixes.some((prefix) => pathname.startsWith(prefix))
  );
}

const maximumDecodePasses = 2;

function canonicalizePath(pathname: string): string | null {
  if (!pathname.startsWith("/")) return null;

  let decoded = pathname;
  for (let pass = 0; decoded.includes("%"); pass += 1) {
    if (pass >= maximumDecodePasses) return null;
    try {
      decoded = decodeURIComponent(decoded);
    } catch {
      return null;
    }
  }

  // A percent sign after the bounded decode is either an encoded literal or
  // a deeper encoding layer. Neither has a stable routing meaning across the
  // Worker, Static Assets, and the private origin, so fail closed.
  if (decoded.includes("%")) return null;
  if (
    decoded.includes("\\") ||
    [...decoded].some((character) => {
      const codePoint = character.codePointAt(0) ?? 0;
      return codePoint <= 0x1f || codePoint === 0x7f;
    })
  ) {
    return null;
  }

  const hadTrailingSlash = decoded.length > 1 && decoded.endsWith("/");
  const segments: string[] = [];
  for (const segment of decoded.split("/")) {
    if (segment === "" || segment === ".") continue;
    if (segment === "..") {
      segments.pop();
      continue;
    }
    segments.push(segment);
  }

  const normalized = `/${segments.join("/")}`;
  return hadTrailingSlash && normalized !== "/" ? `${normalized}/` : normalized;
}

export function decidePath(pathname: string): RouteDecision {
  const canonicalPath = canonicalizePath(pathname);
  if (canonicalPath === null) {
    return { canonicalPath: null, disposition: "deny" };
  }

  // The Firebase helper namespace is exact lowercase: every proxied response
  // is same-origin with the app and carries no app CSP, so case variants like
  // /__/AUTH/handler (Firebase Hosting 404 HTML) are denied rather than
  // forwarded.
  if (canonicalPath === "/__/auth" || canonicalPath.startsWith("/__/auth/")) {
    return { canonicalPath, disposition: "firebase-auth" };
  }
  // Everything else under Firebase's reserved /__/ namespace is not an app
  // route: deny it rather than serving SPA fallback.
  if (canonicalPath === "/__" || canonicalPath.startsWith("/__/")) {
    return { canonicalPath, disposition: "deny" };
  }

  const policyPath = canonicalPath.toLowerCase();
  if (policyPath === "/sw.js") {
    return { canonicalPath, disposition: "service-worker" };
  }
  if (policyPath === "/release.json") {
    return { canonicalPath, disposition: "release-manifest" };
  }
  if (matches(policyPath, deniedRoutes)) {
    return { canonicalPath, disposition: "deny" };
  }
  if (policyPath.endsWith(".map")) {
    return { canonicalPath, disposition: "deny" };
  }
  if (sourceLikeSuffixes.some((suffix) => policyPath.endsWith(suffix))) {
    return { canonicalPath, disposition: "deny" };
  }
  if (matches(policyPath, originRoutes)) {
    return { canonicalPath, disposition: "origin" };
  }
  if (matches(policyPath, staticAssetRoutes)) {
    return { canonicalPath, disposition: "static-asset" };
  }
  const finalSegment = policyPath.slice(policyPath.lastIndexOf("/") + 1);
  const disposition = /\.[^./]+$/.test(finalSegment)
    ? "static-asset"
    : "navigation";
  return { canonicalPath, disposition };
}

export function classifyPath(pathname: string): RouteDisposition {
  return decidePath(pathname).disposition;
}

// Static Assets' SPA fallback is only safe after the Worker has classified the
// request. A narrow pattern list lets an unrecognized source or private path
// bypass policy and become index.html, so every request runs policy first.
export const runWorkerFirst = true;
