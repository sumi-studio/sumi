export const originRoutes = Object.freeze({
  exact: Object.freeze(["/health", "/workspaces", "/app-installations"]),
  prefixes: Object.freeze([
    "/auth/",
    "/direct-chat/",
    "/messaging/",
    "/workspaces/",
    "/workspace-invites/",
    "/apps/",
    "/app-installations/",
  ]),
});

export const deniedRoutes = Object.freeze({
  exact: Object.freeze([
    "/auth",
    "/direct-chat",
    "/messaging",
    "/workspace-invites",
    "/apps",
    "/agent",
    "/local-control",
    "/ready",
    "/mcp-app-sandbox.html",
  ]),
  prefixes: Object.freeze([
    "/agent/",
    "/health/",
    "/local-control/",
    "/ready/",
  ]),
});

function matches(pathname, routes) {
  return (
    routes.exact.includes(pathname) ||
    routes.prefixes.some((prefix) => pathname.startsWith(prefix))
  );
}

export function classifyPath(pathname) {
  if (typeof pathname !== "string" || !pathname.startsWith("/")) {
    return "deny";
  }
  if (matches(pathname, deniedRoutes)) return "deny";
  if (matches(pathname, originRoutes)) return "origin";
  return "asset";
}

export const workerFirstPatterns = Object.freeze([
  "/auth",
  "/auth/*",
  "/direct-chat",
  "/direct-chat/*",
  "/messaging",
  "/messaging/*",
  "/workspaces",
  "/workspaces/*",
  "/workspace-invites",
  "/workspace-invites/*",
  "/apps",
  "/apps/*",
  "/app-installations",
  "/app-installations/*",
  "/health",
  "/health/*",
  "/ready",
  "/ready/*",
  "/agent",
  "/agent/*",
  "/local-control",
  "/local-control/*",
  "/mcp-app-sandbox.html",
]);
