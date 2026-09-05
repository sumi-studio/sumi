/** Shared app/Service Worker route builder for one Messaging place. */
export function messagingBasePath(workspaceId) {
  if (typeof workspaceId !== "string" || workspaceId.length === 0) return "/";
  return `/w/${encodeURIComponent(workspaceId)}/messaging`;
}

export function messagingPlacePath(workspaceId, placeKind, placeId) {
  const base = messagingBasePath(workspaceId);
  if (base === "/" || typeof placeId !== "string" || placeId.length === 0) {
    return base;
  }
  const encoded = encodeURIComponent(placeId);
  if (placeKind === "channel") return `${base}/c/${encoded}`;
  if (placeKind === "dm") return `${base}/dm/${encoded}`;
  if (placeKind === "group_dm") return `${base}/group/${encoded}`;
  return base;
}

export function isMessagingPath(pathname, workspaceId) {
  const base = messagingBasePath(workspaceId);
  return (
    base !== "/" &&
    typeof pathname === "string" &&
    (pathname === base || pathname.startsWith(`${base}/`))
  );
}
