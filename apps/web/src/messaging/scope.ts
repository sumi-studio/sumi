export interface MessagingScope {
  workspaceId: string;
  installationId: string;
}

const WORKSPACE_QUERY = "workspace_id";
const INSTALLATION_QUERY = "installation_id";

let activeScope: MessagingScope | null = null;
let activeScopeController = new AbortController();

export function messagingScopeKey(scope: MessagingScope): string {
  return `${scope.workspaceId}:${scope.installationId}`;
}

export function sameMessagingScope(
  left: MessagingScope | null,
  right: MessagingScope | null,
): boolean {
  return (
    left === right ||
    (left !== null &&
      right !== null &&
      left.workspaceId === right.workspaceId &&
      left.installationId === right.installationId)
  );
}

export function validateMessagingScope(scope: MessagingScope): MessagingScope {
  if (!scope.workspaceId || !scope.installationId) {
    throw new Error("Messaging requires an exact Workspace and installation");
  }
  return { ...scope };
}

export function setActiveMessagingScope(scope: MessagingScope | null): void {
  const next = scope === null ? null : validateMessagingScope(scope);
  if (sameMessagingScope(activeScope, next)) return;
  activeScopeController.abort();
  activeScope = next;
  activeScopeController = new AbortController();
}

export function getActiveMessagingScope(): MessagingScope | null {
  return activeScope === null ? null : { ...activeScope };
}

export function requireActiveMessagingScope(): MessagingScope {
  if (!activeScope) throw new Error("Messaging scope is not bound");
  return { ...activeScope };
}

/**
 * Captures the exact scope and the lifetime signal in one synchronous read.
 * A Workspace/installation switch aborts the signal before the new scope is
 * published, so independent REST layers cannot finish an old scoped write.
 */
export function requireActiveMessagingBoundary(): {
  scope: MessagingScope;
  signal: AbortSignal;
} {
  return {
    scope: requireActiveMessagingScope(),
    signal: activeScopeController.signal,
  };
}

/**
 * Adds one immutable scope pair to a Messaging path. Callers may not smuggle a
 * second pair into the input path; duplicate authority context fails closed.
 */
export function scopedMessagingPath(
  path: string,
  scope: MessagingScope,
): string {
  const url = new URL(path, "https://messaging.invalid");
  if (
    !url.pathname.startsWith("/messaging/") &&
    url.pathname !== "/messaging"
  ) {
    throw new Error("Messaging scope can only bind Messaging routes");
  }
  if (
    url.searchParams.has(WORKSPACE_QUERY) ||
    url.searchParams.has(INSTALLATION_QUERY)
  ) {
    throw new Error("Messaging scope query is already present");
  }
  const exact = validateMessagingScope(scope);
  url.searchParams.append(WORKSPACE_QUERY, exact.workspaceId);
  url.searchParams.append(INSTALLATION_QUERY, exact.installationId);
  return `${url.pathname}?${url.searchParams.toString()}`;
}

export function bindMessagingScopeToURL(url: URL, scope: MessagingScope): URL {
  const scoped = scopedMessagingPath(`${url.pathname}${url.search}`, scope);
  const next = new URL(url.href);
  const parsed = new URL(scoped, next.origin);
  next.pathname = parsed.pathname;
  next.search = parsed.search;
  return next;
}
