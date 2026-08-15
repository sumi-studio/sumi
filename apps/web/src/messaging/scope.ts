export interface MessagingScope {
  workspaceId: string;
  installationId: string;
  /** Canonical positive signed-int64 decimal authority generation. */
  authorityEpoch: string;
}

const WORKSPACE_QUERY = "workspace_id";
const INSTALLATION_QUERY = "installation_id";
const AUTHORITY_EPOCH_QUERY = "authority_epoch";
const MAX_AUTHORITY_EPOCH = 9_223_372_036_854_775_807n;

let activeScope: MessagingScope | null = null;
let activeScopeController = new AbortController();

export function messagingScopeKey(scope: MessagingScope): string {
  return `${scope.workspaceId}:${scope.installationId}:${scope.authorityEpoch}`;
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
      left.installationId === right.installationId &&
      left.authorityEpoch === right.authorityEpoch)
  );
}

export function validateMessagingScope(scope: MessagingScope): MessagingScope {
  if (
    !scope.workspaceId ||
    !scope.installationId ||
    !isCanonicalAuthorityEpoch(scope.authorityEpoch)
  ) {
    throw new Error("Messaging requires an exact Workspace and installation");
  }
  return { ...scope };
}

export function isCanonicalAuthorityEpoch(value: unknown): value is string {
  return (
    typeof value === "string" &&
    /^[1-9][0-9]*$/.test(value) &&
    value.length <= 19 &&
    BigInt(value) <= MAX_AUTHORITY_EPOCH
  );
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
 * Adds one immutable scope tuple to a Messaging path. Callers may not smuggle
 * a second authority field into the input path; duplicate context fails closed.
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
    url.searchParams.has(INSTALLATION_QUERY) ||
    url.searchParams.has(AUTHORITY_EPOCH_QUERY)
  ) {
    throw new Error("Messaging scope query is already present");
  }
  const exact = validateMessagingScope(scope);
  url.searchParams.append(WORKSPACE_QUERY, exact.workspaceId);
  url.searchParams.append(INSTALLATION_QUERY, exact.installationId);
  url.searchParams.append(AUTHORITY_EPOCH_QUERY, exact.authorityEpoch);
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
