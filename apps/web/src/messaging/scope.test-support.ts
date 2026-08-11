import type { MessagingScope } from "./scope";

export const MESSAGING_SCOPE = {
  workspaceId: "workspace-1",
  installationId: "installation-1",
} as const satisfies MessagingScope;

const TEST_ORIGIN = "https://messaging.test";

/**
 * Proves that a request carries exactly one copy of the test authority scope,
 * then returns the original path with only that scope removed.
 */
export function expectScopedMessagingPath(input: RequestInfo | URL): string {
  const url = new URL(String(input), TEST_ORIGIN);
  expectExactQueryValue(url, "workspace_id", MESSAGING_SCOPE.workspaceId);
  expectExactQueryValue(url, "installation_id", MESSAGING_SCOPE.installationId);
  url.searchParams.delete("workspace_id");
  url.searchParams.delete("installation_id");
  return `${url.pathname}${url.search}`;
}

/** Builds the exact URL expected at fetch without sharing production code. */
export function scopedMessagingTestPath(path: string): string {
  const url = new URL(path, TEST_ORIGIN);
  if (
    url.searchParams.has("workspace_id") ||
    url.searchParams.has("installation_id")
  ) {
    throw new Error("test path already contains Messaging scope");
  }
  url.searchParams.append("workspace_id", MESSAGING_SCOPE.workspaceId);
  url.searchParams.append("installation_id", MESSAGING_SCOPE.installationId);
  return `${url.pathname}${url.search}`;
}

function expectExactQueryValue(url: URL, key: string, expected: string): void {
  const values = url.searchParams.getAll(key);
  if (values.length !== 1 || values[0] !== expected) {
    throw new Error(
      `expected exactly one ${key}=${expected}, received ${JSON.stringify(values)}`,
    );
  }
}
