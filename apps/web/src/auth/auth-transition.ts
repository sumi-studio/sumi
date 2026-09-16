/**
 * A synchronous, tab-local signal that an identity teardown (logout or the
 * sign-out leg of an account switch) has been initiated for a Firebase UID.
 *
 * Provider operations finish asynchronously: a backend-owned unlink ends with
 * a Firebase `reload()` whose IndexedDB write is queued behind whichever auth
 * work is already in flight. If logout was *initiated* during that window but
 * its queued sign-out has not applied yet, the operation's own persist can
 * still be ordered last and silently reinstall the old identity. A
 * UID-scoped counter lets the late completion detect a teardown that targeted
 * its own account and honour it, without misreading an unrelated teardown —
 * for example the residue cleanup of an abandoned account switch, where the
 * current account legitimately continues.
 *
 * The counter is process-local by design: it cannot observe another tab's
 * teardown before that tab's IndexedDB write lands, which is the same
 * last-writer-wins residual the stock Firebase queue already has.
 */
const teardownCounts = new Map<string, number>();

/** Record that a sign-out was initiated for `uid`. Call before awaiting it. */
export function noteAuthTeardown(uid: string | null | undefined): void {
  if (!uid) {
    return;
  }
  teardownCounts.set(uid, (teardownCounts.get(uid) ?? 0) + 1);
}

/** How many sign-outs have been initiated for `uid` in this page lifetime. */
export function authTeardownCount(uid: string): number {
  return teardownCounts.get(uid) ?? 0;
}
