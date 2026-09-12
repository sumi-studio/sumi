const storageKey = "sumi.enrollment-invitation";
const workspaceStorageKey = "sumi.workspace-invitation";
let memoryWorkspace: string | null | undefined;
let memoryToken: string | null | undefined;
const tokenPattern = /^[A-Za-z0-9_-]{43}$/;

/** Capture the invitation before external identity-provider navigation. */
export function captureEnrollmentInvitation(): string | null {
  const fragment = new URLSearchParams(globalThis.location.hash.slice(1));
  const supplied = fragment.get("invite");
  const workspace = fragment.get("workspace_invite");
  if (workspace !== null || supplied !== null) {
    memoryWorkspace =
      workspace && /^[A-Za-z0-9_-]{16,256}$/.test(workspace) ? workspace : null;
    try {
      if (memoryWorkspace)
        sessionStorage.setItem(workspaceStorageKey, memoryWorkspace);
      else sessionStorage.removeItem(workspaceStorageKey);
    } catch {}
  }
  fragment.delete("workspace_invite");
  if (supplied !== null || workspace !== null) {
    fragment.delete("invite");
    const rest = fragment.toString();
    globalThis.history.replaceState(
      globalThis.history.state,
      "",
      `${globalThis.location.pathname}${globalThis.location.search}${rest ? `#${rest}` : ""}`,
    );
    if (supplied === null) return readEnrollmentInvitation();
    if (!tokenPattern.test(supplied)) {
      clearEnrollmentInvitation();
      return null;
    }
    memoryToken = supplied;
    try {
      globalThis.sessionStorage.setItem(storageKey, supplied);
    } catch {
      // Do not move the secret into a URL or persistent storage as a fallback.
    }
    return supplied;
  }
  return readEnrollmentInvitation();
}

export function readEnrollmentInvitation(): string | null {
  if (memoryToken !== undefined) return memoryToken;
  try {
    const token = globalThis.sessionStorage.getItem(storageKey);
    return token !== null && tokenPattern.test(token) ? token : null;
  } catch {
    return null;
  }
}

export function clearEnrollmentInvitation(): void {
  memoryToken = null;
  try {
    globalThis.sessionStorage.removeItem(storageKey);
  } catch {
    // Storage may be disabled in the browser.
  }
}

export function readWorkspaceInvitation(): string | null {
  if (memoryWorkspace !== undefined) return memoryWorkspace;
  try {
    const code = sessionStorage.getItem(workspaceStorageKey);
    return code && /^[A-Za-z0-9_-]{16,256}$/.test(code) ? code : null;
  } catch {
    return null;
  }
}
export function clearWorkspaceInvitation(): void {
  memoryWorkspace = null;
  try {
    sessionStorage.removeItem(workspaceStorageKey);
  } catch {}
}
export function buildWorkspaceInvitationLink(
  code: string,
  enrollmentToken?: string,
): string {
  const fragment = new URLSearchParams();
  if (enrollmentToken) fragment.set("invite", enrollmentToken);
  fragment.set("workspace_invite", code);
  return `${location.origin}/#${fragment}`;
}

export const workspaceInvitationChanged = "sumi:workspace-invitation";

/** Restore the invitation paired with a completed email flow in another tab. */
export function restoreWorkspaceInvitation(code: string): void {
  if (!/^[A-Za-z0-9_-]{16,256}$/.test(code)) return;
  memoryWorkspace = code;
  try {
    sessionStorage.setItem(workspaceStorageKey, code);
  } catch {}
  window.dispatchEvent(new Event(workspaceInvitationChanged));
}
