import { useConversation } from "./store";

const authorityIdentityStorageKey = "sumi.direct-chat.authority-user";
let currentIdentity: string | null | undefined;

/**
 * Ends all browser-owned direct-chat state at an authenticated identity
 * boundary. This is intentionally separate from reconnect-oriented
 * disconnect(), which preserves replay cursor and pending delivery state.
 */
export function resetDirectChatAuthority(): void {
  useConversation.getState().resetAuthority();
}

export function bindDirectChatAuthority(identity: string): void {
  const previous = readCurrentIdentity();
  if (previous !== identity) {
    resetDirectChatAuthority();
  }
  currentIdentity = identity;
  try {
    globalThis.sessionStorage.setItem(authorityIdentityStorageKey, identity);
  } catch {
    // In-memory identity still protects transitions in this document.
  }
}

export function clearDirectChatAuthority(): void {
  resetDirectChatAuthority();
  currentIdentity = null;
  try {
    globalThis.sessionStorage.removeItem(authorityIdentityStorageKey);
  } catch {
    // The in-memory reset already ended the active authority.
  }
}

function readCurrentIdentity(): string | null {
  if (currentIdentity !== undefined) return currentIdentity;
  try {
    currentIdentity = globalThis.sessionStorage.getItem(
      authorityIdentityStorageKey,
    );
  } catch {
    currentIdentity = null;
  }
  return currentIdentity;
}
