import { useConversation } from "./store";

const authorityBindingStorageKey = "sumi.direct-chat.authority-binding-v1";
let currentBindingID: string | null | undefined;

/**
 * Ends all browser-owned direct-chat state at an authenticated authority
 * boundary. This is intentionally separate from reconnect-oriented
 * disconnect(), which preserves replay cursor and pending delivery state.
 */
export function resetDirectChatAuthority(): boolean {
  return useConversation.getState().resetAuthority();
}

export function bindDirectChatAuthority(authorityBindingID: string): void {
  const previous = readCurrentBindingID();
  const changed = previous !== authorityBindingID;
  if (changed && !resetDirectChatAuthority()) {
    throw new Error(
      "Direct-chat private state could not be cleared for the new authority",
    );
  }
  currentBindingID = authorityBindingID;
  try {
    globalThis.sessionStorage.setItem(
      authorityBindingStorageKey,
      authorityBindingID,
    );
  } catch {
    // In-memory binding still protects transitions in this document.
  }
  if (changed) useConversation.getState().resumeMountedConnection();
}

export function clearDirectChatAuthority(): boolean {
  const cleared = resetDirectChatAuthority();
  currentBindingID = null;
  try {
    globalThis.sessionStorage.removeItem(authorityBindingStorageKey);
  } catch {
    // Storage-key cleanup is best effort; the reset result remains authoritative.
  }
  return cleared;
}

function readCurrentBindingID(): string | null {
  if (currentBindingID !== undefined) return currentBindingID;
  try {
    currentBindingID = globalThis.sessionStorage.getItem(
      authorityBindingStorageKey,
    );
  } catch {
    currentBindingID = null;
  }
  return currentBindingID;
}
