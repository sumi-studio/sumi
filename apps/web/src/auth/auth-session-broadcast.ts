/**
 * Same-origin broadcast announcing that this jar's Sumi session ended by an
 * explicit logout. BroadcastChannel never self-delivers to the posting object
 * but does reach other channels in the same page, so messages carry a sender
 * token: the announcing context skips its own echo while every other live tab
 * re-verifies the server session before tearing anything down — a sign-in
 * that already replaced the session is honoured rather than clobbered.
 */
const sessionEndedChannelName = "sumi.auth.session-ended.v1";

export function publishSessionEnded(sender: string): void {
  if (typeof globalThis.BroadcastChannel !== "function") return;
  try {
    const channel = new globalThis.BroadcastChannel(sessionEndedChannelName);
    try {
      channel.postMessage({ v: 1, sender });
    } finally {
      channel.close();
    }
  } catch {
    // The cross-tab notice is best-effort; the local teardown already ran.
  }
}

export function subscribeSessionEnded(
  ownSender: string,
  handler: () => void,
): () => void {
  if (typeof globalThis.BroadcastChannel !== "function") return () => {};
  try {
    const channel = new globalThis.BroadcastChannel(sessionEndedChannelName);
    const onMessage = (event: MessageEvent<unknown>) => {
      const data = event.data;
      if (
        typeof data === "object" &&
        data !== null &&
        (data as { v?: unknown }).v === 1 &&
        (data as { sender?: unknown }).sender !== ownSender
      ) {
        handler();
      }
    };
    channel.addEventListener("message", onMessage);
    return () => {
      channel.removeEventListener("message", onMessage);
      channel.close();
    };
  } catch {
    return () => {};
  }
}
