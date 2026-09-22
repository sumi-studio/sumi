/** The shared tab uses its host's ordinary network reachability, including
 * private HTTP(S) sites. Block privileged protocols, not private addresses. */
export function filterBrowserRequest(
  details: { url: string },
  callback: (response: { cancel: boolean }) => void,
): void {
  let cancel = true;
  try {
    cancel = ![
      "http:",
      "https:",
      "ws:",
      "wss:",
      "data:",
      "blob:",
      "about:",
    ].includes(new URL(details.url).protocol);
  } catch {
    // Malformed request URLs must still complete Electron's callback.
  }
  callback({ cancel });
}
