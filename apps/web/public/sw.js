/// <reference lib="webworker" />

import { isMessagingPath, messagingPlacePath } from "./place-path.js";

const GENERIC_TITLE = "Sumi";
const GENERIC_BODY = "新しいメッセージがあります";
const PLACE_KINDS = new Set(["channel", "dm", "group_dm"]);

self.addEventListener("install", () => {
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(self.clients.claim());
});

function pointerPayload(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const { workspace_id: workspaceId, place_id: placeId, place_kind: placeKind } =
    value;
  if (
    typeof workspaceId !== "string" ||
    workspaceId.length === 0 ||
    workspaceId.length > 256 ||
    typeof placeId !== "string" ||
    placeId.length === 0 ||
    placeId.length > 256 ||
    !PLACE_KINDS.has(placeKind)
  ) {
    return null;
  }
  return { workspaceId, placeId, placeKind };
}

function pathnameOf(raw) {
  try {
    const url = new URL(raw, self.location.origin);
    return url.origin === self.location.origin ? url.pathname : "";
  } catch {
    return "";
  }
}

async function focusedMessagingClient(workspaceId) {
  const windows = await self.clients.matchAll({
    type: "window",
    includeUncontrolled: true,
  });
  return (
    windows.find(
      (client) =>
        client.focused && isMessagingPath(pathnameOf(client.url), workspaceId),
    ) ?? null
  );
}

self.addEventListener("push", (event) => {
  event.waitUntil(
    (async () => {
      let decoded = null;
      try {
        decoded = event.data ? event.data.json() : null;
      } catch {
        return;
      }
      const pointer = pointerPayload(decoded);
      if (!pointer) return;
      if (await focusedMessagingClient(pointer.workspaceId)) return;

      const url = messagingPlacePath(
        pointer.workspaceId,
        pointer.placeKind,
        pointer.placeId,
      );
      await self.registration.showNotification(GENERIC_TITLE, {
        body: GENERIC_BODY,
        tag: `sumi:${pointer.workspaceId}:${pointer.placeKind}:${pointer.placeId}`,
        icon: "/favicon.svg",
        badge: "/favicon.svg",
        silent: true,
        data: { url },
      });
    })(),
  );
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const target = pathnameOf(event.notification?.data?.url);
  if (!target || !target.startsWith("/w/")) return;
  event.waitUntil(
    (async () => {
      const windows = await self.clients.matchAll({
        type: "window",
        includeUncontrolled: true,
      });
      const matching = windows.filter(
        (client) => pathnameOf(client.url) === target,
      );
      for (const client of [...matching, ...windows]) {
        try {
          if ("navigate" in client) await client.navigate(target);
          await client.focus();
          return;
        } catch {
          // Try another same-origin window before opening a new one.
        }
      }
      await self.clients.openWindow(target);
    })(),
  );
});

self.addEventListener("pushsubscriptionchange", (event) => {
  event.waitUntil(
    (async () => {
      const applicationServerKey =
        event.oldSubscription?.options?.applicationServerKey ?? null;
      if (!applicationServerKey) return;
      try {
        await self.registration.pushManager.subscribe({
          userVisibleOnly: true,
          applicationServerKey,
        });
      } catch {
        // The page reconciles the replacement under its current exact scope.
      }
    })(),
  );
});
