const CACHE_VERSION = "v2";
const SHELL_CACHE = `decision-shell-${CACHE_VERSION}`;
const ASSET_CACHE = `decision-assets-${CACHE_VERSION}`;

self.addEventListener("install", (event) => {
  event.waitUntil((async () => {
    const shell = await caches.open(SHELL_CACHE);
    await shell.addAll(["/", "/manifest.webmanifest", "/icon.svg"]);
    // The first page load fetches its hashed assets before this worker
    // controls the page, so runtime caching alone would leave the offline
    // shell without JS/CSS until a second visit.
    const html = await (await shell.match("/")).text();
    const assets = [...new Set(html.match(/\/assets\/[A-Za-z0-9._-]+/g) ?? [])];
    await (await caches.open(ASSET_CACHE)).addAll(assets);
  })());
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) => Promise.all(keys.filter((key) => ![SHELL_CACHE, ASSET_CACHE].includes(key)).map((key) => caches.delete(key)))),
  );
  self.clients.claim();
});

self.addEventListener("fetch", (event) => {
  const request = event.request;
  const url = new URL(request.url);
  if (request.method !== "GET" || url.origin !== self.location.origin || url.pathname.startsWith("/api/")) return;

  if (request.mode === "navigate") {
    event.respondWith(fetch(request).then((response) => {
      const copy = response.clone();
      event.waitUntil(caches.open(SHELL_CACHE).then((cache) => cache.put("/", copy)));
      return response;
    }).catch(() => caches.match("/").then((cached) => cached || Response.error())));
    return;
  }

  event.respondWith(caches.match(request).then((cached) => {
    const network = fetch(request).then((response) => {
      // Clone before the page starts streaming the body; a deferred clone
      // throws "Response body is already used" and silently skips the cache.
      if (response.ok) {
        const copy = response.clone();
        event.waitUntil(caches.open(ASSET_CACHE).then((cache) => cache.put(request, copy)));
      }
      return response;
    });
    if (cached) {
      event.waitUntil(network.catch(() => undefined));
      return cached;
    }
    return network;
  }));
});

self.addEventListener("push", (event) => {
  let data = {};
  try {
    data = event.data ? event.data.json() : {};
  } catch {
    data = {};
  }
  event.waitUntil(self.registration.showNotification(data.title || "Decision needed", {
    body: data.body || "A request needs your attention.",
    icon: "/icon.svg",
    badge: "/icon.svg",
    tag: data.tag || "decision",
    renotify: true,
    data: { url: data.data?.url || "/" },
  }));
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const target = new URL(event.notification.data?.url || "/", self.location.origin).toString();
  event.waitUntil(self.clients.matchAll({ type: "window", includeUncontrolled: true }).then(async (clients) => {
    for (const client of clients) {
      if (new URL(client.url).origin === self.location.origin) {
        if ("navigate" in client) await client.navigate(target);
        return client.focus();
      }
    }
    return self.clients.openWindow(target);
  }));
});
