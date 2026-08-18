/**
 * Sumi の Service Worker。役割はひとつだけ——「タブを閉じていても呼ばれる」。
 *
 * ここにキャッシュもオフラインシェルも置かない。会話は今この瞬間の状態が
 * すべてで、古い画面を先に見せることは親切ではなく嘘だからである。だから
 * fetch ハンドラも持たない（持たない SW はブラウザが素通しする）。
 *
 * 「呼ぶかどうか」はサーバーが送信時に評価済みで、push が来た時点でその答えは
 * 出ている。この層が決めるのは提示だけ——見ている画面に重ねない、押されたら
 * その place を開く。タブ内の通知層（src/messaging/notifications.ts）と同じ
 * 判断を、タブが無いときにも成り立たせるための対（つい）である。
 *
 * public/ に素の JS として置いてある。ビルド前後で同じ 1 ファイルが同じ URL
 * （/sw.js、scope は /）で配られるので、dev と本番で挙動が分かれない。
 */

/// <reference lib="webworker" />

// 新しい SW は待たずに引き継ぐ。通知の配線は前の版と競合しない。
self.addEventListener("install", () => {
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(self.clients.claim());
});

/** place の URL 形。src/messaging/place-route.ts と同じ規則。 */
function placePath(placeId, placeKind) {
  if (!placeId) return "/";
  if (placeKind === "channel") return `/c/${placeId}`;
  if (placeKind === "group_dm") return `/group/${placeId}`;
  return `/dm/${placeId}`;
}

/**
 * 今この瞬間、画面を見ている窓があるか。あるならタブ内の通知層が同じ出来事を
 * 既に扱っている——OS の通知を重ねると、同じ呼びかけが二回鳴る。
 */
async function focusedClient() {
  const windows = await self.clients.matchAll({
    type: "window",
    includeUncontrolled: true,
  });
  return windows.find((client) => client.focused) ?? null;
}

self.addEventListener("push", (event) => {
  event.waitUntil(
    (async () => {
      let payload = null;
      try {
        payload = event.data ? event.data.json() : null;
      } catch {
        // 読めない payload は無視する。呼びかけの中身が壊れているだけで、
        // メッセージ自体はサーバーに残っている。
      }
      if (!payload || !payload.title) return;

      // 見ている窓があるなら、同じ出来事は WebSocket 経由で既にその窓へ
      // 届いており、タブ内の通知層が「どう提示するか」を決めている。ここで
      // OS 通知を重ねると同じ呼びかけが二回鳴る。だから黙る。
      if (await focusedClient()) return;

      await self.registration.showNotification(payload.title, {
        body: payload.body || "",
        // 同じ place の通知は積み上げず置き換える。タブ内の tag と同じ規則。
        tag: `sumi:${payload.place_kind || "place"}:${payload.place_id || ""}`,
        renotify: true,
        icon: "/favicon.svg",
        badge: "/favicon.svg",
        data: {
          url: placePath(payload.place_id, payload.place_kind),
          reason: payload.reason || "",
        },
      });
    })(),
  );
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const url = (event.notification.data && event.notification.data.url) || "/";
  event.waitUntil(
    (async () => {
      const windows = await self.clients.matchAll({
        type: "window",
        includeUncontrolled: true,
      });
      // 既に開いている窓があればそれを使う。通知ごとにタブが増えるのは、
      // 「呼ばれて向かう」の体験ではない。
      for (const client of windows) {
        if ("navigate" in client) {
          await client.navigate(url);
          await client.focus();
          return;
        }
      }
      await self.clients.openWindow(url);
    })(),
  );
});

/**
 * push service が購読を差し替えたとき、ブラウザ側の購読だけ取り直す。
 *
 * サーバーへの登録はここでしない。Messaging の書き込みは exact な Workspace /
 * installation / authority epoch の内側にしか無く、その宛先は今開いている
 * セッションのものである。SW が覚えている epoch は古い可能性があり、古い
 * authority で書けないことこそ epoch の意味なので、ここで送っても正しくは
 * ならない。取り直した購読は、次にページが開いたとき push.ts が
 * getSubscription() で拾って、そのときの正しい宛先で登録する。
 */
self.addEventListener("pushsubscriptionchange", (event) => {
  event.waitUntil(
    (async () => {
      const old = event.oldSubscription;
      const applicationServerKey =
        (old && old.options && old.options.applicationServerKey) || null;
      if (!applicationServerKey) return;
      await self.registration.pushManager.subscribe({
        userVisibleOnly: true,
        applicationServerKey,
      });
    })(),
  );
});
