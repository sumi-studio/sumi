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

// place の住所の作り方はここに持たない。アプリのルーターと同じ一つの関数を
// 読む（public/place-path.js）。書き写すと、route が変わったときに通知の
// クリックだけが存在しない URL へ進む。そのために SW は module worker として
// 登録される（src/messaging/push.ts）。
import { isMessagingPath, messagingPlacePath } from "./place-path.js";

// 新しい SW は待たずに引き継ぐ。通知の配線は前の版と競合しない。
self.addEventListener("install", () => {
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(self.clients.claim());
});

/** client.url から pathname だけを取り出す。壊れた URL は「別の場所」。 */
function pathnameOf(url) {
  try {
    return new URL(url).pathname;
  } catch {
    return "";
  }
}

/**
 * 今この瞬間、**その Workspace の Messaging** を見ている窓があるか。
 *
 * 抑止してよいのは「同じ知らせが既に画面に見えている」ときだけである。別の
 * Workspace の画面や Messaging 以外の画面は、その Workspace の scoped な
 * WebSocket event を受け取らないので、そこで黙ると呼びかけはどこにも出ない。
 * 購読は人単位（ブラウザは人の身体）で、通知は Workspace ごとに来る——だから
 * 「窓があるか」ではなく「その Workspace を映しているか」で決める。
 */
async function focusedMessagingClient(workspaceId) {
  if (!workspaceId) return null;
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
      let payload = null;
      try {
        payload = event.data ? event.data.json() : null;
      } catch {
        // 読めない payload は無視する。呼びかけの中身が壊れているだけで、
        // メッセージ自体はサーバーに残っている。
      }
      if (!payload || !payload.title) return;

      // その Workspace の Messaging を見ている窓があるなら、同じ出来事は
      // WebSocket 経由で既にその窓へ届いており、タブ内の通知層が「どう提示
      // するか」を決めている。ここで OS 通知を重ねると同じ呼びかけが二回鳴る。
      // 逆に、別 Workspace や Messaging 以外を見ている窓は同じ出来事を受け
      // 取らないので、そこで黙るとどこにも出なくなる。
      if (await focusedMessagingClient(payload.workspace_id)) return;

      await self.registration.showNotification(payload.title, {
        body: payload.body || "",
        // 同じ place の通知は積み上げず置き換える。タブ内の tag と同じ規則。
        tag: `sumi:${payload.place_kind || "place"}:${payload.place_id || ""}`,
        renotify: true,
        icon: "/favicon.svg",
        badge: "/favicon.svg",
        data: {
          url: messagingPlacePath(
            payload.workspace_id,
            payload.place_kind,
            payload.place_id,
          ),
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
