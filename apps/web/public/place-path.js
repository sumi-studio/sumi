/**
 * place の住所の作り方。**この規則はここにしか無い。**
 *
 * アプリのルーター（src/routes/w.$workspaceId.messaging.*）と Service Worker
 * （public/sw.js）の両方がこの一つの関数を通る。二か所に手で書くと、片方だけ
 * route が変わったときに通知のクリックだけが存在しない URL へ進む——実際に
 * 一度そうなった。public/ に素の ESM として置いてあるのは、SW がビルドを
 * 経ずに同じバイト列を読めるようにするためである（app 側は import する）。
 *
 * place は必ず Workspace の内側にある。同じ place ID を別 Workspace の
 * authority で開かないための形であり、通知から戻るときも同じ形を通る。
 */

/** その Workspace の Messaging 画面の根。 */
export function messagingBasePath(workspaceId) {
  if (!workspaceId) return "/";
  return `/w/${encodeURIComponent(workspaceId)}/messaging`;
}

/**
 * 一つの place の住所。place_kind はサーバーの Place.Kind と同じ語彙
 * （channel / dm / group_dm）。分からない形は根に落とす——当てずっぽうの
 * URL へ飛ばすより、Messaging を開いて本人に選んでもらう方がよい。
 */
export function messagingPlacePath(workspaceId, placeKind, placeId) {
  const base = messagingBasePath(workspaceId);
  if (base === "/" || !placeId) return base;
  const id = encodeURIComponent(placeId);
  if (placeKind === "channel") return `${base}/c/${id}`;
  if (placeKind === "dm") return `${base}/dm/${id}`;
  if (placeKind === "group_dm") return `${base}/group/${id}`;
  return base;
}

/**
 * その pathname は、この Workspace の Messaging を映しているか。
 *
 * 「同じ知らせが既に画面に見えているか」を判断するための述語である。別の
 * Workspace の画面や Messaging 以外の画面は、その Workspace の scoped な
 * WebSocket event を受け取らないので、見えているとは言えない。
 */
export function isMessagingPath(pathname, workspaceId) {
  const base = messagingBasePath(workspaceId);
  if (base === "/" || !pathname) return false;
  return pathname === base || pathname.startsWith(`${base}/`);
}
