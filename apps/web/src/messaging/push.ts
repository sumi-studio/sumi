/**
 * Web Push の購読。タブ内の通知層（notifications.ts）が「今この画面で
 * どう提示するか」を決めるのに対し、ここは「この端末を、タブが無いときにも
 * 呼べる状態にしておく」ための配線である。
 *
 * 通知条件はここに無い。判定はサーバー側の NotificationSetting が持っており
 * （凍結契約 v1 §「Push 通知レイヤーとの対応」）、この層が扱うのは端末の
 * 同意と購読の登録だけ。だから購読の有無は本人の通知設定を変えない——許可を
 * 取り消しても、mute にしたことにはならない。
 *
 * MessagingBackend には載せていない。push はブラウザという特定の身体に固有の
 * 経路で、mock backend が持てるものではない。同型性は「同じ判定から、それぞれ
 * の身体に合った出口へ」であって、あらゆる backend が同じ配送方式を持つこと
 * ではない（agent 側の対応物は AttentionCandidate）。
 */

import { getActiveMessagingScope, scopedMessagingPath } from "./scope";

const SW_URL = "/sw.js";
const PUSH_KEY_PATH = "/messaging/push-key";
const SUBSCRIPTIONS_PATH = "/messaging/push-subscriptions";

/**
 * 購読の読み書きも、他の Messaging 経路と同じく exact な Workspace /
 * installation / authority epoch の内側にある。scope が未確定のあいだは
 * 何もしない——端末の登録は会話より後でよく、当てずっぽうの宛先で書く
 * ほうが害が大きい。
 */
function scopedPath(path: string): string | null {
  const scope = getActiveMessagingScope();
  if (!scope) return null;
  try {
    return scopedMessagingPath(path, scope);
  } catch {
    return null;
  }
}

/**
 * VAPID の公開鍵は base64url の文字列で届く。PushManager.subscribe は生の
 * バイト列しか受け取らないので、ここで開く。
 */
export function decodeApplicationServerKey(base64url: string): Uint8Array {
  const normalized = base64url.replace(/-/g, "+").replace(/_/g, "/");
  const padded = normalized.padEnd(
    normalized.length + ((4 - (normalized.length % 4)) % 4),
    "=",
  );
  const binary = atob(padded);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

/** この端末が Web Push を持てるか。持てないことは失敗ではない。 */
export function isPushSupported(): boolean {
  return (
    typeof navigator !== "undefined" &&
    "serviceWorker" in navigator &&
    typeof globalThis.PushManager !== "undefined"
  );
}

/**
 * Service Worker を登録する。通知のためだけに置いてあるので、登録に失敗しても
 * 会話は何も壊れない——タブを閉じている間に呼ばれなくなるだけである。
 */
export async function registerServiceWorker(): Promise<ServiceWorkerRegistration | null> {
  if (!isPushSupported()) return null;
  try {
    // module worker として登録する。SW が place の住所の作り方を書き写さず、
    // アプリのルーターと同じ public/place-path.js を import できるようにする
    // ため——規則が二か所にあると、route が変わったときに通知のクリックだけが
    // 静かに壊れる。module SW を持たないブラウザではここが投げ、その端末は
    // 「タブを閉じている間は呼ばれない」だけになる（会話は何も壊れない）。
    return await navigator.serviceWorker.register(SW_URL, {
      scope: "/",
      type: "module",
    });
  } catch {
    return null;
  }
}

async function applicationServerKey(): Promise<Uint8Array | null> {
  const path = scopedPath(PUSH_KEY_PATH);
  if (!path) return null;
  try {
    const response = await fetch(path, {
      credentials: "include",
      cache: "no-store",
      headers: { Accept: "application/json" },
    });
    // 503 は「この deployment に push は無い」という正直な答え。
    if (!response.ok) return null;
    const body = (await response.json()) as { public_key?: unknown };
    if (typeof body.public_key !== "string" || body.public_key === "") {
      return null;
    }
    return decodeApplicationServerKey(body.public_key);
  } catch {
    return null;
  }
}

/**
 * 端末を購読済みにする。既に購読があればそれを送り直す（サーバー側は endpoint
 * で冪等に上書きするので、二重登録にはならない）。
 *
 * 許可されていない状態では何もしない。ここで requestPermission を呼ばないのは
 * 意図的で、許可を求める瞬間は本人が押したときだけであるべきだから（その導線は
 * 通知バナーが持っている）。
 */
export async function enablePushSubscription(): Promise<boolean> {
  if (!isPushSupported()) return false;
  if (
    typeof Notification === "undefined" ||
    Notification.permission !== "granted"
  ) {
    return false;
  }
  const registration = await registerServiceWorker();
  if (!registration) return false;
  // register() 直後の registration は active でないことがある。ready を待って
  // から購読しないと、subscribe が InvalidStateError で落ちる。
  const ready = await navigator.serviceWorker.ready.catch(() => null);
  const manager = (ready ?? registration).pushManager;
  if (!manager) return false;

  let subscription = await manager.getSubscription().catch(() => null);
  if (!subscription) {
    const key = await applicationServerKey();
    if (!key) return false;
    try {
      subscription = await manager.subscribe({
        // userVisibleOnly はブラウザ側の要求。黙って端末を起こす経路にしない。
        userVisibleOnly: true,
        applicationServerKey: key as BufferSource,
      });
    } catch {
      return false;
    }
  }
  return await postSubscription(subscription);
}

/**
 * 端末の購読を解除する。ブラウザ側とサーバー側の両方から外す——片方だけ
 * 残すと、届かない相手に送り続けるか、解除したのに届くかのどちらかになる。
 */
export async function disablePushSubscription(): Promise<void> {
  if (!isPushSupported()) return;
  const registration = await navigator.serviceWorker
    .getRegistration(SW_URL)
    .catch(() => null);
  const subscription = await registration?.pushManager
    ?.getSubscription()
    .catch(() => null);
  if (!subscription) return;
  const endpoint = subscription.endpoint;
  await subscription.unsubscribe().catch(() => false);
  const path = scopedPath(SUBSCRIPTIONS_PATH);
  if (!path) return;
  try {
    await fetch(path, {
      method: "DELETE",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ endpoint }),
    });
  } catch {
    // サーバー側に残っても、その endpoint は次の送信で 410 として掃除される。
  }
}

async function postSubscription(
  subscription: PushSubscription,
): Promise<boolean> {
  const path = scopedPath(SUBSCRIPTIONS_PATH);
  if (!path) return false;
  try {
    const response = await fetch(path, {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(subscription.toJSON()),
    });
    return response.ok;
  } catch {
    return false;
  }
}
