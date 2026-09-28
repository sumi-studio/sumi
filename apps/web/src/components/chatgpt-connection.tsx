import { Button } from "@sumi/ui/components/button";
import { useEffect, useRef, useState } from "react";
import {
  type APIConnection,
  APIConnectionError,
  type APIConnectionsClient,
  type ChatGPTLogin,
} from "../lib/api-connections";

const inputClass =
  "mt-1 w-full rounded-lg border border-border bg-background px-3 py-2 text-sm";
// Models the Codex backend served to subscriptions at the protocol
// reference; any other id the server's pattern accepts can be typed.
const MODEL_SUGGESTIONS = ["gpt-6-astra", "gpt-6-sol", "gpt-6-luna", "gpt-5.5"];
const EFFORT_LABELS: Record<string, string> = {
  low: "低",
  medium: "中（既定）",
  high: "高",
  xhigh: "とても高い",
  max: "最大",
};
// Every effort the server stores. (The Codex catalog also lists "ultra"
// for some models; this server does not accept it.)
const ALL_EFFORTS = Object.keys(EFFORT_LABELS);
// Efforts each suggested model offers in the Codex model catalog at the
// protocol reference, within what the server accepts. A model typed by
// hand offers every stored value; the backend decides.
const MODEL_EFFORTS: Record<string, string[]> = {
  "gpt-6-astra": ALL_EFFORTS,
  "gpt-6-sol": ALL_EFFORTS,
  "gpt-6-luna": ALL_EFFORTS,
  "gpt-5.5": ["low", "medium", "high", "xhigh"],
};

/**
 * The effort choices for `model`. The connection's current value always
 * stays selectable, so opening and saving the form never changes it.
 */
function effortOptions(model: string, current: string): string[] {
  const list = MODEL_EFFORTS[model.trim()] ?? ALL_EFFORTS;
  return list.includes(current) ? list : [...list, current];
}
const LOGIN_ERRORS: Record<string, string> = {
  login_failed:
    "ChatGPTのログインを完了できませんでした。もう一度はじめてください。",
  device_login_unavailable:
    "ChatGPT側が現在、デバイスコードによるログインの開始を受け付けていません。しばらくしてからもう一度お試しください。",
  save_failed:
    "ログインは完了しましたが、接続を保存できませんでした。もう一度はじめてください。",
};

type LoginIntent = { id: string; action: "login" | "cancel" };
const intentKey = (target?: string) =>
  `sumi.chatgpt.login.v1:${target ?? "new"}`;
function readIntent(target?: string): LoginIntent | null {
  try {
    const v = JSON.parse(sessionStorage.getItem(intentKey(target)) ?? "null");
    return v &&
      typeof v.id === "string" &&
      /^[a-f0-9-]{36}$/i.test(v.id) &&
      (v.action === "login" || v.action === "cancel")
      ? v
      : null;
  } catch {
    return null;
  }
}
function saveIntent(target: string | undefined, intent: LoginIntent | null) {
  try {
    if (intent)
      sessionStorage.setItem(intentKey(target), JSON.stringify(intent));
    else sessionStorage.removeItem(intentKey(target));
  } catch {
    /* The mounted panel still works when browser storage is disabled. */
  }
}

/** The browser retains an attempt ID, never a code or token. Its server row
 * is bound to the initiating person, session and reconnect target. */
export function ChatGPTLoginPanel({
  client,
  connectionId,
  onDone,
  onClose,
}: {
  client: APIConnectionsClient;
  connectionId?: string;
  onDone(connection: APIConnection | undefined): void;
  onClose(): void;
}) {
  const [intent, setIntent] = useState<LoginIntent | null>(() =>
    readIntent(connectionId),
  );
  const [login, setLogin] = useState<ChatGPTLogin | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [attempt, setAttempt] = useState(0);
  const done = useRef(onDone);
  done.current = onDone;
  const close = useRef(onClose);
  close.current = onClose;

  // Only a saved user-initiated attempt is recovered on mount. Opening a
  // fresh panel never issues a code before the explanation below is read.
  // biome-ignore lint/correctness/useExhaustiveDependencies: attempt explicitly retries the same durable login/cancel.
  useEffect(() => {
    if (!intent) return;
    const controller = new AbortController();
    const signal = AbortSignal.any([
      controller.signal,
      AbortSignal.timeout(60_000),
    ]);
    let timer: ReturnType<typeof setTimeout> | undefined;
    setBusy(true);
    setError("");
    const forget = () => {
      saveIntent(connectionId, null);
      setIntent(null);
    };
    const fail = (e: unknown) => {
      if (controller.signal.aborted) return;
      setBusy(false);
      if (e instanceof APIConnectionError && e.status === 404) forget();
      setError(
        intent.action === "cancel"
          ? "キャンセルの結果を確認できませんでした。もう一度確認してください。"
          : e instanceof APIConnectionError
            ? e.message
            : "ログイン状態を確認できませんでした。「再試行」で同じログインを確認できます。",
      );
    };
    const show = (view: ChatGPTLogin) => {
      if (controller.signal.aborted) return;
      setBusy(false);
      setError("");
      setLogin(view);
      if (view.status === "completed") {
        forget();
        done.current(view.connection);
      } else if (view.status !== "pending") {
        forget();
        if (intent.action === "cancel" && view.status === "cancelled")
          close.current();
      } else if (intent.action === "login") {
        timer = setTimeout(poll, Math.max(view.intervalMs, 1000));
      }
    };
    const poll = () => {
      // Each poll gets its own deadline; waiting for the person to approve
      // does not spend the next request's budget.
      const pollSignal = AbortSignal.any([
        controller.signal,
        AbortSignal.timeout(60_000),
      ]);
      client.chatGPTLogin(intent.id, pollSignal).then(show, (e: unknown) => {
        if (controller.signal.aborted) return;
        if (e instanceof APIConnectionError && [404, 409].includes(e.status)) {
          fail(e);
          return;
        }
        setError(
          "接続を確認し直しています。コードの入力が済んでいれば、そのままお待ちください。",
        );
        timer = setTimeout(poll, 5000);
      });
    };
    const work =
      intent.action === "cancel"
        ? client.cancelChatGPTLogin(intent.id, signal)
        : client.beginChatGPTLogin(intent.id, connectionId, signal);
    work.then(show, fail);
    return () => {
      controller.abort();
      if (timer) clearTimeout(timer);
    };
  }, [client, connectionId, intent, attempt]);

  const start = () => {
    const next: LoginIntent = { id: crypto.randomUUID(), action: "login" };
    saveIntent(connectionId, next); // before the POST, including a lost begin response
    setLogin(null);
    setError("");
    setIntent(next);
  };
  const cancel = () => {
    if (!intent) return;
    const next: LoginIntent = { ...intent, action: "cancel" };
    saveIntent(connectionId, next);
    setError("");
    setIntent(next);
  };
  const pending = login?.status === "pending";
  const cancelling = intent?.action === "cancel";
  return (
    <section
      className="mt-5 space-y-3 rounded-lg border border-border p-4 text-sm"
      aria-label="ChatGPTにログイン"
    >
      <h3 className="font-medium">
        {connectionId ? "ChatGPTに再接続" : "ChatGPTで接続"}
      </h3>
      <p className="leading-relaxed">
        Codexのデバイスコード認証で接続します。承認すると、このSumiサーバーが接続に必要な認証情報を受け取り、暗号化して保存します。Sumiの秘書はあなたのCodex利用枠で応答し、会話をOpenAIへ送ります。
      </p>
      <p className="text-muted-foreground text-xs leading-relaxed">
        承認ページには「Codex
        CLI」と表示されることがあります。このSumiで自分が発行したコードだけを承認してください。他の人から渡されたコードは使わないでください。
      </p>
      <p className="text-muted-foreground text-xs leading-relaxed">
        はじめに、ChatGPTのセキュリティ設定でデバイスコードによるログインを有効にしてください。ワークスペースのアカウントでは、管理者による許可も必要です。
      </p>
      {busy && (
        <p role="status">
          {cancelling
            ? "キャンセルしています。認証が完了している場合は、その結果を確認します…"
            : "ログイン状態を確認しています…"}
        </p>
      )}
      {pending && !cancelling && login.verificationUrl && login.userCode && (
        <ol className="list-decimal space-y-3 pl-5">
          <li>
            <a
              className="underline"
              href={login.verificationUrl}
              target="_blank"
              rel="noopener noreferrer"
            >
              ChatGPTのログインページを開く
            </a>
            （{login.verificationUrl}）
          </li>
          <li>
            ChatGPTにログインして、次のコードを入力してください。
            <output
              className="mt-2 block font-mono text-2xl tracking-widest"
              aria-label="ログインコード"
            >
              {login.userCode}
            </output>
            <p className="mt-1 text-muted-foreground text-xs">
              {new Date(login.expiresAt).toLocaleTimeString("ja-JP", {
                hour: "2-digit",
                minute: "2-digit",
              })}
              まで有効です。
            </p>
          </li>
          <li>
            承認を待っています。入力が終わると自動で完了します。途中で閉じても、同じブラウザのタブで開き直すと結果を確認できます。
          </li>
        </ol>
      )}
      {login?.status === "expired" && (
        <p role="alert">
          コードの有効期限が切れました。新しいコードで、もう一度はじめてください。
        </p>
      )}
      {login?.status === "failed" && (
        <p role="alert">
          {LOGIN_ERRORS[login.error ?? ""] ?? LOGIN_ERRORS.login_failed}
        </p>
      )}
      {login?.status === "cancelled" && (
        <p role="status">ログインをキャンセルしました。</p>
      )}
      {error && <p role="alert">{error}</p>}
      <div className="flex flex-wrap gap-3">
        {!intent && (
          <Button size="sm" onClick={start}>
            {login ? "もう一度はじめる" : "ログインコードを発行"}
          </Button>
        )}
        {intent && error && !busy && (
          <Button size="sm" onClick={() => setAttempt((n) => n + 1)}>
            {cancelling ? "キャンセルの結果を確認" : "再試行"}
          </Button>
        )}
        {pending && !cancelling && (
          <Button size="sm" variant="ghost" onClick={cancel}>
            キャンセル
          </Button>
        )}
        <Button size="sm" variant="ghost" onClick={onClose}>
          閉じる
        </Button>
      </div>
    </section>
  );
}

/** Model and reasoning effort of a ChatGPT subscription connection. */
export function ChatGPTSettingsForm({
  connection,
  busy,
  onSave,
  onClose,
}: {
  connection: APIConnection;
  busy: boolean;
  onSave(input: { name: string; model: string; reasoningEffort: string }): void;
  onClose(): void;
}) {
  const [name, setName] = useState(connection.name);
  const [model, setModel] = useState(connection.model);
  const [effort, setEffort] = useState(connection.reasoningEffort || "medium");
  return (
    <form
      className="mt-5 space-y-4"
      onSubmit={(e) => {
        e.preventDefault();
        onSave({ name, model: model.trim(), reasoningEffort: effort });
      }}
    >
      <h3 className="font-medium">ChatGPT接続を編集</h3>
      <label className="block text-sm">
        名前
        <input
          className={inputClass}
          value={name}
          required
          maxLength={120}
          disabled={busy}
          onChange={(e) => setName(e.target.value)}
        />
      </label>
      <label className="block text-sm">
        モデル
        <input
          className={inputClass}
          value={model}
          required
          list="chatgpt-models"
          pattern="[a-z0-9][a-z0-9._\-]{0,63}"
          disabled={busy}
          onChange={(e) => setModel(e.target.value)}
        />
        <datalist id="chatgpt-models">
          {MODEL_SUGGESTIONS.map((m) => (
            <option key={m} value={m} />
          ))}
        </datalist>
      </label>
      <label className="block text-sm">
        推論の強さ
        <select
          className={inputClass}
          value={effort}
          disabled={busy}
          onChange={(e) => setEffort(e.target.value)}
        >
          {effortOptions(model, effort).map((v) => (
            <option key={v} value={v}>
              {EFFORT_LABELS[v] ?? `${v}（現在の設定）`}
            </option>
          ))}
        </select>
      </label>
      <p className="text-muted-foreground text-xs leading-relaxed">
        ログイン情報はそのまま使います。ChatGPTアカウントを変えるときは「ChatGPTに再接続」を使ってください。
      </p>
      <div className="flex gap-3">
        <Button type="submit" disabled={busy}>
          {busy ? "保存中…" : "保存する"}
        </Button>
        <Button type="button" variant="ghost" disabled={busy} onClick={onClose}>
          戻る
        </Button>
      </div>
    </form>
  );
}
