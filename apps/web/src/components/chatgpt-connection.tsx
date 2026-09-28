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

/**
 * ChatGPT subscription sign-in with a device code: the person opens
 * ChatGPT's own page, enters the shown code there, and this panel reads
 * the sign-in until it completes. No token ever passes through the
 * browser. With `connectionId`, a completed sign-in reconnects that
 * connection instead of adding one.
 *
 * Only the explicit "キャンセル" ends a pending sign-in. Closing the sheet,
 * switching to ChatGPT's page or app, or a reload leaves it pending on the
 * server; opening this panel again in the same browser session continues
 * the same login (same code) until it expires — the person may already
 * have authorized it.
 */
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
  const [login, setLogin] = useState<ChatGPTLogin | null>(null);
  const [error, setError] = useState("");
  const [attempt, setAttempt] = useState(0);
  const loginId = useRef<string | null>(null);
  const done = useRef(onDone);
  done.current = onDone;
  // biome-ignore lint/correctness/useExhaustiveDependencies: attempt is the explicit "もう一度はじめる" restart trigger.
  useEffect(() => {
    const controller = new AbortController();
    const { signal } = controller;
    let timer: ReturnType<typeof setTimeout> | undefined;
    setLogin(null);
    setError("");
    const fail = (e: unknown) => {
      if (signal.aborted) return;
      setError(
        e instanceof APIConnectionError
          ? e.message
          : "ChatGPTのログイン状態を確認できませんでした。しばらくしてからもう一度お試しください。",
      );
    };
    const show = (view: ChatGPTLogin) => {
      if (signal.aborted) return;
      setLogin(view);
      if (view.status === "completed") {
        loginId.current = null;
        done.current(view.connection);
        return;
      }
      if (view.status !== "pending") {
        loginId.current = null;
        return;
      }
      timer = setTimeout(poll, Math.max(view.intervalMs, 1000));
    };
    const poll = () => {
      const id = loginId.current;
      if (!id || signal.aborted) return;
      client.chatGPTLogin(id, signal).then(show, (e: unknown) => {
        if (signal.aborted) return;
        // A lost read is retried on the next tick; only a login the server
        // no longer knows or offers ends the panel.
        if (
          e instanceof APIConnectionError &&
          (e.status === 404 || e.status === 409)
        ) {
          loginId.current = null;
          fail(e);
          return;
        }
        timer = setTimeout(poll, 5000);
      });
    };
    client.beginChatGPTLogin(connectionId, signal).then((view) => {
      if (signal.aborted) return;
      loginId.current = view.loginId;
      show(view);
    }, fail);
    return () => {
      controller.abort();
      if (timer) clearTimeout(timer);
      loginId.current = null;
    };
  }, [client, connectionId, attempt]);

  const cancel = () => {
    const id = loginId.current;
    loginId.current = null;
    if (id)
      void client
        .cancelChatGPTLogin(id, AbortSignal.timeout(15000))
        .catch(() => undefined);
    onClose();
  };

  const pending = login?.status === "pending";
  const ended =
    !!error ||
    login?.status === "failed" ||
    login?.status === "expired" ||
    login?.status === "cancelled";
  return (
    <section
      className="mt-5 space-y-3 rounded-lg border border-border p-4 text-sm"
      aria-label="ChatGPTにログイン"
    >
      <h3 className="font-medium">
        {connectionId ? "ChatGPTに再接続" : "ChatGPTで接続"}
      </h3>
      {!login && !error && <p>ログインを準備しています…</p>}
      {pending && login.verificationUrl && login.userCode && (
        <p className="text-muted-foreground text-xs leading-relaxed">
          はじめに、ChatGPTのセキュリティ設定でデバイスコードによるログインを有効にしてください（OpenAIの案内による前提です）。ワークスペースのアカウントでは、管理者がワークスペースの権限で許可している必要があります。
        </p>
      )}
      {pending && login.verificationUrl && login.userCode && (
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
              まで有効です。このコードを他の人に教えないでください。
            </p>
          </li>
          <li>
            入力が終わると、この画面は自動で完了します。途中で閉じても、有効期限までは同じブラウザでもう一度開くと続けられます。
          </li>
        </ol>
      )}
      {login?.status === "expired" && (
        <p role="alert">
          コードの有効期限が切れました。もう一度はじめてください。
        </p>
      )}
      {login?.status === "failed" && (
        <p role="alert">
          {LOGIN_ERRORS[login.error ?? ""] ?? LOGIN_ERRORS.login_failed}
        </p>
      )}
      {error && <p role="alert">{error}</p>}
      <p className="text-muted-foreground text-xs leading-relaxed">
        あなたのChatGPTプランの利用枠で応答します。会話はOpenAIへ送られます。ログイン情報はこのSumiサーバーに暗号化して保存し、表示しません。
      </p>
      <div className="flex gap-3">
        {ended && (
          <Button size="sm" onClick={() => setAttempt((n) => n + 1)}>
            もう一度はじめる
          </Button>
        )}
        {pending ? (
          <Button size="sm" variant="ghost" onClick={cancel}>
            キャンセル
          </Button>
        ) : (
          <Button size="sm" variant="ghost" onClick={onClose}>
            閉じる
          </Button>
        )}
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
