import { Button } from "@sumi/ui/components/button";
import { useEffect, useRef, useState } from "react";
import {
  type APIConnection,
  APIConnectionError,
  type APIConnectionsClient,
  type ConnectionInput,
  type ConnectionSelection,
  type ConnectionsState,
  createAPIConnectionsClient,
} from "../lib/api-connections";

const defaultClient = createAPIConnectionsClient();
const blank: ConnectionInput = {
  name: "",
  preset: "openai-chat",
  baseUrl: "",
  model: "",
};
// Only these presets put an output bound on the wire (Anthropic
// max_tokens / Responses max_output_tokens); the chat-completions
// adapter sends none, so offering the field there would save a setting
// that does nothing.
const OUTPUT_BOUND_PRESETS = new Set(["anthropic", "openai-responses"]);
const OUTPUT_BOUND_MAX = 1_000_000;
const inputClass =
  "mt-1 w-full rounded-lg border border-border bg-background px-3 py-2 text-sm";
export function APIConnectionSettings({
  chatgptConnected,
  client = defaultClient,
}: {
  chatgptConnected: boolean;
  client?: APIConnectionsClient;
}) {
  const [state, setState] = useState<ConnectionsState | null>(null);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [busy, setBusy] = useState(false);
  const [editing, setEditing] = useState<string | null | undefined>(undefined);
  const [form, setForm] = useState<ConnectionInput>(blank);
  const [key, setKey] = useState("");
  const [headersText, setHeadersText] = useState("");
  const [maxOutText, setMaxOutText] = useState("");
  const [clearHeaders, setClearHeaders] = useState(false);
  const [removing, setRemoving] = useState<string | null>(null);
  const lifetime = useRef<AbortController | null>(null);
  useEffect(() => {
    const controller = new AbortController();
    lifetime.current = controller;
    void client
      .list(controller.signal)
      .then((value) => {
        if (!controller.signal.aborted) setState(value);
      })
      .catch(() => {
        if (!controller.signal.aborted)
          setError("API接続を読み込めませんでした。");
      });
    return () => controller.abort();
  }, [client]);
  async function run(action: (signal: AbortSignal) => Promise<void>) {
    const controller = lifetime.current;
    if (!controller || controller.signal.aborted || busy) return;
    setBusy(true);
    setError("");
    setNotice("");
    try {
      await action(controller.signal);
      const next = await client.list(controller.signal);
      if (!controller.signal.aborted) setState(next);
    } catch (e) {
      // Only the client's typed failure is written for display; any other
      // error message may be a transport or provider diagnostic.
      if (!controller.signal.aborted)
        setError(
          e instanceof APIConnectionError
            ? e.message
            : "接続を更新できませんでした。入力と接続状態を確認して、もう一度お試しください。",
        );
    } finally {
      if (!controller.signal.aborted) setBusy(false);
    }
  }
  function edit(connection?: APIConnection) {
    setEditing(connection?.id ?? null);
    setForm(
      connection
        ? {
            name: connection.name,
            preset: connection.preset,
            baseUrl: connection.baseUrl,
            model: connection.model,
          }
        : blank,
    );
    setKey("");
    setHeadersText("");
    setMaxOutText(
      connection?.maxOutputTokens ? String(connection.maxOutputTokens) : "",
    );
    setClearHeaders(false);
    setRemoving(null);
    setError("");
  }
  function choose(selection: ConnectionSelection) {
    void run(async (signal) => {
      await client.select(selection, signal);
      if (!signal.aborted)
        setNotice(
          "使う接続を保存しました。以前の接続は次のリクエストから使われなくなり、新しい接続は作業が止まってから起動します。",
        );
    });
  }
  return (
    <section
      className="mt-8 border-t border-border pt-6"
      aria-label="使う接続とAPI"
    >
      <h2 className="font-medium text-lg">使う接続</h2>
      <p className="mt-1 text-muted-foreground text-sm">
        あなたのSumiが使う接続を選びます。
      </p>
      {state?.selection === null && (
        <p className="mt-3 text-sm">現在はサーバーの既定設定を使っています。</p>
      )}
      {state && (
        <div className="mt-4 space-y-2">
          {chatgptConnected && (
            <ConnectionRow
              name="ChatGPT"
              detail="接続済みのあなたのアカウント"
              selected={state.selection?.kind === "chatgpt"}
              busy={busy || !state.available}
              onSelect={() => choose({ kind: "chatgpt" })}
            />
          )}
          {state.connections.map((c) => (
            <div key={c.id} className="rounded-lg border border-border p-3">
              <ConnectionRow
                name={c.name}
                detail={`${c.model} · ${c.baseUrl}`}
                selected={
                  state?.selection?.kind === "api" &&
                  state.selection.connectionId === c.id
                }
                busy={busy || !state.available}
                onSelect={() => choose({ kind: "api", connectionId: c.id })}
              />
              <div className="mt-2 flex gap-3 text-sm">
                <button type="button" disabled={busy} onClick={() => edit(c)}>
                  編集
                </button>
                <button
                  type="button"
                  disabled={busy}
                  onClick={() => setRemoving(c.id)}
                >
                  削除
                </button>
              </div>
              {removing === c.id && (
                <div className="mt-3 text-sm">
                  <p>
                    この接続を削除します。使用中なら、次のリクエストから接続できなくなります。
                  </p>
                  <div className="mt-2 flex gap-3">
                    <Button
                      size="sm"
                      disabled={busy}
                      onClick={() =>
                        void run(async (signal) => {
                          await client.remove(c.id, signal);
                          if (!signal.aborted) {
                            setRemoving(null);
                            if (editing === c.id) {
                              setEditing(undefined);
                              setKey("");
                              setHeadersText("");
                              setMaxOutText("");
                              setClearHeaders(false);
                            }
                          }
                        })
                      }
                    >
                      接続を削除
                    </Button>
                    <button type="button" onClick={() => setRemoving(null)}>
                      戻る
                    </button>
                  </div>
                </div>
              )}
            </div>
          ))}
          <ConnectionRow
            name="接続しない"
            detail="別のアカウントへ自動で切り替えません"
            selected={state.selection?.kind === "none"}
            busy={busy || !state.available}
            onSelect={() => choose({ kind: "none" })}
          />
        </div>
      )}
      {state?.available && editing === undefined && (
        <Button
          className="mt-4"
          variant="outline"
          disabled={busy}
          onClick={() => edit()}
        >
          APIを追加
        </Button>
      )}
      {state && !state.available && (
        <p className="mt-4 text-sm text-muted-foreground">
          {state.unavailableReason ||
            "このサーバーではAPIの登録を利用できません。"}
        </p>
      )}
      {editing !== undefined && (
        <form
          className="mt-5 space-y-4"
          onSubmit={(e) => {
            e.preventDefault();
            void run(async (signal) => {
              const previous = state?.connections.find(
                (connection) => connection.id === editing,
              );
              // Entering a new key replaces the sealed credential;
              // preset/URL/model changes keep it — the same key is
              // reused under the new settings.
              const replacesCredential = !!key;
              const changedSettings =
                previous?.preset !== form.preset ||
                previous?.baseUrl !== form.baseUrl ||
                previous?.model !== form.model;
              // "Name: value" per line. Stored headers are write-only:
              // an empty field means "keep what is stored".
              const extraHeaders: Record<string, string> = {};
              let headersValid = true;
              for (const line of headersText.split("\n")) {
                const trimmed = line.trim();
                if (!trimmed) continue;
                const colon = trimmed.indexOf(":");
                if (colon < 1) {
                  headersValid = false;
                  continue;
                }
                const name = trimmed.slice(0, colon).trim();
                const value = trimmed.slice(colon + 1).trim();
                if (!name || !value) headersValid = false;
                else extraHeaders[name] = value;
              }
              if (!headersValid) {
                setError(
                  "ヘッダーは「名前: 値」の形式で1行ずつ入力してください。",
                );
                return;
              }
              // Headers are sealed with the credential: setting,
              // changing, or clearing them without resubmitting the key
              // is rejected by the server.
              if ((headersText.trim() || clearHeaders) && !key) {
                setError(
                  "ヘッダーを設定・変更・削除するにはAPIキーも入力してください。",
                );
                return;
              }
              // The bound is a whole number on the wire — validate the
              // full text (a number input can hold "1e3"; truncating it
              // to 1 would silently send a different bound).
              const boundSupported = OUTPUT_BOUND_PRESETS.has(form.preset);
              const rawBound = maxOutText.trim();
              let maxOutputTokens: number | undefined;
              if (boundSupported && rawBound) {
                if (
                  !/^\d+$/.test(rawBound) ||
                  Number(rawBound) < 1 ||
                  Number(rawBound) > OUTPUT_BOUND_MAX
                ) {
                  setError(
                    `最大出力トークンは1〜${OUTPUT_BOUND_MAX.toLocaleString()}の整数で入力してください。`,
                  );
                  return;
                }
                maxOutputTokens = Number(rawBound);
              }
              await client.save(
                {
                  ...form,
                  // Unsupported presets never carry the bound — an
                  // ineffective saved value would be invisible to the
                  // user and is rejected by the server as well.
                  maxOutputTokens: boundSupported ? maxOutputTokens : undefined,
                  ...(key ? { apiKey: key } : {}),
                  ...(clearHeaders
                    ? { extraHeaders: {} }
                    : headersText.trim()
                      ? { extraHeaders }
                      : {}),
                },
                editing ?? undefined,
                signal,
              );
              if (!signal.aborted) {
                setKey("");
                setHeadersText("");
                setMaxOutText("");
                setClearHeaders(false);
                setEditing(undefined);
                setNotice(
                  state?.selection?.kind === "api" &&
                    state.selection.connectionId === editing
                    ? replacesCredential
                      ? "接続を保存しました。以前の認証情報は次のリクエストから使われなくなり、作業が止まってから新しい設定で起動します。"
                      : changedSettings
                        ? "接続を保存しました。認証情報はそのまま引き継がれ、作業が一区切りついてから新しい設定で起動します。"
                        : "接続を保存しました。"
                    : "接続を保存しました。「使う」を押すと切り替わります。",
                );
              }
            });
          }}
        >
          <h3 className="font-medium">
            {editing ? "接続を編集" : "APIを追加"}
          </h3>
          <label className="block text-sm">
            名前
            <input
              className={inputClass}
              value={form.name}
              required
              maxLength={120}
              disabled={busy}
              onChange={(e) => setForm({ ...form, name: e.target.value })}
              placeholder="個人用のAPI"
            />
          </label>
          <label className="block text-sm">
            APIの形式
            <select
              className={inputClass}
              value={form.preset}
              disabled={busy}
              onChange={(e) => setForm({ ...form, preset: e.target.value })}
            >
              <option value="openai-chat">OpenAI互換 · Chat Completions</option>
              <option value="openai-responses">OpenAI Responses</option>
              <option value="anthropic">Anthropic Messages</option>
            </select>
          </label>
          <label className="block text-sm">
            接続先URL
            <input
              className={inputClass}
              type="url"
              value={form.baseUrl}
              required
              disabled={busy}
              onChange={(e) => {
                setForm({ ...form, baseUrl: e.target.value });
                setKey("");
              }}
              placeholder="https://api.example.com/v1"
            />
          </label>
          <label className="block text-sm">
            モデルID
            <input
              className={inputClass}
              value={form.model}
              required
              disabled={busy}
              onChange={(e) => setForm({ ...form, model: e.target.value })}
              placeholder="プロバイダーのモデルID"
            />
          </label>
          {OUTPUT_BOUND_PRESETS.has(form.preset) && (
            <label className="block text-sm">
              最大出力トークン（任意）
              <input
                className={inputClass}
                type="text"
                inputMode="numeric"
                value={maxOutText}
                disabled={busy}
                onChange={(e) => setMaxOutText(e.target.value)}
                placeholder={
                  form.preset === "anthropic" ? "16384" : "モデル既定"
                }
              />
              <span className="mt-1 block text-muted-foreground text-xs">
                {form.preset === "anthropic"
                  ? "空欄なら16,384を送ります。モデルの出力上限がそれより小さい場合はその値に設定してください。"
                  : "空欄ならこの項目を送らず、モデル自身の上限が使われます。出力を制限したい場合に設定してください。"}
                上限を超える値はプロバイダーが拒否します。
              </span>
            </label>
          )}
          <label className="block text-sm">
            APIキー
            <input
              className={inputClass}
              type="password"
              autoComplete="off"
              value={key}
              required={
                !editing ||
                !!headersText.trim() ||
                clearHeaders ||
                state?.connections.find((c) => c.id === editing)?.baseUrl !==
                  form.baseUrl
              }
              disabled={busy}
              onChange={(e) => setKey(e.target.value)}
              placeholder={editing ? "変更しない場合は空欄" : "APIキーを入力"}
            />
          </label>
          <label className="block text-sm">
            追加リクエストヘッダー（任意）
            <textarea
              className={inputClass}
              rows={2}
              value={headersText}
              disabled={busy || clearHeaders}
              onChange={(e) => setHeadersText(e.target.value)}
              placeholder={"X-Header-Name: value"}
              spellCheck={false}
            />
          </label>
          {editing && (
            <label className="flex items-center gap-2 text-sm">
              <input
                type="checkbox"
                checked={clearHeaders}
                disabled={busy}
                onChange={(e) => setClearHeaders(e.target.checked)}
              />
              保存済みの追加ヘッダーをすべて削除する
            </label>
          )}
          <p className="text-muted-foreground text-xs leading-relaxed">
            キーはこのSumiサーバーに暗号化して保存します。選んだ接続先へ会話が送られ、APIの利用料はそのアカウントに発生します。保存済みのキーは表示しません。追加ヘッダーもキーと一緒に暗号化して保存され、この接続先にだけ送られます。変更するにはAPIキーと一緒に再入力してください。
          </p>
          <div className="flex gap-3">
            <Button type="submit" disabled={busy}>
              {busy ? "保存中…" : "保存する"}
            </Button>
            <Button
              type="button"
              variant="ghost"
              disabled={busy}
              onClick={() => {
                setEditing(undefined);
                setKey("");
                setHeadersText("");
                setMaxOutText("");
                setClearHeaders(false);
              }}
            >
              戻る
            </Button>
          </div>
        </form>
      )}
      {error && (
        <div className="mt-3 text-sm">
          <p role="alert">{error}</p>
          <button
            className="mt-2 underline"
            type="button"
            disabled={busy}
            onClick={() => void run(async () => {})}
          >
            状態を確認
          </button>
        </div>
      )}
      {notice && (
        <p role="status" className="mt-3 text-sm">
          {notice}
        </p>
      )}
    </section>
  );
}
function ConnectionRow({
  name,
  detail,
  selected,
  busy,
  onSelect,
}: {
  name: string;
  detail: string;
  selected: boolean;
  busy: boolean;
  onSelect(): void;
}) {
  return (
    <div className="flex items-start justify-between gap-3 py-2">
      <div className="min-w-0">
        <p className="text-sm font-medium">{name}</p>
        <p className="mt-1 break-all text-xs text-muted-foreground">{detail}</p>
      </div>
      <Button
        size="sm"
        variant={selected ? "secondary" : "outline"}
        disabled={selected || busy}
        aria-label={`${name}を使う`}
        onClick={onSelect}
      >
        {selected ? "選択中" : "使う"}
      </Button>
    </div>
  );
}
