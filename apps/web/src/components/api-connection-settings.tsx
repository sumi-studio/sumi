import { Button } from "@sumi/ui/components/button";
import { useEffect, useRef, useState } from "react";
import {
  type APIConnection,
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
    } catch {
      if (!controller.signal.aborted)
        setError(
          "接続を更新できませんでした。入力と接続状態を確認して、もう一度お試しください。",
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
              const revokesCredential =
                !!key ||
                previous?.baseUrl !== form.baseUrl ||
                previous?.preset !== form.preset;
              await client.save(
                { ...form, ...(key ? { apiKey: key } : {}) },
                editing ?? undefined,
                signal,
              );
              if (!signal.aborted) {
                setKey("");
                setEditing(undefined);
                setNotice(
                  state?.selection?.kind === "api" &&
                    state.selection.connectionId === editing
                    ? revokesCredential
                      ? "接続を保存しました。以前の認証情報は次のリクエストから使われなくなり、作業が止まってから新しい設定で起動します。"
                      : previous?.model !== form.model
                        ? "モデルを保存しました。作業が一区切りついてから切り替わります。"
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
          <label className="block text-sm">
            APIキー
            <input
              className={inputClass}
              type="password"
              autoComplete="off"
              value={key}
              required={
                !editing ||
                state?.connections.find((c) => c.id === editing)?.baseUrl !==
                  form.baseUrl
              }
              disabled={busy}
              onChange={(e) => setKey(e.target.value)}
              placeholder={editing ? "変更しない場合は空欄" : "APIキーを入力"}
            />
          </label>
          <p className="text-muted-foreground text-xs leading-relaxed">
            キーはこのSumiサーバーに暗号化して保存します。選んだ接続先へ会話が送られ、APIの利用料はそのアカウントに発生します。保存済みのキーは表示しません。
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
