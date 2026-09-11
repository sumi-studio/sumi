import { ChevronRight } from "lucide-react";
import type { FeedbackDiagnostics } from "./diagnostics";
import "./diagnostic-details.css";

export function Diagnostics({
  details,
  composing = false,
}: {
  details: FeedbackDiagnostics;
  composing?: boolean;
}) {
  const source = details.source;
  const states = source?.states;
  const connection = states?.agent_connection ?? states?.messaging_connection;
  const release = details.tab_release ?? details.served_release;
  const observation = details.server_observation;
  return (
    <details className="feedback-diagnostic-attachment">
      <summary>
        <ChevronRight size={14} aria-hidden="true" />
        <span>
          {composing ? "診断情報を自動添付します" : "添付された診断情報"}
        </span>
      </summary>
      <div className="feedback-diagnostic-content">
        <p className="feedback-diagnostic-description">
          画面や接続の状態、直前の操作とブラウザのエラーを共有します。
          操作履歴に入力内容や会話本文は記録しません。
        </p>
        <dl className="feedback-diagnostic-facts">
          {source && (
            <>
              <dt>元の画面</dt>
              <dd>
                {screenName(source.path)}
                <code className="feedback-diagnostic-path">{source.path}</code>
              </dd>
              {source.workspace_id && (
                <>
                  <dt>Workspace</dt>
                  <dd>
                    <code>{source.workspace_id}</code>
                  </dd>
                </>
              )}
              <dt>画面の記録</dt>
              <dd>{displayTime(source.captured_at, details.time_zone)}</dd>
            </>
          )}
          <dt>添付の記録</dt>
          <dd>{displayTime(details.captured_at, details.time_zone)}</dd>
          <dt>接続</dt>
          <dd>
            {details.online ? "ブラウザはオンライン" : "ブラウザはオフライン"}
            {connection && (
              <span className="feedback-diagnostic-secondary">
                {states?.agent_connection ? "エージェント" : "Messaging"}：
                {connectionName(connection)}
              </span>
            )}
          </dd>
          <dt>ブラウザ</dt>
          <dd>{browserName(details.browser)}</dd>
          <dt>表示領域</dt>
          <dd>
            {details.viewport.width} × {details.viewport.height} px
            <span className="feedback-diagnostic-secondary">
              拡大率 {Math.round(details.viewport.scale * 100)}%
            </span>
          </dd>
          {states?.agent_latest_run_id && (
            <>
              <dt>直近の実行</dt>
              <dd>
                <code>{states.agent_latest_run_id}</code>
              </dd>
            </>
          )}
          {release && (
            <>
              <dt>ビルド</dt>
              <dd>
                <code title={release}>{release.slice(0, 12)}</code>
                {details.tab_release &&
                  details.served_release &&
                  details.tab_release !== details.served_release && (
                    <span className="feedback-diagnostic-secondary">
                      サーバーの配信版と異なります（
                      <code title={details.served_release}>
                        {details.served_release.slice(0, 12)}
                      </code>
                      ）
                    </span>
                  )}
              </dd>
            </>
          )}
        </dl>
        {details.selection && (
          <section
            className="feedback-diagnostic-section"
            aria-label="選択した箇所"
          >
            <h4>選択した箇所</h4>
            <p>{details.selection.label || details.selection.tag}</p>
            <code>{details.selection.selector}</code>
          </section>
        )}
        <section
          className="feedback-diagnostic-section"
          aria-label="ブラウザ内の直近の記録"
        >
          <h4>ブラウザ内の直近の記録</h4>
          <p className="feedback-diagnostic-description">
            このタブで記録した、最大24件の画面移動・操作・エラーです。
          </p>
          {details.client_events?.length ? (
            <ol className="feedback-diagnostic-events">
              {details.client_events.map((event, index) => (
                // biome-ignore lint/suspicious/noArrayIndexKey: An attachment is an immutable snapshot; multiple events can share a timestamp.
                <li key={`${event.at}-${index}`}>
                  <span className="feedback-diagnostic-event-heading">
                    <span>{eventName(event.kind)}</span>
                    <time dateTime={event.at}>
                      {displayTime(event.at, details.time_zone)}
                    </time>
                  </span>
                  <span>{event.summary}</span>
                  <code className="feedback-diagnostic-path">{event.path}</code>
                </li>
              ))}
            </ol>
          ) : (
            <p>記録はありません。</p>
          )}
        </section>
        <section
          className="feedback-diagnostic-section"
          aria-label="サーバーで確認した状態"
        >
          <h4>サーバーで確認した状態</h4>
          {observation ? (
            <dl className="feedback-diagnostic-facts">
              <dt>状態</dt>
              <dd>{observationName(observation.status)}</dd>
              <dt>確認時刻</dt>
              <dd>{displayTime(observation.captured_at, details.time_zone)}</dd>
              {observation.ready !== undefined && (
                <>
                  <dt>実行の準備</dt>
                  <dd>{observation.ready ? "準備完了" : "未完了"}</dd>
                </>
              )}
              {observation.run_in_flight !== undefined && (
                <>
                  <dt>処理</dt>
                  <dd>{observation.run_in_flight ? "実行中" : "待機中"}</dd>
                </>
              )}
              {observation.readiness_reason && (
                <>
                  <dt>準備状態の詳細</dt>
                  <dd>{observation.readiness_reason}</dd>
                </>
              )}
            </dl>
          ) : (
            <p>
              {composing
                ? "診断情報の添付時にサーバーの状態を確認します。"
                : "サーバーの記録はありません。"}
            </p>
          )}
        </section>
        <details className="feedback-diagnostic-raw">
          <summary>
            <ChevronRight size={13} aria-hidden="true" />
            <span>すべての診断データ</span>
          </summary>
          <section
            className="feedback-diagnostic-json"
            // biome-ignore lint/a11y/noNoninteractiveTabindex: Keyboard users need to scroll this bounded diagnostic region.
            tabIndex={0}
            aria-label="診断データ JSON"
          >
            <pre>{JSON.stringify(details, null, 2)}</pre>
          </section>
        </details>
      </div>
    </details>
  );
}

function eventName(kind: string): string {
  const labels: Record<string, string> = {
    navigation: "画面移動",
    click: "クリック",
    error: "ブラウザエラー",
    unhandledrejection: "非同期処理のエラー",
  };
  return labels[kind] ?? kind;
}

function observationName(status: string): string {
  const labels: Record<string, string> = {
    observed: "確認済み",
    ok: "確認済み",
    available: "確認済み",
    unavailable: "確認できませんでした",
    timeout: "確認がタイムアウトしました",
    not_applicable: "この画面は確認対象外です",
    not_requested: "確認していません",
  };
  return labels[status] ?? status;
}

function screenName(path: string): string {
  if (path === "/feedback") return "Feedback";
  if (path === "/direct") return "Direct";
  if (path === "/") return "Workspace 一覧";
  if (/\/messaging\/dm\//.test(path)) return "Messaging · DM";
  if (/\/messaging(?:\/|$)/.test(path)) return "Messaging";
  if (/\/members\/?$/.test(path)) return "Workspace · 参加者と招待";
  if (/\/roles\/?$/.test(path)) return "Workspace · ロール";
  if (/\/apps\/?$/.test(path)) return "Workspace · アプリ";
  if (/^\/w\//.test(path)) return "Workspace";
  return "その他の画面";
}

function connectionName(connection: string): string {
  const labels: Record<string, string> = {
    connected: "接続済み",
    connecting: "接続中",
    reconnecting: "再接続中",
    disconnected: "未接続",
    error: "接続エラー",
    closed: "接続終了",
  };
  return labels[connection] ?? connection;
}

function displayTime(value: string, timeZone: string): string {
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return value;
  try {
    return new Intl.DateTimeFormat("ja-JP", {
      dateStyle: "medium",
      timeStyle: "long",
      timeZone: timeZone || "UTC",
    }).format(date);
  } catch {
    return date.toISOString();
  }
}

function browserName(userAgent: string): string {
  const candidates: [RegExp, string][] = [
    [/Edg(?:e|A|iOS)?\/([\d.]+)/, "Edge"],
    [/(?:Firefox|FxiOS)\/([\d.]+)/, "Firefox"],
    [/(?:Chrome|CriOS)\/([\d.]+)/, "Chrome"],
    [/Version\/([\d.]+).*Safari\//, "Safari"],
  ];
  for (const [pattern, name] of candidates) {
    const match = pattern.exec(userAgent);
    if (match) return `${name} ${match[1]}`;
  }
  return userAgent || "記録なし";
}
