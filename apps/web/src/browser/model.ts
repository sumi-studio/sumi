/**
 * The shared-screen protocol (`sumi.browser.v1`) as seen by the person's
 * viewer, and a pure reducer for everything except frames (frames bypass
 * React state and are painted directly).
 */

export interface RemoteTab {
  id: string;
  url: string;
  title: string;
  active: boolean;
  loading: boolean;
}

export type Phase = "connecting" | "sleeping" | "starting" | "restoring" | "live" | "saving" | "closing" | "unavailable";

export interface Recovery {
  kind: "reconnected" | "restored" | "fresh";
  at: number;
  checkpointAt?: string;
  uncertain?: { kind: string; label?: string };
  skippedOrigins?: string[];
  failedRecords?: number;
}

/** What the latest checkpoint could not keep. */
export interface NotSaved {
  nonJsonIdbValues: number;
  sessionStorageKeys: number;
  skippedOrigins: string[];
  oversizedOrigins?: string[];
}

export interface AgentAction {
  tab: string;
  kind: string;
  label?: string;
  bounds?: { x: number; y: number; width: number; height: number };
}

export type GoalActivity =
  | { state: "running"; goal: string; progress?: Record<string, unknown> }
  | { state: "ended"; goal: string; outcome: string; status: string };

export interface Notice {
  id: number;
  code: string;
}

export interface ViewerState {
  phase: Phase;
  detail?: string;
  viewport: { width: number; height: number };
  control: "agent" | "human";
  /** How long the person's control survives a disconnect (reload, network). */
  controlHoldMs?: number;
  /** Control went back to the secretary because the person stayed away. */
  controlReturned?: { reason: string; at: number };
  /** Set when a takeover stopped the secretary; explains what happened. */
  takeover?: { goalStopped: boolean; inFlight?: { kind: string; label?: string } };
  tabs: RemoteTab[];
  shared: { tab: string; allowActions: boolean }[];
  goal?: GoalActivity;
  agent?: AgentAction;
  recovery?: Recovery;
  checkpointAt?: string;
  notSaved?: NotSaved;
  dialog?: { dialogType: string; message: string };
  unsupported: string[];
  notices: Notice[];
}

export const initialViewerState: ViewerState = {
  phase: "connecting",
  viewport: { width: 1280, height: 800 },
  control: "agent",
  tabs: [],
  shared: [],
  unsupported: [],
  notices: [],
};

export type ServerMessage = { type: string } & Record<string, unknown>;

let noticeSeq = 0;
const PHASES = new Set<Phase>(["sleeping", "starting", "restoring", "live", "saving", "closing", "unavailable"]);

function tabs(value: unknown): RemoteTab[] | undefined {
  if (!Array.isArray(value)) return undefined;
  return value.flatMap((t) =>
    t && typeof t === "object" && typeof t.id === "string"
      ? [{ id: t.id, url: String(t.url ?? ""), title: String(t.title ?? ""), active: t.active === true, loading: t.loading === true }]
      : [],
  );
}

function shared(value: unknown): ViewerState["shared"] | undefined {
  if (!Array.isArray(value)) return undefined;
  return value.flatMap((s) => (s && typeof s.tab === "string" ? [{ tab: s.tab, allowActions: s.allowActions === true }] : []));
}

function notSaved(value: unknown): NotSaved | undefined {
  if (!value || typeof value !== "object") return undefined;
  const v = value as Record<string, unknown>;
  const origins = (list: unknown) => (Array.isArray(list) ? list.filter((o): o is string => typeof o === "string") : []);
  return {
    nonJsonIdbValues: Number(v.nonJsonIdbValues) || 0,
    sessionStorageKeys: Number(v.sessionStorageKeys) || 0,
    skippedOrigins: origins(v.skippedOrigins),
    oversizedOrigins: origins(v.oversizedOrigins),
  };
}

function phase(value: unknown, fallback: Phase): Phase {
  return typeof value === "string" && PHASES.has(value as Phase) ? (value as Phase) : fallback;
}

export function reduceViewer(state: ViewerState, message: ServerMessage): ViewerState {
  switch (message.type) {
    case "hello": {
      const viewport = message.viewport as ViewerState["viewport"] | undefined;
      const info = message.control as { mode?: string; holdMs?: unknown; returned?: { reason?: unknown; at?: unknown } } | undefined;
      const control = info?.mode === "human" ? "human" : "agent";
      const returned = info?.returned;
      return {
        ...state,
        phase: phase(message.phase, state.phase),
        detail: typeof message.detail === "string" ? message.detail : undefined,
        viewport: viewport && viewport.width > 0 && viewport.height > 0 ? viewport : state.viewport,
        control,
        controlHoldMs: typeof info?.holdMs === "number" ? info.holdMs : state.controlHoldMs,
        controlReturned:
          control === "agent" && returned && typeof returned.at === "number" ? { reason: String(returned.reason), at: returned.at } : undefined,
        takeover: control === "human" ? state.takeover : undefined,
        tabs: tabs(message.tabs) ?? state.tabs,
        shared: shared(message.shared) ?? state.shared,
        goal: (message.goal as GoalActivity | undefined) ?? state.goal,
        recovery: (message.recovery as Recovery | undefined) ?? state.recovery,
        checkpointAt: typeof message.checkpointAt === "string" ? message.checkpointAt : state.checkpointAt,
        notSaved: notSaved(message.notSaved) ?? state.notSaved,
        dialog: message.dialog as ViewerState["dialog"],
        unsupported: Array.isArray(message.unsupported) ? message.unsupported.map(String) : state.unsupported,
      };
    }
    case "status":
      return {
        ...state,
        phase: phase(message.phase, state.phase),
        detail: typeof message.detail === "string" ? message.detail : undefined,
        checkpointAt: typeof message.checkpointAt === "string" ? message.checkpointAt : state.checkpointAt,
        agent: message.phase === "live" ? state.agent : undefined,
      };
    case "tabs":
      return { ...state, tabs: tabs(message.tabs) ?? state.tabs, shared: shared(message.shared) ?? state.shared };
    case "control":
      return message.mode === "human"
        ? {
            ...state,
            control: "human",
            controlReturned: undefined,
            takeover: {
              goalStopped: message.goalStopped === true,
              inFlight: message.inFlight as { kind: string; label?: string } | undefined,
            },
          }
        : {
            ...state,
            control: "agent",
            takeover: undefined,
            controlReturned:
              typeof message.reason === "string" ? { reason: message.reason, at: typeof message.at === "number" ? message.at : Date.now() } : state.controlReturned,
          };
    case "agent":
      return message.phase === "start"
        ? {
            ...state,
            agent: {
              tab: String(message.tab),
              kind: String(message.kind),
              label: typeof message.label === "string" ? message.label : undefined,
              bounds: message.bounds as AgentAction["bounds"],
            },
          }
        : { ...state, agent: undefined };
    case "goal":
      return { ...state, goal: message.activity as GoalActivity };
    case "checkpoint":
      return {
        ...state,
        checkpointAt: typeof message.at === "string" ? message.at : state.checkpointAt,
        notSaved: notSaved(message.notSaved) ?? state.notSaved,
      };
    case "dialog":
      return message.closed
        ? { ...state, dialog: undefined }
        : { ...state, dialog: { dialogType: String(message.dialogType ?? "alert"), message: String(message.message ?? "") } };
    case "notice":
    case "error":
      return {
        ...state,
        notices: [...state.notices.slice(-2), { id: ++noticeSeq, code: String(message.code ?? "failed") }],
      };
    default:
      return state;
  }
}

export function dismissNotice(state: ViewerState, id: number): ViewerState {
  return { ...state, notices: state.notices.filter((n) => n.id !== id) };
}

// ---------- person-facing wording ----------

const time = (iso?: string | number) => {
  if (iso === undefined) return "";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? "" : d.toLocaleTimeString("ja-JP", { hour: "2-digit", minute: "2-digit" });
};

export function phaseText(state: Pick<ViewerState, "phase" | "detail">): string {
  switch (state.phase) {
    case "connecting":
      return "接続しています…";
    case "sleeping":
      return state.detail === "reset" ? "ブラウザはリセットされました。" : "ブラウザは休止中です。開くと保存済みの状態から再開します。";
    case "starting":
      return "ブラウザを起動しています…";
    case "restoring":
      return "前回保存した状態を復元しています…";
    case "live":
      return "";
    case "saving":
      return "状態を保存しています…";
    case "closing":
      return "ブラウザを休止しています…";
    case "unavailable":
      return state.detail === "browser_limit"
        ? "Cloud ブラウザの利用上限に達しているため起動できません。しばらくしてから再試行してください。"
        : state.detail === "browser_lost"
          ? "ブラウザとの接続が切れました。保存済みの状態から再開できます。"
          : "ブラウザを起動できませんでした。再試行してください。";
  }
}

const KIND: Record<string, string> = {
  click: "クリック",
  type: "文字入力",
  select: "選択",
  press: "キー操作",
  scroll: "スクロール",
  navigate: "ページ移動",
};

export function actionText(action: { kind: string; label?: string }): string {
  const kind = KIND[action.kind] ?? "操作";
  return action.label ? `${kind}（${action.label}）` : kind;
}

export function recoveryText(recovery: Recovery): string[] {
  const lines: string[] = [];
  if (recovery.kind === "reconnected")
    lines.push("ブラウザに再接続しました。開いていたページ・ログイン状態・入力中の内容はそのままです。");
  else if (recovery.kind === "restored")
    lines.push(
      `ブラウザを起動し直し、${time(recovery.checkpointAt)} に保存した状態から復元しました。Cookie・localStorage・IndexedDB・開いていたタブとスクロール位置は戻っています。ページ内の未送信の入力、sessionStorage、実行中の処理は失われています。`,
    );
  else lines.push("新しいブラウザで開始しました。復元できる保存状態はありませんでした。");
  if (recovery.skippedOrigins?.length)
    lines.push(
      `${recovery.skippedOrigins.length} 件のサイト（${recovery.skippedOrigins.map(host).join("、")}）のデータは保存または復元できなかったため戻っていません。そのサイトでは再ログインなどが必要になることがあります。`,
    );
  if (recovery.failedRecords)
    lines.push(`IndexedDB の ${recovery.failedRecords} 件のデータは新しいブラウザが受け付けなかったため戻っていません。`);
  if (recovery.uncertain)
    lines.push(
      `秘書の最後の操作「${actionText(recovery.uncertain)}」が相手のサイトに届いたかは確認できません。自動では再実行しません。サイト上で結果を確かめてください。`,
    );
  return lines;
}

export function checkpointText(at?: string): string {
  return at ? `最終保存 ${time(at)}` : "未保存";
}

function host(origin: string): string {
  try {
    return new URL(origin).host;
  } catch {
    return origin;
  }
}

/** What the latest checkpoint left out, or undefined when it kept everything
 * it supports. */
export function notSavedText(notSaved?: NotSaved): string | undefined {
  if (!notSaved) return undefined;
  const parts: string[] = [];
  if (notSaved.oversizedOrigins?.length)
    parts.push(`容量の上限（合計 2 MB）に収まらないサイトのデータ: ${notSaved.oversizedOrigins.map(host).join("、")}`);
  if (notSaved.skippedOrigins.length) parts.push(`読み取れなかったサイトのデータ: ${notSaved.skippedOrigins.map(host).join("、")}`);
  if (notSaved.nonJsonIdbValues)
    parts.push(`IndexedDB のうち JSON で表せないデータ（ファイル・日付・バイナリなど） ${notSaved.nonJsonIdbValues} 件`);
  if (notSaved.sessionStorageKeys) parts.push(`sessionStorage ${notSaved.sessionStorageKeys} 件`);
  return parts.length ? `前回の保存に含まれていないもの — ${parts.join("／")}` : undefined;
}

const minutes = (ms: number) => Math.max(1, Math.round(ms / 60_000));

/** How the person's control behaves when their page reloads or disconnects. */
export function controlHoldText(holdMs = 120_000): string {
  return `ページを再読み込みしたり接続が一時的に切れたりしても、${minutes(holdMs)} 分間はあなたの操作のままです。それより長く離れると秘書に戻ります。`;
}

/** Why control is the secretary's again. The takeover survives reloads,
 * brief disconnects and server restarts; only these two end it. */
export function controlReturnedText(returned: { reason?: string; at: number }, holdMs = 120_000): string {
  if (returned.reason === "person_release")
    return `${time(returned.at)} に「秘書に戻す」で操作を秘書に戻しました。止めた秘書の作業は再開していません。`;
  return `接続が切れたまま ${minutes(holdMs)} 分たったため、${time(returned.at)} に操作を秘書に戻しました。止めた秘書の作業は再開していません。`;
}

export const NOTICE_TEXT: Record<string, string> = {
  upload_unsupported: "Cloud ブラウザではファイルを選んでアップロードできません。",
  file_drag_drop: "Cloud ブラウザにはファイルをドラッグ＆ドロップできません。",
  download_unsupported: "Cloud ブラウザではファイルをダウンロードできません。",
  tab_limit: "開けるタブの上限に達しました。",
  tab_crashed: "タブが応答しなくなりました。閉じて開き直してください。",
  checkpoint_too_large: "Cookie と開いているタブだけで保存の上限を超えたため、今回は保存できませんでした。",
  control_not_saved: "操作の交代を保存できませんでした。再試行しています。保存できるまでは、サーバーの再起動で秘書に戻る場合があります。",
  tab_changed: "表示中のタブが切り替わったため、その操作は送りませんでした。画面を確かめてからもう一度操作してください。",
  not_live: "ブラウザがまだ準備できていません。",
  navigation_failed: "ページを開けませんでした。",
  invalid_url: "開けない URL です。",
};

export const UNSUPPORTED_TEXT: Record<string, string> = {
  file_upload: "ファイルのアップロード",
  file_drag_drop: "ファイルのドラッグ＆ドロップ",
  download: "ダウンロード",
  clipboard_copy_out: "リモート側でコピーした内容の手元への貼り付け",
  audio: "音声",
  extensions: "拡張機能",
};

export function goalText(goal: GoalActivity): string {
  if (goal.state === "running") return `秘書が作業中: ${goal.goal}`;
  const outcome: Record<string, string> = {
    stopped_by_person: "あなたの操作で停止しました",
    cancelled: "取り消されました",
    jev_reported_done: "完了しました",
  };
  return `秘書の作業「${goal.goal}」: ${outcome[goal.outcome] ?? "終了しました"}`;
}
