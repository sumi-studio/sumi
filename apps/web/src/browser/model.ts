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
  /** Set when a takeover stopped the secretary; explains what happened. */
  takeover?: { goalStopped: boolean; inFlight?: { kind: string; label?: string } };
  tabs: RemoteTab[];
  shared: { tab: string; allowActions: boolean }[];
  goal?: GoalActivity;
  agent?: AgentAction;
  recovery?: Recovery;
  checkpointAt?: string;
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

function phase(value: unknown, fallback: Phase): Phase {
  return typeof value === "string" && PHASES.has(value as Phase) ? (value as Phase) : fallback;
}

export function reduceViewer(state: ViewerState, message: ServerMessage): ViewerState {
  switch (message.type) {
    case "hello": {
      const viewport = message.viewport as ViewerState["viewport"] | undefined;
      const control = (message.control as { mode?: string } | undefined)?.mode === "human" ? "human" : "agent";
      return {
        ...state,
        phase: phase(message.phase, state.phase),
        detail: typeof message.detail === "string" ? message.detail : undefined,
        viewport: viewport && viewport.width > 0 && viewport.height > 0 ? viewport : state.viewport,
        control,
        takeover: control === "human" ? state.takeover : undefined,
        tabs: tabs(message.tabs) ?? state.tabs,
        shared: shared(message.shared) ?? state.shared,
        goal: (message.goal as GoalActivity | undefined) ?? state.goal,
        recovery: (message.recovery as Recovery | undefined) ?? state.recovery,
        checkpointAt: typeof message.checkpointAt === "string" ? message.checkpointAt : state.checkpointAt,
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
            takeover: {
              goalStopped: message.goalStopped === true,
              inFlight: message.inFlight as { kind: string; label?: string } | undefined,
            },
          }
        : { ...state, control: "agent", takeover: undefined };
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
      return { ...state, checkpointAt: typeof message.at === "string" ? message.at : state.checkpointAt };
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
    lines.push(`${recovery.skippedOrigins.length} 件のサイトのデータは上限を超えたため保存されていません。`);
  if (recovery.uncertain)
    lines.push(
      `秘書の最後の操作「${actionText(recovery.uncertain)}」が相手のサイトに届いたかは確認できません。自動では再実行しません。サイト上で結果を確かめてください。`,
    );
  return lines;
}

export function checkpointText(at?: string): string {
  return at ? `最終保存 ${time(at)}` : "未保存";
}

export const NOTICE_TEXT: Record<string, string> = {
  upload_unsupported: "Cloud ブラウザではファイルを選んでアップロードできません。",
  file_drag_drop: "Cloud ブラウザにはファイルをドラッグ＆ドロップできません。",
  download_unsupported: "Cloud ブラウザではファイルをダウンロードできません。",
  tab_limit: "開けるタブの上限に達しました。",
  tab_crashed: "タブが応答しなくなりました。閉じて開き直してください。",
  checkpoint_too_large: "サイトのデータが保存上限を超えたため、今回は保存できませんでした。",
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
