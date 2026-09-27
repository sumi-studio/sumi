import { Button } from "@sumi/ui/components/button";
import { Popover, PopoverContent, PopoverTrigger } from "@sumi/ui/components/popover";
import {
  ArrowLeft,
  ArrowRight,
  CircleAlert,
  Globe,
  Hand,
  Info,
  KeyRound,
  Plus,
  RotateCw,
  Share2,
  Square,
  X,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useReducer, useRef, useState } from "react";
import { type CloudBrowserAPI, type CloudBrowserOverview, type CloudBrowserProfile, createCloudBrowserAPI } from "./api";
import { BrowserScreen, type BrowserScreenHandle, type ViewerInput } from "./browser-screen";
import {
  actionText,
  checkpointText,
  controlHoldText,
  controlReturnedText,
  dismissNotice,
  goalText,
  initialViewerState,
  NOTICE_TEXT,
  notSavedText,
  phaseText,
  recoveryText,
  reduceViewer,
  type ServerMessage,
  UNSUPPORTED_TEXT,
} from "./model";
import { HIDDEN_RELEASE_MS, type SocketStatus, ViewerSocket } from "./viewer-socket";

const defaultAPI = createCloudBrowserAPI();

/**
 * 秘書のブラウザ = the secretary's Cloud browser profile, shown as the same
 * remote screen the secretary acts on. The person can browse, sign in, hand
 * control back and forth, and share individual tabs with the secretary.
 */
export function BrowserWorkspace({ api = defaultAPI }: { api?: CloudBrowserAPI }) {
  const [overview, setOverview] = useState<CloudBrowserOverview | null>(null);
  const [error, setError] = useState(false);
  const [personaId, setPersonaId] = useState<string>("");
  const [creating, setCreating] = useState(false);

  const reload = useCallback(
    async (signal?: AbortSignal) => {
      try {
        const next = await api.overview(signal);
        setOverview(next);
        setError(false);
      } catch {
        if (!signal?.aborted) setError(true);
      }
    },
    [api],
  );

  useEffect(() => {
    const controller = new AbortController();
    void reload(controller.signal);
    return () => controller.abort();
  }, [reload]);

  if (error && !overview)
    return (
      <Centered title="秘書のブラウザを読み込めませんでした">
        <Button variant="outline" size="sm" onClick={() => void reload()}>
          再読み込み
        </Button>
      </Centered>
    );
  if (!overview) return <Centered title="秘書のブラウザを確認しています…" />;
  if (!overview.configured)
    return (
      <Centered title="この環境では秘書のブラウザを使えません">
        Cloud ブラウザが設定されていません。Sumi Local のデスクトップアプリでは、手元のブラウザのタブを秘書と共有できます。
      </Centered>
    );
  if (!overview.personas.length)
    return <Centered title="秘書がまだいません">秘書を用意すると、秘書専用のブラウザを使えるようになります。</Centered>;

  const selected = personaId || overview.profiles[0]?.personaId || overview.personas[0]?.personaId || "";
  const profile = overview.profiles.find((p) => p.personaId === selected);
  const persona = overview.personas.find((p) => p.personaId === selected);

  return (
    <div className="flex h-full min-h-0 flex-col bg-background text-foreground">
      <header className="flex items-center gap-2 border-border border-b px-3 py-2">
        <Globe aria-hidden className="size-4 shrink-0 text-muted-foreground" />
        <h1 className="shrink-0 font-semibold text-sm tracking-tight">秘書のブラウザ</h1>
        {overview.personas.length > 1 ? (
          <select
            aria-label="秘書"
            className="h-8 min-w-0 rounded-md border border-border bg-background px-2 text-sm"
            value={selected}
            onChange={(e) => setPersonaId(e.target.value)}
          >
            {overview.personas.map((p) => (
              <option key={p.personaId} value={p.personaId}>
                {p.name || "秘書"}
              </option>
            ))}
          </select>
        ) : (
          <span className="truncate text-muted-foreground text-sm">{persona?.name}</span>
        )}
        <div className="ml-auto flex items-center gap-1">
          <JevSettings api={api} jev={overview.jev} onChanged={() => void reload()} />
        </div>
      </header>
      {profile ? (
        <BrowserSession key={profile.profileId} api={api} profile={profile} onChanged={() => void reload()} />
      ) : (
        <Centered title={`${persona?.name || "秘書"}のブラウザを用意します`}>
          <p>
            秘書専用のブラウザを Cloud 上に用意します。あなたと秘書は同じ画面を見て、交代しながら操作できます。あなたの手元のブラウザの
            Cookie やログイン状態は持ち込みません。必要なサイトにはこの画面からログインしてください。
          </p>
          <Button
            className="mt-4"
            disabled={creating}
            onClick={async () => {
              setCreating(true);
              try {
                await api.createProfile(selected);
                await reload();
              } catch {
                setError(true);
              } finally {
                setCreating(false);
              }
            }}
          >
            ブラウザを用意する
          </Button>
        </Centered>
      )}
    </div>
  );
}

function Centered({ title, children }: { title: string; children?: React.ReactNode }) {
  return (
    <div className="grid h-full flex-1 place-items-center px-6">
      <div className="flex max-w-md flex-col items-center text-center">
        <span className="mb-4 grid size-11 place-items-center rounded-xl border border-border bg-muted/35">
          <Globe className="size-5 text-muted-foreground" />
        </span>
        <h2 className="font-semibold text-base tracking-tight">{title}</h2>
        {children ? <div className="mt-2 text-muted-foreground text-sm leading-6">{children}</div> : null}
      </div>
    </div>
  );
}

function BrowserSession({
  api,
  profile,
  onChanged,
}: {
  api: CloudBrowserAPI;
  profile: CloudBrowserProfile;
  onChanged(): void;
}) {
  const [state, dispatch] = useReducer(
    (s: typeof initialViewerState, m: ServerMessage | { type: "dismiss"; id: number } | { type: "reset" }) =>
      m.type === "dismiss" ? dismissNotice(s, m.id as number) : m.type === "reset" ? initialViewerState : reduceViewer(s, m),
    initialViewerState,
  );
  const [socketStatus, setSocketStatus] = useState<SocketStatus>("connecting");
  const [closedReason, setClosedReason] = useState<string>();
  const [recoveryDismissed, setRecoveryDismissed] = useState<number>();
  const [returnedDismissed, setReturnedDismissed] = useState<number>();
  const screen = useRef<BrowserScreenHandle>(null);
  // The tab of the last painted frame: the person's input is for that tab.
  const frameTab = useRef<string | undefined>(undefined);
  const socket = useRef<ViewerSocket | null>(null);
  // The viewer lives exactly as long as the profile; it calls the latest
  // overview refresh without reconnecting when that callback changes.
  const refreshOverview = useRef(onChanged);
  refreshOverview.current = onChanged;

  useEffect(() => {
    const viewer = new ViewerSocket({
      api,
      profileId: profile.profileId,
      onMessage: dispatch,
      onFrame: (frame) => {
        frameTab.current = frame.tab;
        screen.current?.paint(frame);
      },
      onStatus: (status, reason) => {
        setSocketStatus(status);
        if (reason) setClosedReason(reason);
        if (reason) refreshOverview.current();
      },
    });
    socket.current = viewer;
    viewer.start();
    // A hidden page stops keeping the remote browser alive after a while;
    // coming back reconnects (the profile saves and sleeps meanwhile).
    let hiddenTimer: ReturnType<typeof setTimeout> | undefined;
    const onVisibility = () => {
      clearTimeout(hiddenTimer);
      if (document.hidden) hiddenTimer = setTimeout(() => viewer.pause(), HIDDEN_RELEASE_MS);
      else viewer.resume();
    };
    document.addEventListener("visibilitychange", onVisibility);
    return () => {
      document.removeEventListener("visibilitychange", onVisibility);
      clearTimeout(hiddenTimer);
      viewer.stop();
      socket.current = null;
    };
  }, [api, profile.profileId]);

  const send = useCallback((message: Record<string, unknown>) => {
    if (!socket.current?.send(message)) dispatch({ type: "notice", code: "not_live" });
  }, []);
  const onInput = useCallback((input: ViewerInput) => send({ type: "input", ...input, tab: frameTab.current }), [send]);
  const onUnsupported = useCallback((code: string) => dispatch({ type: "notice", code }), []);

  const active = state.tabs.find((t) => t.active);
  const grant = active ? profile.grants.find((g) => g.tabId === active.id) : undefined;
  const live = state.phase === "live" && socketStatus === "open";
  const recoveryLines = useMemo(() => (state.recovery ? recoveryText(state.recovery) : []), [state.recovery]);
  const notSavedLine = notSavedText(state.notSaved);

  if (closedReason === "reset" || closedReason === "forbidden")
    return (
      <Centered title={closedReason === "reset" ? "ブラウザはリセットされました" : "このブラウザは表示できません"}>
        <Button variant="outline" size="sm" onClick={onChanged}>
          再読み込み
        </Button>
      </Centered>
    );

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <TabStrip
        tabs={state.tabs}
        shared={profile.grants.map((g) => g.tabId)}
        disabled={!live}
        onActivate={(id) => send({ type: "tab", op: "activate", id })}
        onClose={(id) => send({ type: "tab", op: "close", id })}
        onNew={() => send({ type: "tab", op: "new" })}
      />
      <div className="flex items-center gap-1 border-border border-b px-2 py-1.5">
        <Button variant="ghost" size="icon" className="size-8" aria-label="戻る" disabled={!live} onClick={() => send({ type: "tab", op: "back", id: active?.id })}>
          <ArrowLeft className="size-4" />
        </Button>
        <Button variant="ghost" size="icon" className="size-8" aria-label="進む" disabled={!live} onClick={() => send({ type: "tab", op: "forward", id: active?.id })}>
          <ArrowRight className="size-4" />
        </Button>
        <Button variant="ghost" size="icon" className="size-8" aria-label="再読み込み" disabled={!live} onClick={() => send({ type: "tab", op: "reload", id: active?.id })}>
          <RotateCw className="size-4" />
        </Button>
        <AddressBar key={active?.id ?? "none"} url={active?.url ?? ""} disabled={!live} onNavigate={(url) => send(active ? { type: "tab", op: "navigate", id: active.id, url } : { type: "tab", op: "new", url })} />
        <ShareButton api={api} profile={profile} tab={active} grant={grant} disabled={!live} onChanged={onChanged} />
        <ControlPill control={state.control} disabled={!live} onTakeover={() => send({ type: "takeover" })} onRelease={() => send({ type: "release" })} />
      </div>

      {state.goal?.state === "running" ? (
        <Banner tone="info" icon={<Hand className="size-3.5 shrink-0 text-sky-600" />}>
          <span className="min-w-0 flex-1 truncate">{goalText(state.goal)}</span>
          <Button size="sm" variant="outline" className="h-7 shrink-0 gap-1" onClick={() => send({ type: "stop-goal" })}>
            <Square className="size-3" />
            停止
          </Button>
        </Banner>
      ) : null}
      {state.control === "human" ? (
        <Banner tone="info" icon={<Hand className="size-3.5 shrink-0 text-muted-foreground" />}>
          <span className="min-w-0 flex-1" data-testid="cloud-browser-human-control">
            あなたが操作中です。秘書は操作しません
            {state.takeover?.goalStopped ? "（秘書の作業は停止しました）" : ""}
            {state.takeover?.inFlight ? `。直前に始まっていた秘書の${actionText(state.takeover.inFlight)}は完了まで進みます` : ""}。
            {controlHoldText(state.controlHoldMs)}
          </span>
        </Banner>
      ) : null}
      {state.control === "agent" && state.controlReturned && returnedDismissed !== state.controlReturned.at ? (
        <Banner tone="info" icon={<Info className="size-3.5 shrink-0 text-muted-foreground" />}>
          <span className="min-w-0 flex-1" data-testid="cloud-browser-control-returned">
            {controlReturnedText(state.controlReturned, state.controlHoldMs)}
          </span>
          <Button size="icon" variant="ghost" className="size-7" aria-label="閉じる" onClick={() => setReturnedDismissed(state.controlReturned?.at)}>
            <X className="size-3.5" />
          </Button>
        </Banner>
      ) : null}
      {state.agent && state.control === "agent" ? (
        <Banner tone="info" icon={<Hand className="size-3.5 shrink-0 text-sky-600" />}>
          <span className="min-w-0 flex-1 truncate">秘書が{actionText(state.agent)}をしています。画面に触れると操作を代わります。</span>
        </Banner>
      ) : null}
      {recoveryLines.length && recoveryDismissed !== state.recovery?.at ? (
        <Banner tone={state.recovery?.uncertain ? "warn" : "info"} icon={<Info className="size-3.5 shrink-0 text-muted-foreground" />}>
          <span className="min-w-0 flex-1" data-testid="cloud-browser-recovery">
            {recoveryLines.join(" ")}
          </span>
          <Button size="icon" variant="ghost" className="size-7" aria-label="閉じる" onClick={() => setRecoveryDismissed(state.recovery?.at)}>
            <X className="size-3.5" />
          </Button>
        </Banner>
      ) : null}
      {state.phase !== "live" || socketStatus !== "open" ? (
        <Banner tone={state.phase === "unavailable" ? "warn" : "info"} icon={<CircleAlert className="size-3.5 shrink-0 text-muted-foreground" />}>
          <span className="min-w-0 flex-1" data-testid="cloud-browser-status">
            {socketStatus === "retrying"
              ? "接続が切れました。再接続しています…"
              : socketStatus === "paused"
                ? "画面を離れていたため表示を止めています。ブラウザは状態を保存して休止します。"
                : phaseText(state)}
          </span>
          {state.phase === "unavailable" || state.phase === "sleeping" ? (
            <Button size="sm" variant="outline" className="h-7" onClick={() => send({ type: "start" })}>
              再開
            </Button>
          ) : null}
        </Banner>
      ) : null}
      {state.notices.map((n) => (
        <Banner key={n.id} tone="warn" icon={<CircleAlert className="size-3.5 shrink-0 text-amber-600" />}>
          <span className="min-w-0 flex-1">{NOTICE_TEXT[n.code] ?? "操作を完了できませんでした。"}</span>
          <Button size="icon" variant="ghost" className="size-7" aria-label="閉じる" onClick={() => dispatch({ type: "dismiss", id: n.id })}>
            <X className="size-3.5" />
          </Button>
        </Banner>
      ))}

      <div className="relative grid min-h-0 flex-1 place-items-center p-3" style={{ containerType: "size" }}>
        <BrowserScreen
          ref={screen}
          viewport={state.viewport}
          control={state.control}
          agent={state.agent}
          activeTab={active?.id}
          disabled={!live}
          onInput={onInput}
          onUnsupported={onUnsupported}
        />
        {state.dialog ? (
          <DialogPanel dialog={state.dialog} onAnswer={(accept, text) => send({ type: "dialog", accept, text })} />
        ) : null}
      </div>

      <footer className="flex flex-wrap items-center gap-x-3 gap-y-1 border-border border-t px-3 py-1.5 text-muted-foreground text-xs">
        <span data-testid="cloud-browser-checkpoint" title={notSavedLine}>
          {checkpointText(state.checkpointAt ?? profile.checkpointAt ?? undefined)}
          {notSavedLine ? "（一部は保存対象外）" : ""}
        </span>
        <span className="hidden sm:inline">画面の大きさは {state.viewport.width}×{state.viewport.height} で固定です（表示だけ拡大縮小します）。</span>
        <Popover>
          <PopoverTrigger render={<button type="button" className="underline-offset-2 hover:underline" />}>
            保存される内容と使えない機能
          </PopoverTrigger>
          <PopoverContent align="start" className="w-80 p-3 text-xs leading-5">
            <p className="font-medium text-foreground">保存される内容</p>
            <p className="mt-1">
              Cookie、サイトごとの localStorage と IndexedDB（JSON で表せる値）、開いているタブと表示位置を、ページの読み込み後や交代時に保存します（合計{" "}
              2 MB まで）。ブラウザは使っていないと 1 分ほどで休止し、次に開くと保存した状態から再開します。ページ内の未送信の入力と sessionStorage は保存されません。
              IndexedDB のファイル・日付・バイナリなどの値は保存せず件数だけ数えます。上限に収まらないサイトのデータは保存せず、Cookie・タブ・ほかのサイトは保存します。
            </p>
            {notSavedLine ? (
              <p className="mt-1" data-testid="cloud-browser-not-saved">
                {notSavedLine}
              </p>
            ) : null}
            <p className="mt-2 font-medium text-foreground">操作の交代</p>
            <p className="mt-1">
              画面に触れるとあなたの操作になり、「秘書に戻す」で秘書に返します。{controlHoldText(state.controlHoldMs)}止めた秘書の作業は自動では再開しません。
            </p>
            <p className="mt-2 font-medium text-foreground">使えない機能</p>
            <p className="mt-1">{(state.unsupported.length ? state.unsupported : Object.keys(UNSUPPORTED_TEXT)).map((u) => UNSUPPORTED_TEXT[u] ?? u).join("、")}。</p>
          </PopoverContent>
        </Popover>
        <ResetButton api={api} profileId={profile.profileId} onChanged={onChanged} />
      </footer>
    </div>
  );
}

function Banner({ tone, icon, children }: { tone: "info" | "warn"; icon: React.ReactNode; children: React.ReactNode }) {
  return (
    <div
      role={tone === "warn" ? "alert" : "status"}
      className={`flex items-center gap-2 border-border border-b px-3 py-1.5 text-xs ${tone === "warn" ? "bg-amber-500/10" : "bg-muted/40"}`}
    >
      {icon}
      {children}
    </div>
  );
}

function TabStrip({
  tabs,
  shared,
  disabled,
  onActivate,
  onClose,
  onNew,
}: {
  tabs: { id: string; title: string; url: string; active: boolean; loading: boolean }[];
  shared: string[];
  disabled: boolean;
  onActivate(id: string): void;
  onClose(id: string): void;
  onNew(): void;
}) {
  return (
    <div role="tablist" aria-label="タブ" className="flex items-center gap-1 overflow-x-auto border-border border-b bg-muted/30 px-2 pt-1.5">
      {tabs.map((tab) => (
        <div
          key={tab.id}
          className={`group flex max-w-56 shrink-0 items-center gap-1 rounded-t-md border border-b-0 px-2 py-1 text-xs ${
            tab.active ? "border-border bg-background" : "border-transparent text-muted-foreground hover:bg-muted"
          }`}
        >
          <button
            type="button"
            role="tab"
            aria-selected={tab.active}
            disabled={disabled}
            className="flex min-w-0 items-center gap-1"
            onClick={() => onActivate(tab.id)}
          >
            {shared.includes(tab.id) ? <Share2 aria-label="秘書と共有中" className="size-3 shrink-0 text-sky-600" /> : null}
            <span className="truncate">{tab.loading ? "読み込み中…" : tab.title || tab.url || "新しいタブ"}</span>
          </button>
          <button type="button" aria-label="タブを閉じる" disabled={disabled} className="rounded p-0.5 opacity-60 hover:bg-muted hover:opacity-100" onClick={() => onClose(tab.id)}>
            <X className="size-3" />
          </button>
        </div>
      ))}
      <Button variant="ghost" size="icon" className="mb-1 size-7" aria-label="新しいタブ" disabled={disabled} onClick={onNew}>
        <Plus className="size-3.5" />
      </Button>
    </div>
  );
}

function AddressBar({ url, disabled, onNavigate }: { url: string; disabled: boolean; onNavigate(url: string): void }) {
  const [value, setValue] = useState(url);
  const [editing, setEditing] = useState(false);
  return (
    <form
      className="min-w-0 flex-1"
      onSubmit={(e) => {
        e.preventDefault();
        const trimmed = value.trim();
        if (!trimmed) return;
        onNavigate(/^[a-z][a-z0-9+.-]*:/i.test(trimmed) ? trimmed : `https://${trimmed}`);
        setEditing(false);
      }}
    >
      <input
        aria-label="アドレス"
        className="h-8 w-full rounded-md border border-border bg-background px-2 text-sm"
        disabled={disabled}
        value={editing ? value : url}
        onFocus={(e) => {
          setValue(url);
          setEditing(true);
          e.currentTarget.select();
        }}
        onBlur={() => setEditing(false)}
        onChange={(e) => setValue(e.target.value)}
      />
    </form>
  );
}

function ControlPill({
  control,
  disabled,
  onTakeover,
  onRelease,
}: {
  control: "agent" | "human";
  disabled: boolean;
  onTakeover(): void;
  onRelease(): void;
}) {
  return (
    <div className="flex shrink-0 items-center gap-1.5">
      <span
        data-testid="cloud-browser-control"
        className="hidden items-center gap-1.5 rounded-full border border-border px-2 py-0.5 text-xs sm:inline-flex"
      >
        <span className={`size-1.5 rounded-full ${control === "human" ? "bg-amber-500" : "bg-sky-500"}`} />
        {control === "human" ? "あなたが操作中" : "秘書が操作できます"}
      </span>
      {control === "human" ? (
        <Button size="sm" variant="outline" className="h-8" disabled={disabled} onClick={onRelease}>
          秘書に戻す
        </Button>
      ) : (
        <Button size="sm" variant="outline" className="h-8" disabled={disabled} onClick={onTakeover}>
          操作を代わる
        </Button>
      )}
    </div>
  );
}

function ShareButton({
  api,
  profile,
  tab,
  grant,
  disabled,
  onChanged,
}: {
  api: CloudBrowserAPI;
  profile: CloudBrowserProfile;
  tab?: { id: string; title: string; url: string };
  grant?: { attachmentId: string; allowActions: boolean };
  disabled: boolean;
  onChanged(): void;
}) {
  const [open, setOpen] = useState(false);
  const [allowActions, setAllowActions] = useState(true);
  const [busy, setBusy] = useState(false);
  const [failed, setFailed] = useState(false);
  const run = async (effect: () => Promise<unknown>) => {
    setBusy(true);
    setFailed(false);
    try {
      await effect();
      setOpen(false);
      onChanged();
    } catch {
      setFailed(true);
    } finally {
      setBusy(false);
    }
  };
  return (
    <Popover
      open={open}
      onOpenChange={(next) => {
        // Each share starts from the default; a choice made for one tab does
        // not carry over to the next.
        if (next) {
          setAllowActions(true);
          setFailed(false);
        }
        setOpen(next);
      }}
    >
      <PopoverTrigger
        render={
          <Button size="sm" variant={grant ? "secondary" : "outline"} className="h-8 shrink-0 gap-1.5" disabled={disabled || !tab} />
        }
      >
        <Share2 className="size-3.5" />
        <span className="hidden md:inline">{grant ? "共有中" : "秘書と共有"}</span>
      </PopoverTrigger>
      <PopoverContent align="end" className="w-72 p-3 text-sm">
        {grant ? (
          <>
            <p>このタブを秘書と共有しています（{grant.allowActions ? "閲覧と操作" : "閲覧のみ"}）。</p>
            <Button className="mt-3 w-full" variant="outline" size="sm" disabled={busy} onClick={() => run(() => api.revoke(profile.profileId, grant.attachmentId))}>
              共有をやめる
            </Button>
          </>
        ) : (
          <>
            <p>このタブを秘書に見せます。秘書が画面を読み取る・操作するのはこのタブだけです。</p>
            <label className="mt-2 flex items-center gap-2 text-sm">
              <input type="checkbox" checked={allowActions} onChange={(e) => setAllowActions(e.target.checked)} />
              操作も任せる
            </label>
            <Button
              className="mt-3 w-full"
              size="sm"
              disabled={busy || !tab}
              onClick={() =>
                tab && run(() => api.grant(profile.profileId, tab.id, (tab.title || tab.url || "Cloud tab").slice(0, 80), allowActions))
              }
            >
              共有する
            </Button>
          </>
        )}
        {failed ? (
          <p role="alert" className="mt-2 text-red-600 text-xs">
            変更できませんでした。もう一度お試しください。
          </p>
        ) : null}
      </PopoverContent>
    </Popover>
  );
}

function DialogPanel({ dialog, onAnswer }: { dialog: { dialogType: string; message: string }; onAnswer(accept: boolean, text?: string): void }) {
  const [text, setText] = useState("");
  return (
    <div role="alertdialog" aria-label="ページからの確認" className="absolute inset-x-0 top-6 mx-auto w-full max-w-sm rounded-lg border border-border bg-background p-4 shadow-lg">
      <p className="text-muted-foreground text-xs">ページからの{dialog.dialogType === "confirm" ? "確認" : dialog.dialogType === "prompt" ? "入力" : "お知らせ"}</p>
      <p className="mt-1 whitespace-pre-wrap break-words text-sm">{dialog.message}</p>
      {dialog.dialogType === "prompt" ? (
        <input aria-label="入力" className="mt-2 h-8 w-full rounded-md border border-border bg-background px-2 text-sm" value={text} onChange={(e) => setText(e.target.value)} />
      ) : null}
      <div className="mt-3 flex justify-end gap-2">
        {dialog.dialogType === "alert" ? null : (
          <Button size="sm" variant="outline" onClick={() => onAnswer(false)}>
            キャンセル
          </Button>
        )}
        <Button size="sm" onClick={() => onAnswer(true, dialog.dialogType === "prompt" ? text : undefined)}>
          OK
        </Button>
      </div>
    </div>
  );
}

function ResetButton({ api, profileId, onChanged }: { api: CloudBrowserAPI; profileId: string; onChanged(): void }) {
  const [confirming, setConfirming] = useState(false);
  if (!confirming)
    return (
      <button type="button" className="ml-auto underline-offset-2 hover:underline" onClick={() => setConfirming(true)}>
        ブラウザをリセット
      </button>
    );
  return (
    <span className="ml-auto flex items-center gap-2">
      保存したログイン状態とタブを消去します。
      <Button
        size="sm"
        variant="destructive"
        className="h-7"
        onClick={async () => {
          try {
            await api.resetProfile(profileId);
          } finally {
            setConfirming(false);
            onChanged();
          }
        }}
      >
        リセット
      </Button>
      <Button size="sm" variant="ghost" className="h-7" onClick={() => setConfirming(false)}>
        やめる
      </Button>
    </span>
  );
}

function JevSettings({ api, jev, onChanged }: { api: CloudBrowserAPI; jev: { configured: boolean; rejected: boolean }; onChanged(): void }) {
  const [key, setKey] = useState("");
  const [busy, setBusy] = useState(false);
  const [failed, setFailed] = useState(false);
  const run = async (effect: () => Promise<unknown>) => {
    setBusy(true);
    setFailed(false);
    try {
      await effect();
      setKey("");
      onChanged();
    } catch {
      setFailed(true);
    } finally {
      setBusy(false);
    }
  };
  return (
    <Popover>
      <PopoverTrigger render={<Button size="sm" variant="ghost" className="h-8 gap-1.5" />}>
        <KeyRound className="size-3.5" />
        <span className="hidden sm:inline">Jev {jev.configured ? (jev.rejected ? "（要確認）" : "設定済み") : "未設定"}</span>
      </PopoverTrigger>
      <PopoverContent align="end" className="w-80 p-3 text-sm">
        <p className="font-medium">Jev（任意）</p>
        <p className="mt-1 text-muted-foreground text-xs leading-5">
          秘書は Jev がなくても共有タブを直接読み取り・操作できます。Jev の API キーを登録すると、秘書が「〜まで進めて」のような作業をまとめて任せられます。キーは暗号化して保存し、表示しません。
        </p>
        {jev.rejected ? <p className="mt-2 text-amber-700 text-xs">登録済みのキーが Jev に受け付けられませんでした。新しいキーを登録してください。</p> : null}
        <input
          type="password"
          aria-label="Jev の API キー"
          autoComplete="off"
          placeholder={jev.configured ? "新しいキーで置き換える" : "API キー"}
          className="mt-2 h-8 w-full rounded-md border border-border bg-background px-2 text-sm"
          value={key}
          onChange={(e) => setKey(e.target.value)}
        />
        <div className="mt-2 flex gap-2">
          <Button size="sm" className="flex-1" disabled={busy || !key.trim()} onClick={() => run(() => api.setJevKey(key.trim()))}>
            保存
          </Button>
          {jev.configured ? (
            <Button size="sm" variant="outline" disabled={busy} onClick={() => run(() => api.deleteJevKey())}>
              削除
            </Button>
          ) : null}
        </div>
        {failed ? (
          <p role="alert" className="mt-2 text-red-600 text-xs">
            保存できませんでした。キーを確認してください。
          </p>
        ) : null}
      </PopoverContent>
    </Popover>
  );
}
