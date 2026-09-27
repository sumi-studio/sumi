import { Button } from "@sumi/ui/components/button";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@sumi/ui/components/popover";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@sumi/ui/components/tooltip";
import { FitAddon } from "@xterm/addon-fit";
import { Terminal } from "@xterm/xterm";
import "@xterm/xterm/css/xterm.css";
import {
  CircleAlert,
  Hand,
  Keyboard,
  Plus,
  RefreshCw,
  SendHorizontal,
  SquareTerminal,
  WifiOff,
  X,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useParticipantApps } from "../participant/app-store";
import {
  TERMINAL_SIGNALS,
  TerminalAPIError,
  TerminalAPIUncertainError,
  TerminalApiClient,
  type TerminalScope,
} from "./api";
import {
  TerminalAttach,
  type TerminalAttachState,
  type TerminalEndedInfo,
} from "./attach";
import {
  isLiveTerminalStatus,
  isTerminalAcceptingInput,
  type TerminalInputReceipt,
  type TerminalSession,
  terminalInputUnresolved,
  terminalStatusLabel,
} from "./model";

const LIST_REFRESH_MS = 15_000;
const SESSION_REFRESH_MS = 15_000;
const NOTICE_TTL_MS = 4_000;
const INPUT_OBSERVE_MS = 2_500;
const RESIZE_RETRY_MAX = 3;

/**
 * The shared Cloud terminal screen: one named durable session that the
 * person and the secretary both operate. The viewer is only an attach —
 * leaving the route detaches the socket and the session keeps running.
 * Reopening replays retained output from the durable stream base, with
 * explicit gap markers where retention trimmed bytes.
 */
export function TerminalScreen({
  installationId,
  authorityEpoch,
  sessionId,
  onSelectSession,
  apiClient,
  onAuthorizationExpired,
}: TerminalScope & {
  sessionId?: string;
  onSelectSession?: (sessionId: string | null) => void;
  apiClient?: TerminalApiClient;
  onAuthorizationExpired?: () => Promise<void>;
}) {
  const client = useMemo(
    () => apiClient ?? new TerminalApiClient({ installationId, authorityEpoch }),
    [installationId, authorityEpoch, apiClient],
  );
  const [sessions, setSessions] = useState<TerminalSession[] | null>(null);
  const [listError, setListError] = useState<string | null>(null);
  const [internalSelection, setInternalSelection] = useState<string | null>(
    null,
  );
  const selected = sessionId ?? internalSelection;
  const select = useCallback(
    (id: string | null) => {
      if (onSelectSession) onSelectSession(id);
      else setInternalSelection(id);
    },
    [onSelectSession],
  );
  const [creating, setCreating] = useState(false);
  const [createOpen, setCreateOpen] = useState(false);
  const [newName, setNewName] = useState("");
  const [openError, setOpenError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const list = await client.listSessions();
      setSessions(list);
      setListError(null);
    } catch (error) {
      setListError(terminalErrorMessage(error));
    }
  }, [client]);

  useEffect(() => {
    void refresh();
    const timer = setInterval(() => void refresh(), LIST_REFRESH_MS);
    return () => clearInterval(timer);
  }, [refresh]);

  const patchSession = useCallback((session: TerminalSession) => {
    setSessions((previous) =>
      previous === null
        ? previous
        : previous.map((entry) =>
            entry.sessionId === session.sessionId ? session : entry,
          ),
    );
  }, []);

  const openSession = useCallback(async () => {
    setCreating(true);
    setOpenError(null);
    try {
      const session = await client.openSession(newName.trim());
      setSessions((previous) => [...(previous ?? []), session]);
      setNewName("");
      setCreateOpen(false);
      select(session.sessionId);
    } catch (error) {
      setOpenError(terminalErrorMessage(error));
    } finally {
      setCreating(false);
    }
  }, [client, newName, select]);

  const selectedSession = sessions?.find(
    (entry) => entry.sessionId === selected,
  );

  return (
    <div className="flex h-full flex-col bg-background text-foreground">
      <header className="flex items-center gap-2 border-border border-b px-3 py-2">
        <SquareTerminal
          className="size-4 shrink-0 text-muted-foreground"
          aria-hidden
        />
        <h1 className="shrink-0 font-semibold text-sm tracking-tight">
          ターミナル
        </h1>
        <label className="sr-only" htmlFor="terminal-session-picker">
          セッションを選択
        </label>
        <select
          id="terminal-session-picker"
          className="h-8 min-w-0 flex-1 rounded-md border border-border bg-background px-2 text-sm sm:max-w-64"
          value={selected ?? ""}
          onChange={(event) =>
            select(event.target.value === "" ? null : event.target.value)
          }
        >
          <option value="">セッションを選択…</option>
          {(sessions ?? []).map((session) => (
            <option key={session.sessionId} value={session.sessionId}>
              {session.name || "ターミナル"} —{" "}
              {terminalStatusLabel(session.status)}
            </option>
          ))}
        </select>
        {selectedSession ? <StatusPill session={selectedSession} /> : null}
        <Button
          variant="ghost"
          size="icon"
          className="size-8 shrink-0"
          aria-label="セッション一覧を更新"
          onClick={() => void refresh()}
        >
          <RefreshCw className="size-4" />
        </Button>
        <Popover open={createOpen} onOpenChange={setCreateOpen}>
          <PopoverTrigger
            render={
              <Button
                variant="outline"
                size="sm"
                className="shrink-0 gap-1.5"
                aria-label="新しいターミナルセッション"
              >
                <Plus className="size-4" />
                <span className="hidden sm:inline">新規</span>
              </Button>
            }
          />
          <PopoverContent align="end" className="w-72 p-3">
            <form
              onSubmit={(event) => {
                event.preventDefault();
                void openSession();
              }}
            >
              <label
                htmlFor="terminal-new-name"
                className="font-medium text-sm"
              >
                新しいターミナル
              </label>
              <input
                id="terminal-new-name"
                className="mt-2 h-8 w-full rounded-md border border-border bg-background px-2 text-sm"
                placeholder="名前（省略可）"
                maxLength={80}
                value={newName}
                onChange={(event) => setNewName(event.target.value)}
              />
              <p className="mt-1.5 text-muted-foreground text-xs">
                あなたと秘書が同じセッションを共有します。
              </p>
              {openError ? (
                <p role="alert" className="mt-1.5 text-red-600 text-xs">
                  {openError}
                </p>
              ) : null}
              <Button
                type="submit"
                size="sm"
                className="mt-2 w-full"
                disabled={creating}
              >
                {creating ? "作成しています…" : "作成"}
              </Button>
            </form>
          </PopoverContent>
        </Popover>
      </header>

      {listError ? (
        <div
          role="alert"
          className="flex items-center gap-2 border-border border-b bg-muted/40 px-3 py-2 text-xs"
        >
          <CircleAlert className="size-4 shrink-0 text-red-600" />
          <span className="min-w-0 flex-1">
            セッション一覧を読み込めません：{listError}
          </span>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => void refresh()}
            className="h-7 shrink-0"
          >
            再試行
          </Button>
        </div>
      ) : null}

      {selected ? (
        <TerminalSessionView
          key={`${installationId}:${selected}`}
          client={client}
          scope={{ installationId, authorityEpoch }}
          sessionId={selected}
          session={selectedSession}
          onSession={patchSession}
          onDeselect={() => select(null)}
          onAuthorizationExpired={onAuthorizationExpired}
        />
      ) : (
        <div className="grid flex-1 place-items-center px-6">
          <div className="flex max-w-sm flex-col items-center text-center">
            <span className="mb-4 grid size-11 place-items-center rounded-xl border border-border bg-muted/35">
              <SquareTerminal className="size-5 text-muted-foreground" />
            </span>
            <h2 className="font-semibold text-base tracking-tight">
              セッションを選んでください
            </h2>
            <p className="mt-2 text-muted-foreground text-sm leading-6">
              セッションはサーバーに保持されます。この画面を閉じても終了せず、
              あとで同じところから続きを見られます。
            </p>
          </div>
        </div>
      )}
    </div>
  );
}

function StatusPill({ session }: { session: TerminalSession }) {
  const live = isLiveTerminalStatus(session.status);
  return (
    <span
      className={`hidden shrink-0 items-center gap-1.5 rounded-full border border-border px-2 py-0.5 text-xs sm:inline-flex ${
        live ? "" : "text-muted-foreground"
      }`}
    >
      <span
        className={`size-1.5 rounded-full ${
          session.status === "active"
            ? "bg-emerald-500"
            : live
              ? "bg-amber-500"
              : "bg-muted-foreground"
        }`}
      />
      {terminalStatusLabel(session.status)}
    </span>
  );
}

interface ViewNotice {
  key: number;
  text: string;
}

/**
 * One attach to one session. Mount = open the viewer socket; unmount =
 * detach only. All durable mutations (control, close) go through REST so
 * the response carries the authoritative session row back.
 */
export function TerminalSessionView({
  client,
  scope,
  sessionId,
  session,
  onSession,
  onDeselect,
  onAuthorizationExpired,
}: {
  client: TerminalApiClient;
  scope: TerminalScope;
  sessionId: string;
  session: TerminalSession | undefined;
  onSession: (session: TerminalSession) => void;
  onDeselect: () => void;
  onAuthorizationExpired?: () => Promise<void>;
}) {
  const refreshParticipantApps = useParticipantApps((state) => state.refresh);

  const [current, setCurrent] = useState<TerminalSession | null>(
    session ?? null,
  );
  const [connState, setConnState] = useState<TerminalAttachState>("connecting");
  const [willRetry, setWillRetry] = useState(true);
  const [ended, setEnded] = useState<TerminalEndedInfo | null>(null);
  const [gapBytes, setGapBytes] = useState(0);
  const [lossBoundaries, setLossBoundaries] = useState(0);
  const [notice, setNotice] = useState<ViewNotice | null>(null);
  const [serverError, setServerError] = useState<string | null>(null);
  const [lastAck, setLastAck] = useState<TerminalInputReceipt | null>(null);
  const [confirmClose, setConfirmClose] = useState(false);
  const [pending, setPending] = useState<string | null>(null);
  const [fatalError, setFatalError] = useState<string | null>(null);

  const hostRef = useRef<HTMLDivElement | null>(null);
  const attachRef = useRef<TerminalAttach | null>(null);
  const termRef = useRef<Terminal | null>(null);
  const noticeSeq = useRef(0);
  const noticeTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  // Latest authoritative input-admission state. Every path that learns the
  // session lifecycle — WS session frames, REST mutation responses and the
  // session poll — feeds this through applySession, so housekeeping input
  // (resize) and user input (stdin) never run on a stale snapshot.
  const admissionRef = useRef(
    session === undefined
      ? { accepting: false, final: false }
      : {
          accepting: isTerminalAcceptingInput(session),
          final: !isLiveTerminalStatus(session.status),
        },
  );

  const flashNotice = useCallback((text: string) => {
    noticeSeq.current += 1;
    setNotice({ key: noticeSeq.current, text });
    if (noticeTimer.current) clearTimeout(noticeTimer.current);
    noticeTimer.current = setTimeout(() => setNotice(null), NOTICE_TTL_MS);
  }, []);

  const applySession = useCallback(
    (next: TerminalSession) => {
      // Session lifecycle is monotonic — once a final state is
      // authoritative, a delayed REST/poll response claiming a live
      // status is stale and must not resurrect admission.
      if (admissionRef.current.final && isLiveTerminalStatus(next.status)) {
        return;
      }
      admissionRef.current = {
        accepting: isTerminalAcceptingInput(next),
        final: !isLiveTerminalStatus(next.status),
      };
      setCurrent(next);
      onSession(next);
    },
    [onSession],
  );

  // The attach scope is fixed per mounted view (keyed remount on change).
  // biome-ignore lint/correctness/useExhaustiveDependencies: the effect intentionally binds the mount scope only.
  useEffect(() => {
    const host = hostRef.current;
    if (!host) return;

    const term = new Terminal({
      convertEol: false,
      cursorBlink: true,
      disableStdin: false,
      fontFamily:
        'ui-monospace, "SF Mono", Menlo, Consolas, "Noto Sans Mono CJK JP", monospace',
      fontSize: 13,
      scrollback: 10_000,
      screenReaderMode: true,
      theme: xtermThemeFromDocument(),
    });
    const fit = new FitAddon();
    term.loadAddon(fit);
    term.open(host);
    fit.fit();

    // Resize is housekeeping input the session can reject: it must follow
    // the latest authoritative admission state (admissionRef), rechecked at
    // fire time — otherwise a resize scheduled while live lands after the
    // session stopped accepting input and the server correctly rejects it,
    // producing a stale error banner next to the real lifecycle result.
    let resizeTimer: ReturnType<typeof setTimeout> | null = null;
    const attach = new TerminalAttach({
      scope,
      sessionId,
      events: {
        onSession: (next) => {
          applySession(next);
          // The server sends a session frame on every connect, so this
          // covers the first attach and every reconnect — no resize on
          // socket open, which would race the fresh lifecycle state.
          if (admissionRef.current.accepting) {
            attach.sendResize(term.cols, term.rows);
          }
        },
        onOutput: (_base, data) => term.write(data),
        onGap: (base, to) => {
          if (to !== null && to === base) {
            // Zero-width journal-loss boundary: the runtime lost an
            // unknown amount of output at this position — name the
            // boundary honestly instead of inventing a byte range.
            setLossBoundaries((previous) => previous + 1);
            term.writeln(
              "\x1b[2;33m―― この位置で出力が失われた可能性があります（ジャーナル断絶）――\x1b[0m",
            );
            return;
          }
          const missing = to !== null ? Math.max(0, to - base) : 0;
          setGapBytes((previous) => previous + missing);
          term.writeln(
            "\x1b[2;33m―― 出力が欠落しています（サーバー保持範囲外）――\x1b[0m",
          );
        },
        onInputAck: (ack) => setLastAck(ack),
        onEnded: (info) => {
          admissionRef.current = { accepting: false, final: true };
          if (resizeTimer) {
            clearTimeout(resizeTimer);
            resizeTimer = null;
          }
          setEnded(info);
          term.options.disableStdin = true;
        },
        onServerError: (code, message, inputKind) => {
          // Rejected housekeeping input (resize during a lifecycle
          // transition, etc.) must not surface as a user-facing
          // failure — the ledger observer retries/surfaces it when it
          // matters. Genuine input rejections still show.
          if (inputKind === "resize") return;
          setServerError(serverErrorMessage(code, message));
        },
        onConnection: (state, info) => {
          setConnState(state);
          if (state === "open") setServerError(null);
          if (state === "closed") {
            setWillRetry(info?.willRetry ?? false);
            if (info?.reason === "authorization") {
              // The install epoch may have rotated; re-read the binding.
              void (onAuthorizationExpired ?? refreshParticipantApps)().catch(() => undefined);
            }
          }
        },
      },
    });
    attachRef.current = attach;
    termRef.current = term;
    attach.open();

    const resizeObserver = new ResizeObserver(() => {
      try {
        fit.fit();
      } catch {
        // Host is mid-layout; the next observation retries.
      }
    });
    resizeObserver.observe(host);

    const resizeSub = term.onResize(({ cols, rows }) => {
      if (!admissionRef.current.accepting) return;
      if (resizeTimer) clearTimeout(resizeTimer);
      resizeTimer = setTimeout(() => {
        resizeTimer = null;
        if (admissionRef.current.accepting) attach.sendResize(cols, rows);
      }, 150);
    });
    const dataSub = term.onData((data) => {
      if (!admissionRef.current.accepting) {
        flashNotice(
          admissionRef.current.final
            ? "セッションはすでに終了しています"
            : "セッションはまだ入力を受け付けていません",
        );
        return;
      }
      if (!attach.sendStdin(data)) {
        flashNotice("接続されていないため送信できません");
      }
    });
    host.tabIndex = 0;

    return () => {
      if (resizeTimer) clearTimeout(resizeTimer);
      dataSub.dispose();
      resizeSub.dispose();
      resizeObserver.disconnect();
      attachRef.current = null;
      termRef.current = null;
      attach.detach();
      term.dispose();
    };
  }, [sessionId]);

  // Observe the durable input ledger so real delivery outcomes are
  // visible: `failed` user input surfaces a notice, `unknown` is shown
  // as unresolvable and is never resent, and a provably-undelivered
  // housekeeping resize is re-sent (bounded) since dims are idempotent.
  // The read window always restarts at the OLDEST unresolved row —
  // after_seq pages by creation order, so advancing past an `intended`
  // row would hide the failed/unknown outcome it reaches later. The
  // unresolved set survives socket reconnects because the ledger is
  // durable state, not a connection property.
  useEffect(() => {
    if (ended !== null) return;
    let cancelled = false;
    const unresolved = new Map<string, { seq: number }>();
    const reported = new Set<string>();
    let lastSeq = 0;
    let resizeRetries = 0;
    const timer = setInterval(() => {
      let windowStart = lastSeq;
      for (const row of unresolved.values()) {
        if (row.seq - 1 < windowStart) windowStart = row.seq - 1;
      }
      client
        .listInputs(sessionId, Math.max(0, windowStart))
        .then((rows) => {
          if (cancelled) return;
          for (const row of rows) {
            if (row.seq > lastSeq) lastSeq = row.seq;
            if (terminalInputUnresolved(row.status)) {
              if (!unresolved.has(row.inputId)) {
                unresolved.set(row.inputId, { seq: row.seq });
              }
              continue;
            }
            unresolved.delete(row.inputId);
            if (reported.has(row.inputId)) continue;
            if (row.status === "written") {
              if (row.kind === "resize") resizeRetries = 0;
              continue;
            }
            if (row.status !== "failed" && row.status !== "unknown") continue;
            reported.add(row.inputId);
            if (row.kind === "resize") {
              if (
                row.status === "failed" &&
                resizeRetries < RESIZE_RETRY_MAX &&
                admissionRef.current.accepting
              ) {
                resizeRetries += 1;
                const term = termRef.current;
                if (term) attachRef.current?.sendResize(term.cols, term.rows);
              }
              continue;
            }
            flashNotice(
              row.status === "failed"
                ? "入力を届けられませんでした"
                : "入力の結果が不明です（自動再送しません）",
            );
          }
        })
        .catch(() => undefined);
    }, INPUT_OBSERVE_MS);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [client, sessionId, ended, flashNotice]);

  // Poll the session row so fields the socket does not push (for example
  // control_holder after the secretary yields) stay honest.
  useEffect(() => {
    if (ended !== null) return;
    const timer = setInterval(() => {
      client
        .getSession(sessionId)
        .then(applySession)
        .catch((error) => {
          if (error instanceof TerminalAPIError && error.code === "not_found") {
            setFatalError(
              "セッションが見つかりません。削除された可能性があります。",
            );
          }
        });
    }, SESSION_REFRESH_MS);
    return () => clearInterval(timer);
  }, [client, sessionId, ended, applySession]);

  useEffect(
    () => () => {
      if (noticeTimer.current) clearTimeout(noticeTimer.current);
    },
    [],
  );

  const status = ended?.status ?? current?.status ?? "requested";
  const acceptingInput = ended === null && isTerminalAcceptingInput({ status });
  const controlHeld = current?.controlHolder === "human";

  // Keep xterm's own input gate aligned with authoritative lifecycle —
  // REST mutations and the poll learn about endings the socket has not
  // reported yet.
  useEffect(() => {
    const term = termRef.current;
    if (term) term.options.disableStdin = !acceptingInput;
  }, [acceptingInput]);

  const mutate = useCallback(
    async (label: string, operation: () => Promise<TerminalSession>) => {
      setPending(label);
      try {
        applySession(await operation());
      } catch (error) {
        if (error instanceof TerminalAPIUncertainError) {
          // The mutation may have committed; reconcile from the server
          // instead of guessing or retrying blind. The notice only claims
          // a completed refresh after the read actually succeeds.
          flashNotice("操作の結果を確認できません。状態を確認しています…");
          try {
            applySession(await client.getSession(sessionId));
            flashNotice("操作の結果は不明のままです。最新の状態に更新しました");
          } catch {
            flashNotice(
              "操作の結果を確認できず、状態の再読み込みにも失敗しました",
            );
          }
        } else {
          flashNotice(terminalErrorMessage(error));
        }
      } finally {
        setPending(null);
      }
    },
    [applySession, client, sessionId, flashNotice],
  );

  const liveSession =
    current !== null && ended === null && isLiveTerminalStatus(current.status);

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      {gapBytes > 0 ? (
        <div
          role="status"
          className="flex items-center gap-2 border-border border-b bg-amber-500/10 px-3 py-1.5 text-xs"
        >
          <CircleAlert className="size-3.5 shrink-0 text-amber-600" />
          出力の一部が欠落しています（約{gapBytes}バイト）。
          サーバーの保持範囲を超えた出力は復元できません。
        </div>
      ) : null}

      {lossBoundaries > 0 ? (
        <div
          role="status"
          className="flex items-center gap-2 border-border border-b bg-amber-500/10 px-3 py-1.5 text-xs"
        >
          <CircleAlert className="size-3.5 shrink-0 text-amber-600" />
          出力ジャーナルの断絶を検出しました（{lossBoundaries}
          箇所）。その境界で失われた量は不明です。
        </div>
      ) : null}

      {connState === "closed" && ended === null ? (
        <div
          role="alert"
          className="flex items-center gap-2 border-border border-b bg-muted/40 px-3 py-1.5 text-xs"
        >
          <WifiOff className="size-3.5 shrink-0 text-muted-foreground" />
          <span className="min-w-0 flex-1">
            {willRetry
              ? "接続が切れました。再接続しています…"
              : "接続が切れました。セッション自体は継続しています。"}
          </span>
          {!willRetry ? (
            <Button
              variant="ghost"
              size="sm"
              className="h-7 shrink-0"
              onClick={() => attachRef.current?.open()}
            >
              再接続
            </Button>
          ) : null}
        </div>
      ) : null}

      {ended !== null ? (
        <div
          role="status"
          className="flex items-center gap-2 border-border border-b bg-muted/40 px-3 py-1.5 text-xs"
        >
          <SquareTerminal className="size-3.5 shrink-0 text-muted-foreground" />
          <span className="min-w-0 flex-1">{endedSummary(ended)}</span>
          <Button
            variant="ghost"
            size="sm"
            className="h-7 shrink-0"
            onClick={onDeselect}
          >
            一覧へ戻る
          </Button>
        </div>
      ) : null}

      {current?.outputAttached === false && ended === null ? (
        <div
          role="status"
          className="flex items-center gap-2 border-border border-b bg-muted/40 px-3 py-1.5 text-xs"
        >
          <CircleAlert className="size-3.5 shrink-0 text-amber-600" />
          <span className="min-w-0 flex-1">
            出力を受信できていません。セッションは動いている可能性がありますが、新しい表示が届きません。
          </span>
        </div>
      ) : null}

      {fatalError ? (
        <div
          role="alert"
          className="flex items-center gap-2 border-border border-b bg-muted/40 px-3 py-1.5 text-red-600 text-xs"
        >
          <CircleAlert className="size-3.5 shrink-0" />
          <span className="min-w-0 flex-1">{fatalError}</span>
          <Button
            variant="ghost"
            size="sm"
            className="h-7 shrink-0"
            onClick={onDeselect}
          >
            一覧へ戻る
          </Button>
        </div>
      ) : null}

      {serverError ? (
        <div
          role="alert"
          className="flex items-center gap-2 border-border border-b bg-muted/40 px-3 py-1.5 text-red-600 text-xs"
        >
          <CircleAlert className="size-3.5 shrink-0" />
          <span className="min-w-0 flex-1">{serverError}</span>
          <Button
            variant="ghost"
            size="icon"
            className="size-7 shrink-0"
            aria-label="エラーを閉じる"
            onClick={() => setServerError(null)}
          >
            <X className="size-3.5" />
          </Button>
        </div>
      ) : null}

      <div
        ref={hostRef}
        className="min-h-0 flex-1 bg-[#0d1117] p-1"
        aria-label={`ターミナル出力: ${current?.name || sessionId}`}
        role="application"
      />

      <footer className="flex flex-wrap items-center gap-1.5 border-border border-t px-2 py-1.5">
        <Tooltip>
          <TooltipTrigger
            render={
              <Button
                variant="ghost"
                size="sm"
                className="h-8 gap-1.5"
                disabled={!acceptingInput}
                onClick={() => {
                  if (!attachRef.current?.sendStdin("\x03")) {
                    flashNotice("接続されていないため送信できません");
                  }
                }}
              >
                <Keyboard className="size-4" />
                中断
              </Button>
            }
          />
          <TooltipContent>Ctrl+C（SIGINT）を送ります</TooltipContent>
        </Tooltip>

        <Button
          variant="ghost"
          size="sm"
          className="h-8"
          disabled={!acceptingInput}
          onClick={() => {
            if (!attachRef.current?.sendEof()) {
              flashNotice("接続されていないため送信できません");
            }
          }}
        >
          EOF
        </Button>

        <Popover>
          <PopoverTrigger
            render={
              <Button
                variant="ghost"
                size="sm"
                className="h-8"
                disabled={!acceptingInput}
                aria-label="シグナルを送信"
              >
                シグナル
              </Button>
            }
          />
          <PopoverContent align="start" className="w-44 p-1">
            <p className="px-2 py-1 text-muted-foreground text-xs">
              セッションへシグナルを送信
            </p>
            {TERMINAL_SIGNALS.map((signal) => (
              <Button
                key={signal}
                variant="ghost"
                size="sm"
                className="w-full justify-start"
                onClick={() => {
                  if (!attachRef.current?.sendSignal(signal)) {
                    flashNotice("接続されていないため送信できません");
                  }
                }}
              >
                SIG{signal}
              </Button>
            ))}
          </PopoverContent>
        </Popover>

        <div className="mx-1 hidden h-5 border-border border-l sm:block" />

        <Tooltip>
          <TooltipTrigger
            render={
              <Button
                variant={controlHeld ? "secondary" : "ghost"}
                size="sm"
                className="h-8 gap-1.5"
                disabled={!liveSession || pending !== null}
                onClick={() =>
                  void mutate("control", () =>
                    client.setControl(sessionId, !controlHeld),
                  )
                }
              >
                <Hand className="size-4" />
                {controlHeld ? "独占を解除" : "操作を独占"}
              </Button>
            }
          />
          <TooltipContent>
            {controlHeld
              ? "秘書の入力を再び許可します"
              : "あなたが操作中は秘書の入力を拒否します"}
          </TooltipContent>
        </Tooltip>
        {controlHeld ? (
          <span className="rounded-full bg-secondary px-2 py-0.5 text-secondary-foreground text-xs">
            秘書の入力をブロック中
          </span>
        ) : null}

        <div className="flex-1" />

        {notice ? (
          <span
            key={notice.key}
            role="status"
            className="text-muted-foreground text-xs"
          >
            {notice.text}
          </span>
        ) : null}
        {lastAck ? (
          <Tooltip>
            <TooltipTrigger
              render={
                <span className="cursor-default rounded-full border border-border px-2 py-0.5 text-muted-foreground text-xs">
                  {ackLabel(lastAck)}
                </span>
              }
            />
            <TooltipContent>
              サーバーが入力を受理した順序です。端末への反映までは保証されません。
            </TooltipContent>
          </Tooltip>
        ) : null}

        {confirmClose ? (
          <span className="flex items-center gap-1.5">
            <span className="text-xs">セッションを終了しますか？</span>
            <Button
              variant="destructive"
              size="sm"
              className="h-7"
              disabled={pending !== null}
              onClick={() => {
                setConfirmClose(false);
                void mutate("close", () => client.closeSession(sessionId));
              }}
            >
              終了する
            </Button>
            <Button
              variant="ghost"
              size="sm"
              className="h-7"
              onClick={() => setConfirmClose(false)}
            >
              キャンセル
            </Button>
          </span>
        ) : (
          <Button
            variant="ghost"
            size="sm"
            className="h-8 gap-1.5 text-red-600 hover:text-red-600"
            disabled={!liveSession}
            onClick={() => setConfirmClose(true)}
          >
            <SendHorizontal className="size-4 rotate-180" />
            終了
          </Button>
        )}
      </footer>
    </div>
  );
}

function ackLabel(ack: TerminalInputReceipt): string {
  switch (ack.status) {
    case "written":
      return `書き込み済み #${ack.seq}`;
    case "failed":
      return `送信失敗 #${ack.seq}`;
    case "interrupted":
      return `中断 #${ack.seq}`;
    case "expired":
      return `期限切れ #${ack.seq}`;
    case "unknown":
      return `状態不明 #${ack.seq}`;
    default:
      // 'intended' / 'dequeued' mean accepted into the queue only.
      return `受理 #${ack.seq}`;
  }
}

function endedSummary(info: TerminalEndedInfo): string {
  const parts: string[] = [];
  if (info.status === "lost") {
    parts.push("セッションを見失いました");
  } else {
    parts.push("セッションは終了しました");
  }
  if (info.exitCode !== null) parts.push(`終了コード ${info.exitCode}`);
  if (info.exitSignal) parts.push(`シグナル ${info.exitSignal}`);
  switch (info.reason) {
    case "closed":
    case "closed by the person":
      parts.push("手動で終了されました");
      break;
    case "session removed":
      parts.push("セッションが削除されました");
      break;
    case "":
      break;
    default:
      parts.push(info.reason);
  }
  return parts.join(" — ");
}

function serverErrorMessage(code: string, message: string): string {
  switch (code) {
    case "ended":
      return "セッションはすでに終了しています";
    case "not_live":
      return "セッションはまだ入力を受け付けていません";
    case "control_held":
      return "操作が独占されているため拒否されました";
    case "not_found":
      return "セッションが見つかりません";
    case "bad_input":
      return `入力が拒否されました（${message}）`;
    case "unavailable":
      return "ターミナルのバックエンドを利用できません";
    default:
      return message ? `エラー: ${message}` : `エラー: ${code}`;
  }
}

export function terminalErrorMessage(error: unknown): string {
  if (error instanceof TerminalAPIError) {
    switch (error.code) {
      case "unavailable":
        return "ターミナルのバックエンドを利用できません";
      case "auth":
        return "ログイン状態を確認できません";
      case "forbidden":
        return "このターミナルへのアクセス権がありません";
      case "invalid_scope":
        return "導入情報が古くなっています。再読み込みしてください";
      case "not_found":
        return "セッションが見つかりません";
      case "ended":
        return "セッションはすでに終了しています";
      case "not_live":
        return "セッションはまだ実行中ではありません";
      case "control_held":
        return "操作が独占されています";
      case "capacity":
        return "同時に開けるセッション数の上限に達しています";
      case "bad_request":
        return "入力が拒否されました";
      default:
        return "要求に失敗しました";
    }
  }
  if (error instanceof TerminalAPIUncertainError) {
    return "応答を確認できません。状態を再読み込みしてください";
  }
  return "予期しないエラーが発生しました";
}

/** Resolve Sumi theme tokens to concrete colors xterm can parse. */
function xtermThemeFromDocument(): {
  background: string;
  foreground: string;
  cursor: string;
} {
  const fallback = {
    background: "#0d1117",
    foreground: "#e6edf3",
    cursor: "#e6edf3",
  };
  const doc = globalThis.document;
  if (!doc?.documentElement) return fallback;
  const probe = doc.createElement("span");
  probe.style.display = "none";
  doc.documentElement.appendChild(probe);
  try {
    const resolve = (variable: string, fallbackValue: string): string => {
      probe.style.color = "";
      probe.style.color = `var(${variable})`;
      const resolved = globalThis
        .getComputedStyle(probe)
        .getPropertyValue("color");
      return resolved || fallbackValue;
    };
    return {
      background: resolve("--background", fallback.background),
      foreground: resolve("--foreground", fallback.foreground),
      cursor: resolve("--foreground", fallback.cursor),
    };
  } finally {
    probe.remove();
  }
}
