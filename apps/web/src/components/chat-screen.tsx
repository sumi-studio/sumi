import { Button } from "@sumi/ui/components/button";
import { ArrowDown, History } from "lucide-react";
import {
  lazy,
  Suspense,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import type { ChatItem } from "../agent/model";
import { collectAgentCopyText, projectConversation } from "../agent/projection";
import { useConversation } from "../agent/store";
import { hasInspectableTrace } from "../agent/work-summary";
import { ChatPromptInput } from "./chat-prompt-input";
import {
  ConversationVirtualizer,
  type ConversationVirtualizerHandle,
} from "./conversation-virtualizer";
import {
  createConversationTimeline,
  MobileTimelineSheet,
  TimelineScrubber,
} from "./timeline-scrubber";

const ChatItemView = lazy(() =>
  import("./chat-item").then((module) => ({ default: module.ChatItemView })),
);

export function ChatScreen({
  installationId,
  authorityEpoch,
}: {
  installationId: string;
  authorityEpoch: string;
}) {
  return (
    <ChatScreenContent
      installationId={installationId}
      authorityEpoch={authorityEpoch}
    />
  );
}

const WAITING_ROW_ID = "__sumi_waiting_for_first_token__";
/**
 * Breathing room below the last row. Kept as virtualizer padding instead of
 * a trailing spacer row: a constant trailing key would hide appends from the
 * virtualizer's end-follow detection, leaving the view behind new messages.
 */
const CONVERSATION_BOTTOM_PADDING = 24;

type ConversationRow =
  | ChatItem
  | { id: typeof WAITING_ROW_ID; kind: "waiting" };

function ChatScreenContent({
  installationId,
  authorityEpoch,
}: {
  installationId: string;
  authorityEpoch: string;
}) {
  const {
    conversation,
    running,
    sendingApprovalRequestId,
    connection,
    ready,
    lastError,
    recoverableDrafts,
    acquireConnection,
    disconnect,
    resumeMountedConnection,
    sendMessage,
    restoreDraft,
    abort,
    decideApproval,
  } = useConversation();
  const [draft, setDraft] = useState("");
  const [timelineOpen, setTimelineOpen] = useState(false);
  const [atEnd, setAtEnd] = useState(true);
  const [visibleMessageIds, setVisibleMessageIds] = useState<string[]>([]);
  const conversationRef = useRef<ConversationVirtualizerHandle>(null);
  const items = useMemo(
    () => projectConversation(conversation),
    [conversation],
  );
  const copyTextByRunId = useMemo(
    () => collectAgentCopyText(conversation),
    [conversation],
  );
  const timeline = useMemo(
    () => createConversationTimeline(items, visibleMessageIds),
    [items, visibleMessageIds],
  );

  useEffect(() => {
    return acquireConnection({ installationId, authorityEpoch });
  }, [acquireConnection, authorityEpoch, installationId]);

  const available = connection === "connected" && ready === "ready";
  const send = () => {
    const text = draft.trim();
    if (!text || !sendMessage(text)) return;
    setDraft("");
    requestAnimationFrame(() => scrollToEnd());
  };
  const lastItem = items.at(-1);
  const waitingForFirstToken =
    running &&
    (!lastItem ||
      lastItem.kind === "user" ||
      (lastItem.kind === "agent-run" && !hasInspectableTrace(lastItem.trace)));
  const rows = useMemo<ConversationRow[]>(() => {
    if (items.length === 0) return [];
    const nextRows: ConversationRow[] = [...items];
    if (waitingForFirstToken) {
      nextRows.push({ id: WAITING_ROW_ID, kind: "waiting" });
    }
    return nextRows;
  }, [items, waitingForFirstToken]);
  const onVisibleRowsChange = useCallback(
    (ids: string[]) => {
      const messageIds = new Set(items.map((item) => item.id));
      setVisibleMessageIds(ids.filter((id) => messageIds.has(id)));
    },
    [items],
  );
  const scrollToEnd = useCallback((behavior: "smooth" | "auto" = "smooth") => {
    conversationRef.current?.scrollToEnd({ behavior });
  }, []);
  const scrollToMessage = useCallback(
    (messageId: string) =>
      conversationRef.current?.scrollToMessage(messageId, {
        align: "start",
        behavior: "smooth",
      }),
    [],
  );
  const lastAssistantMessage = items.findLast(
    (item) => item.kind === "prose" && item.agentMessageFinal,
  );
  const status = describeAvailability(connection, ready);
  const retryAgent = () => {
    disconnect();
    resumeMountedConnection();
  };

  return (
    <div className="flex h-full bg-background text-foreground">
      <main className="flex min-w-0 flex-1 flex-col">
        <header className="flex h-12 shrink-0 items-center gap-3 border-border/70 border-b px-3 sm:px-5">
          <div className="min-w-0 flex-1">
            <h1 className="truncate font-semibold text-[15px]">Sumi</h1>
          </div>
          <span
            className="flex items-center gap-1.5 text-muted-foreground text-xs"
            role="status"
          >
            <span
              className={`size-2 rounded-full ${
                available
                  ? "bg-emerald-500"
                  : connection === "connecting" || ready === "unknown"
                    ? "animate-pulse bg-amber-400"
                    : "bg-neutral-400"
              }`}
            />
            {status}
          </span>
          {timeline.ticks.length > 1 && (
            <Button
              variant="ghost"
              size="icon"
              aria-label="会話タイムライン"
              onClick={() => setTimelineOpen(true)}
              className="md:hidden"
            >
              <History className="size-4.5" />
            </Button>
          )}
        </header>

        <div className="relative min-h-0 flex-1">
          <ConversationVirtualizer
            ref={conversationRef}
            items={rows}
            busy={running}
            paddingEnd={CONVERSATION_BOTTOM_PADDING}
            ariaLabel="Sumiとの会話"
            className="scroll-fade-b scrollbar-ui scrollbar-gutter-stable size-full min-h-0 min-w-0 overscroll-contain contain-content"
            onAtEndChange={setAtEnd}
            onVisibleMessageIdsChange={onVisibleRowsChange}
            renderTranscriptItem={(row) => {
              const text = transcriptText(row);
              return text === null ? null : (
                <p className="whitespace-pre-wrap text-sm">{text}</p>
              );
            }}
            renderItem={(row) => {
              if (row.kind === "waiting") {
                return (
                  <div
                    role="status"
                    className="mx-auto w-full max-w-2xl px-4 py-3 sm:px-6"
                  >
                    <span className="inline-block size-2.5 animate-pulse rounded-full bg-neutral-400" />
                    <span className="sr-only">Sumiが応答を考えています</span>
                  </div>
                );
              }
              return (
                <div className="mx-auto w-full max-w-2xl px-4 sm:px-6">
                  <Suspense fallback={null}>
                    <ChatItemView
                      item={row}
                      copyAlwaysVisible={
                        row.kind === "prose" &&
                        row.id === lastAssistantMessage?.id &&
                        !row.streaming
                      }
                      agentMessageCopyText={
                        row.kind === "prose" && row.runId
                          ? copyTextByRunId.get(row.runId)
                          : undefined
                      }
                      onApprovalDecision={decideApproval}
                      sendingApprovalRequestId={sendingApprovalRequestId}
                    />
                  </Suspense>
                </div>
              );
            }}
          />
          {items.length === 0 && (
            <div className="pointer-events-none absolute inset-0">
              <EmptyState available={available} />
            </div>
          )}
          {items.length > 0 && !atEnd && (
            <Button
              variant="outline"
              size="icon-lg"
              aria-label="最新へ移動"
              onClick={() => scrollToEnd()}
              className="absolute right-3 bottom-3 rounded-full border-border bg-background shadow-[0_2px_12px_rgba(0,0,0,0.08)]"
            >
              <ArrowDown />
            </Button>
          )}

          {timeline.ticks.length > 1 && (
            <div className="-translate-y-1/2 absolute top-1/2 left-2 hidden h-[72%] md:block">
              <TimelineScrubber
                ticks={timeline.ticks}
                visibleRange={timeline.visibleRange}
                onJump={(index) => {
                  const messageId = timeline.messageIds[index];
                  if (messageId) {
                    scrollToMessage(messageId);
                  }
                }}
              />
            </div>
          )}
        </div>

        <div className="mx-auto w-full max-w-2xl shrink-0 px-3 pb-[max(0.75rem,env(safe-area-inset-bottom))] sm:px-4 sm:pb-4">
          {recoverableDrafts.length > 0 && (
            <section
              aria-label="送信されなかったメッセージ"
              aria-live="polite"
              className="mb-2 space-y-2 rounded-xl border border-amber-200 bg-amber-50 p-3"
            >
              <div>
                <h2 className="font-medium text-amber-950 text-sm">
                  送信されなかったメッセージ
                </h2>
                <p className="mt-0.5 text-amber-800 text-xs">
                  {draft.length > 0
                    ? "現在の入力を保持するため、入力欄を空にしてから戻してください。"
                    : "内容を確認して入力欄へ戻せます。自動では再送しません。"}
                </p>
              </div>
              {recoverableDrafts.map((recoverable) => (
                <div
                  key={recoverable.idempotencyKey}
                  className="rounded-lg border border-amber-200/80 bg-background p-2.5"
                >
                  <p className="whitespace-pre-wrap break-words text-foreground text-sm">
                    {previewRecoverableText(recoverable.text)}
                  </p>
                  <div className="mt-2 flex items-center justify-between gap-3">
                    <span className="text-muted-foreground text-xs">
                      {describeRecoveryReason(recoverable.reason)}
                    </span>
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      disabled={draft.length > 0}
                      onClick={() => {
                        const restored = restoreDraft(
                          recoverable.idempotencyKey,
                        );
                        if (restored !== undefined) setDraft(restored);
                      }}
                    >
                      入力欄に戻す
                    </Button>
                  </div>
                </div>
              ))}
            </section>
          )}
          {lastError && (
            <p
              role="alert"
              className="mb-2 rounded-xl bg-red-50 px-3 py-2 text-red-700 text-sm"
            >
              {lastError}
            </p>
          )}
          {ready === "not_ready" && (
            <section
              role="alert"
              className="mb-2 flex items-center justify-between gap-3 rounded-xl border border-amber-200 bg-amber-50 px-3 py-2 text-amber-900 text-sm"
            >
              <span>
                エージェントを起動できませんでした。しばらくしてから再試行してください。
              </span>
              <Button
                type="button"
                size="sm"
                variant="outline"
                onClick={retryAgent}
              >
                再試行
              </Button>
            </section>
          )}
          <ChatPromptInput
            value={draft}
            onValueChange={setDraft}
            onSend={send}
            onAbort={abort}
            streaming={running}
            disabled={!available}
            placeholder={composerPlaceholder(connection, ready)}
          />
        </div>

        <MobileTimelineSheet
          open={timelineOpen}
          onOpenChange={setTimelineOpen}
          ticks={timeline.ticks}
          onJump={(index) => {
            const messageId = timeline.messageIds[index];
            if (messageId) {
              scrollToMessage(messageId);
            }
            setTimelineOpen(false);
          }}
        />
        <p className="sr-only" role="status" aria-live="polite">
          {running ? "Sumiが応答中です" : "応答を待機しています"}
        </p>
      </main>
    </div>
  );
}

function transcriptText(row: ConversationRow): string | null {
  if (row.kind === "waiting") return null;
  switch (row.kind) {
    case "user":
      return `あなた: ${row.text}`;
    case "prose":
      return `Sumi: ${row.text}`;
    case "approval":
      return [
        `承認: ${row.summary}`,
        row.reason,
        row.status === "pending" ? "回答待ち" : `結果: ${row.status}`,
      ]
        .filter(Boolean)
        .join("\n");
    case "steer":
      return `応答への追加指示 (${row.mode})`;
    case "error":
      return `エラー: ${row.message}`;
    case "card":
      return `カード:\n${JSON.stringify(row.node)}`;
    case "agent-run":
      return [
        "エージェントの作業:",
        ...row.trace.map((trace) => {
          switch (trace.type) {
            case "reasoning":
              return trace.text;
            case "tool":
              return `${trace.label} (${trace.status})`;
            case "approval":
              return `${trace.summary} (${trace.status})`;
            case "artifact":
              return trace.label;
            case "error":
              return `エラー: ${trace.message}`;
          }
          return "";
        }),
      ].join("\n");
  }
}

function EmptyState({ available }: { available: boolean }) {
  return (
    <div className="flex h-full flex-col items-center justify-center gap-3 px-6 text-center">
      <h2 className="font-semibold text-2xl text-neutral-800">こんにちは</h2>
      <p className="max-w-sm text-neutral-500 text-sm leading-6">
        {available
          ? "なんでも話しかけてください。"
          : "あなたのエージェントへ接続しています。"}
      </p>
    </div>
  );
}

function describeAvailability(
  connection: "connecting" | "connected" | "closed",
  ready: "unknown" | "ready" | "not_ready",
) {
  // "not_ready" only ever comes from the server saying so: an in-band status
  // frame, or the close code the API uses to name a runtime it could not
  // start. It is a stated fact about the agent, not something inferred from a
  // close the browser cannot attribute, so it outranks the transport blip
  // "再接続中" describes. It must not hide that a retry is already in flight.
  if (ready === "not_ready")
    return connection === "connected"
      ? "エージェント利用不可"
      : "エージェント利用不可（再接続中）";
  if (connection === "connecting") return "接続中";
  if (connection === "closed") return "再接続中";
  if (ready === "ready") return "エージェント利用可能";
  return "エージェント確認中";
}

function composerPlaceholder(
  connection: "connecting" | "connected" | "closed",
  ready: "unknown" | "ready" | "not_ready",
) {
  if (connection !== "connected") return "接続を待っています…";
  if (ready === "not_ready") return "現在エージェントを利用できません";
  if (ready === "unknown") return "エージェントを確認しています…";
  return "メッセージを入力…";
}

function previewRecoverableText(text: string): string {
  const maxLength = 280;
  return text.length > maxLength ? `${text.slice(0, maxLength)}…` : text;
}

function describeRecoveryReason(reason: string): string {
  switch (reason) {
    case "superseded":
      return "別の操作に置き換えられました";
    case "unavailable":
      return "エージェントへ届けられませんでした";
    case "oversized":
      return "送信できる長さを超えています";
    default:
      return "送信されませんでした";
  }
}
