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
import { AppNavigation } from "./app-navigation";
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

export function ChatScreen() {
  return <ChatScreenContent />;
}

const WAITING_ROW_ID = "__sumi_waiting_for_first_token__";
const BOTTOM_SPACER_ROW_ID = "__sumi_conversation_bottom_spacer__";

type ConversationRow =
  | ChatItem
  | { id: typeof WAITING_ROW_ID; kind: "waiting" }
  | { id: typeof BOTTOM_SPACER_ROW_ID; kind: "spacer" };

function ChatScreenContent() {
  const {
    conversation,
    running,
    connection,
    ready,
    lastError,
    connect,
    disconnect,
    sendMessage,
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
    connect();
    return disconnect;
  }, [connect, disconnect]);

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
    return [
      ...items,
      ...(waitingForFirstToken
        ? [{ id: WAITING_ROW_ID, kind: "waiting" as const }]
        : []),
      { id: BOTTOM_SPACER_ROW_ID, kind: "spacer" },
    ];
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

  return (
    <div className="flex h-dvh bg-background text-foreground">
      <AppNavigation />
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
            ariaLabel="Sumiとの会話"
            className="scroll-fade-b scrollbar-ui scrollbar-gutter-stable size-full min-h-0 min-w-0 overscroll-contain contain-content"
            onAtEndChange={setAtEnd}
            onVisibleMessageIdsChange={onVisibleRowsChange}
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
              if (row.kind === "spacer") return <div className="h-6" />;

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
          {lastError && (
            <p
              role="alert"
              className="mb-2 rounded-xl bg-red-50 px-3 py-2 text-red-700 text-sm"
            >
              {lastError}
            </p>
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
  if (connection === "connecting") return "接続中";
  if (connection === "closed") return "再接続中";
  if (ready === "ready") return "エージェント利用可能";
  if (ready === "not_ready") return "エージェント利用不可";
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
