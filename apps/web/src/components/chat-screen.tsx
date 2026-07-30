import {
  Conversation,
  ConversationContent,
  ConversationItem,
  ConversationProvider,
  ConversationScrollButton,
  ConversationViewport,
  useConversationScroll,
  useConversationVisibility,
} from "@sumi/ui/ai-elements/conversation";
import { Button } from "@sumi/ui/components/button";
import { History } from "lucide-react";
import { lazy, Suspense, useEffect, useMemo, useState } from "react";
import { collectAgentCopyText, projectConversation } from "../agent/projection";
import { useConversation } from "../agent/store";
import { hasInspectableTrace } from "../agent/work-summary";
import { AppNavigation } from "./app-navigation";
import { ChatPromptInput } from "./chat-prompt-input";
import {
  createConversationTimeline,
  MobileTimelineSheet,
  TimelineScrubber,
} from "./timeline-scrubber";

const ChatItemView = lazy(() =>
  import("./chat-item").then((module) => ({ default: module.ChatItemView })),
);

export function ChatScreen() {
  return (
    <ConversationProvider autoScroll defaultScrollPosition="end">
      <ChatScreenContent />
    </ConversationProvider>
  );
}

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
  const items = useMemo(
    () => projectConversation(conversation),
    [conversation],
  );
  const copyTextByRunId = useMemo(
    () => collectAgentCopyText(conversation),
    [conversation],
  );
  const { scrollToEnd, scrollToMessage } = useConversationScroll();
  const { visibleMessageIds } = useConversationVisibility();
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
    requestAnimationFrame(() => scrollToEnd({ behavior: "smooth" }));
  };
  const lastItem = items.at(-1);
  const waitingForFirstToken =
    running &&
    (!lastItem ||
      lastItem.kind === "user" ||
      (lastItem.kind === "agent-run" && !hasInspectableTrace(lastItem.trace)));
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
          <Conversation>
            <ConversationViewport
              role="log"
              aria-label="Sumiとの会話"
              aria-busy={running}
            >
              <ConversationContent>
                {items.length === 0 ? (
                  <EmptyState available={available} />
                ) : (
                  <>
                    {items.map((item) => (
                      <ConversationItem
                        key={item.id}
                        messageId={item.id}
                        scrollAnchor={item.kind === "user"}
                        className="w-full"
                      >
                        <div className="mx-auto w-full max-w-2xl px-4 sm:px-6">
                          <Suspense fallback={null}>
                            <ChatItemView
                              item={item}
                              copyAlwaysVisible={
                                item.kind === "prose" &&
                                item.id === lastAssistantMessage?.id &&
                                !item.streaming
                              }
                              agentMessageCopyText={
                                item.kind === "prose" && item.runId
                                  ? copyTextByRunId.get(item.runId)
                                  : undefined
                              }
                              onApprovalDecision={decideApproval}
                              onCardAction={(label) => {
                                if (sendMessage(label)) {
                                  requestAnimationFrame(() =>
                                    scrollToEnd({ behavior: "smooth" }),
                                  );
                                }
                              }}
                            />
                          </Suspense>
                        </div>
                      </ConversationItem>
                    ))}
                    {waitingForFirstToken && (
                      <div
                        role="status"
                        className="mx-auto w-full max-w-2xl px-4 py-3 sm:px-6"
                      >
                        <span className="inline-block size-2.5 animate-pulse rounded-full bg-neutral-400" />
                        <span className="sr-only">
                          Sumiが応答を考えています
                        </span>
                      </div>
                    )}
                    <div className="h-6" />
                  </>
                )}
              </ConversationContent>
            </ConversationViewport>
            {items.length > 0 && (
              <ConversationScrollButton
                onClick={() => scrollToEnd({ behavior: "smooth" })}
              />
            )}
          </Conversation>

          {timeline.ticks.length > 1 && (
            <div className="-translate-y-1/2 absolute top-1/2 left-2 hidden h-[72%] md:block">
              <TimelineScrubber
                ticks={timeline.ticks}
                visibleRange={timeline.visibleRange}
                onJump={(index) => {
                  const messageId = timeline.messageIds[index];
                  if (messageId) {
                    scrollToMessage(messageId, {
                      align: "start",
                      behavior: "smooth",
                    });
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
              scrollToMessage(messageId, {
                align: "start",
                behavior: "smooth",
              });
            }
            setTimelineOpen(false);
          }}
        />
        <p className="sr-only" role="status" aria-live="polite">
          {running ? "Sumiが応答中です" : "応答が完了しました"}
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
  if (ready === "not_ready") return "エージェント停止中";
  return "エージェント確認中";
}

function composerPlaceholder(
  connection: "connecting" | "connected" | "closed",
  ready: "unknown" | "ready" | "not_ready",
) {
  if (connection !== "connected") return "接続を待っています…";
  if (ready === "not_ready") return "エージェントを起動すると話せます";
  if (ready === "unknown") return "エージェントを確認しています…";
  return "メッセージを入力…";
}
