import type { ApprovalDecision } from "@sumi/api-client";
import { SduiView } from "@sumi/sdui";
import {
  Message,
  MessageContent,
  MessageMetadata,
  MessageResponse,
} from "@sumi/ui/ai-elements/message";
import { Button } from "@sumi/ui/components/button";
import { Marker, MarkerContent } from "@sumi/ui/components/marker";
import { useCallback, useState } from "react";
import type { ChatItem } from "../agent/model";
import { userItemSourceLabel, userItemText } from "../lib/user-item-text";
import { ApprovalConfirmation } from "./approval-confirmation";
import { ModelProviderSettings } from "./model-provider-settings";
import { TraceRow } from "./work-summary";

export interface ChatItemViewProps {
  item: ChatItem;
  operationOpen?: boolean;
  onOperationOpenChange?: (open: boolean) => void;
  copyAlwaysVisible?: boolean;
  agentMessageCopyText?: string;
  onApprovalDecision?: (requestId: string, decision: ApprovalDecision) => void;
  sendingApprovalRequestId?: string | null;
  onWorkSummaryOpen?: () => void;
  onRichContentReady?: (itemId: string) => void;
}

/** Renders one derived item from the personality agent's canonical log. */
export function ChatItemView({
  item,
  operationOpen,
  onOperationOpenChange,
  copyAlwaysVisible = false,
  agentMessageCopyText,
  onApprovalDecision,
  sendingApprovalRequestId = null,
  onRichContentReady,
}: ChatItemViewProps) {
  const handleRichContentReady = useCallback(
    () => onRichContentReady?.(item.id),
    [item.id, onRichContentReady],
  );

  switch (item.kind) {
    case "agent-run":
      return null;
    case "trace":
      return (
        <div className="direct-chat-activity py-0.5">
          <TraceRow
            event={item.trace}
            phase={item.phase}
            open={operationOpen}
            onOpenChange={onOperationOpenChange}
          />
        </div>
      );
    case "user": {
      const text = userItemText(item);
      return (
        <Message
          from={item.source ? "assistant" : "user"}
          className="direct-chat-message max-w-full py-2"
        >
          {item.source && (
            <div className="text-muted-foreground text-xs leading-5">
              {userItemSourceLabel(item)}
            </div>
          )}
          <MessageContent className="direct-chat-message-content whitespace-pre-wrap break-words text-base leading-relaxed">
            {text}
          </MessageContent>
          <MessageMetadata
            timestamp={item.timestamp}
            copyText={text}
            align={item.source ? "left" : "right"}
            className="pr-1"
          />
          {item.delivery === "pending" && (
            <span className="pr-1 text-neutral-400 text-xs">送信中…</span>
          )}
          {item.delivery === "rejected" && (
            <span role="alert" className="pr-1 text-red-600 text-xs">
              送信できませんでした
              {item.rejectReason ? ` (${item.rejectReason})` : ""}
            </span>
          )}
        </Message>
      );
    }
    case "prose":
      return (
        <Message
          from="assistant"
          className="direct-chat-message max-w-full py-2"
        >
          <MessageContent className="direct-chat-message-content text-base leading-relaxed">
            <MessageResponse
              mode={item.streaming ? "streaming" : "static"}
              onRenderSettled={
                onRichContentReady ? handleRichContentReady : undefined
              }
            >
              {item.text}
            </MessageResponse>
          </MessageContent>
          {!item.streaming && item.agentMessageFinal && (
            <MessageMetadata
              timestamp={item.timestamp}
              copyText={agentMessageCopyText ?? item.text}
              copyFirst
              copyAlwaysVisible={copyAlwaysVisible}
            />
          )}
        </Message>
      );
    case "card":
      return (
        <div className="py-3">
          <SduiView node={item.node} />
        </div>
      );
    case "approval":
      return (
        <div className="py-3">
          <ApprovalConfirmation
            summary={item.summary}
            reason={item.reason ?? "この操作には明示的な承認が必要です。"}
            status={item.status}
            sending={sendingApprovalRequestId === item.requestId}
            onDecision={
              item.status === "pending" && onApprovalDecision
                ? (decision) => onApprovalDecision(item.requestId, decision)
                : undefined
            }
          />
        </div>
      );
    case "steer":
      return (
        <Marker variant="separator" className="py-3 text-xs">
          <MarkerContent>
            応答へ追加の指示を送りました ({item.mode})
          </MarkerContent>
        </Marker>
      );
    case "error":
      if (item.cause === "no_model_connection") {
        return <NoModelConnectionError />;
      }
      return (
        <div
          role="alert"
          className="my-3 rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-red-700 text-sm"
        >
          {item.message}
        </div>
      );
  }
}

/**
 * The one classified failure the user can fix themselves: no model
 * connection is selected. States the cause in Japanese and opens the
 * existing connection settings — the committed turn stays visible above,
 * so the copy says nothing about resending and nothing about work already
 * applied.
 */
function NoModelConnectionError() {
  const [settingsOpen, setSettingsOpen] = useState(false);
  return (
    <>
      <div
        role="alert"
        className="my-3 rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-red-700 text-sm"
      >
        <p>モデル接続が選択されていないため、応答できませんでした。</p>
        <p className="mt-1">
          「AIの接続」で使う接続を選ぶと、次のメッセージに応答できるようになります。
        </p>
        <Button
          variant="outline"
          size="sm"
          className="mt-2 border-red-300 bg-white text-red-700 hover:bg-red-100 hover:text-red-800"
          onClick={() => setSettingsOpen(true)}
        >
          接続設定を開く
        </Button>
      </div>
      <ModelProviderSettings
        open={settingsOpen}
        onOpenChange={setSettingsOpen}
      />
    </>
  );
}
