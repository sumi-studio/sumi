import type { ApprovalDecision } from "@sumi/api-client";
import { SduiView } from "@sumi/sdui";
import {
  Message,
  MessageContent,
  MessageMetadata,
  MessageResponse,
} from "@sumi/ui/ai-elements/message";
import { Marker, MarkerContent } from "@sumi/ui/components/marker";
import { useCallback } from "react";
import type { ChatItem } from "../agent/model";
import { ApprovalConfirmation } from "./approval-confirmation";
import { TraceRow, WorkSummary } from "./work-summary";

interface ChatItemViewProps {
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
  onWorkSummaryOpen,
  onRichContentReady,
}: ChatItemViewProps) {
  const handleRichContentReady = useCallback(
    () => onRichContentReady?.(item.id),
    [item.id, onRichContentReady],
  );

  switch (item.kind) {
    case "agent-run":
      return (
        <WorkSummary
          headingOnly
          run={item}
          onOpenChange={(open) => open && onWorkSummaryOpen?.()}
        />
      );
    case "trace":
      return (
        <div className="py-2">
          <TraceRow
            event={item.trace}
            phase={item.phase}
            open={operationOpen}
            onOpenChange={onOperationOpenChange}
          />
        </div>
      );
    case "user":
      return (
        <Message
          from={item.source ? "assistant" : "user"}
          className="max-w-full py-3"
        >
          {item.source && (
            <div className="text-muted-foreground text-xs leading-5">
              {`${item.source.source.place.kind === "dm" ? "DM" : item.source.source.place.kind === "group_dm" ? "グループDM" : "Messaging"} · ${item.source.source.place.name} · ${item.source.actor.display_name || item.source.actor.principal_id}${item.source.source.kind === "reply_later_due" ? " · リマインダー" : ""}`}
            </div>
          )}
          <MessageContent className="whitespace-pre-wrap break-words text-base leading-relaxed">
            {item.text}
          </MessageContent>
          <MessageMetadata
            timestamp={item.timestamp}
            copyText={item.text}
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
    case "prose":
      return (
        <Message from="assistant" className="max-w-full py-3">
          <MessageContent className="text-base leading-relaxed">
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
