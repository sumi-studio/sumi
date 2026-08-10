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
import { WorkSummary } from "./work-summary";

interface ChatItemViewProps {
  item: ChatItem;
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
          run={item}
          onOpenChange={(open) => open && onWorkSummaryOpen?.()}
        />
      );
    case "user":
      return (
        <Message from="user" className="py-3">
          <MessageContent className="whitespace-pre-wrap">
            {item.text}
          </MessageContent>
          <MessageMetadata
            timestamp={item.timestamp}
            copyText={item.text}
            align="right"
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
        <Message from="assistant" className="py-3">
          <MessageContent>
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
            decision={item.decision}
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
