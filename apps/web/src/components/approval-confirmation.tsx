import type { ApprovalDecision } from "@sumi/api-client";
import {
  Confirmation,
  ConfirmationAccepted,
  ConfirmationAction,
  ConfirmationActions,
  ConfirmationRejected,
  ConfirmationRequest,
  ConfirmationTitle,
} from "@sumi/ui/ai-elements/confirmation";

interface ApprovalConfirmationProps {
  summary: string;
  reason: string;
  status?: "pending" | "allowed" | "denied" | "cancelled";
  decision?: ApprovalDecision | null;
  sending?: boolean;
  onDecision?: (decision: ApprovalDecision) => void;
}

/** A narrow v1 approval surface: approve this action once, or deny it. */
export function ApprovalConfirmation({
  summary,
  reason,
  status = "pending",
  decision = null,
  sending = false,
  onDecision,
}: ApprovalConfirmationProps) {
  return (
    <Confirmation
      state={
        status === "pending"
          ? "approval-requested"
          : status === "denied" || status === "cancelled"
            ? "output-denied"
            : "approval-responded"
      }
      className="mb-2 shadow-[0_2px_12px_rgba(0,0,0,0.04)]"
      aria-busy={sending}
    >
      <ConfirmationTitle>{summary}</ConfirmationTitle>
      <ConfirmationRequest>{reason}</ConfirmationRequest>
      <ConfirmationAccepted>
        {decision?.type === "approve_always"
          ? "このルールで許可しました"
          : "今回のみ許可しました"}
      </ConfirmationAccepted>
      <ConfirmationRejected>
        {status === "cancelled" ? "キャンセルされました" : "拒否しました"}
      </ConfirmationRejected>
      <ConfirmationActions>
        {sending && (
          <span role="status" className="text-muted-foreground text-xs">
            承認を送信中…
          </span>
        )}
        <ConfirmationAction
          disabled={sending}
          onClick={() => onDecision?.({ type: "approve_once" })}
        >
          今回のみ許可
        </ConfirmationAction>
        <ConfirmationAction
          variant="outline"
          disabled={sending}
          onClick={() => onDecision?.({ type: "deny" })}
        >
          拒否
        </ConfirmationAction>
      </ConfirmationActions>
    </Confirmation>
  );
}
