import {
  Confirmation,
  ConfirmationAccepted,
  ConfirmationAction,
  ConfirmationActions,
  ConfirmationRejected,
  ConfirmationRequest,
  ConfirmationTitle,
} from "@sumi/ui/ai-elements/confirmation";
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
import { Link } from "@tanstack/react-router";
import { ShieldCheck } from "lucide-react";
import { useEffect } from "react";
import type { CoreApproval } from "./model";
import { useCoreApprovals } from "./store";

const POLL_INTERVAL_MS = 30_000;

/**
 * Keeps the durable approval inbox converged for one signed-in human:
 * initial read, slow poll, and refresh on focus/visibility. The live
 * `core_approval_changed` socket event refreshes it promptly; these fallbacks
 * cover a lost event or a decision committed by another tab.
 *
 * accountID is the session human — when it changes or the inbox unmounts
 * (logout, account replacement) the cleanup resets the store, ending that
 * account's ownership and fencing every continuation it left in flight.
 */
function useApprovalsSync(accountID: string) {
  const refresh = useCoreApprovals((state) => state.refresh);
  const reset = useCoreApprovals((state) => state.reset);
  // biome-ignore lint/correctness/useExhaustiveDependencies: A changed accountID must end the previous human's ownership of the inbox.
  useEffect(() => {
    void refresh();
    const interval = window.setInterval(() => void refresh(), POLL_INTERVAL_MS);
    const onVisible = () => {
      if (document.visibilityState === "visible") void refresh();
    };
    document.addEventListener("visibilitychange", onVisible);
    window.addEventListener("focus", onVisible);
    return () => {
      window.clearInterval(interval);
      document.removeEventListener("visibilitychange", onVisible);
      window.removeEventListener("focus", onVisible);
      reset();
    };
  }, [accountID, refresh, reset]);
}

/** Rail entry point for the session human's secretary approval inbox. */
export function CoreApprovalsInbox({ accountID }: { accountID: string }) {
  useApprovalsSync(accountID);
  const status = useCoreApprovals((state) => state.status);
  const pending = useCoreApprovals((state) => state.pending);
  const resolved = useCoreApprovals((state) => state.resolved);
  const pendingCount = pending.length;

  return (
    <Popover>
      <Tooltip>
        <TooltipTrigger
          render={
            <PopoverTrigger
              render={
                <Button
                  variant="ghost"
                  size="icon"
                  aria-label={
                    pendingCount > 0
                      ? `承認待ち ${pendingCount} 件`
                      : status === "ready"
                        ? "承認待ちはありません"
                        : "承認"
                  }
                  className="relative size-10"
                />
              }
            />
          }
        >
          <ShieldCheck className="size-4" />
          {pendingCount > 0 ? (
            <span className="absolute -top-0.5 -right-0.5 grid min-w-4 place-items-center rounded-full bg-destructive px-1 font-semibold text-[10px] text-destructive-foreground leading-4">
              {pendingCount > 99 ? "99+" : pendingCount}
            </span>
          ) : null}
        </TooltipTrigger>
        <TooltipContent side="right">承認</TooltipContent>
      </Tooltip>
      <PopoverContent
        side="right"
        align="end"
        className="max-h-[70dvh] w-96 overflow-y-auto p-2"
        aria-label="承認"
      >
        <p className="px-2 py-1 font-medium text-muted-foreground text-xs">
          承認
        </p>
        {status !== "ready" && pending.length === 0 ? (
          <p className="px-2 py-3 text-muted-foreground text-sm">
            {status === "error"
              ? "承認の一覧を読み込めませんでした。"
              : "読み込み中…"}
          </p>
        ) : pending.length === 0 ? (
          <p className="px-2 py-3 text-muted-foreground text-sm">
            承認待ちはありません
          </p>
        ) : (
          <div className="flex flex-col gap-2">
            {pending.map((approval) => (
              <ApprovalCard key={approval.approval_id} approval={approval} />
            ))}
          </div>
        )}
        {resolved.length > 0 ? (
          <>
            <p className="mt-3 border-border border-t px-2 pt-2 pb-1 font-medium text-muted-foreground text-xs">
              最近の承認
            </p>
            <div className="flex flex-col gap-2">
              {resolved.slice(0, 5).map((approval) => (
                <ApprovalCard key={approval.approval_id} approval={approval} />
              ))}
            </div>
          </>
        ) : null}
      </PopoverContent>
    </Popover>
  );
}

function ApprovalCard({ approval }: { approval: CoreApproval }) {
  const deciding = useCoreApprovals(
    (state) => state.deciding[approval.approval_id],
  );
  const error = useCoreApprovals(
    (state) => state.decisionErrors[approval.approval_id],
  );
  const decide = useCoreApprovals((state) => state.decide);
  const pending = approval.status === "pending";

  return (
    <Confirmation
      state={
        pending
          ? "approval-requested"
          : approval.status === "approved"
            ? "approval-responded"
            : "output-denied"
      }
      aria-busy={Boolean(deciding)}
      className="shadow-none"
    >
      <ConfirmationTitle>{approvalTitle(approval)}</ConfirmationTitle>
      <ConfirmationRequest>
        <span className="block">{requiredByText(approval)}</span>
        <RequestDetail approval={approval} />
        <PlaceLink approval={approval} />
        {error ? (
          <span className="mt-1 block text-destructive">
            {decisionErrorText(error)}
          </span>
        ) : null}
      </ConfirmationRequest>
      <ConfirmationAccepted>
        {approval.secretary_name} に今回のみ許可しました
      </ConfirmationAccepted>
      <ConfirmationRejected>拒否しました</ConfirmationRejected>
      <ConfirmationActions>
        {deciding ? (
          <span role="status" className="text-muted-foreground text-xs">
            承認を送信中…
          </span>
        ) : null}
        <ConfirmationAction
          disabled={Boolean(deciding)}
          onClick={() => void decide(approval, "approve_once")}
        >
          今回のみ許可
        </ConfirmationAction>
        <ConfirmationAction
          variant="outline"
          disabled={Boolean(deciding)}
          onClick={() => void decide(approval, "deny_once")}
        >
          拒否
        </ConfirmationAction>
      </ConfirmationActions>
    </Confirmation>
  );
}

function approvalTitle(approval: CoreApproval): string {
  const detail = requestSummary(approval);
  return detail
    ? `${approval.secretary_name}: ${approval.tool} — ${detail}`
    : `${approval.secretary_name}: ${approval.tool}`;
}

function requiredByText(approval: CoreApproval): string {
  return approval.required_by === "route"
    ? "この操作は実行に人の承認が必要なルートで要求されました。"
    : "この操作は実行に人の承認が必要です。";
}

function decisionErrorText(code: string): string {
  switch (code) {
    case "approval_conflict":
    case "approval_not_pending":
      return "この承認はすでに処理されました。";
    case "forbidden":
      return "この承認を操作する権限がありません。";
    case "unauthorized":
      return "セッションを確認してください。";
    default:
      return "承認の送信に失敗しました。もう一度お試しください。";
  }
}

/**
 * A short human-readable gist of what will run for tools the product knows;
 * other tools show their name and the full request is still listed below.
 */
function requestSummary(approval: CoreApproval): string {
  const req = approval.request;
  // message.send carries `text`; the delegated messaging.send effect carries
  // `content` — both are a message the secretary wants to post.
  const body =
    (typeof req.text === "string" ? req.text : "") ||
    (typeof req.content === "string" ? req.content : "");
  if (
    (approval.tool === "message.send" || approval.tool === "messaging.send") &&
    body.trim()
  ) {
    return `メッセージ「${truncate(body.trim(), 48)}」を送信`;
  }
  if (typeof req.command === "string" && req.command.trim()) {
    return truncate(req.command.trim(), 48);
  }
  if (typeof req.path === "string" && req.path.trim()) {
    return truncate(req.path.trim(), 48);
  }
  return "";
}

function RequestDetail({ approval }: { approval: CoreApproval }) {
  const keys = Object.keys(approval.request);
  if (keys.length === 0) return null;
  const body = JSON.stringify(approval.request, null, 2);
  return (
    <details className="mt-1">
      <summary className="cursor-pointer select-none text-muted-foreground text-xs">
        実行内容の詳細
      </summary>
      <pre className="mt-1 max-h-40 overflow-auto rounded-md bg-muted p-2 text-[11px] leading-relaxed">
        {body}
      </pre>
    </details>
  );
}

function PlaceLink({ approval }: { approval: CoreApproval }) {
  const input = approval.input;
  const place = input?.place;
  const workspaceId = input?.workspace_id;
  if (!place || !workspaceId) return null;
  const destination = placeRoute(workspaceId, place.kind ?? "", place.id);
  if (!destination) return null;
  return (
    <Link
      to={destination.to}
      params={destination.params}
      className="mt-1 inline-block text-primary text-xs underline underline-offset-2"
    >
      {place.name ? `${place.name} で会話を開く` : "会話を開く"}
    </Link>
  );
}

function placeRoute(
  workspaceId: string,
  kind: string,
  placeId: string,
): { to: string; params: Record<string, string> } | null {
  switch (kind) {
    case "channel":
      return {
        to: "/w/$workspaceId/messaging/c/$channelId",
        params: { workspaceId, channelId: placeId },
      };
    case "dm":
      return {
        to: "/w/$workspaceId/messaging/dm/$dmId",
        params: { workspaceId, dmId: placeId },
      };
    case "group_dm":
      return {
        to: "/w/$workspaceId/messaging/group/$dmId",
        params: { workspaceId, dmId: placeId },
      };
    case "thread":
      return {
        to: "/w/$workspaceId/messaging/t/$threadId",
        params: { workspaceId, threadId: placeId },
      };
    default:
      return null;
  }
}

function truncate(value: string, max: number): string {
  return value.length > max ? `${value.slice(0, max)}…` : value;
}
