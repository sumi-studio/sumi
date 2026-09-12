import { Button } from "@sumi/ui/components/button";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@sumi/ui/components/sheet";
import { useNavigate } from "@tanstack/react-router";
import { useEffect, useMemo, useRef, useState } from "react";
import { useAuth } from "../../auth/auth-context";
import { clearEnrollmentInvitation } from "../../auth/enrollment-invitation-state";
import { WorkspaceAPIError, WorkspaceApiClient } from "../api-client";
import type { WorkspaceInvitePreview } from "../model";
import { redeemWorkspaceInvitation } from "../redeem-workspace-invitation";
import { useWorkspaceControl } from "../store";

export function WorkspaceInvitationPrompt({
  code,
  onDismiss,
}: {
  code: string | null;
  onDismiss(): void;
}) {
  const { authenticated, user } = useAuth();
  return authenticated && user?.id && code ? (
    <Recipient key={`${user.id}:${code}`} code={code} onDismiss={onDismiss} />
  ) : null;
}
function Recipient({ code, onDismiss }: { code: string; onDismiss(): void }) {
  const { user, logout } = useAuth();
  const navigate = useNavigate();
  const client = useMemo(() => new WorkspaceApiClient(), []);
  const [preview, setPreview] = useState<WorkspaceInvitePreview | null>(null);
  const [attempt, setAttempt] = useState(0);
  const [loading, setLoading] = useState(true);
  const [joining, setJoining] = useState(false);
  const [error, setError] = useState("");
  const [invalid, setInvalid] = useState(false);
  const active = useRef(true);
  useEffect(() => {
    active.current = true;
    return () => {
      active.current = false;
    };
  }, []);
  // biome-ignore lint/correctness/useExhaustiveDependencies: attempt explicitly retries preview.
  useEffect(() => {
    let current = true;
    setLoading(true);
    setError("");
    setInvalid(false);
    void client
      .previewInvite(code)
      .then((value) => {
        if (current) setPreview(value);
      })
      .catch((reason) => {
        if (!current) return;
        const unavailable =
          reason instanceof WorkspaceAPIError &&
          [403, 404, 410].includes(reason.status);
        setInvalid(unavailable);
        setError(
          unavailable
            ? "この招待は使用済み、期限切れ、または取り消されています。"
            : "招待を確認できませんでした。もう一度お試しください。",
        );
      })
      .finally(() => {
        if (current) setLoading(false);
      });
    return () => {
      current = false;
    };
  }, [client, code, attempt]);
  async function join() {
    if (!preview || joining) return;
    setJoining(true);
    setError("");
    try {
      const membership = await redeemWorkspaceInvitation(
        (proof) =>
          proof ? client.redeemInvite(code, proof) : client.redeemInvite(code),
        () => active.current,
      );
      if (!active.current) return;
      clearEnrollmentInvitation();
      onDismiss();
      void useWorkspaceControl
        .getState()
        .refreshWorkspaces()
        .catch(() => undefined);
      await navigate({
        to: "/w/$workspaceId",
        params: { workspaceId: membership.workspaceId },
      });
    } catch (reason) {
      if (active.current)
        setError(
          reason instanceof WorkspaceAPIError &&
            reason.code === "invitation_email_verification_required"
            ? "この招待のメールアドレスを確認できませんでした。招待されたアカウントでログインしてください。"
            : reason instanceof Error &&
                reason.message === "invitation_identity_unavailable"
              ? "メールアドレスの本人確認が必要です。招待されたアカウントでログインし直してください。"
              : "参加を確認できませんでした。もう一度お試しください。",
        );
    } finally {
      if (active.current) setJoining(false);
    }
  }
  return (
    <Sheet
      open
      onOpenChange={(open) => {
        if (!open && !joining) onDismiss();
      }}
    >
      <SheetContent className="gap-5 p-6 sm:max-w-md">
        <SheetHeader>
          <SheetTitle>
            {preview
              ? `${preview.workspaceName}に参加しますか？`
              : "Workspaceへの招待"}
          </SheetTitle>
          <SheetDescription>
            {user?.displayName ?? "現在のアカウント"}
            として参加します。参加するまでは、Workspaceのメンバーには追加されません。
          </SheetDescription>
        </SheetHeader>
        {loading ? <p role="status">招待を確認しています…</p> : null}
        {error ? (
          <p role="alert" className="text-sm">
            {error}
          </p>
        ) : null}
        {!loading && !invalid && !preview ? (
          <Button onClick={() => setAttempt((value) => value + 1)}>
            再試行
          </Button>
        ) : null}
        <div className="flex flex-wrap justify-end gap-2">
          <Button variant="ghost" disabled={joining} onClick={onDismiss}>
            今は参加しない
          </Button>
          <Button
            variant="ghost"
            disabled={joining}
            onClick={() =>
              void logout().catch(() =>
                setError(
                  "ログアウトできませんでした。もう一度お試しください。",
                ),
              )
            }
          >
            別のアカウントで続ける
          </Button>
          <Button
            disabled={loading || joining || invalid || !preview}
            onClick={() => void join()}
          >
            {joining ? "参加しています…" : "参加する"}
          </Button>
        </div>
      </SheetContent>
    </Sheet>
  );
}
