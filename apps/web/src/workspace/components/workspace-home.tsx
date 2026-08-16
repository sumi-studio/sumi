import { Button } from "@sumi/ui/components/button";
import { useNavigate } from "@tanstack/react-router";
import {
  AppWindow,
  ArrowRight,
  Check,
  Copy,
  Crown,
  KeyRound,
  MessageCircle,
  Pencil,
  Plus,
  ShieldCheck,
  Trash2,
  UserRound,
  Users,
} from "lucide-react";
import { type FormEvent, useEffect, useState } from "react";
import { useAuth } from "../../auth/auth-context";
import { WORKSPACE_APP_RENDERERS } from "../../shell/app-descriptors";
import { WorkspaceAPIError } from "../api-client";
import type {
  AppDescriptor,
  AppInstallation,
  WorkspaceMembership,
  WorkspacePermission,
  WorkspaceRole,
  WorkspaceRoleCapabilityDescriptor,
  WorkspaceRoleCapabilityRef,
  WorkspaceRoleInput,
} from "../model";
import { participantID, WORKSPACE_PERMISSIONS } from "../model";
import {
  effectiveWorkspacePermissions,
  exactHumanMembership,
  useWorkspaceControl,
} from "../store";

type Section = "overview" | "members" | "roles" | "apps";

const SECTION_LABEL: Record<Section, string> = {
  overview: "概要",
  members: "参加者と招待",
  roles: "ロール",
  apps: "アプリ",
};

const PERMISSION_LABEL: Record<WorkspacePermission, string> = {
  manage_workspace: "Workspaceの設定",
  manage_members: "参加者の管理",
  manage_roles: "ロールの管理",
  manage_apps: "アプリの管理",
};

const PLATFORM_CAPABILITIES: WorkspaceRoleCapabilityDescriptor[] =
  WORKSPACE_PERMISSIONS.map((ref) => ({ ref, label: PERMISSION_LABEL[ref] }));

const INPUT_CLASS =
  "w-full rounded-md border border-border bg-background px-2.5 py-1.5 text-sm outline-none focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/20 disabled:opacity-50";

export function WorkspaceHome({ workspaceId }: { workspaceId: string }) {
  const [section, setSection] = useState<Section>("overview");
  const workspace = useWorkspaceControl((state) => state.selectedWorkspace);
  const members = useWorkspaceControl((state) => state.members);
  const roles = useWorkspaceControl((state) => state.roles);
  const { user } = useAuth();
  const ownMembership = exactHumanMembership(members, user?.id);
  const permissions = effectiveWorkspacePermissions(ownMembership, roles);
  const can = (permission: WorkspacePermission) => permissions.has(permission);

  if (!workspace) return null;
  return (
    <div className="flex h-full bg-background text-foreground">
      <aside className="flex w-56 shrink-0 flex-col border-border border-r bg-muted/15 px-3 py-4">
        <div className="mb-6 px-2">
          <p className="text-muted-foreground text-[11px] uppercase tracking-[0.14em]">
            Workspace
          </p>
          <h1 className="mt-1 truncate font-semibold text-sm">
            {workspace.name}
          </h1>
        </div>
        <nav className="space-y-1" aria-label="Workspace設定">
          {(Object.keys(SECTION_LABEL) as Section[]).map((candidate) => (
            <button
              key={candidate}
              type="button"
              aria-current={section === candidate ? "page" : undefined}
              onClick={() => setSection(candidate)}
              className={`block w-full rounded-md px-2.5 py-2 text-left text-sm transition-colors ${
                section === candidate
                  ? "bg-accent font-medium"
                  : "text-muted-foreground hover:bg-accent/60 hover:text-foreground"
              }`}
            >
              {SECTION_LABEL[candidate]}
            </button>
          ))}
        </nav>
        <div className="mt-auto px-2 text-muted-foreground text-[11px] leading-5">
          別のWorkspaceへ移動しても、参加状態やほかの人の作業には影響しません。
        </div>
      </aside>
      <main className="min-w-0 flex-1 overflow-y-auto">
        <div className="mx-auto w-full max-w-4xl px-8 py-10 lg:px-12">
          <header className="mb-8">
            <p className="font-medium text-muted-foreground text-xs">
              {workspace.name}
            </p>
            <h2 className="mt-1 font-semibold text-2xl tracking-tight">
              {SECTION_LABEL[section]}
            </h2>
          </header>
          {section === "overview" ? (
            <OverviewSection
              workspaceId={workspaceId}
              ownMembership={ownMembership}
              canManageWorkspace={can("manage_workspace")}
            />
          ) : null}
          {section === "members" ? (
            <MembersSection
              userId={user?.id ?? ""}
              canManage={can("manage_members")}
              canTransferOwnership={ownMembership?.owner ?? false}
            />
          ) : null}
          {section === "roles" ? (
            <RolesSection canManage={can("manage_roles")} />
          ) : null}
          {section === "apps" ? (
            <AppsSection
              workspaceId={workspaceId}
              canManage={can("manage_apps")}
            />
          ) : null}
        </div>
      </main>
    </div>
  );
}

function OverviewSection({
  workspaceId,
  ownMembership,
  canManageWorkspace,
}: {
  workspaceId: string;
  ownMembership: WorkspaceMembership | null;
  canManageWorkspace: boolean;
}) {
  const navigate = useNavigate();
  const workspace = useWorkspaceControl((state) => state.selectedWorkspace);
  const members = useWorkspaceControl((state) => state.members);
  const installations = useWorkspaceControl((state) => state.installations);
  const mutation = useWorkspaceControl((state) => state.mutation);
  const updateWorkspace = useWorkspaceControl((state) => state.updateWorkspace);
  const leaveWorkspace = useWorkspaceControl((state) => state.leaveWorkspace);
  const [name, setName] = useState(workspace?.name ?? "");
  const [notice, setNotice] = useState("");
  const [failed, setFailed] = useState("");
  const messaging = exactInstallation(installations, "messaging");

  const rename = async (event: FormEvent) => {
    event.preventDefault();
    if (!name.trim() || mutation) return;
    setNotice("");
    setFailed("");
    try {
      await updateWorkspace(name);
      setNotice("保存しました");
    } catch {
      setFailed("Workspace名を保存できませんでした。");
    }
  };

  return (
    <div className="space-y-8">
      <section className="grid gap-3 sm:grid-cols-3">
        <SummaryCard
          icon={<Users className="size-4" />}
          label="参加者"
          value={`${members.length}人`}
        />
        <SummaryCard
          icon={<AppWindow className="size-4" />}
          label="有効なアプリ"
          value={`${installations.filter((item) => item.state === "enabled").length}件`}
        />
        <SummaryCard
          icon={<MessageCircle className="size-4" />}
          label="Messaging"
          value={
            messaging === "duplicate"
              ? "状態エラー"
              : messaging?.state === "enabled"
                ? "利用できます"
                : messaging?.state === "disabled"
                  ? "停止中"
                  : "未導入"
          }
        />
      </section>

      {messaging !== "duplicate" && messaging?.state === "enabled" ? (
        <button
          type="button"
          onClick={() =>
            void navigate({
              to: "/w/$workspaceId/messaging",
              params: { workspaceId },
            })
          }
          className="group flex w-full items-center gap-4 rounded-xl border border-border bg-muted/10 p-4 text-left hover:bg-muted/25"
        >
          <span className="grid size-10 place-items-center rounded-lg bg-foreground text-background">
            <MessageCircle className="size-5" />
          </span>
          <span className="min-w-0 flex-1">
            <span className="block font-semibold text-sm">Messagingを開く</span>
            <span className="mt-0.5 block text-muted-foreground text-xs">
              このWorkspaceのチャンネルとDMへ移動します
            </span>
          </span>
          <ArrowRight className="size-4 text-muted-foreground transition-transform group-hover:translate-x-0.5" />
        </button>
      ) : null}

      <section className="rounded-xl border border-border p-5">
        <h3 className="font-semibold text-sm">Workspace名</h3>
        <p className="mt-1 text-muted-foreground text-xs">
          参加者とアプリレールに表示される名前です。
        </p>
        <form
          onSubmit={(event) => void rename(event)}
          className="mt-4 flex max-w-lg gap-2"
        >
          <input
            value={name}
            onChange={(event) => setName(event.target.value)}
            disabled={!canManageWorkspace || mutation !== null}
            maxLength={80}
            aria-label="Workspace名"
            className={INPUT_CLASS}
          />
          <Button
            type="submit"
            disabled={
              !canManageWorkspace ||
              mutation !== null ||
              !name.trim() ||
              name.trim() === workspace?.name
            }
          >
            保存
          </Button>
        </form>
        {!canManageWorkspace ? (
          <p className="mt-2 text-muted-foreground text-xs">
            Workspace設定を変更する権限がありません。
          </p>
        ) : null}
        {notice ? (
          <p role="status" className="mt-2 text-xs">
            {notice}
          </p>
        ) : null}
        {failed ? (
          <p role="alert" className="mt-2 text-red-600 text-xs">
            {failed}
          </p>
        ) : null}
      </section>

      {ownMembership && !ownMembership.owner ? (
        <section className="rounded-xl border border-red-500/25 p-5">
          <h3 className="font-semibold text-sm">Workspaceから退出</h3>
          <p className="mt-1 text-muted-foreground text-xs leading-5">
            このWorkspaceへの参加を終了します。退出後はWorkspace一覧へ戻ります。
          </p>
          <Button
            variant="destructive"
            size="sm"
            disabled={mutation !== null}
            className="mt-4"
            onClick={() => {
              if (!window.confirm("このWorkspaceから退出しますか？")) return;
              void leaveWorkspace().then(() => navigate({ to: "/" }));
            }}
          >
            退出する
          </Button>
        </section>
      ) : null}
    </div>
  );
}

function SummaryCard({
  icon,
  label,
  value,
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
}) {
  return (
    <div className="rounded-xl border border-border bg-muted/10 p-4">
      <span className="mb-5 block text-muted-foreground">{icon}</span>
      <span className="block text-muted-foreground text-xs">{label}</span>
      <span className="mt-1 block font-semibold text-sm">{value}</span>
    </div>
  );
}

function MembersSection({
  userId,
  canManage,
  canTransferOwnership,
}: {
  userId: string;
  canManage: boolean;
  canTransferOwnership: boolean;
}) {
  const members = useWorkspaceControl((state) => state.members);
  const roles = useWorkspaceControl((state) => state.roles);
  const invites = useWorkspaceControl((state) => state.invites);
  const currentAgentInvite = useWorkspaceControl(
    (state) => state.currentAgentInvite,
  );
  const inviteSecret = useWorkspaceControl(
    (state) => state.createdInviteSecret,
  );
  const mutation = useWorkspaceControl((state) => state.mutation);
  const createInvite = useWorkspaceControl((state) => state.createInvite);
  const createCurrentAgentInvite = useWorkspaceControl(
    (state) => state.createCurrentAgentInvite,
  );
  const revokeInvite = useWorkspaceControl((state) => state.revokeInvite);
  const clearCreatedInviteSecret = useWorkspaceControl(
    (state) => state.clearCreatedInviteSecret,
  );
  const setMemberRoles = useWorkspaceControl((state) => state.setMemberRoles);
  const removeMember = useWorkspaceControl((state) => state.removeMember);
  const transferOwnership = useWorkspaceControl(
    (state) => state.transferOwnership,
  );
  const [copied, setCopied] = useState(false);
  const [copyFailed, setCopyFailed] = useState("");
  const [failed, setFailed] = useState("");
  const targetedInvite =
    currentAgentInvite.status === "pending" ? currentAgentInvite.invite : null;
  const shareInvites = invites.filter((invite) => invite.kind === "share_code");
  const anonymousTargetedInvites = invites.filter(
    (invite) =>
      invite.kind === "targeted_personality_agent" &&
      invite.inviteId !== targetedInvite?.inviteId,
  );

  useEffect(() => () => clearCreatedInviteSecret(), [clearCreatedInviteSecret]);

  const run = async (action: () => Promise<unknown>) => {
    setFailed("");
    try {
      await action();
    } catch (error) {
      setFailed(workspaceMutationErrorMessage(error));
    }
  };

  const copyInvite = async () => {
    setCopied(false);
    setCopyFailed("");
    try {
      if (!inviteSecret || !navigator.clipboard?.writeText) {
        throw new Error("clipboard_unavailable");
      }
      await navigator.clipboard.writeText(inviteSecret.code);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1200);
    } catch {
      setCopyFailed(
        "コピーできませんでした。コード欄を選択して手動でコピーしてください。",
      );
    }
  };

  return (
    <div className="space-y-8">
      <section className="rounded-xl border border-border p-5">
        <div className="border-border border-b pb-5">
          <div className="flex items-start justify-between gap-4">
            <div>
              <div className="flex items-center gap-2">
                <MessageCircle className="size-4" />
                <h3 className="font-semibold text-sm">
                  Direct Chatの相手を招待
                </h3>
              </div>
              <p className="mt-1 text-muted-foreground text-xs leading-5">
                今話している人格エージェント本人へ、このWorkspaceへの招待を送ります。参加は本人の承諾後です。
              </p>
            </div>
            {currentAgentInvite.status === "none" ? (
              <Button
                size="sm"
                disabled={!canManage || mutation !== null}
                onClick={() => void run(createCurrentAgentInvite)}
              >
                <Plus className="size-3.5" />
                招待する
              </Button>
            ) : null}
          </div>
          {currentAgentInvite.status === "member" ? (
            <div className="mt-4 rounded-lg border border-border bg-muted/20 p-3">
              <p className="font-medium text-xs">このWorkspaceに参加済みです</p>
            </div>
          ) : targetedInvite ? (
            <div className="mt-4 flex items-center justify-between gap-4 rounded-lg border border-border bg-muted/20 p-3">
              <div>
                <p className="font-medium text-xs">招待済み・承諾待ち</p>
                <p className="mt-1 text-muted-foreground text-[11px]">
                  有効期限
                  {new Date(targetedInvite.expiresAt).toLocaleString("ja-JP")}
                </p>
                <p className="mt-1 text-muted-foreground text-[11px]">
                  Direct Chatで招待を確認してもらってください。
                </p>
              </div>
              <Button
                size="sm"
                variant="ghost"
                disabled={mutation !== null}
                aria-label="Direct Chatの相手への招待を取り消す"
                onClick={() =>
                  void run(() => revokeInvite(targetedInvite.inviteId))
                }
              >
                取り消す
              </Button>
            </div>
          ) : currentAgentInvite.status === "unavailable" ? (
            <p className="mt-3 text-muted-foreground text-xs">
              現在のDirect Chatとの関係では、この招待を操作できません。
            </p>
          ) : currentAgentInvite.status === "error" ? (
            <p role="alert" className="mt-3 text-red-600 text-xs">
              現在のDirect Chatの招待状態を確認できませんでした。
            </p>
          ) : !canManage ? (
            <p className="mt-3 text-muted-foreground text-xs">
              この招待を作成する権限がありません。
            </p>
          ) : null}
          {anonymousTargetedInvites.length > 0 ? (
            <div className="mt-5 space-y-2">
              <p className="font-medium text-muted-foreground text-[11px]">
                対象を表示しない人格エージェントへの招待
              </p>
              {anonymousTargetedInvites.map((invite) => (
                <div
                  key={invite.inviteId}
                  className="flex items-center justify-between gap-4 rounded-lg border border-border bg-muted/20 p-3"
                >
                  <div>
                    <p className="font-medium text-xs">承諾待ち</p>
                    <p className="mt-1 text-muted-foreground text-[11px]">
                      招待 {invite.inviteId.slice(-8)} · 有効期限
                      {new Date(invite.expiresAt).toLocaleString("ja-JP")}
                    </p>
                  </div>
                  <Button
                    size="sm"
                    variant="ghost"
                    disabled={mutation !== null}
                    aria-label={`人格エージェントへの招待 ${invite.inviteId.slice(-8)} を取り消す`}
                    onClick={() =>
                      void run(() => revokeInvite(invite.inviteId))
                    }
                  >
                    取り消す
                  </Button>
                </div>
              ))}
            </div>
          ) : null}
        </div>

        <div className="flex items-start justify-between gap-4">
          <div className="pt-5">
            <div className="flex items-center gap-2">
              <KeyRound className="size-4" />
              <h3 className="font-semibold text-sm">招待</h3>
            </div>
            <p className="mt-1 text-muted-foreground text-xs leading-5">
              24時間有効・1回限りのコードです。平文は作成直後だけ表示されます。
            </p>
          </div>
          <Button
            className="mt-5"
            size="sm"
            disabled={!canManage || mutation !== null}
            onClick={() => void run(createInvite)}
          >
            <Plus className="size-3.5" />
            招待を作成
          </Button>
        </div>
        {canManage && shareInvites.length === 0 ? (
          <p className="mt-4 text-muted-foreground text-xs">
            有効な招待はありません。
          </p>
        ) : null}
        {shareInvites.length > 0 ? (
          <div className="mt-4 space-y-2">
            {shareInvites.map((invite) => {
              const secret =
                inviteSecret?.inviteId === invite.inviteId
                  ? inviteSecret.code
                  : null;
              return (
                <div
                  key={invite.inviteId}
                  className="rounded-lg border border-border bg-muted/20 p-3"
                >
                  <p className="font-medium text-[11px] text-muted-foreground">
                    招待 {invite.inviteId.slice(-8)} · 有効期限
                    {new Date(invite.expiresAt).toLocaleString("ja-JP")}
                  </p>
                  {secret ? (
                    <div className="mt-2 space-y-2">
                      <textarea
                        aria-label="招待コード"
                        readOnly
                        rows={2}
                        value={secret}
                        onFocus={(event) => event.currentTarget.select()}
                        className={`${INPUT_CLASS} resize-none break-all font-mono text-xs`}
                      />
                      <p className="text-muted-foreground text-[11px]">
                        この画面を離れるとコードは再表示できません。
                      </p>
                    </div>
                  ) : (
                    <p className="mt-2 text-muted-foreground text-xs">
                      コードは再表示できません。この招待は有効なまま取り消せます。
                    </p>
                  )}
                  <div className="mt-2 flex items-center gap-2">
                    {secret ? (
                      <Button
                        size="sm"
                        variant="secondary"
                        onClick={() => void copyInvite()}
                      >
                        {copied ? (
                          <Check className="size-3.5" />
                        ) : (
                          <Copy className="size-3.5" />
                        )}
                        {copied ? "コピー済み" : "コピー"}
                      </Button>
                    ) : null}
                    <Button
                      size="sm"
                      variant="ghost"
                      disabled={mutation !== null}
                      aria-label={`招待 ${invite.inviteId.slice(-8)} を取り消す`}
                      onClick={() =>
                        void run(() => revokeInvite(invite.inviteId))
                      }
                    >
                      取り消す
                    </Button>
                  </div>
                </div>
              );
            })}
            {copyFailed ? (
              <p role="alert" className="text-red-600 text-xs">
                {copyFailed}
              </p>
            ) : null}
          </div>
        ) : null}
        {!canManage ? (
          <p className="mt-3 text-muted-foreground text-xs">
            招待を作成する権限がありません。
          </p>
        ) : null}
      </section>

      <section>
        <div className="mb-3 flex items-center gap-2">
          <Users className="size-4" />
          <h3 className="font-semibold text-sm">現在の参加者</h3>
          <span className="ml-auto text-muted-foreground text-xs">
            {members.length}人
          </span>
        </div>
        <div className="divide-y divide-border overflow-hidden rounded-xl border border-border">
          {members.map((member) => {
            const id = participantID(member.participant);
            const isSelf = member.participant.kind === "human" && id === userId;
            const identityHint = participantIdentityHint(member);
            const accessibleIdentity = `${member.displayName}（${identityHint}）`;
            return (
              <fieldset
                key={member.workspaceMemberId}
                aria-label={accessibleIdentity}
                className="flex items-start gap-3 p-4"
              >
                <span className="grid size-9 shrink-0 place-items-center rounded-full bg-muted">
                  {member.participant.kind === "human" ? (
                    <UserRound className="size-4" />
                  ) : (
                    <ShieldCheck className="size-4" />
                  )}
                </span>
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2">
                    <p className="truncate font-medium text-sm">
                      {member.displayName}
                    </p>
                    {member.owner ? (
                      <span className="rounded bg-muted px-1.5 py-0.5 font-medium text-[10px]">
                        Owner
                      </span>
                    ) : null}
                    {isSelf ? (
                      <span className="text-muted-foreground text-[10px]">
                        自分
                      </span>
                    ) : null}
                  </div>
                  <p className="mt-0.5 block truncate text-muted-foreground text-[11px]">
                    {identityHint}
                  </p>
                  <div className="mt-3 flex flex-wrap gap-2">
                    {roles.length === 0 ? (
                      <span className="text-muted-foreground text-xs">
                        ロールなし
                      </span>
                    ) : (
                      roles.map((role) => {
                        const checked = member.roleIds.includes(role.roleId);
                        return (
                          <label
                            key={role.roleId}
                            className="flex items-center gap-1.5 rounded-md border border-border px-2 py-1 text-xs"
                          >
                            <input
                              type="checkbox"
                              aria-label={`${accessibleIdentity}の${role.name}`}
                              checked={checked}
                              disabled={
                                !canManage || member.owner || mutation !== null
                              }
                              onChange={() =>
                                void run(() =>
                                  setMemberRoles(
                                    member.workspaceMemberId,
                                    checked
                                      ? member.roleIds.filter(
                                          (id) => id !== role.roleId,
                                        )
                                      : [...member.roleIds, role.roleId],
                                  ),
                                )
                              }
                            />
                            <RoleDot color={role.color} />
                            {role.name}
                          </label>
                        );
                      })
                    )}
                  </div>
                </div>
                {!member.owner && !isSelf ? (
                  <div className="flex shrink-0 items-center gap-1">
                    {canTransferOwnership ? (
                      <Button
                        variant="ghost"
                        size="sm"
                        disabled={mutation !== null}
                        aria-label={`${accessibleIdentity}へWorkspace Ownerを引き継ぐ`}
                        onClick={() => {
                          if (
                            !window.confirm(
                              `「${accessibleIdentity}」へWorkspace Ownerを引き継ぎますか？`,
                            )
                          ) {
                            return;
                          }
                          void run(() =>
                            transferOwnership(member.workspaceMemberId),
                          );
                        }}
                      >
                        <Crown className="size-3.5" />
                        Ownerにする
                      </Button>
                    ) : null}
                    {canManage ? (
                      <Button
                        variant="ghost"
                        size="sm"
                        disabled={mutation !== null}
                        aria-label={`${accessibleIdentity}をWorkspaceから外す`}
                        onClick={() => {
                          if (
                            !window.confirm(
                              `「${accessibleIdentity}」をWorkspaceから外しますか？`,
                            )
                          )
                            return;
                          void run(() =>
                            removeMember(member.workspaceMemberId),
                          );
                        }}
                      >
                        <Trash2 className="size-3.5" />
                        外す
                      </Button>
                    ) : null}
                  </div>
                ) : null}
              </fieldset>
            );
          })}
        </div>
      </section>
      {failed ? (
        <p role="alert" className="text-red-600 text-xs">
          {failed}
        </p>
      ) : null}
    </div>
  );
}

function participantIdentityHint(member: WorkspaceMembership): string {
  const type =
    member.participant.kind === "human" ? "Human" : "PersonalityAgent";
  return `${type} · ${participantID(member.participant).slice(-8)}`;
}

export function workspaceMutationErrorMessage(error: unknown): string {
  if (!(error instanceof WorkspaceAPIError)) {
    return "変更を完了できませんでした。最新の状態を確認して、もう一度実行してください。";
  }
  switch (error.code) {
    case "last_administrator":
      return "最後の管理者は外せません。先に別の参加者へ管理権限を付与してください。";
    case "forbidden":
      return "この変更を行う権限がないか、対象の権限が管理範囲を超えています。";
    case "owner_protected":
      return "Workspace Ownerの参加状態やロールはここでは変更できません。";
    case "membership_not_active":
      return "対象の参加状態はすでに終了しています。参加者一覧を更新してください。";
    case "conflict":
      return "最新の状態と競合しました。内容を確認して、もう一度実行してください。";
    case "not_found":
      return "対象が見つからないか、すでに利用できません。最新の一覧を確認してください。";
    default:
      return "変更を完了できませんでした。最新の状態を確認して、もう一度実行してください。";
  }
}

function RolesSection({ canManage }: { canManage: boolean }) {
  const roles = useWorkspaceControl((state) => state.roles);
  const catalog = useWorkspaceControl((state) => state.catalog);
  const mutation = useWorkspaceControl((state) => state.mutation);
  const createRole = useWorkspaceControl((state) => state.createRole);
  const updateRole = useWorkspaceControl((state) => state.updateRole);
  const deleteRole = useWorkspaceControl((state) => state.deleteRole);
  const [creating, setCreating] = useState(false);
  const [editing, setEditing] = useState<string | null>(null);
  const [failed, setFailed] = useState("");
  const grantable = grantableRoleCapabilities(catalog);

  const run = async (action: () => Promise<unknown>) => {
    setFailed("");
    try {
      await action();
      setCreating(false);
      setEditing(null);
    } catch (error) {
      setFailed(workspaceMutationErrorMessage(error));
    }
  };

  return (
    <div className="space-y-4">
      <div className="flex items-start justify-between gap-4">
        <p className="max-w-xl text-muted-foreground text-sm leading-6">
          ロールを使って、参加者や人格エージェントに必要な権限をまとめて割り当てられます。
        </p>
        <Button
          size="sm"
          disabled={!canManage || mutation !== null || creating}
          onClick={() => setCreating(true)}
        >
          <Plus className="size-3.5" />
          ロールを作成
        </Button>
      </div>
      {creating ? (
        <RoleForm
          grantable={grantable}
          busy={mutation !== null}
          onCancel={() => setCreating(false)}
          onSubmit={(input) => void run(() => createRole(input))}
        />
      ) : null}
      <div className="space-y-3">
        {roles.length === 0 && !creating ? (
          <div className="rounded-xl border border-border border-dashed p-8 text-center text-muted-foreground text-sm">
            カスタムロールはまだありません
          </div>
        ) : null}
        {roles.map((role) =>
          editing === role.roleId ? (
            <RoleForm
              key={role.roleId}
              role={role}
              grantable={grantable}
              busy={mutation !== null}
              onCancel={() => setEditing(null)}
              onSubmit={(input) =>
                void run(() => updateRole(role.roleId, input))
              }
            />
          ) : (
            <div
              key={role.roleId}
              className="rounded-xl border border-border p-4"
            >
              <div className="flex items-start gap-3">
                <RoleDot color={role.color} />
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2">
                    <h3 className="font-semibold text-sm">{role.name}</h3>
                    <span className="text-muted-foreground text-[11px]">
                      position {role.position}
                    </span>
                  </div>
                  <p className="mt-1 text-muted-foreground text-xs leading-5">
                    {role.permissions
                      .map((permission) =>
                        roleCapabilityLabel(permission, grantable),
                      )
                      .join("、") || "権限なし"}
                  </p>
                </div>
                {canManage ? (
                  <div className="flex gap-1">
                    <Button
                      size="sm"
                      variant="ghost"
                      disabled={mutation !== null}
                      onClick={() => setEditing(role.roleId)}
                    >
                      <Pencil className="size-3.5" />
                      編集
                    </Button>
                    <Button
                      size="sm"
                      variant="ghost"
                      disabled={mutation !== null}
                      onClick={() => {
                        if (
                          !window.confirm(
                            `ロール「${role.name}」を削除しますか？`,
                          )
                        )
                          return;
                        void run(() => deleteRole(role.roleId));
                      }}
                    >
                      <Trash2 className="size-3.5" />
                      削除
                    </Button>
                  </div>
                ) : null}
              </div>
            </div>
          ),
        )}
      </div>
      {!canManage ? (
        <p className="text-muted-foreground text-xs">
          ロールを変更する権限がありません。
        </p>
      ) : null}
      {failed ? (
        <p role="alert" className="text-red-600 text-xs">
          {failed}
        </p>
      ) : null}
    </div>
  );
}

function RoleForm({
  role,
  grantable,
  busy,
  onSubmit,
  onCancel,
}: {
  role?: WorkspaceRole;
  grantable: readonly WorkspaceRoleCapabilityDescriptor[];
  busy: boolean;
  onSubmit: (input: WorkspaceRoleInput) => void;
  onCancel: () => void;
}) {
  const [name, setName] = useState(role?.name ?? "");
  const [color, setColor] = useState(role?.color ?? "");
  const [position, setPosition] = useState(role?.position ?? 0);
  const [permissions, setPermissions] = useState<WorkspaceRoleCapabilityRef[]>(
    role?.permissions ?? [],
  );
  const grantableRefs = new Set(grantable.map((capability) => capability.ref));
  const retired = permissions.filter((ref) => !grantableRefs.has(ref));
  return (
    <form
      onSubmit={(event) => {
        event.preventDefault();
        if (!name.trim() || busy) return;
        onSubmit({
          name: name.trim(),
          ...(color ? { color } : {}),
          position,
          permissions,
        });
      }}
      className="rounded-xl border border-border bg-muted/10 p-4"
    >
      <div className="grid gap-3 sm:grid-cols-[minmax(0,1fr)_7rem_6rem]">
        <label>
          <span className="mb-1 block font-medium text-xs">名前</span>
          <input
            value={name}
            onChange={(event) => setName(event.target.value)}
            disabled={busy}
            maxLength={60}
            aria-label="ロール名"
            className={INPUT_CLASS}
          />
        </label>
        <label>
          <span className="mb-1 block font-medium text-xs">色</span>
          <input
            type="color"
            value={color || "#737373"}
            onChange={(event) => setColor(event.target.value)}
            disabled={busy}
            aria-label="ロールの色"
            className="h-8 w-full rounded-md border border-border bg-background"
          />
        </label>
        <label>
          <span className="mb-1 block font-medium text-xs">順序</span>
          <input
            type="number"
            value={position}
            onChange={(event) => setPosition(Number(event.target.value))}
            disabled={busy}
            aria-label="ロールの順序"
            className={INPUT_CLASS}
          />
        </label>
      </div>
      <fieldset className="mt-4">
        <legend className="mb-2 font-medium text-xs">権限</legend>
        <div className="grid gap-2 sm:grid-cols-2">
          {grantable.map((capability) => (
            <label
              key={capability.ref}
              className="flex items-center gap-2 rounded-md border border-border bg-background px-2.5 py-2 text-xs"
            >
              <input
                type="checkbox"
                checked={permissions.includes(capability.ref)}
                disabled={busy}
                onChange={() =>
                  setPermissions((current) =>
                    current.includes(capability.ref)
                      ? current.filter(
                          (candidate) => candidate !== capability.ref,
                        )
                      : [...current, capability.ref],
                  )
                }
              />
              {capability.label}
            </label>
          ))}
        </div>
      </fieldset>
      {retired.length > 0 ? (
        <div className="mt-4 rounded-md border border-border bg-background px-3 py-2">
          <p className="font-medium text-xs">
            現在は追加できない保存済みの権限
          </p>
          <p className="mt-1 text-muted-foreground text-xs leading-5">
            {retired.join("、")}
          </p>
        </div>
      ) : null}
      <div className="mt-4 flex justify-end gap-2">
        <Button type="button" variant="ghost" size="sm" onClick={onCancel}>
          キャンセル
        </Button>
        <Button type="submit" size="sm" disabled={busy || !name.trim()}>
          {role ? "保存" : "作成"}
        </Button>
      </div>
    </form>
  );
}

function grantableRoleCapabilities(
  catalog: readonly AppDescriptor[],
): WorkspaceRoleCapabilityDescriptor[] {
  const capabilities = [
    ...PLATFORM_CAPABILITIES,
    ...catalog.flatMap((app) => app.workspaceRoleCapabilities),
  ];
  const seen = new Set<string>();
  return capabilities.filter((capability) => {
    if (seen.has(capability.ref)) return false;
    seen.add(capability.ref);
    return true;
  });
}

function roleCapabilityLabel(
  ref: string,
  grantable: readonly WorkspaceRoleCapabilityDescriptor[],
): string {
  return (
    grantable.find((capability) => capability.ref === ref)?.label ??
    `${ref}（現在は利用できません）`
  );
}

function AppsSection({
  workspaceId,
  canManage,
}: {
  workspaceId: string;
  canManage: boolean;
}) {
  const catalog = useWorkspaceControl((state) => state.catalog);
  const installations = useWorkspaceControl((state) => state.installations);
  const mutation = useWorkspaceControl((state) => state.mutation);
  const installApp = useWorkspaceControl((state) => state.installApp);
  const setInstallationState = useWorkspaceControl(
    (state) => state.setInstallationState,
  );
  const uninstallApp = useWorkspaceControl((state) => state.uninstallApp);
  const [failed, setFailed] = useState("");
  const workspaceApps = catalog.filter((app) => app.workspaceOwnerAllowed);

  const run = async (action: () => Promise<unknown>) => {
    setFailed("");
    try {
      await action();
    } catch {
      setFailed(
        "アプリの状態を変更できませんでした。権限と最新状態を確認してください。",
      );
    }
  };

  return (
    <div className="space-y-4">
      <p className="max-w-2xl text-muted-foreground text-sm leading-6">
        このWorkspaceで使うアプリを追加・停止できます。無効にするとデータを残したまま利用を止め、アンインストールするとWorkspaceから取り外します。
      </p>
      {workspaceApps.map((descriptor) => (
        <AppCard
          key={descriptor.appId}
          workspaceId={workspaceId}
          descriptor={descriptor}
          installations={installations.filter(
            (installation) => installation.appId === descriptor.appId,
          )}
          canManage={canManage}
          busy={mutation !== null}
          onInstall={() => run(() => installApp(descriptor.appId))}
          onState={(installationId, state) =>
            run(() => setInstallationState(installationId, state))
          }
          onUninstall={(installationId) =>
            run(() => uninstallApp(installationId))
          }
        />
      ))}
      {workspaceApps.length === 0 ? (
        <div className="rounded-xl border border-border border-dashed p-8 text-center text-muted-foreground text-sm">
          このWorkspaceへ導入できるアプリはありません
        </div>
      ) : null}
      {!canManage ? (
        <p className="text-muted-foreground text-xs">
          アプリを変更する権限がありません。
        </p>
      ) : null}
      {failed ? (
        <p role="alert" className="text-red-600 text-xs">
          {failed}
        </p>
      ) : null}
    </div>
  );
}

function AppCard({
  workspaceId,
  descriptor,
  installations,
  canManage,
  busy,
  onInstall,
  onState,
  onUninstall,
}: {
  workspaceId: string;
  descriptor: AppDescriptor;
  installations: AppInstallation[];
  canManage: boolean;
  busy: boolean;
  onInstall: () => Promise<unknown>;
  onState: (
    installationId: string,
    state: "enabled" | "disabled",
  ) => Promise<unknown>;
  onUninstall: (installationId: string) => Promise<unknown>;
}) {
  const navigate = useNavigate();
  const installation = installations.length === 1 ? installations[0] : null;
  const renderer = WORKSPACE_APP_RENDERERS[descriptor.appId];
  const Icon = descriptor.appId === "messaging" ? MessageCircle : AppWindow;
  return (
    <article className="rounded-xl border border-border p-5">
      <div className="flex items-start gap-4">
        <span className="grid size-11 shrink-0 place-items-center rounded-xl bg-muted">
          <Icon className="size-5" />
        </span>
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            <h3 className="font-semibold text-sm">{descriptor.displayName}</h3>
            {installation ? (
              <span className="rounded bg-muted px-1.5 py-0.5 text-[10px]">
                {installation.state === "enabled" ? "有効" : "無効"}
              </span>
            ) : null}
          </div>
          <p className="mt-1 text-muted-foreground text-xs">
            {renderer
              ? "この画面から利用できます"
              : "追加できますが、この端末ではまだ開けません"}
          </p>
          {installations.length > 1 ? (
            <p role="alert" className="mt-2 text-red-600 text-xs">
              同じアプリが複数登録されています。利用する前に設定の修復が必要です。
            </p>
          ) : null}
        </div>
        <div className="flex flex-wrap justify-end gap-2">
          {!installation && installations.length === 0 ? (
            <Button
              size="sm"
              disabled={!canManage || busy}
              onClick={() => void onInstall()}
            >
              インストール
            </Button>
          ) : null}
          {installation?.state === "disabled" ? (
            <Button
              size="sm"
              disabled={!canManage || busy}
              onClick={() =>
                void onState(installation.installationId, "enabled")
              }
            >
              有効にする
            </Button>
          ) : null}
          {installation?.state === "enabled" && renderer ? (
            <Button
              size="sm"
              onClick={() => void navigate({ to: renderer.route(workspaceId) })}
            >
              開く
              <ArrowRight className="size-3.5" />
            </Button>
          ) : null}
          {installation?.state === "enabled" ? (
            <Button
              size="sm"
              variant="secondary"
              disabled={!canManage || busy}
              onClick={() =>
                void onState(installation.installationId, "disabled")
              }
            >
              無効にする
            </Button>
          ) : null}
          {installation ? (
            <Button
              size="sm"
              variant="ghost"
              disabled={!canManage || busy}
              onClick={() => {
                if (
                  !window.confirm(
                    `${descriptor.displayName}をアンインストールしますか？`,
                  )
                )
                  return;
                void onUninstall(installation.installationId);
              }}
            >
              アンインストール
            </Button>
          ) : null}
        </div>
      </div>
    </article>
  );
}

function RoleDot({ color }: { color?: string }) {
  return (
    <span
      className={`mt-1 size-2.5 shrink-0 rounded-full ${color ? "" : "border border-muted-foreground/50"}`}
      style={color ? { backgroundColor: color } : undefined}
    />
  );
}

function exactInstallation(
  installations: AppInstallation[],
  appId: string,
): AppInstallation | "duplicate" | null {
  const matches = installations.filter((item) => item.appId === appId);
  if (matches.length > 1) return "duplicate";
  return matches[0] ?? null;
}
