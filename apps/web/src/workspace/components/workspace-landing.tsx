import { Button } from "@sumi/ui/components/button";
import { useNavigate } from "@tanstack/react-router";
import { ArrowRight, Building2, KeyRound, Plus, RefreshCw } from "lucide-react";
import { type FormEvent, useEffect, useState } from "react";
import { AppRail } from "../../shell/app-rail";
import type { WorkspaceInvitePreview } from "../model";
import { useWorkspaceControl } from "../store";

const INPUT_CLASS =
  "w-full rounded-lg border border-border bg-background px-3 py-2 text-sm outline-none placeholder:text-muted-foreground/60 focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/20 disabled:opacity-50";

export function WorkspaceLanding() {
  const navigate = useNavigate();
  const listStatus = useWorkspaceControl((state) => state.listStatus);
  const workspaces = useWorkspaceControl((state) => state.workspaces);
  const errorCode = useWorkspaceControl((state) => state.errorCode);
  const mutation = useWorkspaceControl((state) => state.mutation);
  const init = useWorkspaceControl((state) => state.init);
  const refresh = useWorkspaceControl((state) => state.refreshWorkspaces);
  const createWorkspace = useWorkspaceControl((state) => state.createWorkspace);
  const redeemInvite = useWorkspaceControl((state) => state.redeemInvite);
  const previewInvite = useWorkspaceControl((state) => state.previewInvite);
  const [name, setName] = useState("");
  const [inviteCode, setInviteCode] = useState("");
  const [invitePreview, setInvitePreview] =
    useState<WorkspaceInvitePreview | null>(null);
  const [previewedCode, setPreviewedCode] = useState("");
  const [previewing, setPreviewing] = useState(false);
  const [localError, setLocalError] = useState("");

  useEffect(() => {
    void init();
  }, [init]);

  const create = async (event: FormEvent) => {
    event.preventDefault();
    const exactName = name.trim();
    if (!exactName || mutation) return;
    setLocalError("");
    try {
      const workspace = await createWorkspace(exactName);
      await navigate({
        to: "/w/$workspaceId",
        params: { workspaceId: workspace.workspaceId },
      });
    } catch {
      setLocalError(
        "Workspaceを作成できませんでした。名前を確認して再試行してください。",
      );
    }
  };

  const reviewInvite = async (event: FormEvent) => {
    event.preventDefault();
    const code = inviteCode.trim();
    if (!code || mutation || previewing) return;
    setPreviewing(true);
    setLocalError("");
    try {
      const preview = await previewInvite(code);
      setInvitePreview(preview);
      setPreviewedCode(code);
    } catch {
      setInvitePreview(null);
      setPreviewedCode("");
      setLocalError(
        "招待を確認できませんでした。期限切れ、使用済み、または無効なコードです。",
      );
    } finally {
      setPreviewing(false);
    }
  };

  const redeem = async () => {
    const code = inviteCode.trim();
    if (!code || code !== previewedCode || !invitePreview || mutation) return;
    setLocalError("");
    try {
      const membership = await redeemInvite(code);
      await navigate({
        to: "/w/$workspaceId",
        params: { workspaceId: membership.workspaceId },
      });
    } catch {
      setLocalError(
        "招待を使えませんでした。最新の状態を確認して、もう一度お試しください。",
      );
    }
  };

  return (
    <div className="flex min-h-dvh bg-background text-foreground">
      <AppRail activeAppId="workspace" />
      <main className="min-w-0 flex-1 overflow-y-auto">
        <div className="mx-auto flex min-h-dvh w-full max-w-5xl flex-col px-8 py-10 lg:px-12">
          <header className="mb-10 flex items-end justify-between gap-6">
            <div>
              <p className="mb-2 font-medium text-muted-foreground text-xs uppercase tracking-[0.16em]">
                Developer Workspace
              </p>
              <h1 className="font-semibold text-3xl tracking-tight">
                どこで一緒に働きますか
              </h1>
              <p className="mt-2 max-w-xl text-muted-foreground text-sm leading-6">
                Workspaceごとに参加者、役割、アプリ、会話が分かれます。Workspaceを切り替えても、参加状態やほかの人の作業には影響しません。
              </p>
            </div>
            {listStatus === "ready" ? (
              <Button
                variant="ghost"
                size="sm"
                onClick={() => void refresh()}
                className="gap-2"
              >
                <RefreshCw className="size-3.5" />
                更新
              </Button>
            ) : null}
          </header>

          {listStatus === "idle" || listStatus === "loading" ? (
            <LandingStatus title="Workspaceを確認しています…" />
          ) : listStatus === "error" ? (
            <LandingStatus
              title="Workspaceを読み込めませんでした"
              detail={errorCode ?? undefined}
              action={
                <Button onClick={() => void refresh()} className="mt-2">
                  再試行
                </Button>
              }
            />
          ) : (
            <div className="grid flex-1 gap-8 lg:grid-cols-[minmax(0,1fr)_21rem]">
              <section aria-labelledby="workspace-list-title">
                <div className="mb-3 flex items-center justify-between">
                  <h2
                    id="workspace-list-title"
                    className="font-semibold text-sm"
                  >
                    参加中のWorkspace
                  </h2>
                  <span className="text-muted-foreground text-xs">
                    {workspaces.length}件
                  </span>
                </div>
                {workspaces.length === 0 ? (
                  <div className="grid min-h-64 place-items-center rounded-2xl border border-border border-dashed bg-muted/15 px-8 text-center">
                    <div className="max-w-sm">
                      <span className="mx-auto mb-4 grid size-11 place-items-center rounded-xl border border-border bg-background shadow-sm">
                        <Building2 className="size-5 text-muted-foreground" />
                      </span>
                      <h3 className="font-semibold text-base">
                        まだWorkspaceに参加していません
                      </h3>
                      <p className="mt-2 text-muted-foreground text-sm leading-6">
                        右のフォームから最初のWorkspaceを作るか、受け取った招待コードで参加してください。
                      </p>
                    </div>
                  </div>
                ) : (
                  <div className="grid gap-3 sm:grid-cols-2">
                    {workspaces.map((workspace) => (
                      <button
                        key={workspace.workspaceId}
                        type="button"
                        onClick={() =>
                          void navigate({
                            to: "/w/$workspaceId",
                            params: { workspaceId: workspace.workspaceId },
                          })
                        }
                        className="group flex min-h-32 flex-col rounded-xl border border-border bg-background p-4 text-left shadow-sm transition-colors hover:border-foreground/25 hover:bg-muted/20 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/40"
                      >
                        <span className="mb-5 grid size-9 place-items-center rounded-lg bg-foreground font-semibold text-background text-sm">
                          {(workspace.name.trim().at(0) || "W").toUpperCase()}
                        </span>
                        <span className="flex w-full items-center gap-2">
                          <span className="min-w-0 flex-1 truncate font-semibold text-sm">
                            {workspace.name}
                          </span>
                          <ArrowRight className="size-4 shrink-0 text-muted-foreground transition-transform group-hover:translate-x-0.5" />
                        </span>
                      </button>
                    ))}
                  </div>
                )}
              </section>

              <aside className="space-y-4">
                <section className="rounded-xl border border-border bg-muted/10 p-4">
                  <div className="mb-4 flex items-center gap-2">
                    <Plus className="size-4" />
                    <h2 className="font-semibold text-sm">Workspaceを作成</h2>
                  </div>
                  <form
                    onSubmit={(event) => void create(event)}
                    onKeyDown={(event) => {
                      if (
                        event.key === "Enter" &&
                        event.nativeEvent.isComposing
                      ) {
                        event.preventDefault();
                      }
                    }}
                    className="space-y-3"
                  >
                    <label className="block">
                      <span className="mb-1.5 block font-medium text-xs">
                        名前
                      </span>
                      <input
                        value={name}
                        onChange={(event) => setName(event.target.value)}
                        maxLength={80}
                        disabled={mutation !== null}
                        placeholder="例: Sumi Studio"
                        aria-label="新しいWorkspaceの名前"
                        className={INPUT_CLASS}
                      />
                    </label>
                    <Button
                      type="submit"
                      disabled={!name.trim() || mutation !== null}
                      className="w-full"
                    >
                      {mutation === "create_workspace"
                        ? "作成中…"
                        : "作成して開く"}
                    </Button>
                  </form>
                </section>

                <section className="rounded-xl border border-border bg-muted/10 p-4">
                  <div className="mb-4 flex items-center gap-2">
                    <KeyRound className="size-4" />
                    <h2 className="font-semibold text-sm">招待で参加</h2>
                  </div>
                  <form
                    onSubmit={(event) => void reviewInvite(event)}
                    className="space-y-3"
                  >
                    <label className="block">
                      <span className="mb-1.5 block font-medium text-xs">
                        招待コード
                      </span>
                      <input
                        value={inviteCode}
                        onChange={(event) => {
                          setInviteCode(event.target.value);
                          setInvitePreview(null);
                          setPreviewedCode("");
                        }}
                        disabled={mutation !== null}
                        autoComplete="off"
                        aria-label="Workspace招待コード"
                        className={`${INPUT_CLASS} font-mono`}
                      />
                    </label>
                    <Button
                      type="submit"
                      variant="secondary"
                      disabled={
                        !inviteCode.trim() || mutation !== null || previewing
                      }
                      className="w-full"
                    >
                      {previewing ? "確認中…" : "招待を確認"}
                    </Button>
                    {invitePreview ? (
                      <div className="rounded-lg border border-border bg-background p-3">
                        <p className="font-semibold text-sm">
                          {invitePreview.workspaceName}
                        </p>
                        <p className="mt-1 text-muted-foreground text-xs">
                          {new Date(invitePreview.expiresAt).toLocaleString(
                            "ja-JP",
                          )}
                          まで有効
                        </p>
                        <Button
                          type="button"
                          disabled={mutation !== null}
                          className="mt-3 w-full"
                          onClick={() => void redeem()}
                        >
                          {mutation === "redeem_invite"
                            ? "参加中…"
                            : "このWorkspaceに参加"}
                        </Button>
                      </div>
                    ) : null}
                  </form>
                </section>

                {localError ? (
                  <p role="alert" className="text-red-600 text-xs leading-5">
                    {localError}
                  </p>
                ) : null}
              </aside>
            </div>
          )}
        </div>
      </main>
    </div>
  );
}

function LandingStatus({
  title,
  detail,
  action,
}: {
  title: string;
  detail?: string;
  action?: React.ReactNode;
}) {
  return (
    <section
      className="grid flex-1 place-items-center text-center"
      aria-live="polite"
    >
      <div>
        <p className="font-medium text-sm">{title}</p>
        {detail ? (
          <p className="mt-1 text-muted-foreground text-xs">{detail}</p>
        ) : null}
        {action}
      </div>
    </section>
  );
}
