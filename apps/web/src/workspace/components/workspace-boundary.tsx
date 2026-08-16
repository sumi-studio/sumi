import { Button } from "@sumi/ui/components/button";
import { useNavigate } from "@tanstack/react-router";
import { AlertCircle } from "lucide-react";
import { type ReactNode, useEffect, useLayoutEffect } from "react";
import { bindMessagingScope } from "../../messaging/store";
import { useWorkspaceControl } from "../store";

export function WorkspaceBoundary({
  workspaceId,
  children,
}: {
  workspaceId: string;
  children: ReactNode;
}) {
  const navigate = useNavigate();
  const listStatus = useWorkspaceControl((state) => state.listStatus);
  const selectionStatus = useWorkspaceControl((state) => state.selectionStatus);
  const selectedWorkspaceId = useWorkspaceControl(
    (state) => state.selectedWorkspaceId,
  );
  const init = useWorkspaceControl((state) => state.init);
  const refreshWorkspaces = useWorkspaceControl(
    (state) => state.refreshWorkspaces,
  );
  const selectWorkspace = useWorkspaceControl((state) => state.selectWorkspace);

  useEffect(() => {
    void init();
  }, [init]);

  // A changed URL stops rendering the old subtree immediately. The layout
  // fence then disposes its Messaging scope before the browser paints.
  useLayoutEffect(() => {
    if (listStatus !== "ready") return;
    if (selectedWorkspaceId === workspaceId && selectionStatus !== "idle") {
      return;
    }
    bindMessagingScope(null);
    void selectWorkspace(workspaceId);
  }, [
    listStatus,
    selectedWorkspaceId,
    selectionStatus,
    selectWorkspace,
    workspaceId,
  ]);

  const exactSelection = selectedWorkspaceId === workspaceId;
  if (listStatus === "error" || selectionStatus === "error") {
    return (
      <BoundaryStatus
        title="Workspaceを読み込めませんでした"
        detail="接続を確認して再試行してください。別のWorkspaceへ自動では移動しません。"
        action={
          <Button
            onClick={() => {
              if (listStatus === "error") {
                void refreshWorkspaces();
                return;
              }
              void selectWorkspace(workspaceId);
            }}
          >
            再試行
          </Button>
        }
      />
    );
  }
  if (exactSelection && selectionStatus === "invalid") {
    return (
      <BoundaryStatus
        title="このWorkspaceを開けません"
        detail="所属が終了したか、URLが古くなっています。別のWorkspaceを明示的に選んでください。"
        action={
          <Button onClick={() => void navigate({ to: "/" })}>
            Workspace一覧へ
          </Button>
        }
      />
    );
  }
  if (
    listStatus === "idle" ||
    listStatus === "loading" ||
    !exactSelection ||
    selectionStatus === "idle" ||
    selectionStatus === "loading"
  ) {
    return <BoundaryStatus title="Workspaceを開いています…" />;
  }
  return children;
}

function BoundaryStatus({
  title,
  detail,
  action,
}: {
  title: string;
  detail?: string;
  action?: ReactNode;
}) {
  return (
    <main className="grid h-full place-items-center bg-background px-6 text-foreground">
      <section
        className="flex max-w-md flex-col items-center gap-3 text-center"
        aria-live="polite"
      >
        {detail ? (
          <AlertCircle className="size-5 text-muted-foreground" />
        ) : null}
        <h1 className="font-semibold text-lg">{title}</h1>
        {detail ? (
          <p className="text-muted-foreground text-sm leading-6">{detail}</p>
        ) : null}
        {action}
      </section>
    </main>
  );
}
