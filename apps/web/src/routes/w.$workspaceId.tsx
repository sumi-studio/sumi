import { createFileRoute, Outlet } from "@tanstack/react-router";
import { WorkspaceBoundary } from "../workspace/components/workspace-boundary";

export const Route = createFileRoute("/w/$workspaceId")({
  component: WorkspaceLayout,
});

function WorkspaceLayout() {
  const { workspaceId } = Route.useParams();
  return (
    <WorkspaceBoundary workspaceId={workspaceId}>
      <Outlet />
    </WorkspaceBoundary>
  );
}
