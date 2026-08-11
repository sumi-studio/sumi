import { createFileRoute, Outlet } from "@tanstack/react-router";
import { AuthGate } from "../auth/auth-gate";
import { WorkspaceBoundary } from "../workspace/components/workspace-boundary";

export const Route = createFileRoute("/w/$workspaceId")({
  component: WorkspaceLayout,
});

function WorkspaceLayout() {
  const { workspaceId } = Route.useParams();
  return (
    <AuthGate>
      <WorkspaceBoundary workspaceId={workspaceId}>
        <Outlet />
      </WorkspaceBoundary>
    </AuthGate>
  );
}
