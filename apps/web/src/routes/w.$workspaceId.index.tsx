import { createFileRoute } from "@tanstack/react-router";
import { WorkspaceHome } from "../workspace/components/workspace-home";

export const Route = createFileRoute("/w/$workspaceId/")({
  component: WorkspaceIndex,
});

function WorkspaceIndex() {
  const { workspaceId } = Route.useParams();
  return <WorkspaceHome workspaceId={workspaceId} />;
}
