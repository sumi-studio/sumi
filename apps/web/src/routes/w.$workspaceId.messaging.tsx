import { createFileRoute, Outlet } from "@tanstack/react-router";
import { MessagingScopeGate } from "../workspace/components/messaging-scope-gate";

export const Route = createFileRoute("/w/$workspaceId/messaging")({
  component: MessagingLayout,
});

function MessagingLayout() {
  const { workspaceId } = Route.useParams();
  return (
    <MessagingScopeGate workspaceId={workspaceId}>
      <Outlet />
    </MessagingScopeGate>
  );
}
