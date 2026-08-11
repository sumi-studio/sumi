import { createFileRoute } from "@tanstack/react-router";
import { WorkspaceContextRequired } from "../workspace/components/workspace-context-required";

export const Route = createFileRoute("/group/$dmId")({
  component: WorkspaceContextRequired,
});
