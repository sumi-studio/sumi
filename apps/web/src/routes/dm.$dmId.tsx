import { createFileRoute } from "@tanstack/react-router";
import { WorkspaceContextRequired } from "../workspace/components/workspace-context-required";

export const Route = createFileRoute("/dm/$dmId")({
  component: WorkspaceContextRequired,
});
