import { createFileRoute } from "@tanstack/react-router";
import { AuthGate } from "../auth/auth-gate";
import { WorkspaceLanding } from "../workspace/components/workspace-landing";

/** Side-effect-free control-plane entry. Messaging is not initialized here. */
export const Route = createFileRoute("/")({
  component: HomeRoute,
});

export function HomeRoute() {
  return (
    <AuthGate>
      <WorkspaceLanding />
    </AuthGate>
  );
}
