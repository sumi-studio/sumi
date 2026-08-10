import { createFileRoute } from "@tanstack/react-router";
import { AuthGate } from "../auth/auth-gate";
import { MessagingScreen } from "../messaging/components/messaging-screen";

/**
 * ルートは特定のWorkspaceやplaceを推測しないホーム。現在地があるときは
 * URL（/c/:id、/dm/:id）が正本になり、ここでは明示的な選択を待つ。
 */
export const Route = createFileRoute("/")({
  component: HomeRoute,
});

export function HomeRoute() {
  return (
    <AuthGate>
      <MessagingScreen />
    </AuthGate>
  );
}
