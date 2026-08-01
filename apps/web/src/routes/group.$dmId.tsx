import { createFileRoute } from "@tanstack/react-router";
import { AuthGate } from "../auth/auth-gate";
import { MessagingScreen } from "../messaging/components/messaging-screen";

export const Route = createFileRoute("/group/$dmId")({
  component: () => (
    <AuthGate>
      <GroupDmRoute />
    </AuthGate>
  ),
});

function GroupDmRoute() {
  const { dmId } = Route.useParams();
  return <MessagingScreen placeKey={`group_dm:${dmId}`} />;
}
