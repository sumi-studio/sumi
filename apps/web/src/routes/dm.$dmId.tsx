import { createFileRoute } from "@tanstack/react-router";
import { AuthGate } from "../auth/auth-gate";
import { MessagingScreen } from "../messaging/components/messaging-screen";

export const Route = createFileRoute("/dm/$dmId")({
  component: () => (
    <AuthGate>
      <DmRoute />
    </AuthGate>
  ),
});

function DmRoute() {
  const { dmId } = Route.useParams();
  return <MessagingScreen placeKey={`dm:${dmId}`} />;
}
