import { createFileRoute } from "@tanstack/react-router";
import { AuthGate } from "../auth/auth-gate";
import { MessagingScreen } from "../messaging/components/messaging-screen";

export const Route = createFileRoute("/t/$threadId")({
  component: () => (
    <AuthGate>
      <ThreadRoute />
    </AuthGate>
  ),
});

function ThreadRoute() {
  const { threadId } = Route.useParams();
  return <MessagingScreen placeKey={`thread:${threadId}`} />;
}
