import { createFileRoute } from "@tanstack/react-router";
import { AuthGate } from "../auth/auth-gate";
import { MessagingScreen } from "../messaging/components/messaging-screen";

export const Route = createFileRoute("/c/$channelId")({
  component: () => (
    <AuthGate>
      <ChannelRoute />
    </AuthGate>
  ),
});

function ChannelRoute() {
  const { channelId } = Route.useParams();
  return <MessagingScreen placeKey={`channel:${channelId}`} />;
}
