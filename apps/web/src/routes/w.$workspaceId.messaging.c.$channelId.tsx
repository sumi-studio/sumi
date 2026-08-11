import { createFileRoute } from "@tanstack/react-router";
import { MessagingScreen } from "../messaging/components/messaging-screen";

export const Route = createFileRoute("/w/$workspaceId/messaging/c/$channelId")({
  component: ChannelRoute,
});

function ChannelRoute() {
  const { channelId } = Route.useParams();
  return <MessagingScreen placeKey={`channel:${channelId}`} />;
}
