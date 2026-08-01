import { createFileRoute } from "@tanstack/react-router";
import { MessagingScreen } from "../messaging/components/messaging-screen";

export const Route = createFileRoute("/group/$dmId")({
  component: GroupDmRoute,
});

function GroupDmRoute() {
  const { dmId } = Route.useParams();
  return <MessagingScreen placeKey={`group_dm:${dmId}`} />;
}
