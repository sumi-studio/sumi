import { createFileRoute } from "@tanstack/react-router";
import { MessagingScreen } from "../messaging/components/messaging-screen";

export const Route = createFileRoute("/dm/$dmId")({
  component: DmRoute,
});

function DmRoute() {
  const { dmId } = Route.useParams();
  return <MessagingScreen placeKey={`dm:${dmId}`} />;
}
