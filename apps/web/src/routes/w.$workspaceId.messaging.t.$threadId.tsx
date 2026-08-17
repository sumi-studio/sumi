import { createFileRoute } from "@tanstack/react-router";
import { MessagingScreen } from "../messaging/components/messaging-screen";

export const Route = createFileRoute("/w/$workspaceId/messaging/t/$threadId")({
  component: ThreadRoute,
});

function ThreadRoute() {
  const { threadId } = Route.useParams();
  return <MessagingScreen placeKey={`thread:${threadId}`} />;
}
