import { createFileRoute } from "@tanstack/react-router";
import { FeedbackGate } from "../feedback/gate";
import type { InboxLocation } from "../feedback/inbox";

export const Route = createFileRoute("/feedback")({
  validateSearch: (search: Record<string, unknown>): InboxLocation => ({
    ...(typeof search.thread === "string" && search.thread
      ? { thread: search.thread }
      : {}),
    ...(search.compose === true || search.compose === "true"
      ? { compose: true }
      : {}),
  }),
  component: FeedbackRoute,
});
function FeedbackRoute() {
  const location = Route.useSearch();
  const navigate = Route.useNavigate();
  return (
    <FeedbackGate
      location={location}
      navigate={(search) => void navigate({ search })}
    />
  );
}
