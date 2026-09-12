import { useAuth } from "../auth/auth-context";
import { FeedbackInbox, type InboxLocation } from "./inbox";

export function FeedbackGate({
  location,
  navigate,
}: {
  location: InboxLocation;
  navigate(location: InboxLocation): void;
}) {
  const { authenticated, user } = useAuth();
  if (!authenticated || !user) return null;
  return (
    <FeedbackInbox
      key={user.id}
      actor={`human:${user.id}`}
      location={location}
      navigate={navigate}
    />
  );
}
