import { createRootRoute, Outlet } from "@tanstack/react-router";
import { MessagingDraftAttachmentsSession } from "../messaging/components/composer-attachments";

export const Route = createRootRoute({
  component: RootLayout,
});

function RootLayout() {
  return (
    <MessagingDraftAttachmentsSession>
      <Outlet />
    </MessagingDraftAttachmentsSession>
  );
}
