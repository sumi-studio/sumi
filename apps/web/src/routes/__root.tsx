import { createRootRoute } from "@tanstack/react-router";
import { useEffect, useState } from "react";
import { AuthGate } from "../auth/auth-gate";
import {
  captureEnrollmentInvitation,
  clearWorkspaceInvitation,
  readWorkspaceInvitation,
  workspaceInvitationChanged,
} from "../auth/enrollment-invitation-state";
import { ParticipantAppBinding } from "../participant/app-binding";
import { AppShell } from "../shell/app-shell";
import { WorkspaceInvitationPrompt } from "../workspace/components/workspace-invitation-prompt";

export const Route = createRootRoute({
  component: RootLayout,
});

export function RootLayout() {
  const [invitation, setInvitation] = useState(() => {
    captureEnrollmentInvitation();
    return readWorkspaceInvitation();
  });
  useEffect(() => {
    const capture = () => {
      captureEnrollmentInvitation();
      setInvitation(readWorkspaceInvitation());
    };
    window.addEventListener("hashchange", capture);
    const restore = () => setInvitation(readWorkspaceInvitation());
    window.addEventListener(workspaceInvitationChanged, restore);
    return () => {
      window.removeEventListener("hashchange", capture);
      window.removeEventListener(workspaceInvitationChanged, restore);
    };
  }, []);
  return (
    <AuthGate>
      <ParticipantAppBinding>
        <AppShell />
        <WorkspaceInvitationPrompt
          code={invitation}
          onDismiss={() => {
            clearWorkspaceInvitation();
            setInvitation(null);
          }}
        />
      </ParticipantAppBinding>
    </AuthGate>
  );
}
