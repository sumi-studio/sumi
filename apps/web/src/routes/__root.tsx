import { createRootRoute } from "@tanstack/react-router";
import { AuthGate } from "../auth/auth-gate";
import { ParticipantAppBinding } from "../participant/app-binding";
import { AppShell } from "../shell/app-shell";

export const Route = createRootRoute({
  component: RootLayout,
});

export function RootLayout() {
  return (
    <AuthGate>
      <ParticipantAppBinding>
        <AppShell />
      </ParticipantAppBinding>
    </AuthGate>
  );
}
