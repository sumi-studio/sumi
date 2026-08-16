import { createRootRoute } from "@tanstack/react-router";
import { AuthGate } from "../auth/auth-gate";
import { AppShell } from "../shell/app-shell";

export const Route = createRootRoute({
  component: RootLayout,
});

export function RootLayout() {
  return (
    <AuthGate>
      <AppShell />
    </AuthGate>
  );
}
