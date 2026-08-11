import { TooltipProvider } from "@sumi/ui/components/tooltip";
import { createRouter, RouterProvider } from "@tanstack/react-router";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { routeTree } from "./routeTree.gen";
import "gen-interface-jp/400.css";
import "gen-interface-jp/600.css";
import "@sumi/ui/globals.css";
import { AuthProvider } from "./auth/auth-context";
import { AuthOutcomeNoticeHost } from "./auth/auth-outcome-notice-host";
import { ParticipantAppBinding } from "./participant/app-binding";
import { initializeTheme, ThemeProvider } from "./theme/theme-provider";

const router = createRouter({ routeTree });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}

const rootElement = document.getElementById("root");
if (!rootElement) {
  throw new Error("Root element #root not found");
}

initializeTheme();

createRoot(rootElement).render(
  <StrictMode>
    <ThemeProvider>
      <AuthProvider>
        <ParticipantAppBinding>
          <AuthOutcomeNoticeHost />
          <TooltipProvider>
            <RouterProvider router={router} />
          </TooltipProvider>
        </ParticipantAppBinding>
      </AuthProvider>
    </ThemeProvider>
  </StrictMode>,
);
