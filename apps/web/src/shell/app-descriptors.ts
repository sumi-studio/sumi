import { DoorOpen, MessageCircle } from "lucide-react";
import type { ComponentType } from "react";

/**
 * Renderer knowledge only. Availability and lifecycle come from the canonical
 * catalog + exact enabled installation; this registry cannot make an app
 * appear by itself.
 */
export interface WorkspaceAppRenderer {
  appId: string;
  icon: ComponentType<{ className?: string }>;
  route(workspaceId: string): string;
  renderer: "builtin";
}

export const WORKSPACE_APP_RENDERERS: Record<string, WorkspaceAppRenderer> = {
  messaging: {
    appId: "messaging",
    icon: MessageCircle,
    route: (workspaceId) => `/w/${encodeURIComponent(workspaceId)}/messaging`,
    renderer: "builtin",
  },
};

/** Direct Chat is Participant-owned and never joins the Workspace app list. */
export const DIRECT_CHAT_RENDERER = {
  appId: "direct-chat",
  label: "直通",
  icon: DoorOpen,
  route: "/direct",
} as const;
