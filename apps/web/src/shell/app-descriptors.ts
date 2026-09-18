import {
  DoorOpen,
  MessageCircle,
  MessageSquarePlus,
  SquareTerminal,
} from "lucide-react";
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

/**
 * The shared Cloud terminal is also Participant-owned: the person and the
 * secretary attach to the same durable session, independent of Workspace
 * selection. `/terminal` is the SPA page; `/terminal/*` stays API.
 */
export const TERMINAL_RENDERER = {
  appId: "terminal",
  label: "ターミナル",
  icon: SquareTerminal,
  route: "/terminal",
} as const;

export const FEEDBACK_RENDERER = {
  appId: "feedback",
  label: "Feedback",
  icon: MessageSquarePlus,
  route: "/feedback",
} as const;
