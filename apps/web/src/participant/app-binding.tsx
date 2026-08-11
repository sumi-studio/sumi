import { type ReactNode, useLayoutEffect } from "react";
import { useConversation } from "../agent/store";
import { useAuth } from "../auth/auth-context";
import { DIRECT_CHAT_RENDERER } from "../shell/app-descriptors";
import type { ParticipantAppState } from "./app-store";
import { participantInstallation, useParticipantApps } from "./app-store";

/**
 * Binds the browser's authenticated Human to the Participant-owned app arm.
 * Workspace navigation is deliberately absent from this boundary: changing
 * Workspace must not change which personal apps belong to the Human.
 */
export function ParticipantAppBinding({ children }: { children: ReactNode }) {
  const { authenticated, user } = useAuth();
  const bindParticipant = useParticipantApps((state) => state.bindParticipant);
  const suspendInstallation = useConversation(
    (state) => state.suspendInstallation,
  );
  const humanId = authenticated ? (user?.id ?? null) : null;

  useLayoutEffect(() => {
    void bindParticipant(humanId ? { kind: "human", humanId } : null);
  }, [bindParticipant, humanId]);

  useLayoutEffect(() => {
    if (!humanId) return;

    // Subscribe to the vanilla store rather than observing a rendered value.
    // Lifecycle mutations can complete in one React batch while this route is
    // elsewhere; every enabled -> disabled/uninstalled/replaced transition
    // must still suspend the old installation's transport authority epoch.
    let previousInstallationId = enabledDirectChatInstallationId(
      useParticipantApps.getState(),
      humanId,
    );
    return useParticipantApps.subscribe((state) => {
      const installationId = enabledDirectChatInstallationId(state, humanId);
      if (
        previousInstallationId !== null &&
        previousInstallationId !== installationId
      ) {
        suspendInstallation(previousInstallationId);
      }
      previousInstallationId = installationId;
    });
  }, [humanId, suspendInstallation]);

  return children;
}

function enabledDirectChatInstallationId(
  state: ParticipantAppState,
  humanId: string,
): string | null {
  if (
    state.owner?.kind !== "participant" ||
    state.owner.participant.kind !== "human" ||
    state.owner.participant.humanId !== humanId
  ) {
    return null;
  }
  const installation = participantInstallation(
    state.installations,
    DIRECT_CHAT_RENDERER.appId,
  );
  return installation !== "duplicate" && installation?.state === "enabled"
    ? installation.installationId
    : null;
}
