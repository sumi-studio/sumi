import { type ReactNode, useLayoutEffect } from "react";
import { useConversation } from "../agent/store";
import { useAuth } from "../auth/auth-context";
import type { DirectChatInstallationBinding } from "../lib/direct-chat-socket";
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
    let previousBinding = enabledDirectChatBinding(
      useParticipantApps.getState(),
      humanId,
    );
    return useParticipantApps.subscribe((state) => {
      const binding = enabledDirectChatBinding(state, humanId);
      if (previousBinding !== null && !sameBinding(previousBinding, binding)) {
        suspendInstallation(previousBinding);
      }
      previousBinding = binding;
    });
  }, [humanId, suspendInstallation]);

  return children;
}

function enabledDirectChatBinding(
  state: ParticipantAppState,
  humanId: string,
): DirectChatInstallationBinding | null {
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
    ? {
        installationId: installation.installationId,
        authorityEpoch: installation.authorityEpoch,
      }
    : null;
}

function sameBinding(
  left: DirectChatInstallationBinding,
  right: DirectChatInstallationBinding | null,
): boolean {
  return (
    right !== null &&
    left.installationId === right.installationId &&
    left.authorityEpoch === right.authorityEpoch
  );
}
