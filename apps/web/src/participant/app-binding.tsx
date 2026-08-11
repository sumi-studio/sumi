import { type ReactNode, useLayoutEffect } from "react";
import { useAuth } from "../auth/auth-context";
import { useParticipantApps } from "./app-store";

/**
 * Binds the browser's authenticated Human to the Participant-owned app arm.
 * Workspace navigation is deliberately absent from this boundary: changing
 * Workspace must not change which personal apps belong to the Human.
 */
export function ParticipantAppBinding({ children }: { children: ReactNode }) {
  const { authenticated, user } = useAuth();
  const bindParticipant = useParticipantApps((state) => state.bindParticipant);
  const humanId = authenticated ? (user?.id ?? null) : null;

  useLayoutEffect(() => {
    void bindParticipant(humanId ? { kind: "human", humanId } : null);
  }, [bindParticipant, humanId]);

  return children;
}
