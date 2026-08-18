import { create } from "zustand";
import {
  type MemberProfile,
  type ParticipantKey,
  participantKey,
} from "../messaging/model";

interface SelfProfileState {
  /** Participant-global confirmed profiles, independent of Messaging scope. */
  profilesByKey: Record<ParticipantKey, MemberProfile>;
  /** Session data seeds a name; server profiles alone participate in ordering. */
  confirmedKeys: Record<ParticipantKey, true>;
}

export const useSelfProfile = create<SelfProfileState>(() => ({
  profilesByKey: {},
  confirmedKeys: {},
}));

const revisionOf = (profile: MemberProfile): number => profile.revision ?? 0;

/** Clear application-level profile projections at the authenticated-session boundary. */
export function clearSelfProfiles(): void {
  useSelfProfile.setState((current) =>
    Object.keys(current.profilesByKey).length === 0
      ? current
      : { profilesByKey: {}, confirmedKeys: {} },
  );
}

/**
 * Seed a Human profile from an auth response. The response is not a second
 * authority: a revisioned server profile for this participant always wins.
 */
export function seedSelfProfileFromSession(
  humanId: string | null,
  displayName: string | null = null,
): void {
  if (humanId === null) {
    clearSelfProfiles();
    return;
  }
  const profile: MemberProfile | null =
    displayName === null
      ? null
      : {
          participant: { kind: "human", humanId },
          displayName,
          tagline: "",
        };
  if (profile === null) return;
  const key = participantKey(profile.participant);
  useSelfProfile.setState((current) => {
    if (
      current.confirmedKeys[key] ||
      current.profilesByKey[key]?.displayName === displayName
    ) {
      return current;
    }
    return {
      ...current,
      profilesByKey: { ...current.profilesByKey, [key]: profile },
    };
  });
}

/**
 * The sole revision gate for participant-global profile projections. Messaging
 * scopes consume this value as a derived member entry; they do not own it.
 */
export function applyConfirmedSelfProfile(
  profile: MemberProfile,
): MemberProfile {
  const key = participantKey(profile.participant);
  let accepted = profile;
  useSelfProfile.setState((current) => {
    const existing = current.profilesByKey[key];
    if (
      current.confirmedKeys[key] &&
      existing !== undefined &&
      revisionOf(profile) <= revisionOf(existing)
    ) {
      accepted = existing;
      return current;
    }
    return {
      profilesByKey: { ...current.profilesByKey, [key]: profile },
      confirmedKeys: { ...current.confirmedKeys, [key]: true },
    };
  });
  return accepted;
}
