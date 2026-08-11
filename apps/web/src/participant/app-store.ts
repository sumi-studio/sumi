import { create } from "zustand";
import {
  WorkspaceAPIError,
  WorkspaceApiClient,
  type WorkspaceControlClient,
} from "../workspace/api-client";
import type {
  AppDescriptor,
  AppInstallation,
  AppInstallationState,
  AppOwnerRef,
  ParticipantRef,
} from "../workspace/model";
import { appOwnerKey, isOwnedBy, participantKey } from "../workspace/model";

export type ParticipantAppStatus = "idle" | "loading" | "ready" | "error";

/**
 * Participant-owned app lifecycle.
 *
 * `AppInstallationOwnerRef = Workspace | Participant` (ADR 0008). This store
 * holds the Participant arm only. It is deliberately not Workspace-scoped:
 * the owner is the signed-in Participant, so switching Workspace A/B must
 * never move, duplicate, reload, or implicitly disable these installations.
 * Lifecycle verbs are the same domain operations the Workspace arm uses; only
 * the owner ref differs.
 */
export interface ParticipantAppState {
  owner: AppOwnerRef | null;
  status: ParticipantAppStatus;
  catalog: AppDescriptor[];
  installations: AppInstallation[];
  errorCode: string | null;
  mutation: string | null;

  bindParticipant(participant: ParticipantRef | null): Promise<void>;
  refresh(): Promise<void>;
  installApp(appId: string): Promise<AppInstallation>;
  setInstallationState(
    installationId: string,
    state: AppInstallationState,
  ): Promise<AppInstallation>;
  uninstallApp(installationId: string): Promise<void>;
}

interface OwnerScopeToken {
  ownerKey: string;
  generation: number;
}

/**
 * Exactly one installation addresses one app for one owner; the server holds a
 * matching `UNIQUE (owner_kind, owner_id, app_id)`. A duplicate is reported
 * rather than resolved, so neither the UI nor an agent ever picks between
 * candidate installation IDs.
 */
export function participantInstallation(
  installations: readonly AppInstallation[],
  appId: string,
): AppInstallation | "duplicate" | null {
  const matches = installations.filter(
    (installation) => installation.appId === appId,
  );
  if (matches.length > 1) return "duplicate";
  return matches[0] ?? null;
}

export function createParticipantAppStore(client: WorkspaceControlClient) {
  let generation = 0;
  let load: { ownerKey: string; promise: Promise<void> } | null = null;

  return create<ParticipantAppState>((set, get) => {
    const currentScope = (): OwnerScopeToken => {
      const owner = get().owner;
      if (!owner) throw new Error("Participant app owner is not bound");
      return { ownerKey: appOwnerKey(owner), generation };
    };

    const isCurrentScope = (token: OwnerScopeToken): boolean => {
      const owner = get().owner;
      return (
        owner !== null &&
        appOwnerKey(owner) === token.ownerKey &&
        generation === token.generation
      );
    };

    const beginMutation = (name: string): OwnerScopeToken => {
      const token = currentScope();
      if (get().mutation) {
        throw new Error("Participant app mutation is already running");
      }
      set({ mutation: name, errorCode: null });
      return token;
    };

    const endMutation = (token: OwnerScopeToken, error?: unknown): void => {
      if (!isCurrentScope(token)) return;
      set({ mutation: null, errorCode: error ? errorCode(error) : null });
    };

    const loadOwner = async (owner: AppOwnerRef): Promise<void> => {
      const ownerKey = appOwnerKey(owner);
      if (load?.ownerKey === ownerKey && get().status === "loading") {
        return load.promise;
      }
      const token: OwnerScopeToken = { ownerKey, generation: ++generation };
      set({ status: "loading", errorCode: null, mutation: null });

      const promise = Promise.all([
        client.listAppCatalog(),
        client.listInstallations(owner),
      ])
        .then(([catalog, installations]) => {
          if (!isCurrentScope(token)) return;
          validateOwnedSnapshot(owner, catalog, installations);
          set({
            status: "ready",
            catalog,
            installations,
            errorCode: null,
          });
        })
        .catch((error: unknown) => {
          if (!isCurrentScope(token)) return;
          set({ status: "error", errorCode: errorCode(error) });
        })
        .finally(() => {
          if (load?.ownerKey === ownerKey) load = null;
        });

      load = { ownerKey, promise };
      return promise;
    };

    return {
      owner: null,
      status: "idle",
      catalog: [],
      installations: [],
      errorCode: null,
      mutation: null,

      async bindParticipant(participant) {
        if (!participant) {
          generation += 1;
          load = null;
          set({
            owner: null,
            status: "idle",
            catalog: [],
            installations: [],
            errorCode: null,
            mutation: null,
          });
          return;
        }
        const owner: AppOwnerRef = { kind: "participant", participant };
        const current = get().owner;
        // Re-binding the same Participant is a no-op. Workspace navigation
        // re-runs this on every screen; it must not restart the lifecycle.
        if (current && appOwnerKey(current) === appOwnerKey(owner)) {
          if (get().status === "idle") await loadOwner(owner);
          return;
        }
        generation += 1;
        load = null;
        set({
          owner,
          status: "idle",
          catalog: [],
          installations: [],
          errorCode: null,
          mutation: null,
        });
        await loadOwner(owner);
      },

      async refresh() {
        const owner = get().owner;
        if (!owner) return;
        await loadOwner(owner);
      },

      async installApp(appId) {
        const token = beginMutation("install_app");
        const owner = get().owner;
        if (!owner) throw new Error("Participant app owner is not bound");
        const descriptor = get().catalog.find((app) => app.appId === appId);
        if (!descriptor?.participantOwnerAllowed) {
          const error = new Error("App does not allow a Participant owner");
          endMutation(token, error);
          throw error;
        }
        if (participantInstallation(get().installations, appId) !== null) {
          const error = new Error("App is already installed");
          endMutation(token, error);
          throw error;
        }
        try {
          const installation = await client.installApp(owner, appId);
          if (!isCurrentScope(token)) return installation;
          validateInstallation(owner, installation, appId);
          set((state) => ({
            installations: [...state.installations, installation],
          }));
          endMutation(token);
          return installation;
        } catch (error) {
          endMutation(token, error);
          throw error;
        }
      },

      async setInstallationState(installationId, state) {
        const token = beginMutation(`set_installation_${state}`);
        const owner = get().owner;
        const current = get().installations.find(
          (installation) => installation.installationId === installationId,
        );
        if (!owner || !current) {
          const error = new Error("App installation is not active");
          endMutation(token, error);
          throw error;
        }
        try {
          const installation = await client.setInstallationState(
            installationId,
            state,
          );
          if (!isCurrentScope(token)) return installation;
          validateInstallation(owner, installation, current.appId);
          if (
            installation.installationId !== installationId ||
            installation.state !== state
          ) {
            throw new Error("App lifecycle response does not match intent");
          }
          set((ownerState) => ({
            installations: ownerState.installations.map((entry) =>
              entry.installationId === installationId ? installation : entry,
            ),
          }));
          endMutation(token);
          return installation;
        } catch (error) {
          endMutation(token, error);
          throw error;
        }
      },

      async uninstallApp(installationId) {
        const token = beginMutation("uninstall_app");
        if (
          !get().installations.some(
            (installation) => installation.installationId === installationId,
          )
        ) {
          const error = new Error("App installation is not active");
          endMutation(token, error);
          throw error;
        }
        try {
          await client.uninstallApp(installationId);
          if (!isCurrentScope(token)) return;
          // Uninstall removes only the owner binding. App-owned data, grants,
          // and credentials are separate app operations and are not cascaded.
          set((state) => ({
            installations: state.installations.filter(
              (installation) => installation.installationId !== installationId,
            ),
          }));
          endMutation(token);
        } catch (error) {
          endMutation(token, error);
          throw error;
        }
      },
    };
  });
}

function validateOwnedSnapshot(
  owner: AppOwnerRef,
  catalog: readonly AppDescriptor[],
  installations: readonly AppInstallation[],
): void {
  const appIds = new Set<string>();
  for (const descriptor of catalog) {
    if (appIds.has(descriptor.appId)) {
      throw new Error("App catalog repeats an app");
    }
    appIds.add(descriptor.appId);
  }
  const installedApps = new Set<string>();
  for (const installation of installations) {
    if (!isOwnedBy(installation, owner)) {
      throw new Error("App installation response crossed owner scope");
    }
    if (installedApps.has(installation.appId)) {
      throw new Error("App installations repeat an app for one owner");
    }
    installedApps.add(installation.appId);
  }
}

function validateInstallation(
  owner: AppOwnerRef,
  installation: AppInstallation,
  appId: string,
): void {
  if (installation.appId !== appId || !isOwnedBy(installation, owner)) {
    throw new Error("App installation response crossed owner scope");
  }
}

function errorCode(error: unknown): string {
  if (error instanceof WorkspaceAPIError) return error.code;
  if (error instanceof Error && error.message) return error.message;
  return "participant_app_request_failed";
}

export const useParticipantApps = createParticipantAppStore(
  new WorkspaceApiClient(),
);

/** Stable key for the bound Participant, for effect dependencies. */
export function boundParticipantKey(owner: AppOwnerRef | null): string | null {
  return owner?.kind === "participant"
    ? participantKey(owner.participant)
    : null;
}
