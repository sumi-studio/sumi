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
import { appOwnerKey, isOwnedBy } from "../workspace/model";

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

interface OwnerAuthorityToken {
  ownerKey: string;
  authorityGeneration: number;
}

interface LoadToken extends OwnerAuthorityToken {
  loadSequence: number;
}

interface MutationToken extends OwnerAuthorityToken {
  mutationSequence: number;
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
  let authorityGeneration = 0;
  let loadSequence = 0;
  let mutationSequence = 0;
  let activeMutationSequence: number | null = null;
  let load: { token: LoadToken; promise: Promise<void> } | null = null;
  let ownerOperations: {
    authorityGeneration: number;
    tail: Promise<void>;
  } | null = null;

  return create<ParticipantAppState>((set, get) => {
    const currentAuthority = (): OwnerAuthorityToken => {
      const owner = get().owner;
      if (!owner) throw new Error("Participant app owner is not bound");
      return {
        ownerKey: appOwnerKey(owner),
        authorityGeneration,
      };
    };

    const isCurrentAuthority = (token: OwnerAuthorityToken): boolean => {
      const owner = get().owner;
      return (
        owner !== null &&
        appOwnerKey(owner) === token.ownerKey &&
        authorityGeneration === token.authorityGeneration
      );
    };

    const enqueueOwnerOperation = <T>(
      token: OwnerAuthorityToken,
      operation: () => Promise<T>,
    ): Promise<T> => {
      if (
        !ownerOperations ||
        ownerOperations.authorityGeneration !== token.authorityGeneration
      ) {
        ownerOperations = {
          authorityGeneration: token.authorityGeneration,
          tail: Promise.resolve(),
        };
      }
      const queue = ownerOperations;
      const result = queue.tail.then(operation, operation);
      queue.tail = result.then(
        () => undefined,
        () => undefined,
      );
      return result;
    };

    const beginMutation = (name: string): MutationToken => {
      const authority = currentAuthority();
      if (get().mutation) {
        throw new Error("Participant app mutation is already running");
      }
      const token: MutationToken = {
        ...authority,
        mutationSequence: ++mutationSequence,
      };
      activeMutationSequence = token.mutationSequence;
      set({ mutation: name, errorCode: null });
      return token;
    };

    const endMutation = (token: MutationToken, error?: unknown): void => {
      if (
        !isCurrentAuthority(token) ||
        activeMutationSequence !== token.mutationSequence
      ) {
        return;
      }
      activeMutationSequence = null;
      set({ mutation: null, errorCode: error ? errorCode(error) : null });
    };

    const loadOwner = async (owner: AppOwnerRef): Promise<void> => {
      const ownerKey = appOwnerKey(owner);
      if (
        load?.token.ownerKey === ownerKey &&
        load.token.authorityGeneration === authorityGeneration
      ) {
        return load.promise;
      }
      const token: LoadToken = {
        ownerKey,
        authorityGeneration,
        loadSequence: ++loadSequence,
      };
      set({ status: "loading", errorCode: null });

      const promise = enqueueOwnerOperation(token, async () => {
        if (!isCurrentAuthority(token)) return;
        const [catalog, installations] = await Promise.all([
          client.listAppCatalog(),
          client.listInstallations(owner),
        ]);
        if (
          !isCurrentAuthority(token) ||
          load?.token.loadSequence !== token.loadSequence
        ) {
          return;
        }
        validateOwnedSnapshot(owner, catalog, installations);
        set({
          status: "ready",
          catalog,
          installations,
          errorCode: null,
        });
      })
        .catch((error: unknown) => {
          if (
            !isCurrentAuthority(token) ||
            load?.token.loadSequence !== token.loadSequence
          ) {
            return;
          }
          set({ status: "error", errorCode: errorCode(error) });
        })
        .finally(() => {
          if (load?.token.loadSequence === token.loadSequence) load = null;
        });

      load = { token, promise };
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
          authorityGeneration += 1;
          activeMutationSequence = null;
          load = null;
          ownerOperations = null;
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
        authorityGeneration += 1;
        activeMutationSequence = null;
        load = null;
        ownerOperations = null;
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
        try {
          const installation = await enqueueOwnerOperation(token, async () => {
            const owner = get().owner;
            if (!owner || !isCurrentAuthority(token)) {
              throw new Error("Participant app owner changed");
            }
            const descriptor = get().catalog.find((app) => app.appId === appId);
            if (!descriptor?.participantOwnerAllowed) {
              throw new Error("App does not allow a Participant owner");
            }
            if (participantInstallation(get().installations, appId) !== null) {
              throw new Error("App is already installed");
            }
            const response = await client.installApp(owner, appId);
            if (!isCurrentAuthority(token)) return response;
            validateInstallation(owner, response, appId);
            set((state) => ({
              installations: [...state.installations, response],
            }));
            return response;
          });
          endMutation(token);
          return installation;
        } catch (error) {
          endMutation(token, error);
          throw error;
        }
      },

      async setInstallationState(installationId, state) {
        const token = beginMutation(`set_installation_${state}`);
        try {
          const installation = await enqueueOwnerOperation(token, async () => {
            const owner = get().owner;
            const current = get().installations.find(
              (entry) => entry.installationId === installationId,
            );
            if (!owner || !current || !isCurrentAuthority(token)) {
              throw new Error("App installation is not active");
            }
            const response = await client.setInstallationState(
              installationId,
              state,
            );
            if (!isCurrentAuthority(token)) return response;
            validateInstallation(owner, response, current.appId);
            if (
              response.installationId !== installationId ||
              response.state !== state
            ) {
              throw new Error("App lifecycle response does not match intent");
            }
            set((ownerState) => ({
              installations: ownerState.installations.map((entry) =>
                entry.installationId === installationId ? response : entry,
              ),
            }));
            return response;
          });
          endMutation(token);
          return installation;
        } catch (error) {
          endMutation(token, error);
          throw error;
        }
      },

      async uninstallApp(installationId) {
        const token = beginMutation("uninstall_app");
        try {
          await enqueueOwnerOperation(token, async () => {
            if (
              !isCurrentAuthority(token) ||
              !get().installations.some(
                (installation) =>
                  installation.installationId === installationId,
              )
            ) {
              throw new Error("App installation is not active");
            }
            await client.uninstallApp(installationId);
            if (!isCurrentAuthority(token)) return;
            // Uninstall removes only the owner binding. App-owned data,
            // grants, and credentials are separate app operations and are not
            // cascaded.
            set((state) => ({
              installations: state.installations.filter(
                (installation) =>
                  installation.installationId !== installationId,
              ),
            }));
          });
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
