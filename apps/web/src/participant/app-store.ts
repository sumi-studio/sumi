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
  snapshotInvalidationGeneration: number;
}

interface MutationToken extends OwnerAuthorityToken {
  mutationSequence: number;
}

export interface ParticipantAppLifecycleCoordinator {
  runExclusive<T>(ownerKey: string, operation: () => Promise<T>): Promise<T>;
  publishMutation(ownerKey: string): void;
  subscribeMutations(listener: (ownerKey: string) => void): () => void;
  subscribeResume(listener: () => void): () => void;
}

const participantAppLifecycleChannel = "sumi:participant-app-lifecycle:v1";
const participantAppLifecycleLockPrefix =
  "sumi:participant-app-lifecycle:owner:";

function createIsolatedLifecycleCoordinator(): ParticipantAppLifecycleCoordinator {
  const tails = new Map<string, Promise<void>>();
  return {
    runExclusive<T>(ownerKey: string, operation: () => Promise<T>) {
      const previous = tails.get(ownerKey) ?? Promise.resolve();
      const result = previous.then(operation, operation);
      const tail = result.then(
        () => undefined,
        () => undefined,
      );
      tails.set(ownerKey, tail);
      void tail.finally(() => {
        if (tails.get(ownerKey) === tail) tails.delete(ownerKey);
      });
      return result;
    },
    publishMutation() {},
    subscribeMutations() {
      return () => undefined;
    },
    subscribeResume() {
      return () => undefined;
    },
  };
}

function createBrowserLifecycleCoordinator(): ParticipantAppLifecycleCoordinator {
  const mutationListeners = new Set<(ownerKey: string) => void>();
  const resumeListeners = new Set<() => void>();
  const channel =
    typeof globalThis.BroadcastChannel === "function"
      ? new globalThis.BroadcastChannel(participantAppLifecycleChannel)
      : null;

  channel?.addEventListener("message", (event: MessageEvent<unknown>) => {
    const notice = mutationNotice(event.data);
    if (!notice) return;
    for (const listener of mutationListeners) listener(notice.ownerKey);
  });

  const emitResume = () => {
    for (const listener of resumeListeners) listener();
  };
  globalThis.window?.addEventListener("pageshow", emitResume);
  globalThis.document?.addEventListener("visibilitychange", () => {
    if (globalThis.document.visibilityState === "visible") emitResume();
  });

  const requireCoordination = () => {
    const locks = globalThis.navigator?.locks;
    if (!channel || !locks) {
      throw new Error(
        "Participant app cross-document coordination is unavailable",
      );
    }
    return { channel, locks };
  };

  return {
    runExclusive<T>(ownerKey: string, operation: () => Promise<T>) {
      const { locks } = requireCoordination();
      return locks.request(
        `${participantAppLifecycleLockPrefix}${ownerKey}`,
        { mode: "exclusive" },
        operation,
      );
    },
    publishMutation(ownerKey: string) {
      const { channel: availableChannel } = requireCoordination();
      availableChannel.postMessage({
        version: 1,
        ownerKey,
        operationId: globalThis.crypto.randomUUID(),
      });
    },
    subscribeMutations(listener: (ownerKey: string) => void) {
      mutationListeners.add(listener);
      return () => mutationListeners.delete(listener);
    },
    subscribeResume(listener: () => void) {
      resumeListeners.add(listener);
      return () => resumeListeners.delete(listener);
    },
  };
}

function mutationNotice(
  value: unknown,
): { ownerKey: string; operationId: string } | null {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return null;
  }
  const notice = value as Record<string, unknown>;
  if (
    notice.version !== 1 ||
    typeof notice.ownerKey !== "string" ||
    notice.ownerKey.length === 0 ||
    notice.ownerKey.length > 512 ||
    typeof notice.operationId !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(
      notice.operationId,
    )
  ) {
    return null;
  }
  return {
    ownerKey: notice.ownerKey,
    operationId: notice.operationId,
  };
}

function defaultLifecycleCoordinator(): ParticipantAppLifecycleCoordinator {
  const mode = (
    import.meta as ImportMeta & { env?: Record<string, string | undefined> }
  ).env?.MODE;
  return mode === "test" || typeof window === "undefined"
    ? createIsolatedLifecycleCoordinator()
    : createBrowserLifecycleCoordinator();
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

export function createParticipantAppStore(
  client: WorkspaceControlClient,
  lifecycleCoordinator: ParticipantAppLifecycleCoordinator = defaultLifecycleCoordinator(),
) {
  let authorityGeneration = 0;
  let loadSequence = 0;
  let mutationSequence = 0;
  let snapshotInvalidationGeneration = 0;
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

    const isCurrentLoad = (token: LoadToken): boolean =>
      isCurrentAuthority(token) &&
      snapshotInvalidationGeneration === token.snapshotInvalidationGeneration &&
      load?.token.loadSequence === token.loadSequence;

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
        load.token.authorityGeneration === authorityGeneration &&
        load.token.snapshotInvalidationGeneration ===
          snapshotInvalidationGeneration
      ) {
        return load.promise;
      }
      const token: LoadToken = {
        ownerKey,
        authorityGeneration,
        loadSequence: ++loadSequence,
        snapshotInvalidationGeneration,
      };
      set({ status: "loading", errorCode: null });

      const promise = enqueueOwnerOperation(token, () =>
        lifecycleCoordinator.runExclusive(ownerKey, async () => {
          if (!isCurrentLoad(token)) return;
          const [catalog, installations] = await Promise.all([
            client.listAppCatalog(),
            client.listInstallations(owner),
          ]);
          if (!isCurrentLoad(token)) return;
          validateOwnedSnapshot(owner, catalog, installations);
          set({
            status: "ready",
            catalog,
            installations,
            errorCode: null,
          });
        }),
      )
        .catch((error: unknown) => {
          if (!isCurrentLoad(token)) return;
          set({ status: "error", errorCode: errorCode(error) });
        })
        .finally(() => {
          if (load?.token.loadSequence === token.loadSequence) load = null;
        });

      load = { token, promise };
      return promise;
    };

    const invalidateSnapshotAndRefresh = (ownerKey?: string): void => {
      const owner = get().owner;
      if (!owner || (ownerKey && appOwnerKey(owner) !== ownerKey)) return;
      snapshotInvalidationGeneration += 1;
      // A new load must be allowed to queue behind the current local tail. The
      // previous promise can still finish, but its captured invalidation
      // generation and load sequence can no longer publish a snapshot.
      load = null;
      void loadOwner(owner);
    };

    lifecycleCoordinator.subscribeMutations((ownerKey) => {
      invalidateSnapshotAndRefresh(ownerKey);
    });
    lifecycleCoordinator.subscribeResume(() => {
      invalidateSnapshotAndRefresh();
    });

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
          const installation = await enqueueOwnerOperation(token, () =>
            lifecycleCoordinator.runExclusive(token.ownerKey, async () => {
              const owner = get().owner;
              if (!owner || !isCurrentAuthority(token)) {
                throw new Error("Participant app owner changed");
              }
              const descriptor = get().catalog.find(
                (app) => app.appId === appId,
              );
              if (!descriptor?.participantOwnerAllowed) {
                throw new Error("App does not allow a Participant owner");
              }
              if (
                participantInstallation(get().installations, appId) !== null
              ) {
                throw new Error("App is already installed");
              }
              const response = await client.installApp(owner, appId);
              lifecycleCoordinator.publishMutation(token.ownerKey);
              if (!isCurrentAuthority(token)) return response;
              validateInstallation(owner, response, appId);
              set((state) => ({
                installations: [...state.installations, response],
              }));
              return response;
            }),
          );
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
          const installation = await enqueueOwnerOperation(token, () =>
            lifecycleCoordinator.runExclusive(token.ownerKey, async () => {
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
              lifecycleCoordinator.publishMutation(token.ownerKey);
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
            }),
          );
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
          await enqueueOwnerOperation(token, () =>
            lifecycleCoordinator.runExclusive(token.ownerKey, async () => {
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
              lifecycleCoordinator.publishMutation(token.ownerKey);
              if (!isCurrentAuthority(token)) return;
              // Uninstall removes only the owner binding. App-owned data,
              // grants, and credentials are separate app operations and are
              // not cascaded.
              set((state) => ({
                installations: state.installations.filter(
                  (installation) =>
                    installation.installationId !== installationId,
                ),
              }));
            }),
          );
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
