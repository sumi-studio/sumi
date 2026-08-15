import { create } from "zustand";
import {
  WorkspaceAPIError,
  WorkspaceAPIUncertainError,
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
  loadSequenceAtStart: number;
  lifecycleNotice: ParticipantAppLifecycleUnsettledNotice | null;
  lifecycleAnnouncementCompleted: boolean;
}

export type ParticipantAppLifecycleIntent =
  | { kind: "install"; appId: string }
  | {
      kind: "set_state";
      installationId: string;
      appId: string;
      expectedAuthorityEpoch: string;
      state: AppInstallationState;
    }
  | { kind: "uninstall"; installationId: string };

export interface ParticipantAppLifecycleUnsettledNotice {
  version: 2;
  ownerKey: string;
  operationId: string;
  phase: "unsettled";
  intent: ParticipantAppLifecycleIntent;
}

export interface ParticipantAppLifecycleSettledNotice {
  version: 2;
  ownerKey: string;
  operationId: string;
  phase: "settled";
}

export type ParticipantAppLifecycleNotice =
  | ParticipantAppLifecycleUnsettledNotice
  | ParticipantAppLifecycleSettledNotice;

export interface ParticipantAppLifecycleCoordinator {
  runExclusive<T>(ownerKey: string, operation: () => Promise<T>): Promise<T>;
  publishMutation(notice: ParticipantAppLifecycleNotice): void;
  pendingMutation(
    ownerKey: string,
  ): ParticipantAppLifecycleUnsettledNotice | null;
  subscribeMutations(
    listener: (notice: ParticipantAppLifecycleNotice) => void,
  ): () => void;
  subscribeResume(listener: () => void): () => void;
}

const participantAppLifecycleChannel = "sumi:participant-app-lifecycle:v2";
const participantAppLifecycleLockPrefix =
  "sumi:participant-app-lifecycle:owner:";
const participantAppLifecycleStoragePrefix =
  "sumi:participant-app-lifecycle:unsettled:";

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
    pendingMutation() {
      return null;
    },
    subscribeMutations() {
      return () => undefined;
    },
    subscribeResume() {
      return () => undefined;
    },
  };
}

function createBrowserLifecycleCoordinator(): ParticipantAppLifecycleCoordinator {
  const mutationListeners = new Set<
    (notice: ParticipantAppLifecycleNotice) => void
  >();
  const resumeListeners = new Set<() => void>();
  const deliveredNoticeKeys = new Set<string>();
  const deliveredNoticeOrder: string[] = [];
  const channel =
    typeof globalThis.BroadcastChannel === "function"
      ? new globalThis.BroadcastChannel(participantAppLifecycleChannel)
      : null;

  const emitMutation = (notice: ParticipantAppLifecycleNotice) => {
    const key = `${notice.operationId}:${notice.phase}`;
    if (deliveredNoticeKeys.has(key)) return;
    deliveredNoticeKeys.add(key);
    deliveredNoticeOrder.push(key);
    if (deliveredNoticeOrder.length > 256) {
      const expired = deliveredNoticeOrder.shift();
      if (expired) deliveredNoticeKeys.delete(expired);
    }
    for (const listener of mutationListeners) listener(notice);
  };

  channel?.addEventListener("message", (event: MessageEvent<unknown>) => {
    const notice = mutationNotice(event.data);
    if (notice) emitMutation(notice);
  });

  globalThis.window?.addEventListener("storage", (event: StorageEvent) => {
    if (!event.key?.startsWith(participantAppLifecycleStoragePrefix)) return;
    const unsettled = mutationNotice(event.newValue ?? event.oldValue);
    if (unsettled?.phase !== "unsettled") return;
    emitMutation(
      event.newValue === null
        ? {
            version: 2,
            ownerKey: unsettled.ownerKey,
            operationId: unsettled.operationId,
            phase: "settled",
          }
        : unsettled,
    );
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
    let storage: Storage | null = null;
    try {
      storage = globalThis.localStorage;
    } catch {
      // Access can be denied even when the property exists.
    }
    if (!channel || !locks || !storage) {
      throw new Error(
        "Participant app cross-document coordination is unavailable",
      );
    }
    return { channel, locks, storage };
  };

  const storageKey = (ownerKey: string) =>
    `${participantAppLifecycleStoragePrefix}${encodeURIComponent(ownerKey)}`;

  return {
    runExclusive<T>(ownerKey: string, operation: () => Promise<T>) {
      const { locks } = requireCoordination();
      return locks.request(
        `${participantAppLifecycleLockPrefix}${ownerKey}`,
        { mode: "exclusive" },
        operation,
      );
    },
    publishMutation(notice: ParticipantAppLifecycleNotice) {
      const { channel: availableChannel, storage } = requireCoordination();
      const key = storageKey(notice.ownerKey);
      if (notice.phase === "unsettled") {
        // This synchronous journal write is the durable hand-off point. A
        // renderer may die after it returns; another same-origin document can
        // still take over the exact idempotent intent under the owner lock.
        storage.setItem(key, JSON.stringify(notice));
      } else {
        const pending = mutationNotice(storage.getItem(key));
        if (
          pending?.phase === "unsettled" &&
          pending.operationId === notice.operationId
        ) {
          storage.removeItem(key);
        }
      }
      availableChannel.postMessage(notice);
    },
    pendingMutation(ownerKey: string) {
      const { storage } = requireCoordination();
      const notice = mutationNotice(storage.getItem(storageKey(ownerKey)));
      return notice?.phase === "unsettled" && notice.ownerKey === ownerKey
        ? notice
        : null;
    },
    subscribeMutations(
      listener: (notice: ParticipantAppLifecycleNotice) => void,
    ) {
      mutationListeners.add(listener);
      return () => mutationListeners.delete(listener);
    },
    subscribeResume(listener: () => void) {
      resumeListeners.add(listener);
      return () => resumeListeners.delete(listener);
    },
  };
}

function mutationNotice(value: unknown): ParticipantAppLifecycleNotice | null {
  if (typeof value === "string") {
    try {
      return mutationNotice(JSON.parse(value));
    } catch {
      return null;
    }
  }
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return null;
  }
  const notice = value as Record<string, unknown>;
  if (
    notice.version !== 2 ||
    typeof notice.ownerKey !== "string" ||
    notice.ownerKey.length === 0 ||
    notice.ownerKey.length > 512 ||
    typeof notice.operationId !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(
      notice.operationId,
    ) ||
    (notice.phase !== "unsettled" && notice.phase !== "settled")
  ) {
    return null;
  }
  if (notice.phase === "settled") {
    return {
      version: 2,
      ownerKey: notice.ownerKey,
      operationId: notice.operationId,
      phase: "settled",
    };
  }
  const intent = lifecycleIntent(notice.intent);
  if (!intent) return null;
  return {
    version: 2,
    ownerKey: notice.ownerKey,
    operationId: notice.operationId,
    phase: "unsettled",
    intent,
  };
}

function lifecycleIntent(value: unknown): ParticipantAppLifecycleIntent | null {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return null;
  }
  const intent = value as Record<string, unknown>;
  if (intent.kind === "install" && validAppID(intent.appId)) {
    return { kind: "install", appId: intent.appId };
  }
  if (
    intent.kind === "set_state" &&
    validInstallationID(intent.installationId) &&
    validAppID(intent.appId) &&
    typeof intent.expectedAuthorityEpoch === "string" &&
    /^[1-9][0-9]{0,18}$/.test(intent.expectedAuthorityEpoch) &&
    (intent.state === "enabled" || intent.state === "disabled")
  ) {
    return {
      kind: "set_state",
      installationId: intent.installationId,
      appId: intent.appId,
      expectedAuthorityEpoch: intent.expectedAuthorityEpoch,
      state: intent.state,
    };
  }
  if (
    intent.kind === "uninstall" &&
    validInstallationID(intent.installationId)
  ) {
    return { kind: "uninstall", installationId: intent.installationId };
  }
  return null;
}

function validAppID(value: unknown): value is string {
  return (
    typeof value === "string" &&
    // Keep the durable protocol exactly aligned with app_catalog.app_id. A
    // valid server app must never become an unreadable orphaned intent.
    /^[a-z][a-z0-9-]{0,63}$/.test(value)
  );
}

function validInstallationID(value: unknown): value is string {
  return (
    typeof value === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(
      value,
    )
  );
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
  const observedUnsettledOperations = new Set<string>();
  const observedUnsettledOperationOrder: string[] = [];
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

    const beginMutation = (): MutationToken => {
      const authority = currentAuthority();
      if (activeMutationSequence !== null || get().mutation) {
        throw new Error("Participant app mutation is already running");
      }
      const token: MutationToken = {
        ...authority,
        mutationSequence: ++mutationSequence,
        loadSequenceAtStart: loadSequence,
        lifecycleNotice: null,
        lifecycleAnnouncementCompleted: false,
      };
      activeMutationSequence = token.mutationSequence;
      return token;
    };

    const exposeMutation = (token: MutationToken, name: string): void => {
      if (
        isCurrentAuthority(token) &&
        activeMutationSequence === token.mutationSequence
      ) {
        set({ mutation: name, errorCode: null });
      }
    };

    const endMutation = (token: MutationToken, error?: unknown): void => {
      if (
        !isCurrentAuthority(token) ||
        activeMutationSequence !== token.mutationSequence
      ) {
        return;
      }
      activeMutationSequence = null;
      set((state) => ({
        mutation: null,
        errorCode:
          error !== undefined
            ? errorCode(error)
            : state.status === "error"
              ? state.errorCode
              : null,
      }));
    };

    const announceLifecycleIntent = (
      token: MutationToken,
      intent: ParticipantAppLifecycleIntent,
    ): ParticipantAppLifecycleUnsettledNotice => {
      const notice: ParticipantAppLifecycleUnsettledNotice = {
        version: 2,
        ownerKey: token.ownerKey,
        operationId: globalThis.crypto.randomUUID(),
        phase: "unsettled",
        intent,
      };
      rememberUnsettledOperation(notice.operationId);
      // Record the exact intent before the synchronous durable announcement.
      // If publishing partly succeeds and then throws, reconciliation still
      // takes over this operation instead of performing an unsafe plain read.
      token.lifecycleNotice = notice;
      lifecycleCoordinator.publishMutation(notice);
      token.lifecycleAnnouncementCompleted = true;
      return notice;
    };

    const settleLifecycleIntent = (
      notice: ParticipantAppLifecycleUnsettledNotice,
    ): void => {
      lifecycleCoordinator.publishMutation({
        version: 2,
        ownerKey: notice.ownerKey,
        operationId: notice.operationId,
        phase: "settled",
      });
    };

    const replayLifecycleIntent = async (
      owner: AppOwnerRef,
      notice: ParticipantAppLifecycleUnsettledNotice,
    ): Promise<void> => {
      const { intent } = notice;
      try {
        switch (intent.kind) {
          case "install": {
            const response = await client.installApp(owner, intent.appId);
            validateInstallation(owner, response, intent.appId);
            break;
          }
          case "set_state": {
            const response = await client.setInstallationState(
              intent.installationId,
              intent.state,
              intent.expectedAuthorityEpoch,
            );
            validateInstallation(owner, response, intent.appId);
            if (
              response.installationId !== intent.installationId ||
              response.state !== intent.state
            ) {
              throw new Error("App lifecycle response does not match intent");
            }
            break;
          }
          case "uninstall":
            await client.uninstallApp(intent.installationId);
            break;
        }
      } catch (error) {
        if (!replayConverged(intent, error)) throw error;
      }
      settleLifecycleIntent(notice);
    };

    const resolvePendingLifecycleIntent = async (
      owner: AppOwnerRef,
      ownerKey: string,
    ): Promise<boolean> => {
      const pending = lifecycleCoordinator.pendingMutation(ownerKey);
      if (!pending) return false;
      await replayLifecycleIntent(owner, pending);
      return true;
    };

    function rememberUnsettledOperation(operationId: string): void {
      if (observedUnsettledOperations.has(operationId)) return;
      observedUnsettledOperations.add(operationId);
      observedUnsettledOperationOrder.push(operationId);
      if (observedUnsettledOperationOrder.length > 256) {
        const expired = observedUnsettledOperationOrder.shift();
        if (expired) observedUnsettledOperations.delete(expired);
      }
    }

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
          // A plain read cannot resolve an orphan request: it could observe the
          // old row while that request commits later. Replaying the durable,
          // idempotent exact intent first serializes against the orphan at the
          // same unique/row boundary. Only then is this final read publishable.
          await resolvePendingLifecycleIntent(owner, ownerKey);
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

    const invalidateSnapshotAndRefresh = (
      ownerKey?: string,
    ): Promise<void> | null => {
      const owner = get().owner;
      if (!owner || (ownerKey && appOwnerKey(owner) !== ownerKey)) return null;
      snapshotInvalidationGeneration += 1;
      // A new load must be allowed to queue behind the current local tail. The
      // previous promise can still finish, but its captured invalidation
      // generation and load sequence can no longer publish a snapshot.
      load = null;
      return loadOwner(owner);
    };

    const reconcileAfterMutation = async (
      token: MutationToken,
    ): Promise<void> => {
      if (!isCurrentAuthority(token)) return;
      const queuedLoad = load;
      if (
        queuedLoad?.token.ownerKey === token.ownerKey &&
        queuedLoad.token.authorityGeneration === token.authorityGeneration &&
        queuedLoad.token.loadSequence > token.loadSequenceAtStart &&
        queuedLoad.token.snapshotInvalidationGeneration ===
          snapshotInvalidationGeneration
      ) {
        // A refresh requested after this mutation began is already serialized
        // after its owner operation. Reuse that exact read instead of queuing a
        // second one; it cannot observe the pre-effect snapshot.
        await queuedLoad.promise;
        return;
      }
      await invalidateSnapshotAndRefresh(token.ownerKey);
    };

    const mutationErrorAfterReconciliation = (
      token: MutationToken,
      error: unknown,
    ): unknown | undefined =>
      token.lifecycleAnnouncementCompleted &&
      !definitiveLifecycleRejection(error) &&
      get().status === "ready"
        ? undefined
        : error;

    lifecycleCoordinator.subscribeMutations((notice) => {
      if (notice.phase === "unsettled") {
        if (observedUnsettledOperations.has(notice.operationId)) return;
        rememberUnsettledOperation(notice.operationId);
        void invalidateSnapshotAndRefresh(notice.ownerKey);
        return;
      }
      // The unsettled event already queued a load behind the owner lock. Its
      // settled counterpart only removes the durable journal; queuing another
      // read here would create a duplicate refresh storm.
      if (observedUnsettledOperations.has(notice.operationId)) return;
      void invalidateSnapshotAndRefresh(notice.ownerKey);
    });
    lifecycleCoordinator.subscribeResume(() => {
      void invalidateSnapshotAndRefresh();
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
          observedUnsettledOperations.clear();
          observedUnsettledOperationOrder.length = 0;
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
        observedUnsettledOperations.clear();
        observedUnsettledOperationOrder.length = 0;
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
        const token = beginMutation();
        const operation = enqueueOwnerOperation(token, () =>
          lifecycleCoordinator.runExclusive(token.ownerKey, async () => {
            const owner = get().owner;
            if (!owner || !isCurrentAuthority(token)) {
              throw new Error("Participant app owner changed");
            }
            if (await resolvePendingLifecycleIntent(owner, token.ownerKey)) {
              throw new Error(
                "Participant app lifecycle changed during this action; retry",
              );
            }
            const descriptor = get().catalog.find((app) => app.appId === appId);
            if (!descriptor?.participantOwnerAllowed) {
              throw new Error("App does not allow a Participant owner");
            }
            if (participantInstallation(get().installations, appId) !== null) {
              throw new Error("App is already installed");
            }
            const notice = announceLifecycleIntent(token, {
              kind: "install",
              appId,
            });
            let response: AppInstallation;
            try {
              response = await client.installApp(owner, appId);
            } catch (error) {
              if (definitiveLifecycleRejection(error)) {
                settleLifecycleIntent(notice);
              }
              throw error;
            }
            settleLifecycleIntent(notice);
            if (!isCurrentAuthority(token)) return response;
            validateInstallation(owner, response, appId);
            set((state) => ({
              installations: [...state.installations, response],
            }));
            return response;
          }),
        );
        exposeMutation(token, "install_app");
        try {
          const installation = await operation;
          await reconcileAfterMutation(token);
          endMutation(token);
          return installation;
        } catch (error) {
          await reconcileAfterMutation(token);
          endMutation(token, mutationErrorAfterReconciliation(token, error));
          throw error;
        }
      },

      async setInstallationState(installationId, state) {
        const token = beginMutation();
        const operation = enqueueOwnerOperation(token, () =>
          lifecycleCoordinator.runExclusive(token.ownerKey, async () => {
            const owner = get().owner;
            if (!owner || !isCurrentAuthority(token)) {
              throw new Error("Participant app installation is not active");
            }
            if (await resolvePendingLifecycleIntent(owner, token.ownerKey)) {
              throw new Error(
                "Participant app lifecycle changed during this action; retry",
              );
            }
            const current = get().installations.find(
              (entry) => entry.installationId === installationId,
            );
            if (!current) {
              throw new Error("App installation is not active");
            }
            const notice = announceLifecycleIntent(token, {
              kind: "set_state",
              installationId,
              appId: current.appId,
              expectedAuthorityEpoch: current.authorityEpoch,
              state,
            });
            let response: AppInstallation;
            try {
              response = await client.setInstallationState(
                installationId,
                state,
                current.authorityEpoch,
              );
            } catch (error) {
              if (definitiveLifecycleRejection(error)) {
                settleLifecycleIntent(notice);
              }
              throw error;
            }
            settleLifecycleIntent(notice);
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
        exposeMutation(token, `set_installation_${state}`);
        try {
          const installation = await operation;
          await reconcileAfterMutation(token);
          endMutation(token);
          return installation;
        } catch (error) {
          await reconcileAfterMutation(token);
          endMutation(token, mutationErrorAfterReconciliation(token, error));
          throw error;
        }
      },

      async uninstallApp(installationId) {
        const token = beginMutation();
        const operation = enqueueOwnerOperation(token, () =>
          lifecycleCoordinator.runExclusive(token.ownerKey, async () => {
            const owner = get().owner;
            if (!owner || !isCurrentAuthority(token)) {
              throw new Error("App installation is not active");
            }
            if (await resolvePendingLifecycleIntent(owner, token.ownerKey)) {
              throw new Error(
                "Participant app lifecycle changed during this action; retry",
              );
            }
            if (
              !get().installations.some(
                (installation) =>
                  installation.installationId === installationId,
              )
            ) {
              throw new Error("App installation is not active");
            }
            const notice = announceLifecycleIntent(token, {
              kind: "uninstall",
              installationId,
            });
            try {
              await client.uninstallApp(installationId);
            } catch (error) {
              if (definitiveLifecycleRejection(error)) {
                settleLifecycleIntent(notice);
              }
              throw error;
            }
            settleLifecycleIntent(notice);
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
        exposeMutation(token, "uninstall_app");
        try {
          await operation;
          await reconcileAfterMutation(token);
          endMutation(token);
        } catch (error) {
          await reconcileAfterMutation(token);
          endMutation(token, mutationErrorAfterReconciliation(token, error));
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

function definitiveLifecycleRejection(error: unknown): boolean {
  if (error instanceof WorkspaceAPIUncertainError) return false;
  // A 408 may be produced after an intermediary gives up while the upstream
  // mutation continues, so it has the same ambiguous outcome as no response.
  return (
    error instanceof WorkspaceAPIError &&
    error.status < 500 &&
    error.status !== 408
  );
}

function replayConverged(
  intent: ParticipantAppLifecycleIntent,
  error: unknown,
): boolean {
  if (!(error instanceof WorkspaceAPIError)) return false;
  switch (intent.kind) {
    case "install":
      // PostgreSQL's exact owner/app uniqueness check waits for a competing
      // insert to finish. Conflict therefore proves some replay/orphan has
      // committed the desired installed truth.
      return error.status === 409 && error.code === "conflict";
    case "set_state":
      // A stale epoch is checked by the row update after it serializes against
      // any orphan update. Not-found proves the exact binding cannot later be
      // changed or resurrected by this intent.
      return (
        (error.status === 409 && error.code === "stale_authority") ||
        (error.status === 404 && error.code === "not_found")
      );
    case "uninstall":
      return error.status === 404 && error.code === "not_found";
  }
}

export const useParticipantApps = createParticipantAppStore(
  new WorkspaceApiClient(),
);
