import { describe, expect, it, vi } from "vitest";
import type { WorkspaceControlClient } from "../workspace/api-client";
import type {
  AppDescriptor,
  AppInstallation,
  AppOwnerRef,
  ParticipantRef,
} from "../workspace/model";
import { appOwnerKey } from "../workspace/model";
import {
  createParticipantAppStore,
  type ParticipantAppLifecycleCoordinator,
  participantInstallation,
} from "./app-store";

const HUMAN_A_ID = "0198f0f4-9b72-7000-8000-000000000021";
const HUMAN_B_ID = "0198f0f4-9b72-7000-8000-000000000022";
const HUMAN_A: ParticipantRef = {
  kind: "human",
  humanId: HUMAN_A_ID,
};
const HUMAN_B: ParticipantRef = {
  kind: "human",
  humanId: HUMAN_B_ID,
};
const OWNER_A: AppOwnerRef = { kind: "participant", participant: HUMAN_A };
const OWNER_B: AppOwnerRef = { kind: "participant", participant: HUMAN_B };
const DIRECT_CHAT: AppDescriptor = {
  appId: "direct-chat",
  displayName: "Direct Chat",
  workspaceOwnerAllowed: false,
  participantOwnerAllowed: true,
  workspaceRoleCapabilities: [],
};
const MESSAGING: AppDescriptor = {
  appId: "messaging",
  displayName: "Messaging",
  workspaceOwnerAllowed: true,
  participantOwnerAllowed: false,
  workspaceRoleCapabilities: [],
};

describe("Participant app lifecycle store", () => {
  it("loads one exact Participant owner and does not reload for Workspace navigation", async () => {
    const listInstallations = vi.fn(async () => [installation(OWNER_A)]);
    const client = participantClient({ listInstallations });
    const store = createParticipantAppStore(client);

    await store.getState().bindParticipant(HUMAN_A);
    await store.getState().bindParticipant({ ...HUMAN_A });

    expect(listInstallations).toHaveBeenCalledTimes(1);
    expect(listInstallations).toHaveBeenCalledWith(OWNER_A);
    expect(store.getState()).toMatchObject({
      owner: OWNER_A,
      status: "ready",
      installations: [installation(OWNER_A)],
    });
  });

  it("keeps the last usable installation visible while a same-owner refresh is in flight", async () => {
    const refreshed = deferred<AppInstallation[]>();
    const current = installation(OWNER_A);
    const listInstallations = vi
      .fn<(owner: AppOwnerRef) => Promise<AppInstallation[]>>()
      .mockResolvedValueOnce([current])
      .mockImplementationOnce(() => refreshed.promise);
    const store = createParticipantAppStore(
      participantClient({ listInstallations }),
    );
    await store.getState().bindParticipant(HUMAN_A);

    const refresh = store.getState().refresh();

    expect(store.getState()).toMatchObject({
      status: "loading",
      installations: [current],
    });
    refreshed.resolve([{ ...current, state: "disabled", updatedAt: 3 }]);
    await refresh;
    expect(store.getState()).toMatchObject({
      status: "ready",
      installations: [{ ...current, state: "disabled", updatedAt: 3 }],
    });
  });

  it("uses the same exact lifecycle verbs and retains app data semantics on disable", async () => {
    const installed = installation(OWNER_A);
    const disabled = { ...installed, state: "disabled" as const, updatedAt: 4 };
    let serverSnapshot: AppInstallation[] = [];
    const installApp = vi.fn(async () => {
      serverSnapshot = [installed];
      return installed;
    });
    const setInstallationState = vi.fn(async () => {
      serverSnapshot = [disabled];
      return disabled;
    });
    const uninstallApp = vi.fn(async () => {
      serverSnapshot = [];
    });
    const client = participantClient({
      listInstallations: vi.fn(async () => serverSnapshot),
      installApp,
      setInstallationState,
      uninstallApp,
    });
    const store = createParticipantAppStore(client);
    await store.getState().bindParticipant(HUMAN_A);

    await store.getState().installApp("direct-chat");
    expect(installApp).toHaveBeenCalledWith(OWNER_A, "direct-chat");
    expect(store.getState().installations).toEqual([installed]);

    await store
      .getState()
      .setInstallationState(installed.installationId, "disabled");
    expect(setInstallationState).toHaveBeenCalledWith(
      installed.installationId,
      "disabled",
    );
    expect(store.getState().installations).toEqual([disabled]);

    await store.getState().uninstallApp(installed.installationId);
    expect(uninstallApp).toHaveBeenCalledWith(installed.installationId);
    expect(store.getState().installations).toEqual([]);
  });

  it("queues same-owner refresh behind every lifecycle commit without clearing its mutation", async () => {
    const enabledEpoch1 = installation(OWNER_A);
    const disabledEpoch2 = {
      ...enabledEpoch1,
      state: "disabled" as const,
      authorityEpoch: "2",
      updatedAt: 3,
    };
    const enabledEpoch2 = {
      ...disabledEpoch2,
      state: "enabled" as const,
      updatedAt: 4,
    };
    const reinstalled = installation(
      OWNER_A,
      "0198f0f4-9b72-7000-8000-000000000052",
    );
    const refreshResponses: Array<
      ReturnType<typeof deferred<AppInstallation[]>>
    > = [];
    const listInstallations = vi
      .fn<(owner: AppOwnerRef) => Promise<AppInstallation[]>>()
      .mockResolvedValueOnce([enabledEpoch1])
      .mockImplementation(() => {
        const response = deferred<AppInstallation[]>();
        refreshResponses.push(response);
        return response.promise;
      });
    const disable = deferred<AppInstallation>();
    const enable = deferred<AppInstallation>();
    const uninstall = deferred<void>();
    const reinstall = deferred<AppInstallation>();
    const setInstallationState = vi
      .fn<
        (
          installationId: string,
          state: "enabled" | "disabled",
        ) => Promise<AppInstallation>
      >()
      .mockImplementationOnce(() => disable.promise)
      .mockImplementationOnce(() => enable.promise);
    const uninstallApp = vi.fn(() => uninstall.promise);
    const installApp = vi.fn(() => reinstall.promise);
    const store = createParticipantAppStore(
      participantClient({
        listInstallations,
        setInstallationState,
        uninstallApp,
        installApp,
      }),
    );
    await store.getState().bindParticipant(HUMAN_A);

    const disabling = store
      .getState()
      .setInstallationState(enabledEpoch1.installationId, "disabled");
    const refreshAfterDisable = store.getState().refresh();
    expect(listInstallations).toHaveBeenCalledTimes(1);
    expect(store.getState()).toMatchObject({
      status: "loading",
      mutation: "set_installation_disabled",
      installations: [enabledEpoch1],
    });
    disable.resolve(disabledEpoch2);
    await vi.waitFor(() => expect(listInstallations).toHaveBeenCalledTimes(2));
    refreshResponses[0]?.resolve([disabledEpoch2]);
    await Promise.all([disabling, refreshAfterDisable]);
    expect(store.getState()).toMatchObject({
      mutation: null,
      installations: [disabledEpoch2],
    });
    expect(store.getState().installations).toEqual([disabledEpoch2]);

    const enabling = store
      .getState()
      .setInstallationState(enabledEpoch1.installationId, "enabled");
    const refreshAfterEnable = store.getState().refresh();
    expect(listInstallations).toHaveBeenCalledTimes(2);
    expect(store.getState().mutation).toBe("set_installation_enabled");
    enable.resolve(enabledEpoch2);
    await vi.waitFor(() => expect(listInstallations).toHaveBeenCalledTimes(3));
    refreshResponses[1]?.resolve([enabledEpoch2]);
    await Promise.all([enabling, refreshAfterEnable]);
    expect(store.getState().installations).toEqual([enabledEpoch2]);

    const uninstalling = store
      .getState()
      .uninstallApp(enabledEpoch1.installationId);
    const refreshAfterUninstall = store.getState().refresh();
    expect(listInstallations).toHaveBeenCalledTimes(3);
    expect(store.getState().mutation).toBe("uninstall_app");
    uninstall.resolve();
    await vi.waitFor(() => expect(listInstallations).toHaveBeenCalledTimes(4));
    refreshResponses[2]?.resolve([]);
    await Promise.all([uninstalling, refreshAfterUninstall]);
    expect(store.getState().installations).toEqual([]);

    const reinstalling = store.getState().installApp("direct-chat");
    const refreshAfterReinstall = store.getState().refresh();
    expect(listInstallations).toHaveBeenCalledTimes(4);
    expect(store.getState().mutation).toBe("install_app");
    reinstall.resolve(reinstalled);
    await vi.waitFor(() => expect(listInstallations).toHaveBeenCalledTimes(5));
    refreshResponses[3]?.resolve([reinstalled]);
    await Promise.all([reinstalling, refreshAfterReinstall]);
    expect(store.getState()).toMatchObject({
      status: "ready",
      mutation: null,
      installations: [reinstalled],
    });
  });

  it("holds one owner lock across documents and refreshes the other store after commit", async () => {
    const enabledEpoch1 = installation(OWNER_A);
    const disabledEpoch2 = {
      ...enabledEpoch1,
      state: "disabled" as const,
      authorityEpoch: "2",
      updatedAt: 3,
    };
    let serverSnapshot = [enabledEpoch1];
    const delayedRefresh = deferred<AppInstallation[]>();
    const coordinators = lifecycleCoordinatorPair();
    const setInstallationState = vi.fn(async () => {
      serverSnapshot = [disabledEpoch2];
      return disabledEpoch2;
    });
    const firstStore = createParticipantAppStore(
      participantClient({
        listInstallations: vi.fn(async () => serverSnapshot),
        setInstallationState,
      }),
      coordinators[0],
    );
    const secondList = vi
      .fn<() => Promise<AppInstallation[]>>()
      .mockResolvedValueOnce([enabledEpoch1])
      .mockImplementationOnce(() => delayedRefresh.promise)
      .mockImplementation(async () => serverSnapshot);
    const secondStore = createParticipantAppStore(
      participantClient({ listInstallations: secondList }),
      coordinators[1],
    );
    await Promise.all([
      firstStore.getState().bindParticipant(HUMAN_A),
      secondStore.getState().bindParticipant(HUMAN_A),
    ]);

    const refreshing = secondStore.getState().refresh();
    await vi.waitFor(() => expect(secondList).toHaveBeenCalledTimes(2));
    const disabling = firstStore
      .getState()
      .setInstallationState(enabledEpoch1.installationId, "disabled");
    expect(firstStore.getState().mutation).toBe("set_installation_disabled");
    await Promise.resolve();
    expect(setInstallationState).not.toHaveBeenCalled();

    delayedRefresh.resolve([enabledEpoch1]);
    await refreshing;
    await disabling;
    await vi.waitFor(() => expect(secondList).toHaveBeenCalledTimes(3));
    await vi.waitFor(() =>
      expect(secondStore.getState()).toMatchObject({
        status: "ready",
        installations: [disabledEpoch2],
      }),
    );
    expect(firstStore.getState().installations).toEqual([disabledEpoch2]);
  });

  it("announces invalidation before a delayed remote effect and keeps peer reads behind the owner lock", async () => {
    const enabledEpoch1 = installation(OWNER_A);
    const disabledEpoch2 = {
      ...enabledEpoch1,
      state: "disabled" as const,
      authorityEpoch: "2",
      updatedAt: 3,
    };
    let serverSnapshot = [enabledEpoch1];
    const remoteEffect = deferred<AppInstallation>();
    const coordinators = lifecycleCoordinatorPair();
    const firstList = vi.fn(async () => serverSnapshot);
    const secondList = vi.fn(async () => serverSnapshot);
    const setInstallationState = vi.fn(() => remoteEffect.promise);
    const firstStore = createParticipantAppStore(
      participantClient({
        listInstallations: firstList,
        setInstallationState,
      }),
      coordinators[0],
    );
    const secondStore = createParticipantAppStore(
      participantClient({ listInstallations: secondList }),
      coordinators[1],
    );
    await Promise.all([
      firstStore.getState().bindParticipant(HUMAN_A),
      secondStore.getState().bindParticipant(HUMAN_A),
    ]);

    const disabling = firstStore
      .getState()
      .setInstallationState(enabledEpoch1.installationId, "disabled");
    await vi.waitFor(() => expect(setInstallationState).toHaveBeenCalledOnce());

    expect(secondStore.getState()).toMatchObject({
      status: "loading",
      installations: [enabledEpoch1],
    });
    expect(secondList).toHaveBeenCalledTimes(1);

    serverSnapshot = [disabledEpoch2];
    remoteEffect.resolve(disabledEpoch2);
    await disabling;

    await vi.waitFor(() =>
      expect(secondStore.getState()).toMatchObject({
        status: "ready",
        installations: [disabledEpoch2],
      }),
    );
    expect(firstStore.getState()).toMatchObject({
      status: "ready",
      mutation: null,
      installations: [disabledEpoch2],
    });
    expect(firstList).toHaveBeenCalledTimes(2);
    expect(secondList).toHaveBeenCalledTimes(2);
  });

  it("lets a peer converge after commit plus response loss even when the sender disappears", async () => {
    const enabledEpoch1 = installation(OWNER_A);
    const disabledEpoch2 = {
      ...enabledEpoch1,
      state: "disabled" as const,
      authorityEpoch: "2",
      updatedAt: 3,
    };
    let serverSnapshot = [enabledEpoch1];
    const senderRefresh = deferred<AppInstallation[]>();
    const coordinators = lifecycleCoordinatorPair();
    const firstList = vi
      .fn<() => Promise<AppInstallation[]>>()
      .mockResolvedValueOnce([enabledEpoch1])
      .mockImplementationOnce(() => senderRefresh.promise);
    const secondList = vi.fn(async () => serverSnapshot);
    const firstStore = createParticipantAppStore(
      participantClient({
        listInstallations: firstList,
        setInstallationState: vi.fn(async () => {
          serverSnapshot = [disabledEpoch2];
          throw new Error("response lost after commit");
        }),
      }),
      coordinators[0],
    );
    const secondStore = createParticipantAppStore(
      participantClient({ listInstallations: secondList }),
      coordinators[1],
    );
    await Promise.all([
      firstStore.getState().bindParticipant(HUMAN_A),
      secondStore.getState().bindParticipant(HUMAN_A),
    ]);

    const mutationResult = firstStore
      .getState()
      .setInstallationState(enabledEpoch1.installationId, "disabled")
      .catch((error: unknown) => error);

    await vi.waitFor(() =>
      expect(secondStore.getState()).toMatchObject({
        status: "ready",
        installations: [disabledEpoch2],
      }),
    );
    await vi.waitFor(() => expect(firstList).toHaveBeenCalledTimes(2));
    expect(firstStore.getState()).toMatchObject({
      status: "loading",
      mutation: "set_installation_disabled",
      installations: [enabledEpoch1],
    });

    await firstStore.getState().bindParticipant(null);
    senderRefresh.resolve([disabledEpoch2]);
    const mutationError = await mutationResult;

    expect(mutationError).toEqual(new Error("response lost after commit"));
    expect(firstStore.getState()).toMatchObject({
      owner: null,
      status: "idle",
      mutation: null,
      installations: [],
    });
    expect(secondStore.getState()).toMatchObject({
      status: "ready",
      installations: [disabledEpoch2],
      errorCode: null,
    });
    expect(secondList).toHaveBeenCalledTimes(2);
  });

  it("returns both documents to old truth when an announced effect does not commit", async () => {
    const enabledEpoch1 = installation(OWNER_A);
    const coordinators = lifecycleCoordinatorPair();
    const firstList = vi.fn(async () => [enabledEpoch1]);
    const secondList = vi.fn(async () => [enabledEpoch1]);
    const firstStore = createParticipantAppStore(
      participantClient({
        listInstallations: firstList,
        setInstallationState: vi.fn(async () => {
          throw new Error("write rejected before commit");
        }),
      }),
      coordinators[0],
    );
    const secondStore = createParticipantAppStore(
      participantClient({ listInstallations: secondList }),
      coordinators[1],
    );
    await Promise.all([
      firstStore.getState().bindParticipant(HUMAN_A),
      secondStore.getState().bindParticipant(HUMAN_A),
    ]);

    await expect(
      firstStore
        .getState()
        .setInstallationState(enabledEpoch1.installationId, "disabled"),
    ).rejects.toThrow("write rejected before commit");

    expect(firstStore.getState()).toMatchObject({
      status: "ready",
      mutation: null,
      installations: [enabledEpoch1],
      errorCode: "write rejected before commit",
    });
    expect(secondStore.getState()).toMatchObject({
      status: "ready",
      installations: [enabledEpoch1],
      errorCode: null,
    });
    expect(firstList).toHaveBeenCalledTimes(2);
    expect(secondList).toHaveBeenCalledTimes(2);
  });

  it("fails closed before a lifecycle effect when owner coordination becomes unavailable", async () => {
    const enabledEpoch1 = installation(OWNER_A);
    let coordinationAvailable = true;
    const publishMutation = vi.fn();
    const setInstallationState = vi.fn(async () => ({
      ...enabledEpoch1,
      state: "disabled" as const,
      authorityEpoch: "2",
    }));
    const coordinator: ParticipantAppLifecycleCoordinator = {
      async runExclusive<T>(
        _ownerKey: string,
        operation: () => Promise<T>,
      ): Promise<T> {
        if (!coordinationAvailable) {
          throw new Error(
            "Participant app cross-document coordination is unavailable",
          );
        }
        return operation();
      },
      publishMutation,
      subscribeMutations: () => () => undefined,
      subscribeResume: () => () => undefined,
    };
    const store = createParticipantAppStore(
      participantClient({
        listInstallations: vi.fn(async () => [enabledEpoch1]),
        setInstallationState,
      }),
      coordinator,
    );
    await store.getState().bindParticipant(HUMAN_A);

    coordinationAvailable = false;
    await expect(
      store
        .getState()
        .setInstallationState(enabledEpoch1.installationId, "disabled"),
    ).rejects.toThrow(
      "Participant app cross-document coordination is unavailable",
    );

    expect(publishMutation).not.toHaveBeenCalled();
    expect(setInstallationState).not.toHaveBeenCalled();
    expect(store.getState()).toMatchObject({
      status: "error",
      mutation: null,
      installations: [enabledEpoch1],
      errorCode: "Participant app cross-document coordination is unavailable",
    });
  });

  it("discards an in-flight snapshot when another document announces a commit", async () => {
    const enabledEpoch1 = installation(OWNER_A);
    const disabledEpoch2 = {
      ...enabledEpoch1,
      state: "disabled" as const,
      authorityEpoch: "2",
      updatedAt: 3,
    };
    const staleResponse = deferred<AppInstallation[]>();
    const currentResponse = deferred<AppInstallation[]>();
    const coordinators = lifecycleCoordinatorPair();
    const listInstallations = vi
      .fn<() => Promise<AppInstallation[]>>()
      .mockResolvedValueOnce([enabledEpoch1])
      .mockImplementationOnce(() => staleResponse.promise)
      .mockImplementationOnce(() => currentResponse.promise);
    const store = createParticipantAppStore(
      participantClient({ listInstallations }),
      coordinators[1],
    );
    await store.getState().bindParticipant(HUMAN_A);

    const readySnapshots: AppInstallation[][] = [];
    let observeReadySnapshots = false;
    store.subscribe((state) => {
      if (observeReadySnapshots && state.status === "ready") {
        readySnapshots.push(state.installations);
      }
    });
    const staleRefresh = store.getState().refresh();
    await vi.waitFor(() => expect(listInstallations).toHaveBeenCalledTimes(2));
    observeReadySnapshots = true;
    coordinators[0].publishMutation(appOwnerKey(OWNER_A));
    staleResponse.resolve([enabledEpoch1]);
    await staleRefresh;
    await vi.waitFor(() => expect(listInstallations).toHaveBeenCalledTimes(3));

    expect(store.getState()).toMatchObject({
      status: "loading",
      installations: [enabledEpoch1],
    });
    expect(readySnapshots).toEqual([]);

    currentResponse.resolve([disabledEpoch2]);
    await vi.waitFor(() =>
      expect(store.getState()).toMatchObject({
        status: "ready",
        installations: [disabledEpoch2],
      }),
    );
    expect(readySnapshots).toEqual([[disabledEpoch2]]);
  });

  it("propagates uninstall and reinstall as a new installation at epoch one", async () => {
    const original = installation(OWNER_A);
    const replacement = installation(
      OWNER_A,
      "0198f0f4-9b72-7000-8000-000000000052",
    );
    let serverSnapshot = [original];
    const coordinators = lifecycleCoordinatorPair();
    const firstStore = createParticipantAppStore(
      participantClient({
        listInstallations: vi.fn(async () => serverSnapshot),
        uninstallApp: vi.fn(async () => {
          serverSnapshot = [];
        }),
        installApp: vi.fn(async () => {
          serverSnapshot = [replacement];
          return replacement;
        }),
      }),
      coordinators[0],
    );
    const secondStore = createParticipantAppStore(
      participantClient({
        listInstallations: vi.fn(async () => serverSnapshot),
      }),
      coordinators[1],
    );
    await Promise.all([
      firstStore.getState().bindParticipant(HUMAN_A),
      secondStore.getState().bindParticipant(HUMAN_A),
    ]);

    await firstStore.getState().uninstallApp(original.installationId);
    await vi.waitFor(() =>
      expect(secondStore.getState().installations).toEqual([]),
    );

    await firstStore.getState().installApp("direct-chat");
    await vi.waitFor(() =>
      expect(secondStore.getState().installations).toEqual([replacement]),
    );
    expect(replacement.installationId).not.toBe(original.installationId);
    expect(replacement.authorityEpoch).toBe("1");
  });

  it("forces an authoritative refresh when a document resumes", async () => {
    const enabledEpoch1 = installation(OWNER_A);
    const disabledEpoch2 = {
      ...enabledEpoch1,
      state: "disabled" as const,
      authorityEpoch: "2",
      updatedAt: 3,
    };
    const coordinators = lifecycleCoordinatorPair();
    const listInstallations = vi
      .fn<() => Promise<AppInstallation[]>>()
      .mockResolvedValueOnce([enabledEpoch1])
      .mockResolvedValueOnce([disabledEpoch2]);
    const store = createParticipantAppStore(
      participantClient({ listInstallations }),
      coordinators[1],
    );
    await store.getState().bindParticipant(HUMAN_A);

    coordinators[1].emitResume();

    await vi.waitFor(() =>
      expect(store.getState()).toMatchObject({
        status: "ready",
        installations: [disabledEpoch2],
      }),
    );
    expect(listInstallations).toHaveBeenCalledTimes(2);
  });

  it("never publishes a late snapshot from the previously authenticated Human", async () => {
    const ownerAResponse = deferred<AppInstallation[]>();
    const ownerBResponse = deferred<AppInstallation[]>();
    const listInstallations = vi.fn((owner: AppOwnerRef) =>
      owner.kind === "participant" &&
      owner.participant.kind === "human" &&
      owner.participant.humanId === HUMAN_A_ID
        ? ownerAResponse.promise
        : ownerBResponse.promise,
    );
    const client = participantClient({ listInstallations });
    const store = createParticipantAppStore(client);

    const bindA = store.getState().bindParticipant(HUMAN_A);
    const bindB = store.getState().bindParticipant(HUMAN_B);
    ownerBResponse.resolve([installation(OWNER_B, "installation-b")]);
    await bindB;
    ownerAResponse.resolve([installation(OWNER_A, "installation-a")]);
    await bindA;

    expect(store.getState().owner).toEqual(OWNER_B);
    expect(store.getState().installations).toEqual([
      installation(OWNER_B, "installation-b"),
    ]);
  });

  it("rejects a response that crosses the bound owner scope", async () => {
    const client = participantClient({
      listInstallations: vi.fn(async () => [installation(OWNER_B)]),
    });
    const store = createParticipantAppStore(client);

    await store.getState().bindParticipant(HUMAN_A);

    expect(store.getState()).toMatchObject({
      owner: OWNER_A,
      status: "error",
      installations: [],
      errorCode: "App installation response crossed owner scope",
    });
  });

  it("refuses to choose between duplicate installation identities", () => {
    expect(
      participantInstallation(
        [
          installation(OWNER_A, "installation-a"),
          installation(OWNER_A, "installation-b"),
        ],
        "direct-chat",
      ),
    ).toBe("duplicate");
  });
});

function participantClient({
  listInstallations = vi.fn(async () => []),
  installApp = vi.fn(async () => installation(OWNER_A)),
  setInstallationState = vi.fn(async () => installation(OWNER_A)),
  uninstallApp = vi.fn(async () => undefined),
}: {
  listInstallations?: (owner: AppOwnerRef) => Promise<AppInstallation[]>;
  installApp?: (owner: AppOwnerRef, appId: string) => Promise<AppInstallation>;
  setInstallationState?: (
    installationId: string,
    state: "enabled" | "disabled",
  ) => Promise<AppInstallation>;
  uninstallApp?: (installationId: string) => Promise<void>;
} = {}): WorkspaceControlClient {
  return {
    listAppCatalog: vi.fn(async () => [DIRECT_CHAT, MESSAGING]),
    listInstallations,
    installApp,
    setInstallationState,
    uninstallApp,
  } as unknown as WorkspaceControlClient;
}

function installation(
  owner: AppOwnerRef,
  installationId = "0198f0f4-9b72-7000-8000-000000000051",
): AppInstallation {
  return {
    installationId,
    owner,
    appId: "direct-chat",
    state: "enabled",
    authorityEpoch: "1",
    installedAt: 1,
    updatedAt: 2,
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((settle) => {
    resolve = settle;
  });
  return { promise, resolve };
}

interface TestLifecycleCoordinator extends ParticipantAppLifecycleCoordinator {
  emitResume(): void;
}

function lifecycleCoordinatorPair(): [
  TestLifecycleCoordinator,
  TestLifecycleCoordinator,
] {
  const tails = new Map<string, Promise<void>>();
  const endpoints: Array<{
    listeners: Set<(ownerKey: string) => void>;
    resumeListeners: Set<() => void>;
  }> = [];
  const createEndpoint = (): TestLifecycleCoordinator => {
    const endpoint = {
      listeners: new Set<(ownerKey: string) => void>(),
      resumeListeners: new Set<() => void>(),
    };
    endpoints.push(endpoint);
    return {
      runExclusive<T>(ownerKey: string, operation: () => Promise<T>) {
        const previous = tails.get(ownerKey) ?? Promise.resolve();
        const result = previous.then(operation, operation);
        const tail = result.then(
          () => undefined,
          () => undefined,
        );
        tails.set(ownerKey, tail);
        return result;
      },
      publishMutation(ownerKey: string) {
        for (const candidate of endpoints) {
          if (candidate === endpoint) continue;
          for (const listener of candidate.listeners) listener(ownerKey);
        }
      },
      subscribeMutations(listener: (ownerKey: string) => void) {
        endpoint.listeners.add(listener);
        return () => endpoint.listeners.delete(listener);
      },
      subscribeResume(listener: () => void) {
        endpoint.resumeListeners.add(listener);
        return () => endpoint.resumeListeners.delete(listener);
      },
      emitResume() {
        for (const listener of endpoint.resumeListeners) listener();
      },
    };
  };
  return [createEndpoint(), createEndpoint()];
}
