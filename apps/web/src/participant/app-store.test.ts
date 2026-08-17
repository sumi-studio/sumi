import { afterEach, describe, expect, it, vi } from "vitest";
import {
  WorkspaceAPIError,
  WorkspaceAPIUncertainError,
  type WorkspaceControlClient,
} from "../workspace/api-client";
import type {
  AppDescriptor,
  AppInstallation,
  AppOwnerRef,
  ParticipantRef,
} from "../workspace/model";
import { appOwnerKey } from "../workspace/model";
import {
  createBrowserLifecycleCoordinator,
  createParticipantAppStore,
  inspectParticipantAppLifecycleJournal,
  type ParticipantAppLifecycleCoordinator,
  type ParticipantAppLifecycleNotice,
  type ParticipantAppLifecycleSignal,
  type ParticipantAppLifecycleUnsettledNotice,
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

const NOTES: AppDescriptor = {
  appId: "notes",
  displayName: "Notes",
  workspaceOwnerAllowed: false,
  participantOwnerAllowed: true,
  workspaceRoleCapabilities: [],
};
const TASKS: AppDescriptor = {
  appId: "tasks",
  displayName: "Tasks",
  workspaceOwnerAllowed: false,
  participantOwnerAllowed: true,
  workspaceRoleCapabilities: [],
};

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("Participant app lifecycle store", () => {
  it("loads Participant apps without Web Locks and serializes same-owner installs in this document", async () => {
    const directChat = installation(OWNER_A);
    const notes = {
      ...installation(OWNER_A, "0198f0f4-9b72-7000-8000-000000000052"),
      appId: "notes",
    };
    const tasks = {
      ...installation(OWNER_A, "0198f0f4-9b72-7000-8000-000000000053"),
      appId: "tasks",
    };
    const firstInstall = deferred<AppInstallation>();
    const installsStarted: string[] = [];
    const installsFinished: string[] = [];
    let snapshot = [directChat];
    const listAppCatalog = vi.fn(async () => [DIRECT_CHAT, NOTES, TASKS]);
    const listInstallations = vi.fn(async () => snapshot);
    const installApp = vi.fn(async (_owner: AppOwnerRef, appId: string) => {
      installsStarted.push(appId);
      if (appId === "notes") {
        const result = await firstInstall.promise;
        snapshot = [...snapshot, result];
        installsFinished.push(appId);
        return result;
      }
      snapshot = [...snapshot, tasks];
      installsFinished.push(appId);
      return tasks;
    });
    vi.stubGlobal("navigator", {});
    vi.stubGlobal("localStorage", memoryStorage());
    vi.stubGlobal("BroadcastChannel", undefined);
    const coordinator = createBrowserLifecycleCoordinator();
    const firstStore = createParticipantAppStore(
      participantClient({ listAppCatalog, listInstallations, installApp }),
      coordinator,
    );
    const secondStore = createParticipantAppStore(
      participantClient({ listAppCatalog, listInstallations, installApp }),
      coordinator,
    );

    await Promise.all([
      firstStore.getState().bindParticipant(HUMAN_A),
      secondStore.getState().bindParticipant(HUMAN_A),
    ]);

    expect(coordinator.coordination).toBe("document-only");
    expect(firstStore.getState().coordination).toBe("document-only");
    expect(listAppCatalog).toHaveBeenCalledTimes(2);
    expect(listInstallations).toHaveBeenCalledTimes(2);
    expect(firstStore.getState().installations).toContainEqual(directChat);

    const first = firstStore.getState().installApp("notes");
    const second = secondStore.getState().installApp("tasks");
    await vi.waitFor(() => expect(installsStarted).toEqual(["notes"]));

    firstInstall.resolve(notes);
    await Promise.all([first, second]);

    expect(installsStarted).toEqual(["notes", "tasks"]);
    expect(installsFinished).toEqual(["notes", "tasks"]);
  });

  it("serializes lifecycle effects across documents with a storage lease when Web Locks are missing", async () => {
    // Two coordinators = two tabs sharing one localStorage and no Web Locks.
    vi.stubGlobal("navigator", {});
    vi.stubGlobal("localStorage", memoryStorage());
    vi.stubGlobal("BroadcastChannel", undefined);
    const tabA = createBrowserLifecycleCoordinator();
    const tabB = createBrowserLifecycleCoordinator();
    const order: string[] = [];
    let releaseA: () => void = () => {};
    const aRunning = new Promise<void>((resolve) => {
      releaseA = resolve;
    });
    const a = tabA.runExclusive("owner", async () => {
      order.push("a:start");
      await aRunning;
      order.push("a:end");
    });
    // Let tab A settle its lease before tab B tries.
    await new Promise((resolve) => setTimeout(resolve, 60));
    const b = tabB.runExclusive("owner", async () => {
      order.push("b:start");
      order.push("b:end");
    });
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(order).toEqual(["a:start"]);
    releaseA();
    await Promise.all([a, b]);
    expect(order).toEqual(["a:start", "a:end", "b:start", "b:end"]);
  });

  it("continues to use Web Locks when they are available", async () => {
    const locks = {
      request: vi.fn(
        async <T>(
          _name: string,
          _options: LockOptions,
          operation: () => Promise<T>,
        ) => operation(),
      ),
    };
    const listAppCatalog = vi.fn(async () => [DIRECT_CHAT]);
    const listInstallations = vi.fn(async () => [installation(OWNER_A)]);
    vi.stubGlobal("navigator", { locks });
    vi.stubGlobal("localStorage", memoryStorage());
    vi.stubGlobal("BroadcastChannel", undefined);
    const coordinator = createBrowserLifecycleCoordinator();
    const store = createParticipantAppStore(
      participantClient({ listAppCatalog, listInstallations }),
      coordinator,
    );

    await store.getState().bindParticipant(HUMAN_A);

    expect(coordinator.coordination).toBe("web-locks");
    expect(store.getState().coordination).toBe("web-locks");
    expect(locks.request).toHaveBeenCalledWith(
      `sumi:participant-app-lifecycle:owner:${appOwnerKey(OWNER_A)}`,
      { mode: "exclusive" },
      expect.any(Function),
    );
    expect(store.getState()).toMatchObject({
      status: "ready",
      installations: [installation(OWNER_A)],
    });
  });

  it("distinguishes an absent journal from every present malformed entry", () => {
    const ownerKey = appOwnerKey(OWNER_A);
    const valid = unsettledSetStateNotice(installation(OWNER_A));
    const invalidEntries = [
      "{",
      JSON.stringify({ ...valid, version: 1 }),
      JSON.stringify({ ...valid, unexpected: true }),
      JSON.stringify({
        ...valid,
        intent: { ...valid.intent, unexpected: true },
      }),
      JSON.stringify({ ...valid, phase: "settled", intent: undefined }),
      JSON.stringify({ ...valid, ownerKey: appOwnerKey(OWNER_B) }),
      JSON.stringify({ ...valid, operationId: "not-an-operation-id" }),
      JSON.stringify({
        ...valid,
        intent: { ...valid.intent, installationId: "not-an-installation-id" },
      }),
      JSON.stringify({
        ...valid,
        intent: { ...valid.intent, appId: "INVALID APP" },
      }),
      JSON.stringify({
        ...valid,
        intent: { ...valid.intent, expectedAuthorityEpoch: "0" },
      }),
      JSON.stringify({
        ...valid,
        intent: {
          ...valid.intent,
          expectedAuthorityEpoch: "9223372036854775808",
        },
      }),
      JSON.stringify({
        ...valid,
        intent: { kind: "install", appId: "INVALID APP" },
      }),
      JSON.stringify({
        ...valid,
        intent: { kind: "uninstall", installationId: "not-an-id" },
      }),
    ];

    expect(inspectParticipantAppLifecycleJournal(ownerKey, null)).toEqual({
      state: "absent",
    });
    expect(
      inspectParticipantAppLifecycleJournal(ownerKey, JSON.stringify(valid)),
    ).toEqual({ state: "unsettled", notice: valid });
    for (const raw of invalidEntries) {
      expect(inspectParticipantAppLifecycleJournal(ownerKey, raw)).toEqual({
        state: "invalid",
      });
    }
  });

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
    expect(installApp).toHaveBeenCalledWith(
      OWNER_A,
      "direct-chat",
      expect.stringMatching(
        /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
      ),
    );
    expect(store.getState().installations).toEqual([installed]);

    await store
      .getState()
      .setInstallationState(installed.installationId, "disabled");
    expect(setInstallationState).toHaveBeenCalledWith(
      installed.installationId,
      "disabled",
      "1",
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
          throw new WorkspaceAPIUncertainError(
            new TypeError("response lost after commit"),
          );
        }),
      }),
      coordinators[0],
    );
    const secondReplay = vi.fn(async () => {
      throw new WorkspaceAPIError("stale_authority", 409);
    });
    const secondStore = createParticipantAppStore(
      participantClient({
        listInstallations: secondList,
        setInstallationState: secondReplay,
      }),
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

    expect(mutationError).toBeInstanceOf(WorkspaceAPIUncertainError);
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
    expect(secondReplay).toHaveBeenCalledWith(
      enabledEpoch1.installationId,
      "disabled",
      "1",
    );
  });

  it("clears ambiguous mutation UI state after a surviving sender converges", async () => {
    const enabledEpoch1 = installation(OWNER_A);
    const disabledEpoch2 = {
      ...enabledEpoch1,
      state: "disabled" as const,
      authorityEpoch: "2",
      updatedAt: 3,
    };
    let serverSnapshot = [enabledEpoch1];
    let originalAttempt = true;
    const replay = async () => {
      if (originalAttempt) {
        originalAttempt = false;
        serverSnapshot = [disabledEpoch2];
        throw new WorkspaceAPIUncertainError(new TypeError("response lost"));
      }
      throw new WorkspaceAPIError("stale_authority", 409);
    };
    const coordinators = lifecycleCoordinatorPair();
    const firstStore = createParticipantAppStore(
      participantClient({
        listInstallations: vi.fn(async () => serverSnapshot),
        setInstallationState: vi.fn(replay),
      }),
      coordinators[0],
    );
    const secondStore = createParticipantAppStore(
      participantClient({
        listInstallations: vi.fn(async () => serverSnapshot),
        setInstallationState: vi.fn(async () => {
          throw new WorkspaceAPIError("stale_authority", 409);
        }),
      }),
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
    ).rejects.toBeInstanceOf(WorkspaceAPIUncertainError);
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
      errorCode: null,
    });
  });

  it("takes over an exact intent when the renderer dies before the server outcome", async () => {
    const enabledEpoch1 = installation(OWNER_A);
    const disabledEpoch2 = {
      ...enabledEpoch1,
      state: "disabled" as const,
      authorityEpoch: "2",
      updatedAt: 3,
    };
    let serverSnapshot = [enabledEpoch1];
    const orphanResponse = deferred<AppInstallation>();
    const replayResponse = deferred<AppInstallation>();
    const replayStarted = deferred<void>();
    const coordinators = lifecycleCoordinatorPair();
    const senderEffect = vi.fn(() => orphanResponse.promise);
    const peerReplay = vi.fn(() => {
      replayStarted.resolve();
      return replayResponse.promise;
    });
    const secondList = vi.fn(async () => serverSnapshot);
    const firstStore = createParticipantAppStore(
      participantClient({
        listInstallations: vi.fn(async () => serverSnapshot),
        setInstallationState: senderEffect,
      }),
      coordinators[0],
    );
    const secondStore = createParticipantAppStore(
      participantClient({
        listInstallations: secondList,
        setInstallationState: peerReplay,
      }),
      coordinators[1],
    );
    await Promise.all([
      firstStore.getState().bindParticipant(HUMAN_A),
      secondStore.getState().bindParticipant(HUMAN_A),
    ]);

    const mutation = firstStore
      .getState()
      .setInstallationState(enabledEpoch1.installationId, "disabled")
      .catch((error: unknown) => error);
    await vi.waitFor(() => expect(senderEffect).toHaveBeenCalledOnce());
    expect(secondStore.getState()).toMatchObject({
      status: "loading",
      installations: [enabledEpoch1],
    });

    coordinators[0].crash();
    await replayStarted.promise;
    // The takeover owns the exact cross-document lock, but it has not yet
    // crossed the same DB row boundary as the orphan request. It must not
    // publish a provisional read of the old truth.
    expect(secondList).toHaveBeenCalledTimes(1);
    expect(secondStore.getState()).toMatchObject({
      status: "loading",
      installations: [enabledEpoch1],
    });

    serverSnapshot = [disabledEpoch2];
    orphanResponse.resolve(disabledEpoch2);
    replayResponse.reject(new WorkspaceAPIError("stale_authority", 409));

    await vi.waitFor(() =>
      expect(secondStore.getState()).toMatchObject({
        status: "ready",
        installations: [disabledEpoch2],
        errorCode: null,
      }),
    );
    expect(secondList).toHaveBeenCalledTimes(2);
    expect(peerReplay).toHaveBeenCalledWith(
      enabledEpoch1.installationId,
      "disabled",
      "1",
    );
    await expect(mutation).resolves.toEqual(
      new Error("test renderer disappeared"),
    );
  });

  it("never publishes old truth when an orphan survives a corrupted journal", async () => {
    const enabledEpoch1 = installation(OWNER_A);
    const disabledEpoch2 = {
      ...enabledEpoch1,
      state: "disabled" as const,
      authorityEpoch: "2",
      updatedAt: 3,
    };
    let serverSnapshot = [enabledEpoch1];
    const orphanResponse = deferred<AppInstallation>();
    const coordinators = lifecycleCoordinatorPair();
    const senderEffect = vi.fn(() => orphanResponse.promise);
    const peerReplay = vi.fn(async () => disabledEpoch2);
    const secondList = vi.fn(async () => serverSnapshot);
    const firstStore = createParticipantAppStore(
      participantClient({
        listInstallations: vi.fn(async () => serverSnapshot),
        setInstallationState: senderEffect,
      }),
      coordinators[0],
    );
    const secondStore = createParticipantAppStore(
      participantClient({
        listInstallations: secondList,
        setInstallationState: peerReplay,
      }),
      coordinators[1],
    );
    await Promise.all([
      firstStore.getState().bindParticipant(HUMAN_A),
      secondStore.getState().bindParticipant(HUMAN_A),
    ]);

    const mutation = firstStore
      .getState()
      .setInstallationState(enabledEpoch1.installationId, "disabled")
      .catch((error: unknown) => error);
    await vi.waitFor(() => expect(senderEffect).toHaveBeenCalledOnce());
    const ownerKey = appOwnerKey(OWNER_A);
    coordinators[0].corruptJournal(ownerKey, "{corrupted");
    coordinators[0].crash();

    await vi.waitFor(() =>
      expect(secondStore.getState()).toMatchObject({
        status: "error",
        installations: [enabledEpoch1],
        errorCode: "Participant app lifecycle recovery evidence is invalid",
      }),
    );
    expect(secondList).toHaveBeenCalledOnce();
    expect(peerReplay).not.toHaveBeenCalled();
    expect(coordinators[1].journal(ownerKey)).toBe("{corrupted");

    // The issued request may still commit after the renderer and its Web Lock
    // disappear. Invalid evidence keeps the peer blocked instead of allowing
    // a provisional old read before that late commit.
    serverSnapshot = [disabledEpoch2];
    orphanResponse.resolve(disabledEpoch2);
    await expect(mutation).resolves.toEqual(
      new Error("test renderer disappeared"),
    );
    await secondStore.getState().refresh();

    expect(secondStore.getState()).toMatchObject({
      status: "error",
      installations: [enabledEpoch1],
      errorCode: "Participant app lifecycle recovery evidence is invalid",
    });
    expect(secondList).toHaveBeenCalledOnce();
    expect(peerReplay).not.toHaveBeenCalled();
    expect(coordinators[1].journal(ownerKey)).toBe("{corrupted");
  });

  it("mints the lifecycle operation id on an origin without randomUUID", async () => {
    // Plain-HTTP origins are not secure contexts, so crypto.randomUUID is
    // absent there. A lifecycle mutation must still announce a durable
    // operation id instead of throwing before it reaches the journal.
    const installed = installation(OWNER_A);
    let snapshot: AppInstallation[] = [];
    const installApp = vi.fn(async () => {
      snapshot = [installed];
      return installed;
    });
    const coordinators = lifecycleCoordinatorPair();
    const store = createParticipantAppStore(
      participantClient({
        installApp,
        listInstallations: async () => snapshot,
      }),
      coordinators[0],
    );
    await store.getState().bindParticipant(HUMAN_A);

    vi.stubGlobal("crypto", {
      getRandomValues: (bytes: Uint8Array) => {
        for (let index = 0; index < bytes.length; index += 1) {
          bytes[index] = index;
        }
        return bytes;
      },
    });
    await store.getState().installApp("direct-chat");

    expect(installApp).toHaveBeenCalledWith(
      OWNER_A,
      "direct-chat",
      expect.stringMatching(
        /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
      ),
    );
    expect(store.getState().installations).toEqual([installed]);
  });

  it("recovers a durable install intent when a new document binds later", async () => {
    const installed = installation(OWNER_A);
    const coordinators = lifecycleCoordinatorPair();
    const installReplay = vi.fn(async () => {
      throw new WorkspaceAPIError("install_intent_already_installed", 409);
    });
    coordinators[0].publishMutation({
      version: 2,
      ownerKey: appOwnerKey(OWNER_A),
      operationId: "00000000-0000-4000-8000-000000000002",
      phase: "unsettled",
      intent: { kind: "install", appId: "direct-chat" },
    });

    const listInstallations = vi.fn(async () => [installed]);
    const store = createParticipantAppStore(
      participantClient({ listInstallations, installApp: installReplay }),
      coordinators[1],
    );
    await store.getState().bindParticipant(HUMAN_A);

    expect(installReplay).toHaveBeenCalledWith(
      OWNER_A,
      "direct-chat",
      "00000000-0000-4000-8000-000000000002",
    );
    expect(listInstallations).toHaveBeenCalledOnce();
    expect(store.getState()).toMatchObject({
      status: "ready",
      installations: [installed],
      errorCode: null,
    });
    expect(coordinators[1].pendingMutation(appOwnerKey(OWNER_A))).toEqual({
      state: "absent",
    });
  });

  it("does not settle an install intent from a generic owner/app conflict", async () => {
    const coordinators = lifecycleCoordinatorPair();
    const notice: ParticipantAppLifecycleUnsettledNotice = {
      version: 2,
      ownerKey: appOwnerKey(OWNER_A),
      operationId: "00000000-0000-4000-8000-000000000005",
      phase: "unsettled",
      intent: { kind: "install", appId: "direct-chat" },
    };
    coordinators[0].publishMutation(notice);
    const installReplay = vi.fn(async () => {
      throw new WorkspaceAPIError("conflict", 409);
    });
    const listInstallations = vi.fn(async () => [installation(OWNER_A)]);
    const store = createParticipantAppStore(
      participantClient({ listInstallations, installApp: installReplay }),
      coordinators[1],
    );

    await store.getState().bindParticipant(HUMAN_A);

    expect(store.getState()).toMatchObject({
      status: "error",
      installations: [],
      errorCode: "conflict",
    });
    expect(installReplay).toHaveBeenCalledWith(
      OWNER_A,
      "direct-chat",
      notice.operationId,
    );
    expect(listInstallations).not.toHaveBeenCalled();
    expect(coordinators[1].pendingMutation(notice.ownerKey)).toEqual({
      state: "unsettled",
      notice,
    });
  });

  it("replays exact uninstall to not-found before publishing absence", async () => {
    const original = installation(OWNER_A);
    let serverSnapshot = [original];
    const uninstallReplay = deferred<void>();
    const coordinators = lifecycleCoordinatorPair();
    const listInstallations = vi.fn(async () => serverSnapshot);
    const store = createParticipantAppStore(
      participantClient({
        listInstallations,
        uninstallApp: vi.fn(() => uninstallReplay.promise),
      }),
      coordinators[1],
    );
    await store.getState().bindParticipant(HUMAN_A);

    coordinators[0].publishMutation({
      version: 2,
      ownerKey: appOwnerKey(OWNER_A),
      operationId: "00000000-0000-4000-8000-000000000003",
      phase: "unsettled",
      intent: {
        kind: "uninstall",
        installationId: original.installationId,
      },
    });
    await vi.waitFor(() =>
      expect(store.getState()).toMatchObject({
        status: "loading",
        installations: [original],
      }),
    );
    expect(listInstallations).toHaveBeenCalledTimes(1);

    serverSnapshot = [];
    uninstallReplay.reject(new WorkspaceAPIError("not_found", 404));
    await vi.waitFor(() =>
      expect(store.getState()).toMatchObject({
        status: "ready",
        installations: [],
        errorCode: null,
      }),
    );
    expect(listInstallations).toHaveBeenCalledTimes(2);
  });

  it("deduplicates repeated wakeups for one unsettled operation", async () => {
    const enabledEpoch1 = installation(OWNER_A);
    const disabledEpoch2 = {
      ...enabledEpoch1,
      state: "disabled" as const,
      authorityEpoch: "2",
      updatedAt: 3,
    };
    let serverSnapshot = [enabledEpoch1];
    const replay = deferred<AppInstallation>();
    const replayCall = vi.fn(() => replay.promise);
    const coordinators = lifecycleCoordinatorPair();
    const listInstallations = vi.fn(async () => serverSnapshot);
    const store = createParticipantAppStore(
      participantClient({
        listInstallations,
        setInstallationState: replayCall,
      }),
      coordinators[1],
    );
    await store.getState().bindParticipant(HUMAN_A);
    const notice = unsettledSetStateNotice(enabledEpoch1);

    coordinators[0].publishMutation(notice);
    coordinators[0].emitSignal(notice);
    await vi.waitFor(() => expect(replayCall).toHaveBeenCalledOnce());
    expect(listInstallations).toHaveBeenCalledTimes(1);

    serverSnapshot = [disabledEpoch2];
    replay.reject(new WorkspaceAPIError("stale_authority", 409));
    await vi.waitFor(() =>
      expect(store.getState()).toMatchObject({
        status: "ready",
        installations: [disabledEpoch2],
      }),
    );
    const settled: ParticipantAppLifecycleNotice = {
      version: 2,
      ownerKey: notice.ownerKey,
      operationId: notice.operationId,
      phase: "settled",
    };
    coordinators[0].publishMutation(settled);
    coordinators[0].publishMutation(settled);
    await Promise.resolve();

    expect(replayCall).toHaveBeenCalledOnce();
    expect(listInstallations).toHaveBeenCalledTimes(2);
  });

  it("scopes wakeup deduplication by owner as well as operation id", async () => {
    const enabledEpoch1 = installation(OWNER_A);
    const disabledEpoch2 = {
      ...enabledEpoch1,
      state: "disabled" as const,
      authorityEpoch: "2",
      updatedAt: 3,
    };
    const coordinators = lifecycleCoordinatorPair();
    const replay = vi.fn(async () => disabledEpoch2);
    const listInstallations = vi
      .fn()
      .mockResolvedValueOnce([enabledEpoch1])
      .mockResolvedValue([disabledEpoch2]);
    const store = createParticipantAppStore(
      participantClient({ listInstallations, setInstallationState: replay }),
      coordinators[1],
    );
    await store.getState().bindParticipant(HUMAN_A);
    const ownNotice = unsettledSetStateNotice(enabledEpoch1);
    const foreignNotice = {
      ...unsettledSetStateNotice(installation(OWNER_B)),
      operationId: ownNotice.operationId,
    } satisfies ParticipantAppLifecycleUnsettledNotice;

    coordinators[0].emitSignal(foreignNotice);
    coordinators[0].publishMutation(ownNotice);

    await vi.waitFor(() => expect(replay).toHaveBeenCalledOnce());
    expect(replay).toHaveBeenCalledWith(
      enabledEpoch1.installationId,
      "disabled",
      "1",
    );
    expect(listInstallations).toHaveBeenCalledTimes(2);
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
          throw new WorkspaceAPIError("write rejected before commit", 409);
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

  it("blocks reads and effects without overwriting present invalid JSON", async () => {
    const coordinators = lifecycleCoordinatorPair();
    const ownerKey = appOwnerKey(OWNER_A);
    coordinators[0].corruptJournal(ownerKey, "{invalid-json");
    const listInstallations = vi.fn(async () => [installation(OWNER_A)]);
    const installApp = vi.fn(async () => installation(OWNER_A));
    const store = createParticipantAppStore(
      participantClient({ listInstallations, installApp }),
      coordinators[1],
    );

    await store.getState().bindParticipant(HUMAN_A);
    await expect(store.getState().installApp("direct-chat")).rejects.toThrow(
      "Participant app lifecycle recovery evidence is invalid",
    );

    expect(store.getState()).toMatchObject({
      status: "error",
      mutation: null,
      installations: [],
      errorCode: "Participant app lifecycle recovery evidence is invalid",
    });
    expect(listInstallations).not.toHaveBeenCalled();
    expect(installApp).not.toHaveBeenCalled();
    expect(coordinators[1].journal(ownerKey)).toBe("{invalid-json");
  });

  it("refreshes authoritatively after verified out-of-band journal cleanup", async () => {
    const coordinators = lifecycleCoordinatorPair();
    const ownerKey = appOwnerKey(OWNER_A);
    coordinators[0].corruptJournal(ownerKey, "{invalid-json");
    const installed = installation(OWNER_A);
    const listInstallations = vi.fn(async () => [installed]);
    const store = createParticipantAppStore(
      participantClient({ listInstallations }),
      coordinators[1],
    );
    await store.getState().bindParticipant(HUMAN_A);
    expect(listInstallations).not.toHaveBeenCalled();

    // Product code never performs this cleanup. This seam represents an
    // operator/user removing the evidence only after independently proving no
    // lifecycle effect remains in flight; the storage event then wakes a
    // fresh authoritative read.
    coordinators[0].clearJournal(ownerKey);

    await vi.waitFor(() =>
      expect(store.getState()).toMatchObject({
        status: "ready",
        installations: [installed],
        errorCode: null,
      }),
    );
    expect(listInstallations).toHaveBeenCalledOnce();
    expect(coordinators[1].journal(ownerKey)).toBeNull();
  });

  it("does not replay an owner-mismatched journal and scopes it away on authority change", async () => {
    const coordinators = lifecycleCoordinatorPair();
    const ownerAKey = appOwnerKey(OWNER_A);
    const wrongOwnerNotice = {
      ...unsettledSetStateNotice(installation(OWNER_B)),
      operationId: "00000000-0000-4000-8000-000000000004",
    } satisfies ParticipantAppLifecycleUnsettledNotice;
    const raw = JSON.stringify(wrongOwnerNotice);
    coordinators[0].corruptJournal(ownerAKey, raw);
    const listInstallations = vi.fn(async (owner: AppOwnerRef) =>
      owner.kind === "participant" && owner.participant === HUMAN_B
        ? [installation(OWNER_B)]
        : [],
    );
    const setInstallationState = vi.fn(async () => installation(OWNER_B));
    const store = createParticipantAppStore(
      participantClient({ listInstallations, setInstallationState }),
      coordinators[1],
    );

    await store.getState().bindParticipant(HUMAN_A);
    expect(store.getState()).toMatchObject({
      status: "error",
      installations: [],
      errorCode: "Participant app lifecycle recovery evidence is invalid",
    });
    expect(listInstallations).not.toHaveBeenCalled();
    expect(setInstallationState).not.toHaveBeenCalled();
    expect(coordinators[1].journal(ownerAKey)).toBe(raw);

    await store.getState().bindParticipant(HUMAN_B);
    expect(store.getState()).toMatchObject({
      owner: OWNER_B,
      status: "ready",
      installations: [installation(OWNER_B)],
      errorCode: null,
    });
    expect(listInstallations).toHaveBeenCalledOnce();
    expect(listInstallations).toHaveBeenCalledWith(OWNER_B);
    expect(setInstallationState).not.toHaveBeenCalled();
    expect(coordinators[1].journal(ownerAKey)).toBe(raw);
  });

  it("blocks reads and effects when durable evidence cannot be read", async () => {
    const readFailure = new Error("local storage read denied");
    const listInstallations = vi.fn(async () => [installation(OWNER_A)]);
    const installApp = vi.fn(async () => installation(OWNER_A));
    const publishMutation = vi.fn();
    const coordinator: ParticipantAppLifecycleCoordinator = {
      runExclusive: async <T>(_ownerKey: string, operation: () => Promise<T>) =>
        operation(),
      publishMutation,
      pendingMutation: () => {
        throw new Error(
          "Participant app lifecycle recovery evidence cannot be read",
          { cause: readFailure },
        );
      },
      subscribeMutations: () => () => undefined,
      subscribeResume: () => () => undefined,
    };
    const store = createParticipantAppStore(
      participantClient({ listInstallations, installApp }),
      coordinator,
    );

    await store.getState().bindParticipant(HUMAN_A);
    await expect(store.getState().installApp("direct-chat")).rejects.toThrow(
      "Participant app lifecycle recovery evidence cannot be read",
    );

    expect(store.getState()).toMatchObject({
      status: "error",
      mutation: null,
      installations: [],
      errorCode: "Participant app lifecycle recovery evidence cannot be read",
    });
    expect(listInstallations).not.toHaveBeenCalled();
    expect(installApp).not.toHaveBeenCalled();
    expect(publishMutation).not.toHaveBeenCalled();
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
      pendingMutation: () => ({ state: "absent" }),
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

  it("fails closed when the durable intent journal cannot be written", async () => {
    const enabledEpoch1 = installation(OWNER_A);
    const setInstallationState = vi.fn(async () => ({
      ...enabledEpoch1,
      state: "disabled" as const,
      authorityEpoch: "2",
    }));
    const coordinator: ParticipantAppLifecycleCoordinator = {
      runExclusive: async <T>(_ownerKey: string, operation: () => Promise<T>) =>
        operation(),
      publishMutation: () => {
        throw new Error("Participant app durable journal is unavailable");
      },
      pendingMutation: () => ({ state: "absent" }),
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

    await expect(
      store
        .getState()
        .setInstallationState(enabledEpoch1.installationId, "disabled"),
    ).rejects.toThrow("Participant app durable journal is unavailable");

    expect(setInstallationState).not.toHaveBeenCalled();
    expect(store.getState()).toMatchObject({
      status: "ready",
      mutation: null,
      installations: [enabledEpoch1],
      errorCode: "Participant app durable journal is unavailable",
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
    const announced = unsettledSetStateNotice(enabledEpoch1);
    coordinators[0].publishMutation(announced);
    coordinators[0].publishMutation({
      version: 2,
      ownerKey: announced.ownerKey,
      operationId: announced.operationId,
      phase: "settled",
    });
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
  listAppCatalog = vi.fn(async () => [DIRECT_CHAT, MESSAGING]),
  listInstallations = vi.fn(async () => []),
  installApp = vi.fn(async () => installation(OWNER_A)),
  setInstallationState = vi.fn(async () => installation(OWNER_A)),
  uninstallApp = vi.fn(async () => undefined),
}: {
  listAppCatalog?: () => Promise<AppDescriptor[]>;
  listInstallations?: (owner: AppOwnerRef) => Promise<AppInstallation[]>;
  installApp?: (
    owner: AppOwnerRef,
    appId: string,
    operationId?: string,
  ) => Promise<AppInstallation>;
  setInstallationState?: (
    installationId: string,
    state: "enabled" | "disabled",
    expectedAuthorityEpoch?: string,
  ) => Promise<AppInstallation>;
  uninstallApp?: (installationId: string) => Promise<void>;
} = {}): WorkspaceControlClient {
  return {
    listAppCatalog,
    listInstallations,
    installApp,
    setInstallationState,
    uninstallApp,
  } as unknown as WorkspaceControlClient;
}

function memoryStorage(): Storage {
  const values = new Map<string, string>();
  return {
    get length() {
      return values.size;
    },
    clear() {
      values.clear();
    },
    getItem(key) {
      return values.get(key) ?? null;
    },
    key(index) {
      return [...values.keys()][index] ?? null;
    },
    removeItem(key) {
      values.delete(key);
    },
    setItem(key, value) {
      values.set(key, value);
    },
  };
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
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((settle, fail) => {
    resolve = settle;
    reject = fail;
  });
  return { promise, resolve, reject };
}

interface TestLifecycleCoordinator extends ParticipantAppLifecycleCoordinator {
  emitResume(): void;
  emitSignal(signal: ParticipantAppLifecycleSignal): void;
  crash(): void;
  corruptJournal(ownerKey: string, raw: string): void;
  clearJournal(ownerKey: string): void;
  journal(ownerKey: string): string | null;
}

function lifecycleCoordinatorPair(): [
  TestLifecycleCoordinator,
  TestLifecycleCoordinator,
] {
  const tails = new Map<string, Promise<void>>();
  const pending = new Map<string, string>();
  const endpoints: Array<{
    listeners: Set<(signal: ParticipantAppLifecycleSignal) => void>;
    resumeListeners: Set<() => void>;
    alive: boolean;
    crashed: ReturnType<typeof deferred<void>>;
  }> = [];
  const createEndpoint = (): TestLifecycleCoordinator => {
    const endpoint = {
      listeners: new Set<(signal: ParticipantAppLifecycleSignal) => void>(),
      resumeListeners: new Set<() => void>(),
      alive: true,
      crashed: deferred<void>(),
    };
    endpoints.push(endpoint);
    return {
      runExclusive<T>(ownerKey: string, operation: () => Promise<T>) {
        if (!endpoint.alive) {
          return Promise.reject(new Error("test renderer disappeared"));
        }
        const previous = tails.get(ownerKey) ?? Promise.resolve();
        const operationResult = previous.then(operation, operation);
        const lease = Promise.race([
          operationResult,
          endpoint.crashed.promise.then(() => {
            throw new Error("test renderer disappeared");
          }),
        ]);
        const tail = lease.then(
          () => undefined,
          () => undefined,
        );
        tails.set(ownerKey, tail);
        return lease;
      },
      publishMutation(notice: ParticipantAppLifecycleNotice) {
        if (!endpoint.alive) {
          throw new Error("test renderer disappeared");
        }
        if (notice.phase === "unsettled") {
          if (
            inspectParticipantAppLifecycleJournal(
              notice.ownerKey,
              pending.get(notice.ownerKey) ?? null,
            ).state !== "absent"
          ) {
            throw new Error(
              "Participant app lifecycle recovery evidence is invalid",
            );
          }
          pending.set(notice.ownerKey, JSON.stringify(notice));
        } else {
          const current = inspectParticipantAppLifecycleJournal(
            notice.ownerKey,
            pending.get(notice.ownerKey) ?? null,
          );
          if (current.state === "invalid") {
            throw new Error(
              "Participant app lifecycle recovery evidence is invalid",
            );
          }
          if (current.state === "unsettled") {
            if (current.notice.operationId !== notice.operationId) {
              throw new Error(
                "Participant app lifecycle recovery evidence is invalid",
              );
            }
            pending.delete(notice.ownerKey);
          }
        }
        for (const candidate of endpoints) {
          if (candidate === endpoint) continue;
          for (const listener of candidate.listeners) listener(notice);
        }
      },
      pendingMutation(ownerKey: string) {
        return inspectParticipantAppLifecycleJournal(
          ownerKey,
          pending.get(ownerKey) ?? null,
        );
      },
      subscribeMutations(
        listener: (signal: ParticipantAppLifecycleSignal) => void,
      ) {
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
      emitSignal(signal: ParticipantAppLifecycleSignal) {
        for (const candidate of endpoints) {
          if (candidate === endpoint || !candidate.alive) continue;
          for (const listener of candidate.listeners) listener(signal);
        }
      },
      crash() {
        endpoint.alive = false;
        endpoint.listeners.clear();
        endpoint.resumeListeners.clear();
        endpoint.crashed.resolve();
      },
      corruptJournal(ownerKey: string, raw: string) {
        pending.set(ownerKey, raw);
        for (const candidate of endpoints) {
          if (!candidate.alive) continue;
          for (const listener of candidate.listeners) {
            listener({ ownerKey, phase: "journal_invalid" });
          }
        }
      },
      clearJournal(ownerKey: string) {
        pending.delete(ownerKey);
        for (const candidate of endpoints) {
          if (!candidate.alive) continue;
          for (const listener of candidate.listeners) {
            listener({ ownerKey, phase: "journal_cleared" });
          }
        }
      },
      journal(ownerKey: string) {
        return pending.get(ownerKey) ?? null;
      },
    };
  };
  return [createEndpoint(), createEndpoint()];
}

function unsettledSetStateNotice(
  current: AppInstallation,
): ParticipantAppLifecycleUnsettledNotice {
  return {
    version: 2,
    ownerKey: appOwnerKey(current.owner),
    operationId: "00000000-0000-4000-8000-000000000001",
    phase: "unsettled",
    intent: {
      kind: "set_state",
      installationId: current.installationId,
      appId: current.appId,
      expectedAuthorityEpoch: current.authorityEpoch,
      state: "disabled",
    },
  };
}
