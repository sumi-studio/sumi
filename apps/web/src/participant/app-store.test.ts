import { describe, expect, it, vi } from "vitest";
import type { WorkspaceControlClient } from "../workspace/api-client";
import type {
  AppDescriptor,
  AppInstallation,
  AppOwnerRef,
  ParticipantRef,
} from "../workspace/model";
import {
  createParticipantAppStore,
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
    const installApp = vi.fn(async () => installed);
    const setInstallationState = vi.fn(async () => disabled);
    const uninstallApp = vi.fn(async () => undefined);
    const client = participantClient({
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
    await disabling;
    expect(store.getState()).toMatchObject({
      mutation: null,
      installations: [disabledEpoch2],
    });
    await vi.waitFor(() => expect(listInstallations).toHaveBeenCalledTimes(2));
    refreshResponses[0]?.resolve([disabledEpoch2]);
    await refreshAfterDisable;
    expect(store.getState().installations).toEqual([disabledEpoch2]);

    const enabling = store
      .getState()
      .setInstallationState(enabledEpoch1.installationId, "enabled");
    const refreshAfterEnable = store.getState().refresh();
    expect(listInstallations).toHaveBeenCalledTimes(2);
    expect(store.getState().mutation).toBe("set_installation_enabled");
    enable.resolve(enabledEpoch2);
    await enabling;
    await vi.waitFor(() => expect(listInstallations).toHaveBeenCalledTimes(3));
    refreshResponses[1]?.resolve([enabledEpoch2]);
    await refreshAfterEnable;
    expect(store.getState().installations).toEqual([enabledEpoch2]);

    const uninstalling = store
      .getState()
      .uninstallApp(enabledEpoch1.installationId);
    const refreshAfterUninstall = store.getState().refresh();
    expect(listInstallations).toHaveBeenCalledTimes(3);
    expect(store.getState().mutation).toBe("uninstall_app");
    uninstall.resolve();
    await uninstalling;
    await vi.waitFor(() => expect(listInstallations).toHaveBeenCalledTimes(4));
    refreshResponses[2]?.resolve([]);
    await refreshAfterUninstall;
    expect(store.getState().installations).toEqual([]);

    const reinstalling = store.getState().installApp("direct-chat");
    const refreshAfterReinstall = store.getState().refresh();
    expect(listInstallations).toHaveBeenCalledTimes(4);
    expect(store.getState().mutation).toBe("install_app");
    reinstall.resolve(reinstalled);
    await reinstalling;
    await vi.waitFor(() => expect(listInstallations).toHaveBeenCalledTimes(5));
    refreshResponses[3]?.resolve([reinstalled]);
    await refreshAfterReinstall;
    expect(store.getState()).toMatchObject({
      status: "ready",
      mutation: null,
      installations: [reinstalled],
    });
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
