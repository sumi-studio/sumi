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
