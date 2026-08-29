import type { paths } from "../src";

type InstallAppRequest =
  paths["/app-installations"]["post"]["requestBody"]["content"]["application/json"];
type StateRequest =
  paths["/app-installations/{installation_id}/state"]["put"]["requestBody"]["content"]["application/json"];
type InstallConflictResponse =
  paths["/app-installations"]["post"]["responses"][409]["content"]["application/json"];
type InstallUnavailableResponse =
  paths["/app-installations"]["post"]["responses"][503]["content"]["application/json"];
type StateConflictResponse =
  paths["/app-installations/{installation_id}/state"]["put"]["responses"][409]["content"]["application/json"];
type StateUnavailableResponse =
  paths["/app-installations/{installation_id}/state"]["put"]["responses"][503]["content"]["application/json"];
type UninstallUnavailableResponse =
  paths["/app-installations/{installation_id}"]["delete"]["responses"][503]["content"]["application/json"];

const workspaceId = "018f1e72-6e9a-7c20-8e90-123456789abc";
const humanId = "018f1e72-6e9a-7c20-8e90-123456789abd";
const operationId = "00000000-0000-4000-8000-000000000101";

export const workspaceInstallWithOperation: InstallAppRequest = {
  owner: { kind: "workspace", workspace_id: workspaceId },
  app_id: "messaging",
  operation_id: operationId,
};

// @ts-expect-error All install intents require their durable operation identity.
export const workspaceInstallWithoutOperation: InstallAppRequest = {
  owner: { kind: "workspace", workspace_id: workspaceId },
  app_id: "messaging",
};

export const participantInstallWithOperation: InstallAppRequest = {
  owner: {
    kind: "participant",
    participant: { kind: "human", human_id: humanId },
  },
  app_id: "direct-chat",
  operation_id: operationId,
};

// @ts-expect-error Participant install intents require their durable operation identity.
export const participantInstallWithoutOperation: InstallAppRequest = {
  owner: {
    kind: "participant",
    participant: { kind: "human", human_id: humanId },
  },
  app_id: "direct-chat",
};

export const stateWithoutExpectedEpoch: StateRequest = { state: "disabled" };

export const stateWithExpectedEpoch: StateRequest = {
  state: "enabled",
  expected_authority_epoch: "1",
};

export const stateWithNullExpectedEpoch: StateRequest = {
  state: "enabled",
  // @ts-expect-error Explicit null is not omission and is invalid on the wire.
  expected_authority_epoch: null,
};

export const existingInstallIntentConflict: InstallConflictResponse = {
  error: "install_intent_already_installed",
};

export const mismatchedInstallIntentConflict: InstallConflictResponse = {
  error: "idempotency_conflict",
};

export const staleInstallConflict: InstallConflictResponse = {
  // @ts-expect-error Install conflicts cannot masquerade as stale state authority.
  error: "stale_authority",
};

export const staleStateAuthority: StateConflictResponse = {
  error: "stale_authority",
};

export const genericStateConflict: StateConflictResponse = {
  // @ts-expect-error State mutation 409 has the exact stale-authority shape.
  error: "conflict",
};

export const installUnavailable: InstallUnavailableResponse = {
  error: "unavailable",
};

export const stateUnavailable: StateUnavailableResponse = {
  error: "unavailable",
};

export const uninstallUnavailable: UninstallUnavailableResponse = {
  error: "unavailable",
};

export const unavailableWithDetail: UninstallUnavailableResponse = {
  error: "unavailable",
  // @ts-expect-error Lifecycle error bodies remain strict one-field objects.
  retry_after: 1,
};
