import type { paths } from "../src";

type InstallAppRequest =
  paths["/app-installations"]["post"]["requestBody"]["content"]["application/json"];
type StateRequest =
  paths["/app-installations/{installation_id}/state"]["put"]["requestBody"]["content"]["application/json"];

const workspaceId = "018f1e72-6e9a-7c20-8e90-123456789abc";
const humanId = "018f1e72-6e9a-7c20-8e90-123456789abd";
const operationId = "00000000-0000-4000-8000-000000000101";

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
