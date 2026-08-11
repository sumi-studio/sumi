// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { act, cleanup, render } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { AppInstallation } from "../workspace/model";
import { ParticipantAppBinding } from "./app-binding";
import { useParticipantApps } from "./app-store";

const HUMAN_ID = "0198f0f4-9b72-7000-8000-000000000021";
const INSTALLATION_ID = "0198f0f4-9b72-7000-8000-000000000051";

const mocks = vi.hoisted(() => ({
  bindParticipant: vi.fn(),
  suspendInstallation: vi.fn(),
}));

vi.mock("../agent/store", () => ({
  useConversation: (
    selector: (state: {
      suspendInstallation: typeof mocks.suspendInstallation;
    }) => unknown,
  ) => selector({ suspendInstallation: mocks.suspendInstallation }),
}));

vi.mock("../auth/auth-context", () => ({
  useAuth: () => ({ authenticated: true, user: { id: HUMAN_ID } }),
}));

beforeEach(() => {
  mocks.bindParticipant.mockReset();
  mocks.bindParticipant.mockResolvedValue(undefined);
  mocks.suspendInstallation.mockReset();
  mocks.suspendInstallation.mockReturnValue(true);
  useParticipantApps.setState({
    owner: {
      kind: "participant",
      participant: { kind: "human", humanId: HUMAN_ID },
    },
    status: "ready",
    catalog: [],
    installations: [installation("enabled")],
    mutation: null,
    errorCode: null,
    bindParticipant: mocks.bindParticipant,
  });
});

afterEach(cleanup);

describe("ParticipantAppBinding", () => {
  it("suspends an enabled Direct Chat epoch even while its route is unmounted", () => {
    render(
      <ParticipantAppBinding>
        <div>another app</div>
      </ParticipantAppBinding>,
    );

    act(() => {
      useParticipantApps.setState({
        installations: [installation("disabled")],
      });
      useParticipantApps.setState({
        installations: [installation("enabled")],
      });
    });

    expect(mocks.suspendInstallation).toHaveBeenCalledTimes(1);
    expect(mocks.suspendInstallation).toHaveBeenCalledWith(INSTALLATION_ID);
  });

  it("suspends the old epoch when uninstall and reinstall replace its ID", () => {
    render(
      <ParticipantAppBinding>
        <div>another app</div>
      </ParticipantAppBinding>,
    );

    act(() => {
      useParticipantApps.setState({ installations: [] });
      useParticipantApps.setState({
        installations: [
          {
            ...installation("enabled"),
            installationId: "0198f0f4-9b72-7000-8000-000000000052",
          },
        ],
      });
    });

    expect(mocks.suspendInstallation).toHaveBeenCalledTimes(1);
    expect(mocks.suspendInstallation).toHaveBeenCalledWith(INSTALLATION_ID);
  });

  it("does not suspend on a refresh state while enabled policy is retained", () => {
    render(
      <ParticipantAppBinding>
        <div>another app</div>
      </ParticipantAppBinding>,
    );

    act(() => {
      useParticipantApps.setState({ status: "loading" });
      useParticipantApps.setState({ status: "error" });
      useParticipantApps.setState({ status: "ready" });
    });

    expect(mocks.suspendInstallation).not.toHaveBeenCalled();
  });
});

function installation(state: "enabled" | "disabled"): AppInstallation {
  return {
    installationId: INSTALLATION_ID,
    owner: {
      kind: "participant",
      participant: { kind: "human", humanId: HUMAN_ID },
    },
    appId: "direct-chat",
    state,
    installedAt: 1,
    updatedAt: 2,
  };
}
