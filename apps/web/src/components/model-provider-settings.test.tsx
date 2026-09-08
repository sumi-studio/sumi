// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type {
  ChatGPTConnection,
  ChatGPTLogin,
  ModelConnectionsAPI,
} from "../lib/model-connections";
import { ModelProviderSettings } from "./model-provider-settings";

const disconnected: ChatGPTConnection = {
  connected: false,
  model: "",
  effort: "",
  activation: "next_start",
  reconnectRequired: false,
};
const connected: ChatGPTConnection = {
  connected: true,
  connectionId: "connection-1",
  accountId: "own-chatgpt-account",
  model: "gpt-6-astra",
  effort: "medium",
  reconnectRequired: false,
  activation: "next_start",
};
function login(): ChatGPTLogin {
  return {
    loginId: "login-1",
    userCode: "ABCD-EFGH",
    verificationUrl: "https://auth.openai.com/codex/device",
    expiresAt: new Date(Date.now() + 60_000).toISOString(),
    intervalMs: 5000,
    status: "pending",
  };
}
function mockAPI(initial = disconnected) {
  const api = {
    status: vi.fn<ModelConnectionsAPI["status"]>().mockResolvedValue(initial),
    startLogin: vi
      .fn<ModelConnectionsAPI["startLogin"]>()
      .mockResolvedValue(login()),
    loginStatus: vi
      .fn<ModelConnectionsAPI["loginStatus"]>()
      .mockResolvedValue(login()),
    cancelLogin: vi
      .fn<ModelConnectionsAPI["cancelLogin"]>()
      .mockResolvedValue(undefined),
    disconnect: vi
      .fn<ModelConnectionsAPI["disconnect"]>()
      .mockResolvedValue(undefined),
    selectModel: vi
      .fn<ModelConnectionsAPI["selectModel"]>()
      .mockResolvedValue(connected),
  };
  return api;
}
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

async function begin(api: ReturnType<typeof mockAPI>) {
  fireEvent.click(
    await screen.findByRole("button", { name: "ChatGPTを接続してAstraを使う" }),
  );
  await waitFor(() => expect(api.loginStatus).toHaveBeenCalled());
}

describe("ChatGPT model settings", () => {
  it("shows the actual device code/link and confirms next-start activation only after server completion", async () => {
    const api = mockAPI();
    let complete!: (value: ChatGPTLogin) => void;
    api.loginStatus.mockImplementation(
      () =>
        new Promise((resolve) => {
          complete = resolve;
        }),
    );
    render(<ModelProviderSettings open onOpenChange={vi.fn()} api={api} />);
    await begin(api);
    expect(screen.getByText("ABCD-EFGH")).toBeVisible();
    expect(
      screen.getByRole("link", { name: "ChatGPTでログイン" }),
    ).toHaveAttribute("href", "https://auth.openai.com/codex/device");
    expect(screen.queryByText("接続済み")).not.toBeInTheDocument();
    api.status.mockResolvedValue(connected);
    await act(async () => {
      complete({ ...login(), status: "completed", connection: connected });
    });
    expect(await screen.findByText("接続済み")).toBeVisible();
    expect(screen.getByRole("status")).toHaveTextContent(
      "一区切りついてからAstraに切り替わります",
    );
    expect(screen.getByLabelText("推論の深さ")).toHaveValue("medium");
    expect(api.selectModel).not.toHaveBeenCalled();
  });

  it("closing stops browser polling without cancelling server login, and reopening retrieves its completion", async () => {
    const api = mockAPI();
    api.loginStatus.mockImplementationOnce(() => new Promise(() => {}));
    const view = render(
      <ModelProviderSettings open onOpenChange={vi.fn()} api={api} />,
    );
    await begin(api);
    const signal = api.loginStatus.mock.calls[0][1];
    view.rerender(
      <ModelProviderSettings open={false} onOpenChange={vi.fn()} api={api} />,
    );
    expect(signal.aborted).toBe(true);
    expect(api.cancelLogin).not.toHaveBeenCalled();
    api.status.mockResolvedValue(connected);
    api.loginStatus.mockResolvedValue({
      ...login(),
      status: "completed",
      connection: connected,
    });
    view.rerender(
      <ModelProviderSettings open onOpenChange={vi.fn()} api={api} />,
    );
    expect(await screen.findByText("接続済み")).toBeVisible();
    expect(api.loginStatus).toHaveBeenCalledTimes(2);
  });

  it.each([
    disconnected,
    { ...connected, connectionId: "new-connection", accountId: "new-account" },
  ])("a historical completion cannot overwrite the current connection after reopening", async (current) => {
    const api = mockAPI();
    api.loginStatus.mockImplementationOnce(() => new Promise(() => {}));
    const view = render(
      <ModelProviderSettings open onOpenChange={vi.fn()} api={api} />,
    );
    await begin(api);
    view.rerender(
      <ModelProviderSettings open={false} onOpenChange={vi.fn()} api={api} />,
    );
    api.status.mockResolvedValue(current);
    api.loginStatus.mockResolvedValue({
      ...login(),
      status: "completed",
      connection: connected,
    });
    view.rerender(
      <ModelProviderSettings open onOpenChange={vi.fn()} api={api} />,
    );
    await waitFor(() => expect(api.loginStatus).toHaveBeenCalledTimes(2));
    await waitFor(() =>
      expect(screen.queryByText("ABCD-EFGH")).not.toBeInTheDocument(),
    );
    expect(screen.queryByText("own-chatgpt-account")).not.toBeInTheDocument();
    expect(
      screen.queryByText(
        "ChatGPTに接続しました。作業中の場合は、一区切りついてからAstraに切り替わります。",
      ),
    ).not.toBeInTheDocument();
    if (current.connected)
      expect(screen.getByText("new-account")).toBeVisible();
    else
      expect(
        screen.getByRole("button", { name: "ChatGPTを接続してAstraを使う" }),
      ).toBeVisible();
  });

  it("owner remount ignores the previous owner's delayed response", async () => {
    const old = mockAPI();
    let resolveOld!: (value: ChatGPTConnection) => void;
    old.status.mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveOld = resolve;
        }),
    );
    const current = mockAPI({
      ...connected,
      accountId: "current-owner-account",
    });
    const view = render(
      <ModelProviderSettings
        key="old-owner"
        open
        onOpenChange={vi.fn()}
        api={old}
      />,
    );
    await waitFor(() => expect(old.status).toHaveBeenCalled());
    view.rerender(
      <ModelProviderSettings
        key="new-owner"
        open
        onOpenChange={vi.fn()}
        api={current}
      />,
    );
    expect(await screen.findByText("current-owner-account")).toBeVisible();
    await act(async () => {
      resolveOld({ ...connected, accountId: "old-owner-account" });
    });
    expect(screen.queryByText("old-owner-account")).not.toBeInTheDocument();
    expect(old.status.mock.calls[0][0].aborted).toBe(true);
  });

  it("a rejected model update stays an error and never reports that it applied", async () => {
    const api = mockAPI(connected);
    api.selectModel.mockRejectedValue(
      new Error("接続が更新されました。再確認してください。"),
    );
    render(<ModelProviderSettings open onOpenChange={vi.fn()} api={api} />);
    fireEvent.change(await screen.findByLabelText("推論の深さ"), {
      target: { value: "high" },
    });
    fireEvent.click(screen.getByRole("button", { name: "モデル設定を保存" }));
    expect(await screen.findByRole("alert")).toHaveTextContent(
      "接続が更新されました",
    );
    expect(api.selectModel).toHaveBeenCalledWith(
      "connection-1",
      "high",
      expect.any(AbortSignal),
    );
    expect(
      screen.queryByText(
        "保存しました。作業中の場合は、一区切りついてから切り替わります。",
      ),
    ).not.toBeInTheDocument();
  });

  it("disconnect requires its explicit action and accurately describes idle-time fallback", async () => {
    const api = mockAPI(connected);
    api.disconnect.mockImplementation(async () => {
      api.status.mockResolvedValue(disconnected);
    });
    render(<ModelProviderSettings open onOpenChange={vi.fn()} api={api} />);
    fireEvent.click(await screen.findByRole("button", { name: "接続を解除" }));
    expect(api.disconnect).not.toHaveBeenCalled();
    expect(
      screen.getByText(
        "接続を解除すると、標準のモデル設定に戻ります。作業中なら完了を待って切り替えます。",
      ),
    ).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "接続を解除する" }));
    await waitFor(() => expect(api.disconnect).toHaveBeenCalledTimes(1));
    expect(await screen.findByRole("status")).toHaveTextContent(
      "一区切りついてから標準のモデル設定に戻ります",
    );
    expect(screen.queryByText("接続済み")).not.toBeInTheDocument();
  });
});
