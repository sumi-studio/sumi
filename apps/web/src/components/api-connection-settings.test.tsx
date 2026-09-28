// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import {
  APIConnectionError,
  type APIConnectionsClient,
  type ConnectionsState,
} from "../lib/api-connections";
import { APIConnectionSettings } from "./api-connection-settings";

afterEach(cleanup);
function setup() {
  const state: ConnectionsState = {
    available: true,
    connections: [
      {
        id: "own",
        name: "My API",
        preset: "openai-chat",
        baseUrl: "https://example.com/v1",
        model: "model-a",
      },
    ],
    selection: { kind: "api", connectionId: "own" },
    activation: "next_start",
  };
  const client: APIConnectionsClient = {
    list: vi.fn().mockResolvedValue(state),
    save: vi.fn().mockResolvedValue(state.connections[0]),
    remove: vi.fn().mockResolvedValue(undefined),
    select: vi.fn().mockResolvedValue(undefined),
    beginChatGPTLogin: vi.fn(),
    chatGPTLogin: vi.fn(),
    cancelChatGPTLogin: vi.fn().mockResolvedValue(undefined),
    saveChatGPTSettings: vi.fn(),
  };
  return { client, state };
}
it("retains the saved secret on editing only the name, requires a new key for another endpoint", async () => {
  const { client } = setup();
  render(<APIConnectionSettings client={client} />);
  fireEvent.click(await screen.findByRole("button", { name: "編集" }));
  expect(screen.getByLabelText("APIキー")).toHaveValue("");
  fireEvent.change(screen.getByLabelText("名前"), {
    target: { value: "Renamed" },
  });
  fireEvent.click(screen.getByRole("button", { name: "保存する" }));
  await waitFor(() =>
    expect(client.save).toHaveBeenCalledWith(
      expect.not.objectContaining({ apiKey: expect.anything() }),
      "own",
      expect.any(AbortSignal),
    ),
  );
  fireEvent.click(await screen.findByRole("button", { name: "編集" }));
  fireEvent.change(screen.getByLabelText("接続先URL"), {
    target: { value: "https://other.example/v1" },
  });
  expect(screen.getByLabelText("APIキー")).toBeRequired();
});
it("does not select a newly saved connection silently and clears the entered key on closing", async () => {
  const { client } = setup();
  const view = render(<APIConnectionSettings client={client} />);
  fireEvent.click(await screen.findByRole("button", { name: "APIを追加" }));
  fireEvent.change(screen.getByLabelText("APIキー"), {
    target: { value: "fixture-secret" },
  });
  fireEvent.click(screen.getByRole("button", { name: "戻る" }));
  fireEvent.click(screen.getByRole("button", { name: "APIを追加" }));
  expect(screen.getByLabelText("APIキー")).toHaveValue("");
  expect(client.select).not.toHaveBeenCalled();
  view.unmount();
});
it("keeps edit values after failed save without reporting success", async () => {
  const { client } = setup();
  vi.mocked(client.save).mockRejectedValue(
    new Error("provider diagnostic must not be displayed"),
  );
  render(<APIConnectionSettings client={client} />);
  fireEvent.click(await screen.findByRole("button", { name: "編集" }));
  fireEvent.change(screen.getByLabelText("名前"), {
    target: { value: "My revised name" },
  });
  fireEvent.click(screen.getByRole("button", { name: "保存する" }));
  expect(await screen.findByRole("alert")).toBeVisible();
  expect(screen.getByLabelText("名前")).toHaveValue("My revised name");
  expect(screen.queryByText(/provider diagnostic/)).not.toBeInTheDocument();
  expect(screen.queryByRole("status")).not.toBeInTheDocument();
});
it("hides transport and timeout diagnostics", async () => {
  const { client } = setup();
  vi.mocked(client.save)
    .mockRejectedValueOnce(new TypeError("Failed to fetch 10.0.0.7"))
    .mockRejectedValueOnce(
      new DOMException("signal timed out at provider.internal", "TimeoutError"),
    );
  render(<APIConnectionSettings client={client} />);
  fireEvent.click(await screen.findByRole("button", { name: "編集" }));
  for (let i = 0; i < 2; i++) {
    fireEvent.click(screen.getByRole("button", { name: "保存する" }));
    expect(await screen.findByRole("alert")).toHaveTextContent(
      "接続を更新できませんでした。入力と接続状態を確認して、もう一度お試しください。",
    );
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "保存する" })).toBeEnabled(),
    );
  }
  expect(screen.queryByText(/10\.0\.0\.7|provider\.internal/)).toBeNull();
});
it("shows validation guidance, keeps the entry, then saves after correction", async () => {
  const { client } = setup();
  vi.mocked(client.save).mockRejectedValueOnce(
    new APIConnectionError(
      400,
      'extra header "Authorization" is reserved by the request itself',
    ),
  );
  render(<APIConnectionSettings client={client} />);
  fireEvent.click(await screen.findByRole("button", { name: "編集" }));
  fireEvent.change(screen.getByLabelText("APIキー"), {
    target: { value: "fixture-key" },
  });
  fireEvent.change(screen.getByLabelText("追加リクエストヘッダー（任意）"), {
    target: { value: "Authorization: Bearer x" },
  });
  fireEvent.click(screen.getByRole("button", { name: "保存する" }));
  expect(await screen.findByRole("alert")).toHaveTextContent(
    '入力内容を確認してください。 extra header "Authorization" is reserved by the request itself',
  );
  expect(screen.queryByRole("status")).not.toBeInTheDocument();
  expect(screen.getByLabelText("APIキー")).toHaveValue("fixture-key");
  expect(screen.getByLabelText("追加リクエストヘッダー（任意）")).toHaveValue(
    "Authorization: Bearer x",
  );
  fireEvent.change(screen.getByLabelText("追加リクエストヘッダー（任意）"), {
    target: { value: "X-Route: team-a" },
  });
  fireEvent.click(screen.getByRole("button", { name: "保存する" }));
  expect(await screen.findByRole("status")).toHaveTextContent(
    "接続を保存しました。以前の認証情報は次のリクエストから使われなくなり",
  );
  expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  expect(screen.queryByLabelText("APIキー")).not.toBeInTheDocument();
  expect(client.save).toHaveBeenLastCalledWith(
    expect.objectContaining({
      apiKey: "fixture-key",
      extraHeaders: { "X-Route": "team-a" },
    }),
    "own",
    expect.any(AbortSignal),
  );
});

const chatGPTConnection = {
  id: "sub",
  name: "ChatGPT",
  preset: "chatgpt-codex",
  baseUrl: "https://chatgpt.com/backend-api/codex",
  model: "gpt-6-astra",
  reasoningEffort: "medium",
};
const pendingLogin = {
  loginId: "login-1",
  status: "pending" as const,
  verificationUrl: "https://auth.openai.com/codex/device",
  userCode: "ABCD-1234",
  expiresAt: new Date(Date.now() + 15 * 60_000).toISOString(),
  intervalMs: 0,
};
it("connects ChatGPT with a device code the person enters on ChatGPT's page", async () => {
  const { client, state } = setup();
  const completed = {
    ...state,
    connections: [...state.connections, chatGPTConnection],
    selection: { kind: "api" as const, connectionId: "sub" },
    chatgpt: { available: true },
  };
  vi.mocked(client.list)
    .mockResolvedValueOnce({ ...state, chatgpt: { available: true } })
    .mockResolvedValue(completed);
  vi.mocked(client.beginChatGPTLogin).mockResolvedValue(pendingLogin);
  vi.mocked(client.chatGPTLogin)
    .mockResolvedValueOnce(pendingLogin)
    .mockResolvedValueOnce({
      ...pendingLogin,
      status: "completed",
      verificationUrl: undefined,
      userCode: undefined,
      connection: chatGPTConnection,
    });
  render(<APIConnectionSettings client={client} />);
  fireEvent.click(await screen.findByRole("button", { name: "ChatGPTで接続" }));
  expect(await screen.findByLabelText("ログインコード")).toHaveTextContent(
    "ABCD-1234",
  );
  expect(
    screen.getByRole("link", { name: "ChatGPTのログインページを開く" }),
  ).toHaveAttribute("href", "https://auth.openai.com/codex/device");
  expect(client.beginChatGPTLogin).toHaveBeenCalledWith(
    undefined,
    expect.any(AbortSignal),
  );
  expect(
    await screen.findByText(
      /ChatGPTを接続し、この接続を使うように切り替えました/,
      undefined,
      { timeout: 4000 },
    ),
  ).toBeInTheDocument();
  expect(client.chatGPTLogin).toHaveBeenCalledTimes(2);
  expect(screen.queryByLabelText("ログインコード")).not.toBeInTheDocument();
  expect(
    screen.getByText("ChatGPTのサブスクリプション · gpt-6-astra"),
  ).toBeInTheDocument();
  expect(client.cancelChatGPTLogin).not.toHaveBeenCalled();
});
it("offers reconnecting an expired sign-in and cancels a login left open", async () => {
  const { client, state } = setup();
  vi.mocked(client.list).mockResolvedValue({
    ...state,
    connections: [{ ...chatGPTConnection, reconnectRequired: true }],
    selection: { kind: "api", connectionId: "sub" },
    chatgpt: { available: true },
  });
  vi.mocked(client.beginChatGPTLogin).mockResolvedValue({
    ...pendingLogin,
    intervalMs: 60_000,
  });
  render(<APIConnectionSettings client={client} />);
  expect(
    await screen.findByText(/ChatGPTへのログインが期限切れか取り消されました/),
  ).toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "ChatGPTに再接続" }));
  await screen.findByLabelText("ログインコード");
  expect(client.beginChatGPTLogin).toHaveBeenCalledWith(
    "sub",
    expect.any(AbortSignal),
  );
  fireEvent.click(screen.getByRole("button", { name: "キャンセル" }));
  await waitFor(() =>
    expect(client.cancelChatGPTLogin).toHaveBeenCalledWith(
      "login-1",
      expect.any(AbortSignal),
    ),
  );
  expect(screen.queryByLabelText("ログインコード")).not.toBeInTheDocument();
});
it("shows an expired code and a refused start without a stale code", async () => {
  const { client, state } = setup();
  vi.mocked(client.list).mockResolvedValue({
    ...state,
    chatgpt: { available: true },
  });
  vi.mocked(client.beginChatGPTLogin)
    .mockResolvedValueOnce({
      ...pendingLogin,
      status: "expired",
      verificationUrl: undefined,
      userCode: undefined,
    })
    .mockRejectedValueOnce(
      new APIConnectionError(502, undefined, "デバイスコードを開始できません"),
    );
  render(<APIConnectionSettings client={client} />);
  fireEvent.click(await screen.findByRole("button", { name: "ChatGPTで接続" }));
  expect(
    await screen.findByText(/コードの有効期限が切れました/),
  ).toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "もう一度はじめる" }));
  expect(
    await screen.findByText("デバイスコードを開始できません"),
  ).toBeInTheDocument();
  expect(screen.queryByLabelText("ログインコード")).not.toBeInTheDocument();
  expect(client.chatGPTLogin).not.toHaveBeenCalled();
});
it("edits a ChatGPT connection's model and effort without any API key field", async () => {
  const { client, state } = setup();
  vi.mocked(client.list).mockResolvedValue({
    ...state,
    connections: [chatGPTConnection],
    chatgpt: { available: true },
  });
  vi.mocked(client.saveChatGPTSettings).mockResolvedValue(chatGPTConnection);
  render(<APIConnectionSettings client={client} />);
  fireEvent.click(await screen.findByRole("button", { name: "編集" }));
  expect(screen.queryByLabelText("APIキー")).not.toBeInTheDocument();
  fireEvent.change(screen.getByLabelText("モデル"), {
    target: { value: "gpt-5.5" },
  });
  fireEvent.change(screen.getByLabelText("推論の強さ"), {
    target: { value: "high" },
  });
  fireEvent.click(screen.getByRole("button", { name: "保存する" }));
  await waitFor(() =>
    expect(client.saveChatGPTSettings).toHaveBeenCalledWith(
      "sub",
      { name: "ChatGPT", model: "gpt-5.5", reasoningEffort: "high" },
      expect.any(AbortSignal),
    ),
  );
  expect(client.save).not.toHaveBeenCalled();
});
it("leaves a pending sign-in open when the panel goes away, and continues it when reopened", async () => {
  const { client, state } = setup();
  vi.mocked(client.list).mockResolvedValue({
    ...state,
    chatgpt: { available: true },
  });
  vi.mocked(client.beginChatGPTLogin).mockResolvedValue({
    ...pendingLogin,
    intervalMs: 60_000,
  });
  const first = render(<APIConnectionSettings client={client} />);
  fireEvent.click(await screen.findByRole("button", { name: "ChatGPTで接続" }));
  expect(await screen.findByLabelText("ログインコード")).toHaveTextContent(
    "ABCD-1234",
  );
  expect(
    screen.getByText(/セキュリティ設定でデバイスコードによるログインを有効に/),
  ).toBeInTheDocument();
  // A reload, a discarded tab or a closed sheet: the panel unmounts.
  first.unmount();
  expect(client.cancelChatGPTLogin).not.toHaveBeenCalled();
  // Opening it again in the same browser session asks the server, which
  // returns the same pending login; the same code is shown.
  render(<APIConnectionSettings client={client} />);
  fireEvent.click(await screen.findByRole("button", { name: "ChatGPTで接続" }));
  expect(await screen.findByLabelText("ログインコード")).toHaveTextContent(
    "ABCD-1234",
  );
  expect(client.beginChatGPTLogin).toHaveBeenCalledTimes(2);
  expect(client.cancelChatGPTLogin).not.toHaveBeenCalled();
});
it("keeps a stored reasoning effort and offers the model's own efforts", async () => {
  const { client, state } = setup();
  vi.mocked(client.list).mockResolvedValue({
    ...state,
    connections: [{ ...chatGPTConnection, reasoningEffort: "max" }],
    chatgpt: { available: true },
  });
  vi.mocked(client.saveChatGPTSettings).mockResolvedValue(chatGPTConnection);
  render(<APIConnectionSettings client={client} />);
  fireEvent.click(await screen.findByRole("button", { name: "編集" }));
  const select = screen.getByLabelText("推論の強さ") as HTMLSelectElement;
  expect(select.value).toBe("max");
  const values = () => Array.from(select.options, (o) => o.value);
  expect(values()).toEqual(["low", "medium", "high", "xhigh", "max"]);
  // gpt-5.5 offers up to xhigh; the stored value stays selectable rather
  // than silently becoming another one.
  fireEvent.change(screen.getByLabelText("モデル"), {
    target: { value: "gpt-5.5" },
  });
  expect(values()).toEqual(["low", "medium", "high", "xhigh", "max"]);
  expect(select.value).toBe("max");
  fireEvent.change(screen.getByLabelText("モデル"), {
    target: { value: "gpt-6-astra" },
  });
  fireEvent.click(screen.getByRole("button", { name: "保存する" }));
  await waitFor(() =>
    expect(client.saveChatGPTSettings).toHaveBeenCalledWith(
      "sub",
      { name: "ChatGPT", model: "gpt-6-astra", reasoningEffort: "max" },
      expect.any(AbortSignal),
    ),
  );
});
it("does not offer ChatGPT sign-in when the server does not support it", async () => {
  const { client, state } = setup();
  vi.mocked(client.list).mockResolvedValue({
    ...state,
    chatgpt: { available: false, unavailableReason: "no" },
  });
  render(<APIConnectionSettings client={client} />);
  await screen.findByRole("button", { name: "APIを追加" });
  expect(
    screen.queryByRole("button", { name: "ChatGPTで接続" }),
  ).not.toBeInTheDocument();
});
