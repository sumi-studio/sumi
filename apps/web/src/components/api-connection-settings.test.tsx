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
  };
  return { client, state };
}
it("retains the saved secret on editing only the name, requires a new key for another endpoint", async () => {
  const { client } = setup();
  render(<APIConnectionSettings chatgptConnected client={client} />);
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
  const view = render(
    <APIConnectionSettings chatgptConnected={false} client={client} />,
  );
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
  render(<APIConnectionSettings chatgptConnected client={client} />);
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
  render(<APIConnectionSettings chatgptConnected client={client} />);
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
  render(<APIConnectionSettings chatgptConnected client={client} />);
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
