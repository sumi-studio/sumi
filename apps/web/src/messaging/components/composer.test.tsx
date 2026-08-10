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
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type {
  Attachment,
  ChannelSummary,
  MemberProfile,
  ParticipantRef,
} from "../model";
import { participantKey } from "../model";
import { useMessaging } from "../store";
import { Composer } from "./composer";
import { DraftAttachmentsProvider } from "./composer-attachments";

const mocks = vi.hoisted(() => ({
  send: vi.fn(),
  sendTyping: vi.fn(),
  uploadAttachment: vi.fn(),
  updateAttachment: vi.fn(),
}));

const human: ParticipantRef = { kind: "human", humanId: "h1" };
const agent: ParticipantRef = {
  kind: "personality_agent",
  personalityAgentId: "a1",
};
const humanKey = participantKey(human);

const members: MemberProfile[] = [
  { participant: human, displayName: "余白", tagline: "創業・デザイン" },
  { participant: agent, displayName: "墨", tagline: "秘書" },
];

const channel: ChannelSummary = {
  channelId: "c1",
  workspaceId: "w1",
  name: "general",
  topic: "",
  visibility: "public",
  voice: false,
};

const secondChannel: ChannelSummary = {
  channelId: "c2",
  workspaceId: "w1",
  name: "random",
  topic: "",
  visibility: "public",
  voice: false,
};

function uploadedAttachment(attachmentId: string, file: File): Attachment {
  return {
    attachmentId,
    filename: file.name,
    mime: file.type,
    size: file.size,
    url: `/messaging/attachments/${attachmentId}`,
    spoiler: false,
    alt: "",
  };
}

function composer() {
  return screen.getByRole<HTMLTextAreaElement>("textbox", {
    name: "#general へメッセージ",
  });
}

function openPlusMenu() {
  fireEvent.click(screen.getByRole("button", { name: "作成メニューを開く" }));
}

function DraftOwner({ children }: { children: ReactNode }) {
  return (
    <DraftAttachmentsProvider
      upload={mocks.uploadAttachment}
      update={mocks.updateAttachment}
    >
      {children}
    </DraftAttachmentsProvider>
  );
}

function renderComposer(ui: ReactNode = <Composer />) {
  return render(<DraftOwner>{ui}</DraftOwner>);
}

beforeEach(() => {
  useMessaging.setState({
    ready: true,
    self: human,
    selfKey: humanKey,
    activePlaceKey: `channel:${channel.channelId}`,
    channels: [channel],
    dms: [],
    draftByPlace: {},
    messagesByPlace: {},
    editingMessageId: null,
    replyTargetId: null,
    membersByKey: Object.fromEntries(
      members.map((member) => [participantKey(member.participant), member]),
    ),
    send: mocks.send,
    sendTyping: mocks.sendTyping,
    uploadAttachment: mocks.uploadAttachment,
    updateAttachment: mocks.updateAttachment,
  });
});

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
  vi.unstubAllGlobals();
});

describe("Composer 送信ボタン", () => {
  it("空入力では無効で、文字を入れるとクリックだけで送れる", () => {
    renderComposer();
    const button = screen.getByRole("button", { name: "送信" });
    expect(button).toBeDisabled();

    fireEvent.change(composer(), { target: { value: "こんにちは" } });

    expect(button).toBeEnabled();
    fireEvent.click(button);
    expect(mocks.send).toHaveBeenCalledWith("こんにちは", "normal", []);
  });

  it("空白だけの入力では送れない", () => {
    renderComposer();
    fireEvent.change(composer(), { target: { value: "   " } });

    expect(screen.getByRole("button", { name: "送信" })).toBeDisabled();
    fireEvent.click(screen.getByRole("button", { name: "送信" }));
    expect(mocks.send).not.toHaveBeenCalled();
  });

  it("Enterで送信する説明は残す", () => {
    renderComposer();
    expect(screen.getByText(/Enterで送信/)).toBeInTheDocument();
  });

  it("placeごとの添付だけを送り、送信後も他placeの下書きを残す", async () => {
    const fileA = new File(["A"], "a.txt", { type: "text/plain" });
    const fileB = new File(["B"], "b.txt", { type: "text/plain" });
    let finishA: ((attachment: Attachment) => void) | undefined;
    let finishB: ((attachment: Attachment) => void) | undefined;
    mocks.uploadAttachment
      .mockImplementationOnce(
        () =>
          new Promise<Attachment>((resolve) => {
            finishA = resolve;
          }),
      )
      .mockImplementationOnce(
        () =>
          new Promise<Attachment>((resolve) => {
            finishB = resolve;
          }),
      );
    act(() => useMessaging.setState({ channels: [channel, secondChannel] }));
    const { container } = renderComposer();
    const fileInput =
      container.querySelector<HTMLInputElement>('input[type="file"]');
    if (!fileInput) throw new Error("file input must exist");

    fireEvent.change(fileInput, { target: { files: [fileA] } });
    expect(screen.getByText("a.txt")).toBeInTheDocument();
    expect(screen.getByText("アップロード中…")).toBeInTheDocument();

    act(() => useMessaging.setState({ activePlaceKey: "channel:c2" }));
    expect(
      screen.getByRole("textbox", { name: "#random へメッセージ" }),
    ).toBeInTheDocument();
    expect(screen.queryByText("a.txt")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "送信" })).toBeDisabled();

    fireEvent.change(fileInput, { target: { files: [fileB] } });
    await act(async () => {
      finishB?.(uploadedAttachment("attachment-b", fileB));
      await Promise.resolve();
    });
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "送信" })).toBeEnabled(),
    );
    fireEvent.click(screen.getByRole("button", { name: "送信" }));
    expect(mocks.send).toHaveBeenLastCalledWith("", "normal", [
      uploadedAttachment("attachment-b", fileB),
    ]);
    expect(screen.queryByText("b.txt")).not.toBeInTheDocument();

    // Bを送ってclearしても、Aで始めたuploadと下書きはそのまま残る。
    act(() => useMessaging.setState({ activePlaceKey: "channel:c1" }));
    expect(screen.getByText("a.txt")).toBeInTheDocument();
    expect(screen.getByText("アップロード中…")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "送信" })).toBeDisabled();

    await act(async () => {
      finishA?.(uploadedAttachment("attachment-a", fileA));
      await Promise.resolve();
    });
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "送信" })).toBeEnabled(),
    );
    fireEvent.click(screen.getByRole("button", { name: "送信" }));
    expect(mocks.send).toHaveBeenLastCalledWith("", "normal", [
      uploadedAttachment("attachment-a", fileA),
    ]);
  });

  it("sibling routeでComposerが外れてもsession ownerが下書きとuploadを保つ", async () => {
    const file = new File(["image"], "route.png", { type: "image/png" });
    let finishUpload: ((attachment: Attachment) => void) | undefined;
    mocks.uploadAttachment.mockImplementationOnce(
      () =>
        new Promise<Attachment>((resolve) => {
          finishUpload = resolve;
        }),
    );
    const createObjectURL = vi.fn(() => "blob:route.png");
    const revokeObjectURL = vi.fn<(url: string) => void>();
    vi.stubGlobal("URL", { createObjectURL, revokeObjectURL });
    const route = (showComposer: boolean) => (
      <DraftOwner>
        {showComposer ? <Composer /> : <div>別のsibling route</div>}
      </DraftOwner>
    );
    const { container, rerender, unmount } = render(route(true));
    const fileInput =
      container.querySelector<HTMLInputElement>('input[type="file"]');
    if (!fileInput) throw new Error("file input must exist");

    fireEvent.change(fileInput, { target: { files: [file] } });
    expect(screen.getByText("route.png")).toBeInTheDocument();

    // 実際のsibling route遷移と同じく、Composerだけをownerの下から外す。
    rerender(route(false));
    expect(screen.queryByText("route.png")).not.toBeInTheDocument();
    expect(revokeObjectURL).not.toHaveBeenCalled();

    await act(async () => {
      finishUpload?.(uploadedAttachment("attachment-route", file));
      await Promise.resolve();
    });
    rerender(route(true));

    expect(screen.getByText("route.png")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "送信" })).toBeEnabled();
    expect(revokeObjectURL).not.toHaveBeenCalled();

    // File/object URLの寿命はComposerではなくsession ownerまで。
    unmount();
    expect(revokeObjectURL).toHaveBeenCalledTimes(1);
    expect(revokeObjectURL).toHaveBeenCalledWith("blob:route.png");
  });

  it("添付PATCH失敗後に本文を送っても古いattachment idを渡さない", async () => {
    const file = new File(["draft"], "patch.txt", { type: "text/plain" });
    mocks.uploadAttachment.mockResolvedValueOnce(
      uploadedAttachment("attachment-before-patch", file),
    );
    mocks.updateAttachment.mockRejectedValueOnce(new Error("patch failed"));
    const { container } = renderComposer();
    const fileInput =
      container.querySelector<HTMLInputElement>('input[type="file"]');
    if (!fileInput) throw new Error("file input must exist");

    fireEvent.change(fileInput, { target: { files: [file] } });
    await waitFor(() =>
      expect(
        screen.getByRole("button", { name: "patch.txt のネタバレをマーク" }),
      ).toBeEnabled(),
    );
    fireEvent.click(
      screen.getByRole("button", { name: "patch.txt のネタバレをマーク" }),
    );
    await waitFor(() => expect(screen.getByText("失敗")).toBeInTheDocument());
    expect(screen.getByRole("button", { name: "送信" })).toBeDisabled();

    fireEvent.change(composer(), { target: { value: "本文だけ送る" } });
    fireEvent.click(screen.getByRole("button", { name: "送信" }));

    expect(mocks.send).toHaveBeenLastCalledWith("本文だけ送る", "normal", []);
  });
});

describe("Composer のフォーカス", () => {
  it("インライン編集の終了時に入力欄へ戻す", () => {
    renderComposer(
      <>
        <button type="button">編集中の入力欄</button>
        <Composer />
      </>,
    );
    const editingInput = screen.getByRole("button", {
      name: "編集中の入力欄",
    });
    editingInput.focus();

    act(() => useMessaging.setState({ editingMessageId: "m1" }));
    expect(editingInput).toHaveFocus();

    act(() => useMessaging.setState({ editingMessageId: null }));
    expect(composer()).toHaveFocus();
  });
});

describe("Composer ＋メニュー", () => {
  it("添付は＋メニューの中にあり、選ぶとファイル選択が開く", () => {
    const click = vi.spyOn(HTMLInputElement.prototype, "click");
    renderComposer();
    // クリップ単独のボタンは無くなり、入口は＋に集約されている。
    expect(
      screen.queryByRole("button", { name: "ファイルを添付" }),
    ).not.toBeInTheDocument();

    openPlusMenu();
    fireEvent.click(screen.getByRole("button", { name: /ファイルを添付/ }));

    expect(click).toHaveBeenCalled();
    click.mockRestore();
  });

  it("メンションを選ぶと @ が入り、候補から選んで入力欄に挿入できる", async () => {
    renderComposer();
    fireEvent.change(composer(), { target: { value: "おはよう" } });
    // カーソルは末尾にある想定（changeの後の既定位置）。
    composer().setSelectionRange(4, 4);

    openPlusMenu();
    fireEvent.click(screen.getByRole("button", { name: /メンション/ }));

    await vi.waitFor(() => {
      expect(composer()).toHaveValue("おはよう @");
    });
    // 自分は候補に出さない。
    expect(
      screen.queryByRole("button", { name: /余白/ }),
    ).not.toBeInTheDocument();

    fireEvent.mouseDown(screen.getByRole("button", { name: /墨/ }));

    expect(composer()).toHaveValue("おはよう @墨 ");
  });

  it("未実装の作成導線は席だけ用意して選べない状態で置く", () => {
    renderComposer();
    openPlusMenu();

    expect(
      screen.getByRole("button", { name: /スレッドを作成/ }),
    ).toBeDisabled();
    // 投票は実装済み: 品書きから選べる。
    expect(screen.getByRole("button", { name: /投票を作成/ })).toBeEnabled();
  });
});
