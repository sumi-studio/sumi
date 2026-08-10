// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  act,
  cleanup,
  fireEvent,
  render,
  renderHook,
  screen,
  waitFor,
} from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  type Attachment,
  type AttachmentDraftPatch,
  MAX_ATTACHMENT_BYTES,
} from "../model";
import {
  ComposerAttachments,
  DRAFT_ATTACHMENT_RENEW_BATCH_SIZE,
  DRAFT_ATTACHMENT_RENEW_INTERVAL_MS,
  type DraftAttachment,
  DraftAttachmentsProvider,
  fileExtension,
  useDraftAttachments,
} from "./composer-attachments";

function draft(overrides: Partial<DraftAttachment> = {}): DraftAttachment {
  return {
    localId: "draft-1",
    filename: "avatar.jpg",
    size: 68 * 1024,
    mime: "image/jpeg",
    status: "ready",
    attachment: {
      attachmentId: "attachment-1",
      filename: "avatar.jpg",
      mime: "image/jpeg",
      size: 68 * 1024,
      url: "/messaging/attachments/attachment-1",
      spoiler: false,
      alt: "",
    },
    ...overrides,
  };
}

function ownerWrapper(
  upload: (file: File) => Promise<Attachment>,
  update: (
    attachmentId: string,
    patch: AttachmentDraftPatch,
  ) => Promise<Attachment>,
  renewReadyAttachments?: (attachmentIds: string[]) => Promise<void>,
) {
  return function OwnerWrapper({ children }: { children: ReactNode }) {
    return (
      <DraftAttachmentsProvider
        upload={upload}
        update={update}
        renewReadyAttachments={renewReadyAttachments}
      >
        {children}
      </DraftAttachmentsProvider>
    );
  };
}

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("ComposerAttachments", () => {
  it("画像は中身の見えるサムネイルにする", () => {
    render(
      <ComposerAttachments
        items={[draft({ previewUrl: "blob:preview-1" })]}
        onRemove={() => {}}
      />,
    );

    expect(screen.getByAltText("avatar.jpg のプレビュー")).toHaveAttribute(
      "src",
      "blob:preview-1",
    );
    expect(screen.getByText("avatar.jpg")).toBeInTheDocument();
    expect(screen.getByText("68 KB")).toBeInTheDocument();
  });

  it("画像以外は形式アイコンと拡張子を出す", () => {
    render(
      <ComposerAttachments
        items={[
          draft({
            filename: "契約.pdf",
            mime: "application/pdf",
            size: 3 * 1024 * 1024,
          }),
        ]}
        onRemove={() => {}}
      />,
    );

    expect(screen.queryByRole("img")).toBeNull();
    expect(screen.getByText("PDF")).toBeInTheDocument();
    expect(screen.getByText("契約.pdf")).toBeInTheDocument();
    expect(screen.getByText("3.0 MB")).toBeInTheDocument();
  });

  it("削除は何をするボタンか名前とツールチップで分かる", () => {
    const onRemove = vi.fn();
    render(<ComposerAttachments items={[draft()]} onRemove={onRemove} />);

    const remove = screen.getByRole("button", {
      name: "avatar.jpg の添付を取り消す",
    });
    expect(remove).toHaveAttribute("title", "添付ファイルを削除");
    // ホバーで前に出るだけで、常に辿れる（キーボードから消せなくならない）。
    expect(remove.className).toContain("group-hover:opacity-100");
    expect(remove.className).toContain("hover:bg-rose-500/15");

    fireEvent.click(remove);
    expect(onRemove).toHaveBeenCalledWith("draft-1");
  });

  it("サムネイルのホバー操作からネタバレを切り替えられる", () => {
    const onToggleSpoiler = vi.fn();
    render(
      <ComposerAttachments
        items={[draft({ previewUrl: "blob:preview-1" })]}
        onRemove={() => {}}
        onToggleSpoiler={onToggleSpoiler}
      />,
    );

    fireEvent.click(
      screen.getByRole("button", { name: "avatar.jpg のネタバレをマーク" }),
    );
    expect(onToggleSpoiler).toHaveBeenCalledWith("draft-1");
  });

  it("ネタバレ済みの下書きはぼかして「ネタバレ」と示す", () => {
    render(
      <ComposerAttachments
        items={[
          draft({
            previewUrl: "blob:preview-1",
            attachment: {
              attachmentId: "attachment-1",
              filename: "avatar.jpg",
              mime: "image/jpeg",
              size: 68 * 1024,
              url: "/messaging/attachments/attachment-1",
              spoiler: true,
              alt: "",
            },
          }),
        ]}
        onRemove={() => {}}
        onToggleSpoiler={() => {}}
      />,
    );

    expect(screen.getByAltText("avatar.jpg のプレビュー").className).toContain(
      "blur-md",
    );
    expect(screen.getByText("ネタバレ")).toBeInTheDocument();
  });

  it("宣言の更新中はネタバレ切替と編集を重ねられない", () => {
    render(
      <ComposerAttachments
        items={[draft({ status: "uploading" })]}
        onRemove={() => {}}
        onToggleSpoiler={() => {}}
        onEdit={() => {}}
      />,
    );

    expect(
      screen.getByRole("button", { name: "avatar.jpg のネタバレをマーク" }),
    ).toBeDisabled();
    expect(
      screen.getByRole("button", { name: "avatar.jpg を編集" }),
    ).toBeDisabled();
  });

  it("編集ボタンから添付ファイルの編集を開く", () => {
    render(
      <ComposerAttachments
        items={[draft()]}
        onRemove={() => {}}
        onEdit={() => {}}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "avatar.jpg を編集" }));

    const modal = screen.getByRole("dialog", { name: "添付ファイルを編集" });
    expect(modal).toBeInTheDocument();
    expect(screen.getByLabelText(/スポイラーとしてマーク/)).toBeInTheDocument();
    expect(screen.getByLabelText("概要")).toHaveAttribute("maxLength", "1000");
  });

  it("大きすぎる添付は失敗として見せる", () => {
    render(
      <ComposerAttachments
        items={[draft({ status: "failed", size: 30 * 1024 * 1024 })]}
        onRemove={() => {}}
      />,
    );

    expect(screen.getByText("大きすぎます")).toBeInTheDocument();
  });

  it("添付が無ければ何も描かない", () => {
    const { container } = render(
      <ComposerAttachments items={[]} onRemove={() => {}} />,
    );
    expect(container).toBeEmptyDOMElement();
  });
});

describe("useDraftAttachments", () => {
  it("ready idだけをdedupe・分割してfocus/onlineでrenewする", async () => {
    const renewReadyAttachments = vi.fn(async (_ids: string[]) => {});
    const upload = vi.fn((file: File): Promise<Attachment> => {
      if (file.name === "pending.txt") {
        return new Promise<Attachment>(() => {});
      }
      const id =
        file.name === "duplicate.txt"
          ? "attachment-00"
          : `attachment-${file.name.replace(".txt", "")}`;
      return Promise.resolve({
        attachmentId: id,
        filename: file.name,
        mime: file.type,
        size: file.size,
        url: `/messaging/attachments/${id}`,
        spoiler: false,
        alt: "",
      });
    });
    const update = async (): Promise<Attachment> => {
      throw new Error("not used");
    };
    const { result, rerender } = renderHook(
      ({ placeKey }) => useDraftAttachments({ placeKey }),
      {
        initialProps: { placeKey: "channel:a" as string | null },
        wrapper: ownerWrapper(upload, update, renewReadyAttachments),
      },
    );
    const firstBatch = Array.from(
      { length: DRAFT_ATTACHMENT_RENEW_BATCH_SIZE },
      (_, index) =>
        new File([String(index)], `${String(index).padStart(2, "0")}.txt`, {
          type: "text/plain",
        }),
    );

    act(() => result.current.addFiles(firstBatch));
    await waitFor(() =>
      expect(
        result.current.items.every((entry) => entry.status === "ready"),
      ).toBe(true),
    );

    rerender({ placeKey: "channel:b" });
    const oversized = new File(["large"], "oversized.txt", {
      type: "text/plain",
    });
    Object.defineProperty(oversized, "size", {
      configurable: true,
      value: MAX_ATTACHMENT_BYTES + 1,
    });
    act(() => {
      result.current.addFiles([
        new File(["duplicate"], "duplicate.txt", { type: "text/plain" }),
        new File(["extra"], "extra.txt", { type: "text/plain" }),
        oversized,
        new File(["pending"], "pending.txt", { type: "text/plain" }),
      ]);
    });
    await waitFor(() =>
      expect(result.current.items.map((entry) => entry.status)).toEqual([
        "ready",
        "ready",
        "failed",
        "uploading",
      ]),
    );
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    renewReadyAttachments.mockClear();
    await act(async () => {
      window.dispatchEvent(new Event("focus"));
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(renewReadyAttachments).toHaveBeenCalledTimes(2);
    const focusedIds = renewReadyAttachments.mock.calls.flatMap(([ids]) => ids);
    expect(
      renewReadyAttachments.mock.calls.every(
        ([ids]) => ids.length <= DRAFT_ATTACHMENT_RENEW_BATCH_SIZE,
      ),
    ).toBe(true);
    expect(focusedIds).toHaveLength(DRAFT_ATTACHMENT_RENEW_BATCH_SIZE + 1);
    expect(new Set(focusedIds).size).toBe(focusedIds.length);
    expect(focusedIds).not.toContain("attachment-oversized");
    expect(focusedIds).not.toContain("attachment-pending");

    renewReadyAttachments.mockClear();
    await act(async () => {
      window.dispatchEvent(new Event("online"));
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(renewReadyAttachments).toHaveBeenCalledTimes(2);
  });

  it("online中はheartbeatでもready idをrenewする", async () => {
    vi.useFakeTimers();
    const attachment = draft().attachment;
    if (!attachment) throw new Error("test draft must carry an attachment");
    const renewReadyAttachments = vi.fn(async (_ids: string[]) => {});
    const upload = vi.fn(async (): Promise<Attachment> => attachment);
    const update = vi.fn(async (): Promise<Attachment> => attachment);
    const { result } = renderHook(
      () => useDraftAttachments({ placeKey: "channel:a" }),
      {
        wrapper: ownerWrapper(upload, update, renewReadyAttachments),
      },
    );

    await act(async () => {
      result.current.addFiles([
        new File(["draft"], "avatar.jpg", { type: "text/plain" }),
      ]);
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(result.current.items[0]?.status).toBe("ready");
    renewReadyAttachments.mockClear();

    await act(async () => {
      vi.advanceTimersByTime(DRAFT_ATTACHMENT_RENEW_INTERVAL_MS);
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(renewReadyAttachments).toHaveBeenCalledWith(["attachment-1"]);
  });

  it("offline中は止め、pageshowとvisible復帰でrenewする", async () => {
    const attachment = draft().attachment;
    if (!attachment) throw new Error("test draft must carry an attachment");
    const renewReadyAttachments = vi.fn(async (_ids: string[]) => {});
    const upload = vi.fn(async (): Promise<Attachment> => attachment);
    const update = vi.fn(async (): Promise<Attachment> => attachment);
    const online = vi.spyOn(navigator, "onLine", "get");
    const visibility = vi.spyOn(document, "visibilityState", "get");
    online.mockReturnValue(true);
    visibility.mockReturnValue("visible");
    const { result } = renderHook(
      () => useDraftAttachments({ placeKey: "channel:a" }),
      {
        wrapper: ownerWrapper(upload, update, renewReadyAttachments),
      },
    );

    await act(async () => {
      result.current.addFiles([
        new File(["draft"], "avatar.jpg", { type: "text/plain" }),
      ]);
      await Promise.resolve();
      await Promise.resolve();
    });
    renewReadyAttachments.mockClear();

    online.mockReturnValue(false);
    await act(async () => {
      window.dispatchEvent(new Event("pageshow"));
      window.dispatchEvent(new Event("focus"));
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(renewReadyAttachments).not.toHaveBeenCalled();

    online.mockReturnValue(true);
    await act(async () => {
      window.dispatchEvent(new Event("pageshow"));
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(renewReadyAttachments).toHaveBeenCalledWith(["attachment-1"]);

    renewReadyAttachments.mockClear();
    visibility.mockReturnValue("hidden");
    await act(async () => {
      document.dispatchEvent(new Event("visibilitychange"));
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(renewReadyAttachments).not.toHaveBeenCalled();

    visibility.mockReturnValue("visible");
    await act(async () => {
      document.dispatchEvent(new Event("visibilitychange"));
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(renewReadyAttachments).toHaveBeenCalledWith(["attachment-1"]);
  });

  it("uploadの成否は開始したplaceへ戻し、別placeをuploadingにしない", async () => {
    const pending = new Map<
      string,
      {
        resolve: (attachment: Attachment) => void;
        reject: (reason?: unknown) => void;
      }
    >();
    const upload = vi.fn(
      (file: File) =>
        new Promise<Attachment>((resolve, reject) => {
          pending.set(file.name, { resolve, reject });
        }),
    );
    const update = async (): Promise<Attachment> => {
      throw new Error("not used");
    };
    const { result, rerender } = renderHook(
      ({ placeKey }) => useDraftAttachments({ placeKey }),
      {
        initialProps: { placeKey: "channel:a" as string | null },
        wrapper: ownerWrapper(upload, update),
      },
    );

    act(() => {
      result.current.addFiles([
        new File(["ok"], "ok.txt", { type: "text/plain" }),
        new File(["ng"], "ng.txt", { type: "text/plain" }),
      ]);
    });
    expect(result.current.uploading).toBe(true);

    rerender({ placeKey: "channel:b" });
    expect(result.current.items).toEqual([]);
    expect(result.current.uploading).toBe(false);
    expect(result.current.ready).toEqual([]);

    await act(async () => {
      pending.get("ok.txt")?.resolve({
        attachmentId: "attachment-a",
        filename: "ok.txt",
        mime: "text/plain",
        size: 2,
        url: "/messaging/attachments/attachment-a",
        spoiler: false,
        alt: "",
      });
      pending.get("ng.txt")?.reject(new Error("upload failed"));
      await Promise.resolve();
    });

    // Aの完了・失敗がBの表示や送信用idへ混ざらない。
    expect(result.current.items).toEqual([]);
    expect(result.current.ready).toEqual([]);

    rerender({ placeKey: "channel:a" });
    expect(result.current.items.map((entry) => entry.status)).toEqual([
      "ready",
      "failed",
    ]);
    expect(result.current.ready.map((entry) => entry.attachmentId)).toEqual([
      "attachment-a",
    ]);
  });

  it("place切替後に呼ばれた古いreplaceとremoveも元のplaceだけを変える", async () => {
    const upload = vi.fn(
      async (file: File): Promise<Attachment> => ({
        attachmentId: `attachment-${file.name}`,
        filename: file.name,
        mime: file.type,
        size: file.size,
        url: `/messaging/attachments/${file.name}`,
        spoiler: false,
        alt: "",
      }),
    );
    const update = async (): Promise<Attachment> => {
      throw new Error("not used");
    };
    const { result, rerender } = renderHook(
      ({ placeKey }) => useDraftAttachments({ placeKey }),
      {
        initialProps: { placeKey: "channel:a" as string | null },
        wrapper: ownerWrapper(upload, update),
      },
    );

    act(() => {
      result.current.addFiles([
        new File(["keep"], "keep.txt", { type: "text/plain" }),
        new File(["remove"], "remove.txt", { type: "text/plain" }),
      ]);
    });
    await waitFor(() =>
      expect(
        result.current.items.every((entry) => entry.status === "ready"),
      ).toBe(true),
    );

    const [kept, removed] = result.current.items;
    const replaceInA = result.current.replace;
    const removeInA = result.current.remove;
    rerender({ placeKey: "channel:b" });

    act(() => {
      replaceInA(kept.localId, { filename: "Aだけで改名.txt" });
      removeInA(removed.localId);
    });
    expect(result.current.items).toEqual([]);

    rerender({ placeKey: "channel:a" });
    expect(result.current.items).toHaveLength(1);
    expect(result.current.items[0].filename).toBe("Aだけで改名.txt");
  });

  it("clearは現在のplaceだけを解放し、残りはunmountで解放する", async () => {
    const createObjectURL = vi
      .fn<(file: File) => string>()
      .mockImplementation((file) => `blob:${file.name}`);
    const revokeObjectURL = vi.fn<(url: string) => void>();
    vi.stubGlobal("URL", { createObjectURL, revokeObjectURL });
    const upload = vi.fn(
      async (file: File): Promise<Attachment> => ({
        attachmentId: `attachment-${file.name}`,
        filename: file.name,
        mime: file.type,
        size: file.size,
        url: `/messaging/attachments/${file.name}`,
        spoiler: false,
        alt: "",
      }),
    );
    const update = async (): Promise<Attachment> => {
      throw new Error("not used");
    };
    const { result, rerender, unmount } = renderHook(
      ({ placeKey }) => useDraftAttachments({ placeKey }),
      {
        initialProps: { placeKey: "channel:a" as string | null },
        wrapper: ownerWrapper(upload, update),
      },
    );
    const fileA = new File(["a"], "a.png", { type: "image/png" });
    const fileB = new File(["b"], "b.png", { type: "image/png" });

    act(() => result.current.addFiles([fileA]));
    await waitFor(() => expect(result.current.items[0]?.status).toBe("ready"));
    const fileForA = result.current.fileFor;
    const localIdA = result.current.items[0].localId;

    rerender({ placeKey: "channel:b" });
    act(() => result.current.addFiles([fileB]));
    await waitFor(() => expect(result.current.items[0]?.status).toBe("ready"));
    const fileForB = result.current.fileFor;
    const localIdB = result.current.items[0].localId;

    act(() => result.current.clear());
    expect(result.current.items).toEqual([]);
    expect(fileForB(localIdB)).toBeUndefined();
    expect(revokeObjectURL).toHaveBeenCalledTimes(1);
    expect(revokeObjectURL).toHaveBeenLastCalledWith("blob:b.png");

    rerender({ placeKey: "channel:a" });
    expect(result.current.items).toHaveLength(1);
    expect(fileForA(localIdA)).toBe(fileA);

    unmount();
    expect(fileForA(localIdA)).toBeUndefined();
    expect(revokeObjectURL).toHaveBeenCalledTimes(2);
    expect(revokeObjectURL).toHaveBeenLastCalledWith("blob:a.png");
  });

  it("重なる宣言編集を一件だけ受け付け、完了は開始placeへ戻す", async () => {
    const initial = draft().attachment;
    if (!initial) throw new Error("test draft must carry an attachment");
    let finishUpdate: ((attachment: Attachment) => void) | undefined;
    const update = vi.fn(
      () =>
        new Promise<Attachment>((resolve) => {
          finishUpdate = resolve;
        }),
    );
    const upload = async (): Promise<Attachment> => initial;
    const { result, rerender } = renderHook(
      ({ placeKey }) => useDraftAttachments({ placeKey }),
      {
        initialProps: { placeKey: "channel:a" as string | null },
        wrapper: ownerWrapper(upload, update),
      },
    );

    act(() => {
      result.current.addFiles([
        new File(["draft"], "avatar.jpg", { type: "text/plain" }),
      ]);
    });
    await waitFor(() => expect(result.current.items[0]?.status).toBe("ready"));

    const localId = result.current.items[0].localId;
    act(() => {
      result.current.toggleSpoiler(localId);
      void result.current.applyEdit(localId, { patch: { alt: "後続の編集" } });
    });
    expect(update).toHaveBeenCalledWith("attachment-1", { spoiler: true });
    expect(update).toHaveBeenCalledTimes(1);
    expect(result.current.uploading).toBe(true);

    rerender({ placeKey: "channel:b" });
    expect(result.current.items).toEqual([]);
    expect(result.current.uploading).toBe(false);

    await act(async () => {
      finishUpdate?.({ ...initial, spoiler: true });
    });

    expect(result.current.items).toEqual([]);
    rerender({ placeKey: "channel:a" });
    expect(result.current.uploading).toBe(false);
    expect(result.current.items[0].attachment?.spoiler).toBe(true);
  });

  it("画像差し替えuploadの失敗後は旧idへのmetadata更新を再開しない", async () => {
    const initial = draft().attachment;
    if (!initial) throw new Error("test draft must carry an attachment");
    const createObjectURL = vi
      .fn<(file: File) => string>()
      .mockImplementation((file) => `blob:${file.name}`);
    vi.stubGlobal("URL", {
      createObjectURL,
      revokeObjectURL: vi.fn(),
    });
    const upload = vi
      .fn<(file: File) => Promise<Attachment>>()
      .mockResolvedValueOnce(initial)
      .mockRejectedValueOnce(new Error("edited upload failed"));
    const update = vi.fn(async (): Promise<Attachment> => initial);
    const { result } = renderHook(
      () => useDraftAttachments({ placeKey: "channel:a" }),
      { wrapper: ownerWrapper(upload, update) },
    );

    act(() => {
      result.current.addFiles([
        new File(["original"], "avatar.jpg", { type: "image/jpeg" }),
      ]);
    });
    await waitFor(() => expect(result.current.items[0]?.status).toBe("ready"));
    const localId = result.current.items[0].localId;

    await act(async () => {
      await result.current.applyEdit(localId, {
        editedFile: new File(["edited"], "edited.jpg", {
          type: "image/jpeg",
        }),
        patch: {},
      });
    });

    expect(result.current.items[0].status).toBe("failed");
    expect(result.current.items[0].previewUrl).toBe("blob:edited.jpg");
    // preview/Fileは新しい実体なので、旧server rowをrecovery元に残さない。
    expect(result.current.items[0].attachment).toBeUndefined();
    expect(result.current.ready).toEqual([]);

    await act(async () => {
      result.current.toggleSpoiler(localId);
      await result.current.applyEdit(localId, { patch: { alt: "再試行" } });
    });

    expect(result.current.items[0].status).toBe("failed");
    expect(result.current.ready).toEqual([]);
    expect(update).not.toHaveBeenCalled();
  });

  it("差し替え後のPATCH失敗から復旧しても新しいupload idだけをreadyにする", async () => {
    const initial = draft().attachment;
    if (!initial) throw new Error("test draft must carry an attachment");
    const replacement: Attachment = {
      ...initial,
      attachmentId: "attachment-edited",
      filename: "edited.jpg",
      url: "/messaging/attachments/attachment-edited",
    };
    vi.stubGlobal("URL", {
      createObjectURL: vi.fn((file: File) => `blob:${file.name}`),
      revokeObjectURL: vi.fn(),
    });
    const upload = vi
      .fn<(file: File) => Promise<Attachment>>()
      .mockResolvedValueOnce(initial)
      .mockResolvedValueOnce(replacement);
    const update = vi
      .fn<
        (
          attachmentId: string,
          patch: AttachmentDraftPatch,
        ) => Promise<Attachment>
      >()
      .mockRejectedValueOnce(new Error("replacement patch failed"))
      .mockResolvedValueOnce({ ...replacement, spoiler: true });
    const { result } = renderHook(
      () => useDraftAttachments({ placeKey: "channel:a" }),
      { wrapper: ownerWrapper(upload, update) },
    );

    act(() => {
      result.current.addFiles([
        new File(["original"], "avatar.jpg", { type: "image/jpeg" }),
      ]);
    });
    await waitFor(() => expect(result.current.items[0]?.status).toBe("ready"));
    const localId = result.current.items[0].localId;
    const editedFile = new File(["edited"], "edited.jpg", {
      type: "image/jpeg",
    });

    await act(async () => {
      await result.current.applyEdit(localId, {
        editedFile,
        patch: { alt: "新しい画像" },
      });
    });

    expect(update).toHaveBeenNthCalledWith(1, "attachment-edited", {
      alt: "新しい画像",
    });
    expect(result.current.items[0]).toMatchObject({
      status: "failed",
      previewUrl: "blob:edited.jpg",
      attachment: { attachmentId: "attachment-edited" },
    });
    expect(result.current.fileFor(localId)).toBe(editedFile);
    expect(result.current.ready).toEqual([]);

    act(() => result.current.toggleSpoiler(localId));
    await waitFor(() => expect(result.current.items[0]?.status).toBe("ready"));

    expect(update).toHaveBeenNthCalledWith(2, "attachment-edited", {
      spoiler: true,
    });
    expect(
      update.mock.calls.some(
        ([attachmentId]) => attachmentId === "attachment-1",
      ),
    ).toBe(false);
    expect(result.current.items[0].previewUrl).toBe("blob:edited.jpg");
    expect(result.current.ready.map((entry) => entry.attachmentId)).toEqual([
      "attachment-edited",
    ]);
  });

  it("宣言PATCHの失敗後は古いattachment idをreadyに戻さない", async () => {
    const initial = draft().attachment;
    if (!initial) throw new Error("test draft must carry an attachment");
    const upload = vi.fn(async (): Promise<Attachment> => initial);
    const update = vi.fn(async (): Promise<Attachment> => {
      throw new Error("patch failed");
    });
    const { result } = renderHook(
      () => useDraftAttachments({ placeKey: "channel:a" }),
      { wrapper: ownerWrapper(upload, update) },
    );

    act(() => {
      result.current.addFiles([
        new File(["draft"], "avatar.jpg", { type: "text/plain" }),
      ]);
    });
    await waitFor(() => expect(result.current.items[0]?.status).toBe("ready"));

    await act(async () => {
      await result.current.applyEdit(result.current.items[0].localId, {
        patch: { spoiler: true },
      });
    });

    expect(update).toHaveBeenCalledWith("attachment-1", { spoiler: true });
    expect(result.current.items[0].status).toBe("failed");
    expect(result.current.items[0].attachment?.attachmentId).toBe(
      "attachment-1",
    );
    expect(result.current.ready).toEqual([]);
  });

  it("owner破棄後の遅い編集callbackはuploadもPATCHも始めない", async () => {
    const initial = draft().attachment;
    if (!initial) throw new Error("test draft must carry an attachment");
    const upload = vi.fn(async (): Promise<Attachment> => initial);
    const update = vi.fn(async (): Promise<Attachment> => initial);
    const { result, unmount } = renderHook(
      () => useDraftAttachments({ placeKey: "channel:a" }),
      { wrapper: ownerWrapper(upload, update) },
    );

    act(() => {
      result.current.addFiles([
        new File(["draft"], "avatar.jpg", { type: "text/plain" }),
      ]);
    });
    await waitFor(() => expect(result.current.items[0]?.status).toBe("ready"));
    const applyAfterTeardown = result.current.applyEdit;
    const localId = result.current.items[0].localId;
    unmount();
    upload.mockClear();
    update.mockClear();

    await act(async () => {
      await applyAfterTeardown(localId, {
        editedFile: new File(["late"], "late.jpg", { type: "image/jpeg" }),
        patch: { spoiler: true },
      });
    });

    expect(upload).not.toHaveBeenCalled();
    expect(update).not.toHaveBeenCalled();
  });

  it("差し替えupload中にownerを破棄したら後続PATCHを始めない", async () => {
    const initial = draft().attachment;
    if (!initial) throw new Error("test draft must carry an attachment");
    let finishEditedUpload: ((attachment: Attachment) => void) | undefined;
    const upload = vi
      .fn<(file: File) => Promise<Attachment>>()
      .mockResolvedValueOnce(initial)
      .mockImplementationOnce(
        () =>
          new Promise<Attachment>((resolve) => {
            finishEditedUpload = resolve;
          }),
      );
    const update = vi.fn(async (): Promise<Attachment> => initial);
    const { result, unmount } = renderHook(
      () => useDraftAttachments({ placeKey: "channel:a" }),
      { wrapper: ownerWrapper(upload, update) },
    );

    act(() => {
      result.current.addFiles([
        new File(["draft"], "avatar.jpg", { type: "text/plain" }),
      ]);
    });
    await waitFor(() => expect(result.current.items[0]?.status).toBe("ready"));
    let editing = Promise.resolve();
    act(() => {
      editing = result.current.applyEdit(result.current.items[0].localId, {
        editedFile: new File(["edited"], "edited.jpg", {
          type: "text/plain",
        }),
        patch: { spoiler: true },
      });
    });
    expect(upload).toHaveBeenCalledTimes(2);

    unmount();
    await act(async () => {
      finishEditedUpload?.({
        ...initial,
        attachmentId: "attachment-edited",
      });
      await editing;
    });

    expect(update).not.toHaveBeenCalled();
  });
});

describe("fileExtension", () => {
  it("拡張子だけを大文字で返す", () => {
    expect(fileExtension("契約.pdf")).toBe("PDF");
    expect(fileExtension("archive.tar.gz")).toBe("GZ");
  });

  it("拡張子と呼べないものは空にする", () => {
    expect(fileExtension("README")).toBe("");
    expect(fileExtension(".gitignore")).toBe("");
    expect(fileExtension("trailing.")).toBe("");
    expect(fileExtension("name.verylongext")).toBe("");
  });
});
