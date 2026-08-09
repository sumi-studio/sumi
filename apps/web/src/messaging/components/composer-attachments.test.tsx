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
import { afterEach, describe, expect, it, vi } from "vitest";
import type { Attachment } from "../model";
import {
  ComposerAttachments,
  type DraftAttachment,
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

afterEach(cleanup);

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
  it("宣言のPATCH中も送信を止め、完了後にreadyへ戻す", async () => {
    const initial = draft().attachment;
    if (!initial) throw new Error("test draft must carry an attachment");
    let finishUpdate: ((attachment: Attachment) => void) | undefined;
    const update = vi.fn(
      () =>
        new Promise<Attachment>((resolve) => {
          finishUpdate = resolve;
        }),
    );
    const { result } = renderHook(() =>
      useDraftAttachments({ upload: async () => initial, update }),
    );

    act(() => {
      result.current.addFiles([
        new File(["draft"], "avatar.jpg", { type: "text/plain" }),
      ]);
    });
    await waitFor(() => expect(result.current.items[0]?.status).toBe("ready"));

    act(() => {
      result.current.toggleSpoiler(result.current.items[0].localId);
    });
    expect(update).toHaveBeenCalledWith("attachment-1", { spoiler: true });
    expect(result.current.uploading).toBe(true);

    await act(async () => {
      finishUpdate?.({ ...initial, spoiler: true });
    });
    expect(result.current.uploading).toBe(false);
    expect(result.current.items[0].attachment?.spoiler).toBe(true);
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
