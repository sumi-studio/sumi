// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  ComposerAttachments,
  type DraftAttachment,
  fileExtension,
} from "./composer-attachments";

function draft(overrides: Partial<DraftAttachment> = {}): DraftAttachment {
  return {
    localId: "draft-1",
    filename: "avatar.jpg",
    size: 68 * 1024,
    mime: "image/jpeg",
    status: "ready",
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
