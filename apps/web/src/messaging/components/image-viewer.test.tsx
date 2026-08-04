// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { Attachment } from "../model";
import { ImageViewer } from "./image-viewer";

function attachment(overrides: Partial<Attachment> = {}): Attachment {
  return {
    attachmentId: "attachment-1",
    filename: "shot.png",
    mime: "image/png",
    size: 2048,
    url: "/messaging/attachments/attachment-1",
    ...overrides,
  };
}

afterEach(cleanup);

describe("ImageViewer", () => {
  it("クリックで最大表示と通常サイズを行き来し、カーソルで状態を示す", () => {
    render(<ImageViewer attachment={attachment()} onClose={() => {}} />);

    const toggle = screen.getByRole("button", { name: "最大表示にする" });
    expect(toggle.className).toContain("cursor-zoom-in");
    // 通常サイズではツール群が並ぶ。
    expect(screen.getByRole("button", { name: "保存" })).toBeInTheDocument();

    fireEvent.click(toggle);
    const zoomed = screen.getByRole("button", { name: "通常サイズに戻す" });
    expect(zoomed.className).toContain("cursor-zoom-out");
    // 最大表示は画面フィット。残すのは閉じるだけ。
    expect(screen.queryByRole("button", { name: "保存" })).toBeNull();
    expect(
      screen.getByRole("button", { name: "画像ビューアーを閉じる" }),
    ).toBeInTheDocument();

    fireEvent.click(zoomed);
    expect(
      screen.getByRole("button", { name: "最大表示にする" }).className,
    ).toContain("cursor-zoom-in");
  });

  it("Escと背景クリックで閉じる。画像の上のクリックでは閉じない", () => {
    const onClose = vi.fn();
    const { container } = render(
      <ImageViewer attachment={attachment()} onClose={onClose} />,
    );

    fireEvent.click(screen.getByAltText("shot.png"));
    expect(onClose).not.toHaveBeenCalled();

    fireEvent.keyDown(document, { key: "Escape" });
    expect(onClose).toHaveBeenCalledTimes(1);

    const overlay = container.ownerDocument.querySelector('[role="dialog"]');
    if (!overlay) throw new Error("overlay missing");
    fireEvent.click(overlay);
    expect(onClose).toHaveBeenCalledTimes(2);
  });

  it("その他メニューからメディアリンクと添付IDをコピーできる", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    vi.stubGlobal("navigator", {
      ...navigator,
      clipboard: { writeText },
    });
    render(<ImageViewer attachment={attachment()} onClose={() => {}} />);

    fireEvent.click(screen.getByRole("button", { name: "その他" }));
    fireEvent.click(
      screen.getByRole("button", { name: "メディアリンクをコピー" }),
    );
    expect(writeText).toHaveBeenCalledWith(
      `${window.location.origin}/messaging/attachments/attachment-1`,
    );

    fireEvent.click(screen.getByRole("button", { name: "その他" }));
    fireEvent.click(screen.getByRole("button", { name: "添付IDをコピー" }));
    expect(writeText).toHaveBeenCalledWith("attachment-1");

    vi.unstubAllGlobals();
  });

  it("詳細はファイル名とサイズを見せる", () => {
    render(
      <ImageViewer
        attachment={attachment({ size: 3 * 1024 * 1024 })}
        onClose={() => {}}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "その他" }));
    fireEvent.click(screen.getByRole("button", { name: "詳細" }));

    expect(screen.getByText("3.0 MB・image/png")).toBeInTheDocument();
  });
});
