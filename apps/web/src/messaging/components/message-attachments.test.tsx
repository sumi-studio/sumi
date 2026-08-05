// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import type { Attachment } from "../model";
import { formatFileSize, MessageAttachments } from "./message-attachments";

function attachment(overrides: Partial<Attachment> = {}): Attachment {
  return {
    attachmentId: "attachment-1",
    filename: "shot.png",
    mime: "image/png",
    size: 2048,
    url: "/messaging/attachments/attachment-1",
    spoiler: false,
    alt: "",
    ...overrides,
  };
}

afterEach(cleanup);

describe("MessageAttachments", () => {
  it("画像はインラインプレビューにし、クリックでアプリ内ビューアーを開く", () => {
    render(
      <MessageAttachments
        attachments={[attachment()]}
        authorName="そら"
        createdAt={Date.UTC(2026, 0, 2, 3, 4)}
      />,
    );

    const image = screen.getByAltText("shot.png");
    expect(image).toHaveAttribute("src", "/messaging/attachments/attachment-1");
    // 新規タブへ飛ばさない: 画像はリンクではなくビューアーを開くボタン。
    expect(image.closest("a")).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "shot.png を開く" }));

    const viewer = screen.getByRole("dialog", {
      name: "shot.png の画像ビューアー",
    });
    expect(viewer).toBeInTheDocument();
    expect(screen.getByText("そら")).toBeInTheDocument();

    fireEvent.keyDown(document, { key: "Escape" });
    expect(
      screen.queryByRole("dialog", { name: "shot.png の画像ビューアー" }),
    ).toBeNull();
  });

  it("画像以外はファイル名とサイズのカードにする", () => {
    render(
      <MessageAttachments
        attachments={[
          attachment({
            attachmentId: "attachment-2",
            filename: "契約.pdf",
            mime: "application/pdf",
            size: 3 * 1024 * 1024,
            url: "/messaging/attachments/attachment-2",
          }),
        ]}
      />,
    );

    expect(screen.queryByRole("img")).toBeNull();
    expect(screen.getByText("契約.pdf")).toBeInTheDocument();
    expect(screen.getByText("3.0 MB")).toBeInTheDocument();
    expect(screen.getByRole("link")).toHaveAttribute("download", "契約.pdf");
  });

  it("SVGはインライン表示しない（サーバーもdownloadで配信する）", () => {
    render(
      <MessageAttachments
        attachments={[
          attachment({ filename: "logo.svg", mime: "image/svg+xml" }),
        ]}
      />,
    );

    expect(screen.queryByRole("img")).toBeNull();
    expect(screen.getByText("logo.svg")).toBeInTheDocument();
  });

  it("ネタバレ画像はぼかして隠し、クリックで開示する", () => {
    render(
      <MessageAttachments
        attachments={[attachment({ spoiler: true, alt: "結末の一枚" })]}
      />,
    );

    const image = screen.getByAltText("結末の一枚");
    expect(image.className).toContain("blur-xl");
    // 何かは分かる: 概要はぼかしの上に出す。
    expect(screen.getByText("ネタバレ")).toBeInTheDocument();

    fireEvent.click(screen.getByText("ネタバレ"));

    expect(screen.getByAltText("結末の一枚").className).not.toContain("blur");
    expect(screen.queryByText("ネタバレ")).toBeNull();
  });

  it("ネタバレ付きの画像以外はピルで示す", () => {
    render(
      <MessageAttachments
        attachments={[
          attachment({
            filename: "報告.pdf",
            mime: "application/pdf",
            spoiler: true,
          }),
        ]}
      />,
    );

    expect(screen.getByText("ネタバレ")).toBeInTheDocument();
    expect(screen.getByRole("link")).toHaveAttribute("download", "報告.pdf");
  });

  it("添付が無ければ何も描かない", () => {
    const { container } = render(<MessageAttachments attachments={[]} />);
    expect(container).toBeEmptyDOMElement();
  });
});

describe("formatFileSize", () => {
  it("人が読める単位にする", () => {
    expect(formatFileSize(512)).toBe("512 B");
    expect(formatFileSize(2048)).toBe("2.0 KB");
    expect(formatFileSize(20 * 1024 * 1024)).toBe("20 MB");
  });
});
