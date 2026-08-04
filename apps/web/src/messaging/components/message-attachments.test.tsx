// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
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
    ...overrides,
  };
}

afterEach(cleanup);

describe("MessageAttachments", () => {
  it("画像はインラインプレビューにし、クリックで原寸を開く", () => {
    render(<MessageAttachments attachments={[attachment()]} />);

    const image = screen.getByAltText("shot.png");
    expect(image).toHaveAttribute("src", "/messaging/attachments/attachment-1");
    const link = image.closest("a");
    expect(link).toHaveAttribute("href", "/messaging/attachments/attachment-1");
    expect(link).toHaveAttribute("target", "_blank");
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
