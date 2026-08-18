// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MockMessagingServer } from "../mock-server";
import type { Attachment, AttachmentDraftPatch } from "../model";
import {
  bindMessagingSessionIdentity,
  installMessagingBackend,
  useMessaging,
} from "../store";
import { Composer } from "./composer";
import { ComposerAttachments } from "./composer-attachments";
import { MessageAttachments } from "./message-attachments";

const IMAGE: Attachment = {
  attachmentId: "0190aaaa-aaaa-7aaa-8aaa-aaaaaaaaaaa1",
  filename: "shot.png",
  mime: "image/png",
  sizeBytes: 2048,
  sha256: "ab",
  position: 0,
  spoiler: false,
  alt: "",
};
const DOCUMENT: Attachment = {
  attachmentId: "0190aaaa-aaaa-7aaa-8aaa-aaaaaaaaaaa2",
  filename: "evil.svg",
  mime: "application/octet-stream",
  sizeBytes: 5 * 1024 * 1024,
  sha256: "cd",
  position: 1,
  spoiler: false,
  alt: "",
};

const SPOILER: Attachment = {
  ...IMAGE,
  attachmentId: "0190aaaa-aaaa-7aaa-8aaa-aaaaaaaaaaa3",
  filename: "ending.png",
  spoiler: true,
  alt: "結末の一枚",
};

describe("MessageAttachments", () => {
  afterEach(() => {
    cleanup();
    bindMessagingSessionIdentity(null);
  });

  it("renders safe images inline and everything else as a download card", () => {
    bindMessagingSessionIdentity("human-self");
    installMessagingBackend(new MockMessagingServer());
    render(<MessageAttachments attachments={[IMAGE, DOCUMENT]} />);
    const image = screen.getByRole("img", { name: "shot.png" });
    expect(image).toHaveAttribute(
      "src",
      `/mock/attachments/${IMAGE.attachmentId}`,
    );
    const download = screen.getByTitle("evil.svgをダウンロード");
    expect(download).toHaveAttribute(
      "href",
      `/mock/attachments/${DOCUMENT.attachmentId}`,
    );
    expect(download).toHaveAttribute("download", "evil.svg");
    expect(download).toHaveTextContent("5.0 MB");
    // A scriptable document never becomes an <img>.
    expect(screen.queryByRole("img", { name: "evil.svg" })).toBeNull();
  });
});

describe("Composer attachments", () => {
  afterEach(() => {
    cleanup();
    bindMessagingSessionIdentity(null);
  });

  it("accepts pasted files, shows them as drafts, and lets the person remove one", async () => {
    bindMessagingSessionIdentity("human-self");
    installMessagingBackend(new MockMessagingServer());
    useMessaging.setState({
      ready: true,
      self: { kind: "human", humanId: "self" },
      selfKey: "human:self",
      activePlaceKey: "channel:ch-general",
      channels: [
        {
          channelId: "ch-general",
          workspaceId: "ws",
          name: "general",
          topic: "",
          visibility: "public",
          voice: false,
        },
      ],
      messagesByPlace: { "channel:ch-general": [] },
    });
    render(<Composer />);
    const textarea = screen.getByRole("textbox");
    const file = new File(["png"], "paste.png", { type: "image/png" });
    fireEvent.paste(textarea, {
      clipboardData: { files: [file], getData: () => "" },
    });
    const drafts = await screen.findByTestId("composer-attachments");
    expect(drafts).toHaveTextContent("paste.png");
    fireEvent.click(screen.getByRole("button", { name: "paste.pngを外す" }));
    expect(screen.queryByTestId("composer-attachments")).toBeNull();
    // The paperclip opens the hidden picker; the picker feeds the same path.
    const input = screen.getByTestId("composer-file-input") as HTMLInputElement;
    fireEvent.change(input, {
      target: {
        files: [new File(["x"], "picked.txt", { type: "text/plain" })],
      },
    });
    expect(await screen.findByTestId("composer-attachments")).toHaveTextContent(
      "picked.txt",
    );
  });
});

describe("Attachment spoiler and viewer", () => {
  afterEach(() => {
    cleanup();
    bindMessagingSessionIdentity(null);
  });

  it("keeps a spoilered image covered until the reader opens it, and names it by its alt", () => {
    bindMessagingSessionIdentity("human-self");
    installMessagingBackend(new MockMessagingServer());
    render(<MessageAttachments attachments={[SPOILER]} />);
    // 覆いの下でも「何の画像か」は分かる: altがそのまま読み上げ名になる。
    const image = screen.getByRole("img", { name: "結末の一枚" });
    expect(image.className).toContain("blur");
    const cover = screen.getByRole("button", {
      name: "結末の一枚のネタバレを開く",
    });
    fireEvent.click(cover);
    expect(
      screen.getByRole("img", { name: "結末の一枚" }).className,
    ).not.toContain("blur");
    // 開いても本体を開くのは次のクリック。1クリックでビューアーまで飛ばない。
    expect(screen.queryByTestId("image-viewer")).toBeNull();
  });

  it("opens a plain image in the in-app viewer and closes it with Escape", () => {
    bindMessagingSessionIdentity("human-self");
    installMessagingBackend(new MockMessagingServer());
    render(<MessageAttachments attachments={[IMAGE]} authorName="すみ" />);
    fireEvent.click(
      screen.getByRole("button", { name: "shot.pngを大きく表示（2.0 KB）" }),
    );
    const viewer = screen.getByTestId("image-viewer");
    expect(viewer).toHaveAttribute("aria-modal", "true");
    // 会話から離れないので、ビューアーの中でも同じscopeのURLを使う。
    expect(
      viewer.querySelector<HTMLImageElement>("img")?.getAttribute("src"),
    ).toBe(`/mock/attachments/${IMAGE.attachmentId}`);
    fireEvent.keyDown(document, { key: "Escape" });
    expect(screen.queryByTestId("image-viewer")).toBeNull();
  });
});

describe("Composer attachment cards", () => {
  afterEach(() => {
    cleanup();
    bindMessagingSessionIdentity(null);
  });

  it("shows the picked image as a thumbnail and marks it as a spoiler in place", async () => {
    const createObjectURL = URL.createObjectURL;
    const revokeObjectURL = URL.revokeObjectURL;
    URL.createObjectURL = () => "blob:preview";
    URL.revokeObjectURL = () => {};
    try {
      bindMessagingSessionIdentity("human-self");
      installMessagingBackend(new MockMessagingServer());
      useMessaging.setState({
        ready: true,
        self: { kind: "human", humanId: "self" },
        selfKey: "human:self",
        activePlaceKey: "channel:ch-general",
        channels: [
          {
            channelId: "ch-general",
            workspaceId: "ws",
            name: "general",
            topic: "",
            visibility: "public",
            voice: false,
          },
        ],
        messagesByPlace: { "channel:ch-general": [] },
      });
      render(<Composer />);
      const input = screen.getByTestId(
        "composer-file-input",
      ) as HTMLInputElement;
      fireEvent.change(input, {
        target: {
          files: [new File(["png"], "shot.png", { type: "image/png" })],
        },
      });
      const preview = await screen.findByRole("img", {
        name: "shot.png のプレビュー",
      });
      expect(preview).toHaveAttribute("src", "blob:preview");

      // 受領が返るまでは宣言を付けられない。返ったらホバー操作が出る。
      const toggle = await screen.findByRole("button", {
        name: "shot.pngのネタバレをマーク",
      });
      fireEvent.click(toggle);
      const marked = await screen.findByRole("button", {
        name: "shot.pngのネタバレを解除",
      });
      expect(marked).toHaveAttribute("aria-pressed", "true");
      expect(
        useMessaging.getState().draftAttachmentsByPlace["channel:ch-general"][0]
          .attachment?.spoiler,
      ).toBe(true);
      expect(preview.className).toContain("blur");
    } finally {
      URL.createObjectURL = createObjectURL;
      URL.revokeObjectURL = revokeObjectURL;
    }
  });

  it("shows a rejected edit's reason and its retained declaration with retry and discard", () => {
    const retry = vi.fn();
    const remove = vi.fn();
    const patch: AttachmentDraftPatch = {
      filename: "after.txt",
      alt: "保存される説明",
      spoiler: true,
    };
    render(
      <ComposerAttachments
        drafts={[
          {
            clientNonce: "edit-failure",
            filename: "before.txt",
            sizeBytes: 3,
            contentType: "text/plain",
            status: "edit_failed",
            errorCode: "invalid_request",
            editPatch: patch,
            attachment: {
              ...IMAGE,
              attachmentId: "att-edit",
              filename: "before.txt",
            },
          },
        ]}
        onEdit={vi.fn()}
        onRemove={remove}
        onRetry={retry}
      />,
    );
    expect(screen.getByTestId("composer-attachments")).toHaveTextContent(
      "この内容では保存できません",
    );
    expect(screen.getByTestId("composer-attachments")).toHaveTextContent(
      "after.txt",
    );
    expect(screen.getByTestId("composer-attachments")).toHaveTextContent(
      "保存される説明",
    );
    fireEvent.click(screen.getByRole("button", { name: "after.txtを再送" }));
    expect(retry).toHaveBeenCalledWith("edit-failure");
    fireEvent.click(screen.getByRole("button", { name: "after.txtを外す" }));
    expect(remove).toHaveBeenCalledWith("edit-failure");
  });
});
