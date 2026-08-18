// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from "@testing-library/react";
import { useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  hasForbiddenAttachmentDisplayCharacter,
  sanitizeAttachmentDisplayText,
} from "../attachment-display";
import { MockMessagingServer } from "../mock-server";
import type { Attachment, AttachmentDraftPatch } from "../model";
import {
  bindMessagingSessionIdentity,
  installMessagingBackend,
  useMessaging,
} from "../store";
import { AttachmentEditDialog } from "./attachment-edit-dialog";
import { Composer } from "./composer";
import { ComposerAttachments } from "./composer-attachments";
import { ImageViewer } from "./image-viewer";
import {
  type ImageViewerRequest,
  MessageAttachments,
} from "./message-attachments";

function AttachmentHost({
  attachments,
  authorName,
  visible = true,
}: {
  attachments: Attachment[];
  authorName?: string;
  visible?: boolean;
}) {
  const [revealedAttachmentIds, setRevealedAttachmentIds] = useState<
    ReadonlySet<string>
  >(() => new Set());
  const [viewing, setViewing] = useState<ImageViewerRequest | null>(null);
  const attachmentURL = useMessaging((state) => state.attachmentURL);
  return (
    <>
      {visible ? (
        <MessageAttachments
          attachments={attachments}
          authorName={authorName}
          revealedAttachmentIds={revealedAttachmentIds}
          onReveal={(attachmentId) =>
            setRevealedAttachmentIds((current) =>
              current.has(attachmentId)
                ? current
                : new Set(current).add(attachmentId),
            )
          }
          onOpenImage={setViewing}
        />
      ) : null}
      {viewing ? (
        <ImageViewer
          attachment={viewing.attachment}
          href={attachmentURL(viewing.attachment.attachmentId)}
          authorName={viewing.authorName}
          createdAt={viewing.createdAt}
          onClose={() => setViewing(null)}
        />
      ) : null}
    </>
  );
}

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

describe("attachment display text", () => {
  it("uses the API and agent forbidden-character set", () => {
    for (const character of [
      "\u0085",
      "\u2028",
      "\u2029",
      "\u202e",
      "\u200b",
    ]) {
      expect(hasForbiddenAttachmentDisplayCharacter(character)).toBe(true);
    }
    expect(sanitizeAttachmentDisplayText("before\u202eafter\u200bend")).toBe(
      "before after end",
    );
  });
});

describe("MessageAttachments", () => {
  afterEach(() => {
    cleanup();
    bindMessagingSessionIdentity(null);
  });

  it("renders safe images inline and everything else as a download card", () => {
    bindMessagingSessionIdentity("human-self");
    installMessagingBackend(new MockMessagingServer());
    render(<AttachmentHost attachments={[IMAGE, DOCUMENT]} />);
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

  it("does not render a spoilered image until the reader opens it", () => {
    bindMessagingSessionIdentity("human-self");
    installMessagingBackend(new MockMessagingServer());
    render(<AttachmentHost attachments={[SPOILER]} />);
    expect(screen.queryByRole("img", { name: "結末の一枚" })).toBeNull();
    const cover = screen.getByRole("button", {
      name: "結末の一枚のネタバレを開く",
    });
    expect(cover).toHaveClass("min-h-28", "min-w-48");
    fireEvent.click(cover);
    expect(screen.getByRole("img", { name: "結末の一枚" })).toHaveAttribute(
      "src",
      `/mock/attachments/${SPOILER.attachmentId}`,
    );
    // 開いても本体を開くのは次のクリック。1クリックでビューアーまで飛ばない。
    expect(screen.queryByTestId("image-viewer")).toBeNull();
  });

  it("reveals a spoilered image with Enter", () => {
    bindMessagingSessionIdentity("human-self");
    installMessagingBackend(new MockMessagingServer());
    render(<AttachmentHost attachments={[SPOILER]} />);
    const cover = screen.getByRole("button", {
      name: "結末の一枚のネタバレを開く",
    });
    fireEvent.keyDown(cover, { key: "Enter" });
    expect(screen.getByRole("img", { name: "結末の一枚" })).toHaveAttribute(
      "src",
      `/mock/attachments/${SPOILER.attachmentId}`,
    );
    expect(screen.queryByTestId("image-viewer")).toBeNull();
  });

  it("opens a plain image in the in-app viewer and closes it with Escape", () => {
    bindMessagingSessionIdentity("human-self");
    installMessagingBackend(new MockMessagingServer());
    render(<AttachmentHost attachments={[IMAGE]} authorName="すみ" />);
    const trigger = screen.getByRole("button", {
      name: "shot.pngを大きく表示（2.0 KB）",
    });
    trigger.focus();
    fireEvent.click(trigger);
    const viewer = screen.getByTestId("image-viewer");
    expect(viewer).toHaveAttribute("aria-modal", "true");
    // 会話から離れないので、ビューアーの中でも同じscopeのURLを使う。
    expect(
      viewer.querySelector<HTMLImageElement>("img")?.getAttribute("src"),
    ).toBe(`/mock/attachments/${IMAGE.attachmentId}`);
    fireEvent.keyDown(document, { key: "Escape" });
    expect(screen.queryByTestId("image-viewer")).toBeNull();
    expect(trigger).toHaveFocus();
  });

  it("keeps a viewer and a revealed spoiler when its virtual row unmounts", () => {
    bindMessagingSessionIdentity("human-self");
    installMessagingBackend(new MockMessagingServer());
    const view = render(<AttachmentHost attachments={[SPOILER, IMAGE]} />);
    fireEvent.click(
      screen.getByRole("button", { name: "結末の一枚のネタバレを開く" }),
    );
    fireEvent.click(
      screen.getByRole("button", {
        name: "shot.pngを大きく表示（2.0 KB）",
      }),
    );
    expect(screen.getByTestId("image-viewer")).toBeVisible();
    view.rerender(
      <AttachmentHost attachments={[SPOILER, IMAGE]} visible={false} />,
    );
    expect(screen.getByTestId("image-viewer")).toBeVisible();
    view.rerender(<AttachmentHost attachments={[SPOILER, IMAGE]} />);
    expect(screen.getByRole("img", { name: "結末の一枚" })).toBeVisible();
  });

  it("traps Tab in the viewer", () => {
    bindMessagingSessionIdentity("human-self");
    installMessagingBackend(new MockMessagingServer());
    render(<AttachmentHost attachments={[IMAGE]} />);
    fireEvent.click(
      screen.getByRole("button", { name: "shot.pngを大きく表示（2.0 KB）" }),
    );
    const close = screen.getByRole("button", {
      name: "画像ビューアーを閉じる",
    });
    expect(close).toHaveFocus();
    const displayControls = within(
      screen.getByTestId("image-viewer"),
    ).getAllByRole("button", { name: "最大表示" });
    displayControls[1].focus();
    fireEvent.keyDown(document, { key: "Tab" });
    expect(displayControls[0]).toHaveFocus();
  });
});

describe("AttachmentEditDialog", () => {
  afterEach(cleanup);

  it("accepts a 1,000-code-point description even when its UTF-16 length is larger", () => {
    const onApply = vi.fn();
    const description = "😀".repeat(1000);
    render(
      <AttachmentEditDialog
        filename="shot.png"
        alt=""
        spoiler={false}
        onCancel={vi.fn()}
        onApply={onApply}
      />,
    );
    fireEvent.change(screen.getByRole("textbox", { name: "説明" }), {
      target: { value: description },
    });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    expect(onApply).toHaveBeenCalledWith({ alt: description });
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

  it("keeps declaration controls in the DOM while editing and lets a failed edit reopen", () => {
    const onEdit = vi.fn();
    const { rerender } = render(
      <ComposerAttachments
        drafts={[
          {
            clientNonce: "editing",
            filename: "shot.png",
            sizeBytes: 3,
            contentType: "image/png",
            status: "editing",
            attachment: IMAGE,
          },
        ]}
        onEdit={onEdit}
        onRemove={vi.fn()}
        onRetry={vi.fn()}
      />,
    );
    expect(
      screen.getByRole("button", { name: "shot.pngのネタバレをマーク" }),
    ).toBeDisabled();
    expect(
      screen.getByRole("button", { name: "shot.pngを編集" }),
    ).toBeDisabled();
    rerender(
      <ComposerAttachments
        drafts={[
          {
            clientNonce: "failed",
            filename: "shot.png",
            sizeBytes: 3,
            contentType: "image/png",
            status: "edit_failed",
            errorCode: "invalid_request",
            attachment: IMAGE,
          },
        ]}
        onEdit={onEdit}
        onRemove={vi.fn()}
        onRetry={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByRole("button", { name: "shot.pngを編集" }));
    expect(
      screen.getByRole("dialog", { name: "添付ファイルを編集" }),
    ).toBeVisible();
  });
});
