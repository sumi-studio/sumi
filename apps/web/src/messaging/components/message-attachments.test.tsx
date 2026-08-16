// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { MockMessagingServer } from "../mock-server";
import type { Attachment } from "../model";
import {
  bindMessagingSessionIdentity,
  installMessagingBackend,
  useMessaging,
} from "../store";
import { Composer } from "./composer";
import { MessageAttachments } from "./message-attachments";

const IMAGE: Attachment = {
  attachmentId: "0190aaaa-aaaa-7aaa-8aaa-aaaaaaaaaaa1",
  filename: "shot.png",
  mime: "image/png",
  sizeBytes: 2048,
  sha256: "ab",
  position: 0,
};
const DOCUMENT: Attachment = {
  attachmentId: "0190aaaa-aaaa-7aaa-8aaa-aaaaaaaaaaa2",
  filename: "evil.svg",
  mime: "application/octet-stream",
  sizeBytes: 5 * 1024 * 1024,
  sha256: "cd",
  position: 1,
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
