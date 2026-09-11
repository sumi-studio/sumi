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
import { useState } from "react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { FeedbackAttachment } from "./api";
import { FeedbackAttachments, MediaPreview } from "./attachments";

const mocks = vi.hoisted(() => ({
  upload: vi.fn(),
  screenshot: vi.fn(),
  record: vi.fn(),
  supported: false,
}));
vi.mock("./api", () => ({ uploadFeedbackAttachment: mocks.upload }));
vi.mock("./capture", () => ({
  canCaptureScreenshot: () => mocks.supported,
  canRecordScreen: () => mocks.supported,
  captureScreenshot: mocks.screenshot,
  startRecording: mocks.record,
  MAX_RECORDING_BYTES: 20 * 1024 * 1024,
}));

const attachment: FeedbackAttachment = {
  id: "one",
  name: "one.png",
  mime_type: "image/png",
  size: 4,
  url: "/feedback/attachments/one",
};
function Form() {
  const [attachments, onChange] = useState<FeedbackAttachment[]>([]);
  return (
    <form>
      <textarea aria-label="本文" />
      <FeedbackAttachments attachments={attachments} onChange={onChange} />
    </form>
  );
}

function fileInput(container: HTMLElement): HTMLInputElement {
  const input = container.querySelector('input[type="file"]');
  if (!(input instanceof HTMLInputElement))
    throw new Error("File input missing");
  return input;
}

beforeEach(() => {
  vi.clearAllMocks();
  mocks.supported = false;
});
afterEach(cleanup);

it("uploads image pasted into the surrounding form without intercepting text", async () => {
  mocks.upload.mockResolvedValue(attachment);
  render(<Form />);
  const textPaste = new Event("paste", { bubbles: true, cancelable: true });
  Object.defineProperty(textPaste, "clipboardData", { value: { files: [] } });
  fireEvent(screen.getByLabelText("本文"), textPaste);
  expect(textPaste.defaultPrevented).toBe(false);
  const file = new File(["data"], "one.png", { type: "image/png" });
  fireEvent.paste(screen.getByLabelText("本文"), {
    clipboardData: { files: [file] },
  });
  await screen.findByRole("img", { name: "one.png" });
  expect(mocks.upload).toHaveBeenCalledWith(file, expect.any(AbortSignal));
  fireEvent.click(screen.getByRole("button", { name: "one.pngを削除" }));
  expect(screen.queryByRole("img")).not.toBeInTheDocument();
});

it("preserves successful attachments and retries only failed files", async () => {
  mocks.upload
    .mockResolvedValueOnce(attachment)
    .mockRejectedValueOnce(new Error("接続が切れました"));
  const { container } = render(<Form />);
  const one = new File(["data"], "one.png", { type: "image/png" });
  const two = new File(["data"], "two.png", { type: "image/png" });
  fireEvent.change(fileInput(container), {
    target: { files: [one, two] },
  });
  await screen.findByRole("alert");
  expect(screen.getByRole("img", { name: "one.png" })).toBeInTheDocument();
  mocks.upload.mockResolvedValueOnce({
    ...attachment,
    id: "two",
    name: "two.png",
  });
  fireEvent.click(screen.getByRole("button", { name: "再試行" }));
  await screen.findByRole("img", { name: "two.png" });
  expect(mocks.upload).toHaveBeenCalledTimes(3);
  expect(mocks.upload.mock.calls[2][0]).toBe(two);
});

it("rejects unsupported and excess files before upload", async () => {
  const { container } = render(<Form />);
  const input = fileInput(container);
  fireEvent.change(input, {
    target: {
      files: Array.from(
        { length: 6 },
        () => new File(["x"], "one.png", { type: "image/png" }),
      ),
    },
  });
  expect(await screen.findByRole("alert")).toHaveTextContent("5件まで");
  fireEvent.change(input, {
    target: { files: [new File(["x"], "script.html", { type: "text/html" })] },
  });
  expect(await screen.findByRole("alert")).toHaveTextContent("PNG・JPEG・WebP");
  expect(mocks.upload).not.toHaveBeenCalled();
});

it("reserves a full attachment slot for retry and enables capture after discarding it", async () => {
  mocks.supported = true;
  mocks.upload.mockRejectedValueOnce(new Error("接続が切れました"));
  const attachments = Array.from({ length: 4 }, (_, index) => ({
    ...attachment,
    id: `saved-${index}`,
  }));
  const { container } = render(
    <FeedbackAttachments attachments={attachments} onChange={vi.fn()} />,
  );
  fireEvent.change(fileInput(container), {
    target: {
      files: [new File(["data"], "pending.png", { type: "image/png" })],
    },
  });
  await screen.findByRole("alert");
  expect(screen.getByRole("button", { name: "画面を撮影" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "画面を録画" })).toBeDisabled();
  expect(
    screen.getByRole("button", { name: "画像・動画を追加" }),
  ).toBeDisabled();
  expect(screen.getByRole("button", { name: "再試行" })).toBeEnabled();
  fireEvent.click(screen.getByRole("button", { name: "画面を撮影" }));
  expect(mocks.screenshot).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: "取り消す" }));
  expect(screen.getByRole("button", { name: "画面を撮影" })).toBeEnabled();
  expect(screen.getByRole("button", { name: "画面を録画" })).toBeEnabled();
});

it("aborts pending upload on unmount and ignores a late result", async () => {
  let resolve!: (value: FeedbackAttachment) => void;
  mocks.upload.mockReturnValue(
    new Promise<FeedbackAttachment>((done) => {
      resolve = done;
    }),
  );
  const onChange = vi.fn();
  const { container, unmount } = render(
    <FeedbackAttachments attachments={[]} onChange={onChange} />,
  );
  fireEvent.change(fileInput(container), {
    target: { files: [new File(["x"], "one.png", { type: "image/png" })] },
  });
  const signal = mocks.upload.mock.calls[0][1] as AbortSignal;
  unmount();
  expect(signal.aborted).toBe(true);
  await act(async () => resolve(attachment));
  expect(onChange).not.toHaveBeenCalled();
});

it("restores an interrupted screenshot upload after reopening its draft", async () => {
  mocks.supported = true;
  const file = new File(["PNG"], "captured.png", { type: "image/png" });
  mocks.screenshot.mockResolvedValueOnce(file);
  let completeOriginal: ((value: FeedbackAttachment) => void) | undefined;
  mocks.upload.mockReturnValueOnce(
    new Promise<FeedbackAttachment>((resolve) => {
      completeOriginal = resolve;
    }),
  );
  const onChange = vi.fn();
  const onBusyChange = vi.fn();
  const props = {
    attachments: [],
    onChange,
    onBusyChange,
    draftStorageKey: "actor-capture:draft-interrupted",
  };
  const first = render(<FeedbackAttachments {...props} />);
  fireEvent.click(screen.getByRole("button", { name: "画面を撮影" }));
  await waitFor(() => expect(mocks.upload).toHaveBeenCalledOnce());
  const signal = mocks.upload.mock.calls[0][1] as AbortSignal;
  first.unmount();
  expect(signal.aborted).toBe(true);
  const reopened = render(<FeedbackAttachments {...props} />);
  expect(screen.getByText("未添付: captured.png")).toBeInTheDocument();
  expect(onBusyChange).toHaveBeenLastCalledWith(true);
  expect(screen.getByRole("button", { name: "再試行" })).toBeEnabled();
  mocks.upload.mockResolvedValueOnce({ ...attachment, name: "captured.png" });
  fireEvent.click(screen.getByRole("button", { name: "再試行" }));
  await waitFor(() => expect(onChange).toHaveBeenCalledOnce());
  expect(mocks.upload.mock.calls[1][0]).toBe(file);
  expect(onBusyChange).toHaveBeenLastCalledWith(false);
  await act(async () => completeOriginal?.(attachment));
  expect(onChange).toHaveBeenCalledOnce();
  reopened.unmount();
  render(<FeedbackAttachments {...props} />);
  expect(
    screen.queryByRole("button", { name: "再試行" }),
  ).not.toBeInTheDocument();
});

it("isolates pending files by actor and draft key and clears discarded files", async () => {
  mocks.upload.mockRejectedValueOnce(new Error("接続が切れました"));
  const props = { attachments: [], onChange: vi.fn() };
  const { container, rerender } = render(
    <FeedbackAttachments {...props} draftStorageKey="alice:identity-draft" />,
  );
  fireEvent.change(fileInput(container), {
    target: { files: [new File(["data"], "alice.png", { type: "image/png" })] },
  });
  await screen.findByRole("alert");
  rerender(
    <FeedbackAttachments {...props} draftStorageKey="bob:identity-draft" />,
  );
  expect(screen.queryByText("未添付: alice.png")).not.toBeInTheDocument();
  rerender(
    <FeedbackAttachments {...props} draftStorageKey="alice:another-draft" />,
  );
  expect(screen.queryByText("未添付: alice.png")).not.toBeInTheDocument();
  rerender(
    <FeedbackAttachments {...props} draftStorageKey="alice:identity-draft" />,
  );
  expect(screen.getByText("未添付: alice.png")).toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "取り消す" }));
  rerender(
    <FeedbackAttachments {...props} draftStorageKey="bob:identity-draft" />,
  );
  rerender(
    <FeedbackAttachments {...props} draftStorageKey="alice:identity-draft" />,
  );
  expect(
    screen.queryByRole("button", { name: "再試行" }),
  ).not.toBeInTheDocument();
});

it("restores only uncommitted files when a batch upload is interrupted", async () => {
  mocks.upload
    .mockResolvedValueOnce(attachment)
    .mockReturnValueOnce(new Promise<FeedbackAttachment>(() => {}));
  const props = {
    attachments: [],
    onChange: vi.fn(),
    draftStorageKey: "actor-batch:partial-draft",
  };
  const first = render(<FeedbackAttachments {...props} />);
  fireEvent.change(fileInput(first.container), {
    target: {
      files: [
        new File(["data"], "one.png", { type: "image/png" }),
        new File(["data"], "two.png", { type: "image/png" }),
      ],
    },
  });
  await waitFor(() => expect(mocks.upload).toHaveBeenCalledTimes(2));
  first.unmount();
  render(<FeedbackAttachments {...props} attachments={[attachment]} />);
  expect(screen.getByText("未添付: two.png")).toBeInTheDocument();
  expect(
    screen.queryByText("未添付: one.png、two.png"),
  ).not.toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "取り消す" }));
});

it("starts capture in the click handler and keeps recording controls outside its parent", async () => {
  mocks.supported = true;
  const stop = vi.fn();
  const cancel = vi.fn();
  mocks.record.mockResolvedValue({ stop, cancel });
  const onCaptureChange = vi.fn();
  const { container, unmount } = render(
    <FeedbackAttachments
      attachments={[]}
      onChange={vi.fn()}
      onCaptureChange={onCaptureChange}
    />,
  );
  fireEvent.click(screen.getByRole("button", { name: "画面を録画" }));
  expect(mocks.record).toHaveBeenCalledOnce();
  expect(onCaptureChange).toHaveBeenCalledWith(true);
  await waitFor(() =>
    expect(
      screen.getByRole("button", { name: "停止して添付" }),
    ).toBeInTheDocument(),
  );
  expect(
    container.contains(screen.getByRole("region", { name: "画面録画の操作" })),
  ).toBe(false);
  fireEvent.click(screen.getByRole("button", { name: "停止して添付" }));
  expect(stop).toHaveBeenCalledOnce();
  unmount();
  expect(cancel).toHaveBeenCalledOnce();
  expect((mocks.record.mock.calls[0][2] as AbortSignal).aborted).toBe(true);
});

it("does not render executable attachment links", () => {
  render(
    <MediaPreview
      attachments={[{ ...attachment, url: "javascript:alert(1)" }]}
    />,
  );
  expect(screen.queryByRole("link")).not.toBeInTheDocument();
  expect(screen.queryByRole("img")).not.toBeInTheDocument();
});
