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
import type { DiagnosticAnnotation } from "./diagnostics";
import { AnnotationReferences } from "./references";

const mocks = vi.hoisted(() => ({ capture: vi.fn(), upload: vi.fn() }));
vi.mock("./api", () => ({ uploadFeedbackAttachment: mocks.upload }));
vi.mock("./capture", () => ({
  canCaptureRegionScreenshot: () => true,
  captureRegionScreenshot: mocks.capture,
}));

const image = new File(["region pixels"], "region-1.png", {
  type: "image/png",
});
const attachment: FeedbackAttachment = {
  id: "region-one",
  name: "region-1.png",
  mime_type: "image/png",
  size: image.size,
  url: "/feedback/attachments/region-one",
};
function region(): DiagnosticAnnotation {
  return {
    number: 1,
    kind: "region",
    tag: "",
    selector: "",
    label: "選択した範囲",
    rect: { x: 30, y: 50, width: 240, height: 100 },
    captured_at: "2026-09-11T15:00:00.000Z",
    path: window.location.pathname,
    scroll_x: window.scrollX,
    scroll_y: window.scrollY,
    viewport_width: window.innerWidth,
    viewport_height: window.innerHeight,
  };
}
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
function Form({ draftStorageKey }: { draftStorageKey: string }) {
  const [body, setBody] = useState("before capture");
  const [attachments, setAttachments] = useState<FeedbackAttachment[]>([]);
  const [attachedBody, setAttachedBody] = useState("");
  const [busy, setBusy] = useState(false);
  return (
    <>
      <textarea
        aria-label="本文"
        value={body}
        onChange={(event) => setBody(event.target.value)}
      />
      <output aria-label="添付した件数">{attachments.length}</output>
      <output aria-label="添付時の本文">{attachedBody}</output>
      <button type="button" disabled={busy}>
        送信する
      </button>
      <AnnotationReferences
        annotations={[region()]}
        draftStorageKey={draftStorageKey}
        attachmentCount={attachments.length}
        resolveAnnotation={(annotation) => annotation.rect}
        onBusyChange={setBusy}
        onAttachment={(next) => {
          setAttachedBody(body);
          setAttachments((previous) => [...previous, next]);
        }}
      />
    </>
  );
}

beforeEach(() => {
  mocks.capture.mockReset();
  mocks.upload.mockReset();
  mocks.capture.mockImplementation(
    async (_target, _number, _signal, validate) => {
      if (validate && !validate()) throw new Error("Source frame changed");
      return image;
    },
  );
});
afterEach(cleanup);

it("rejects a region invalidated by source scrolling before opening screen sharing", async () => {
  render(
    <AnnotationReferences
      annotations={[region()]}
      resolveAnnotation={() => undefined}
      onAttachment={vi.fn()}
    />,
  );
  fireEvent.click(screen.getByRole("button", { name: "範囲 1 を画像にする" }));
  expect(await screen.findByRole("alert")).toBeInTheDocument();
  expect(mocks.capture).not.toHaveBeenCalled();
  expect(mocks.upload).not.toHaveBeenCalled();
});

it("revalidates the source frame after the native chooser and refuses a stale crop", async () => {
  const chosen = deferred<void>();
  let valid = true;
  mocks.capture.mockImplementation(
    async (_target, _number, _signal, validate) => {
      await chosen.promise;
      if (!validate()) throw new Error("Source frame changed");
      return image;
    },
  );
  render(
    <AnnotationReferences
      annotations={[region()]}
      resolveAnnotation={(annotation) => (valid ? annotation.rect : undefined)}
      onAttachment={vi.fn()}
    />,
  );
  fireEvent.click(screen.getByRole("button", { name: "範囲 1 を画像にする" }));
  expect(mocks.capture).toHaveBeenCalledWith(
    expect.any(HTMLDivElement),
    1,
    expect.any(AbortSignal),
    expect.any(Function),
  );
  valid = false;
  await act(async () => chosen.resolve());
  expect(await screen.findByRole("alert")).toBeInTheDocument();
  expect(mocks.upload).not.toHaveBeenCalled();
});

it("attaches once using the latest body-driven callback when upload completes", async () => {
  const uploaded = deferred<FeedbackAttachment>();
  mocks.upload.mockReturnValue(uploaded.promise);
  render(<Form draftStorageKey="refs-latest-body" />);
  fireEvent.click(screen.getByRole("button", { name: "範囲 1 を画像にする" }));
  await waitFor(() => expect(mocks.upload).toHaveBeenCalledOnce());
  expect(screen.getByRole("button", { name: "送信する" })).toBeDisabled();
  fireEvent.change(screen.getByRole("textbox", { name: "本文" }), {
    target: { value: "edited while uploading" },
  });
  await act(async () => uploaded.resolve(attachment));
  expect(screen.getByLabelText("添付した件数")).toHaveTextContent("1");
  expect(screen.getByLabelText("添付時の本文")).toHaveTextContent(
    "edited while uploading",
  );
  expect(screen.getByRole("button", { name: "送信する" })).toBeEnabled();
});

it("uses the current resolver after navigation rerenders during the native chooser", async () => {
  const chosen = deferred<void>();
  mocks.capture.mockImplementation(
    async (_target, _number, _signal, validate) => {
      await chosen.promise;
      if (!validate()) throw new Error("Source frame changed");
      return image;
    },
  );
  mocks.upload.mockResolvedValue(attachment);
  const onAttachment = vi.fn();
  const annotations = [region()];
  const { rerender } = render(
    <AnnotationReferences
      annotations={annotations}
      resolveAnnotation={(annotation) => annotation.rect}
      onAttachment={onAttachment}
    />,
  );
  fireEvent.click(screen.getByRole("button", { name: "範囲 1 を画像にする" }));
  rerender(
    <AnnotationReferences
      annotations={annotations}
      resolveAnnotation={() => undefined}
      onAttachment={onAttachment}
    />,
  );
  await act(async () => chosen.resolve());
  expect(mocks.upload).not.toHaveBeenCalled();
  expect(onAttachment).not.toHaveBeenCalled();
  expect(screen.getByRole("alert")).toBeInTheDocument();
});

it("restores a pending region upload only for the same draft and ignores an aborted late result", async () => {
  const uploaded = deferred<FeedbackAttachment>();
  mocks.upload.mockReturnValueOnce(uploaded.promise);
  const original = render(<Form draftStorageKey="refs-restored-owner" />);
  fireEvent.click(screen.getByRole("button", { name: "範囲 1 を画像にする" }));
  await waitFor(() => expect(mocks.upload).toHaveBeenCalledOnce());
  const signal = mocks.upload.mock.calls[0][1] as AbortSignal;
  original.unmount();
  expect(signal.aborted).toBe(true);

  const other = render(<Form draftStorageKey="refs-other-owner" />);
  expect(
    screen.queryByRole("button", { name: "画像の添付を再試行" }),
  ).not.toBeInTheDocument();
  expect(screen.getByRole("button", { name: "送信する" })).toBeEnabled();
  other.unmount();

  render(<Form draftStorageKey="refs-restored-owner" />);
  expect(
    screen.getByRole("button", { name: "画像の添付を再試行" }),
  ).toBeEnabled();
  expect(screen.getByRole("button", { name: "送信する" })).toBeDisabled();
  await act(async () => uploaded.resolve(attachment));
  expect(screen.getByLabelText("添付した件数")).toHaveTextContent("0");
  fireEvent.change(screen.getByRole("textbox", { name: "本文" }), {
    target: { value: "resumed report" },
  });
  mocks.upload.mockResolvedValueOnce(attachment);
  fireEvent.click(screen.getByRole("button", { name: "画像の添付を再試行" }));
  await waitFor(() =>
    expect(screen.getByLabelText("添付した件数")).toHaveTextContent("1"),
  );
  expect(mocks.upload).toHaveBeenCalledTimes(2);
  expect(mocks.upload.mock.calls[1][0]).toBe(image);
  expect(screen.getByLabelText("添付時の本文")).toHaveTextContent(
    "resumed report",
  );
  expect(screen.getByRole("button", { name: "送信する" })).toBeEnabled();
});

it("blocks a restored upload retry at five attachments and allows discarding its pending image", async () => {
  mocks.upload.mockRejectedValueOnce(new Error("offline"));
  const key = "refs-full-attachments";
  const first = render(
    <AnnotationReferences
      annotations={[region()]}
      draftStorageKey={key}
      resolveAnnotation={(annotation) => annotation.rect}
      onAttachment={vi.fn()}
    />,
  );
  fireEvent.click(screen.getByRole("button", { name: "範囲 1 を画像にする" }));
  await screen.findByRole("button", { name: "画像の添付を再試行" });
  first.unmount();
  const busy = vi.fn();
  render(
    <AnnotationReferences
      annotations={[region()]}
      draftStorageKey={key}
      attachmentCount={5}
      resolveAnnotation={(annotation) => annotation.rect}
      onAttachment={vi.fn()}
      onBusyChange={busy}
    />,
  );
  const retry = screen.getByRole("button", { name: "画像の添付を再試行" });
  expect(retry).toBeDisabled();
  fireEvent.click(retry);
  expect(mocks.upload).toHaveBeenCalledOnce();
  fireEvent.click(screen.getByRole("button", { name: "画像を取り除く" }));
  expect(
    screen.queryByRole("button", { name: "画像の添付を再試行" }),
  ).not.toBeInTheDocument();
  expect(busy).toHaveBeenLastCalledWith(false);
});

it("shows read-only references without camera or editing actions", () => {
  render(<AnnotationReferences annotations={[region()]} />);
  expect(screen.getByText("言及された場所")).toBeInTheDocument();
  expect(
    screen.queryByRole("button", { name: "範囲 1 を画像にする" }),
  ).not.toBeInTheDocument();
  expect(
    screen.queryByRole("button", { name: /取り除く/ }),
  ).not.toBeInTheDocument();
  expect(
    screen.getByRole("button", { name: "場所 1: 選択した範囲" }),
  ).toBeDisabled();
});
