// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
} from "@testing-library/react";
import { useState } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { codePointLength } from "../../lib/text-length";
import type { PollInput } from "../model";
import { useMessaging } from "../store";
import { PollCreateDialog } from "./poll-create-dialog";

const placeKey = "channel:general" as const;

function fillPoll() {
  fireEvent.change(screen.getByLabelText("質問"), {
    target: { value: "  いつにしますか？  " },
  });
  fireEvent.change(screen.getByLabelText("選択肢 1"), {
    target: { value: " 今日 " },
  });
  fireEvent.change(screen.getByLabelText("選択肢 2"), {
    target: { value: " 明日 " },
  });
}

function DialogHarness({
  onSubmit,
}: {
  onSubmit: (poll: PollInput) => boolean;
}) {
  const [open, setOpen] = useState(false);
  return (
    <>
      <button type="button" onClick={() => setOpen(true)}>
        投票を開く
      </button>
      {open ? (
        <PollCreateDialog onClose={() => setOpen(false)} onSubmit={onSubmit} />
      ) : null}
    </>
  );
}

beforeEach(() => {
  useMessaging.setState((state) => ({
    activePlaceKey: placeKey,
    draftAttachmentsByPlace: {},
    capabilities: { ...state.capabilities, polls: true },
  }));
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

describe("PollCreateDialog", () => {
  it("trims canonical fields and derives a deadline from a fixed chip", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-08-30T00:00:00Z"));
    const onSubmit = vi.fn((_poll: PollInput) => true);
    const onClose = vi.fn();
    render(<PollCreateDialog onClose={onClose} onSubmit={onSubmit} />);

    fillPoll();
    fireEvent.click(
      screen.getByRole("button", { name: "複数選べるようにする" }),
    );
    fireEvent.click(screen.getByRole("button", { name: "1時間" }));
    fireEvent.click(screen.getByRole("button", { name: "投票を送信" }));

    expect(onSubmit).toHaveBeenCalledWith({
      question: "いつにしますか？",
      options: ["今日", "明日"],
      allowMulti: true,
      closesAt: Date.parse("2026-08-30T01:00:00Z"),
    });
    expect(onClose).toHaveBeenCalledOnce();
  });

  it("keeps inputs intact and explains why attachment plus poll is disabled", () => {
    const onSubmit = vi.fn(() => true);
    render(<PollCreateDialog onClose={vi.fn()} onSubmit={onSubmit} />);
    fillPoll();

    act(() => {
      useMessaging.setState({
        draftAttachmentsByPlace: {
          [placeKey]: [
            {
              clientNonce: "attachment-1",
              filename: "agenda.pdf",
              sizeBytes: 1,
              contentType: "application/pdf",
              status: "uploading",
            },
          ],
        },
      });
    });

    expect(screen.getByRole("button", { name: "投票を送信" })).toBeDisabled();
    expect(
      screen.getByText(
        "添付付きの投票は作成できません。添付を外すと送信できます。",
      ),
    ).toBeVisible();
    expect(screen.getByLabelText("質問")).toHaveValue("  いつにしますか？  ");
    expect(onSubmit).not.toHaveBeenCalled();
  });

  it("keeps the dialog draft if poll capability disappears", () => {
    const onSubmit = vi.fn((_poll: PollInput) => true);
    render(<PollCreateDialog onClose={vi.fn()} onSubmit={onSubmit} />);
    fillPoll();

    act(() => {
      useMessaging.setState((state) => ({
        capabilities: { ...state.capabilities, polls: false },
      }));
    });

    expect(
      screen.getByText("この接続では投票を送信できません。"),
    ).toBeVisible();
    expect(screen.getByRole("button", { name: "投票を送信" })).toBeDisabled();
    expect(screen.getByLabelText("質問")).toHaveValue("  いつにしますか？  ");
    expect(onSubmit).not.toHaveBeenCalled();
  });

  it("warns about duplicates and supports the full 2–10 option range", () => {
    render(<PollCreateDialog onClose={vi.fn()} onSubmit={vi.fn(() => true)} />);
    fillPoll();
    fireEvent.change(screen.getByLabelText("選択肢 2"), {
      target: { value: "今日" },
    });
    expect(screen.getByRole("alert")).toHaveTextContent(
      "同じ選択肢は作れません",
    );
    expect(screen.getByRole("button", { name: "投票を送信" })).toBeDisabled();

    for (let index = 3; index <= 10; index += 1) {
      fireEvent.click(screen.getByRole("button", { name: "選択肢を追加" }));
      expect(screen.getByLabelText(`選択肢 ${index}`)).toBeInTheDocument();
    }
    expect(
      screen.queryByRole("button", { name: "選択肢を追加" }),
    ).not.toBeInTheDocument();
  });

  it("clamps emoji by Unicode code point without splitting surrogate pairs", () => {
    const onSubmit = vi.fn((_poll: PollInput) => true);
    render(<PollCreateDialog onClose={vi.fn()} onSubmit={onSubmit} />);
    const longQuestion = "😀".repeat(501);
    const longOption = "🧭".repeat(201);
    fireEvent.change(screen.getByLabelText("質問"), {
      target: { value: longQuestion },
    });
    fireEvent.change(screen.getByLabelText("選択肢 1"), {
      target: { value: longOption },
    });
    fireEvent.change(screen.getByLabelText("選択肢 2"), {
      target: { value: "明日" },
    });
    fireEvent.click(screen.getByRole("button", { name: "投票を送信" }));

    const poll = onSubmit.mock.calls[0]?.[0];
    expect(codePointLength(poll?.question ?? "")).toBe(500);
    expect(codePointLength(poll?.options[0] ?? "")).toBe(200);
    expect(poll?.question.endsWith("😀")).toBe(true);
    expect(poll?.options[0]?.endsWith("🧭")).toBe(true);
  });

  it("traps focus, ignores IME Escape, closes on committed Escape, and restores the trigger", async () => {
    render(<DialogHarness onSubmit={() => true} />);
    const trigger = screen.getByRole("button", { name: "投票を開く" });
    trigger.focus();
    fireEvent.click(trigger);
    const question = screen.getByLabelText("質問");
    await vi.waitFor(() => expect(question).toHaveFocus());
    fillPoll();

    const close = screen.getByRole("button", { name: "投票作成を閉じる" });
    close.focus();
    fireEvent.keyDown(close, { key: "Tab", shiftKey: true });
    expect(screen.getByRole("button", { name: "投票を送信" })).toHaveFocus();

    fireEvent.keyDown(question, {
      key: "Escape",
      isComposing: true,
      keyCode: 229,
    });
    expect(screen.getByRole("dialog", { name: "投票を作成" })).toBeVisible();

    fireEvent.keyDown(question, { key: "Escape" });
    await vi.waitFor(() =>
      expect(
        screen.queryByRole("dialog", { name: "投票を作成" }),
      ).not.toBeInTheDocument(),
    );
    expect(trigger).toHaveFocus();
  });
});
