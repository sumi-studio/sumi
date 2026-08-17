// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { useMessaging } from "../store";
import { PollCreateDialog } from "./poll-create-dialog";

afterEach(cleanup);

describe("PollCreateDialog", () => {
  it("carries the active composer draft into the poll message", () => {
    const send = vi.fn();
    useMessaging.setState({
      activePlaceKey: "channel:general",
      draftByPlace: { "channel:general": "日程を決めたいです" },
      send,
    });
    render(<PollCreateDialog onClose={vi.fn()} />);

    fireEvent.change(screen.getByLabelText("質問"), {
      target: { value: "いつにしますか？" },
    });
    fireEvent.change(screen.getByLabelText("選択肢 1"), {
      target: { value: "今日" },
    });
    fireEvent.change(screen.getByLabelText("選択肢 2"), {
      target: { value: "明日" },
    });
    fireEvent.click(screen.getByRole("button", { name: "投票を送信" }));

    expect(send).toHaveBeenCalledWith("日程を決めたいです", "normal", {
      question: "いつにしますか？",
      options: ["今日", "明日"],
      allowMulti: false,
      closesAt: null,
    });
  });

  it("keeps the poll dialog open while the composer has an attachment", () => {
    const send = vi.fn();
    const onClose = vi.fn();
    useMessaging.setState({
      activePlaceKey: "channel:general",
      draftByPlace: { "channel:general": "日程を決めたいです" },
      draftAttachmentsByPlace: {
        "channel:general": [
          {
            clientNonce: "attachment-1",
            filename: "agenda.pdf",
            sizeBytes: 1,
            contentType: "application/pdf",
            status: "uploading",
          },
        ],
      },
      send,
    });
    render(<PollCreateDialog onClose={onClose} />);

    fireEvent.change(screen.getByLabelText("質問"), {
      target: { value: "いつにしますか？" },
    });
    fireEvent.change(screen.getByLabelText("選択肢 1"), {
      target: { value: "今日" },
    });
    fireEvent.change(screen.getByLabelText("選択肢 2"), {
      target: { value: "明日" },
    });

    expect(screen.getByRole("button", { name: "投票を送信" })).toBeDisabled();
    expect(
      screen.getByText("添付付きの投票は作成できません。添付を外してから送信してください。"),
    ).toBeInTheDocument();
    expect(send).not.toHaveBeenCalled();
    expect(onClose).not.toHaveBeenCalled();
  });
});
