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
});
