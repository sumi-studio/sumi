// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { ParticipantAvatar } from "./participant-avatar";

describe("ParticipantAvatar", () => {
  it("画像を読めなければ壊れた画像ではなく頭文字へ戻す", () => {
    const { container } = render(
      <ParticipantAvatar
        participantKey="human:h1"
        name="薄明色の忘れ路"
        src="/messaging/attachments/missing"
      />,
    );

    const image = container.querySelector("img");
    expect(image).not.toBeNull();
    fireEvent.error(image as HTMLImageElement);

    expect(container.querySelector("img")).toBeNull();
    expect(screen.getByText("薄")).toBeInTheDocument();
  });
});
