// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { ParticipantAvatar } from "./participant-avatar";

afterEach(cleanup);

describe("ParticipantAvatar", () => {
  it("srcが無ければイニシャルを描く", () => {
    const { container } = render(
      <ParticipantAvatar participantKey="human:1" name="yohaku" />,
    );
    expect(container.querySelector("img")).toBeNull();
    expect(screen.getByText("Y")).toBeInTheDocument();
  });

  it("srcがあれば顔写真を描く", () => {
    const { container } = render(
      <ParticipantAvatar
        participantKey="human:1"
        name="yohaku"
        src="/messaging/attachments/a1"
      />,
    );
    expect(container.querySelector("img")).toHaveAttribute(
      "src",
      "/messaging/attachments/a1",
    );
    expect(screen.queryByText("Y")).toBeNull();
  });

  it("読めない画像は壊れた枠ではなくイニシャルへ落ちる", () => {
    const { container } = render(
      <ParticipantAvatar
        participantKey="human:1"
        name="yohaku"
        src="/messaging/attachments/gone"
      />,
    );
    const image = container.querySelector("img");
    if (!image) throw new Error("expected an image before the load failure");

    fireEvent.error(image);

    expect(container.querySelector("img")).toBeNull();
    expect(screen.getByText("Y")).toBeInTheDocument();
  });

  it("差し替わったsrcは改めて試す", () => {
    const { container, rerender } = render(
      <ParticipantAvatar
        participantKey="human:1"
        name="yohaku"
        src="/messaging/attachments/gone"
      />,
    );
    const image = container.querySelector("img");
    if (!image) throw new Error("expected an image before the load failure");
    fireEvent.error(image);

    rerender(
      <ParticipantAvatar
        participantKey="human:1"
        name="yohaku"
        src="/messaging/attachments/new"
      />,
    );

    expect(container.querySelector("img")).toHaveAttribute(
      "src",
      "/messaging/attachments/new",
    );
  });
});
