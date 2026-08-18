// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ParticipantStatus } from "../model";
import { useMessaging } from "../store";
import { StatusMenu } from "./status-menu";

const SELF = { kind: "human", humanId: "human-a" } as const;
const setStatus = vi.fn();
const realSetStatus = useMessaging.getState().setStatus;

function setSelfStatus(status: ParticipantStatus | undefined) {
  useMessaging.setState({
    self: SELF,
    selfKey: "human:human-a",
    membersByKey: {
      "human:human-a": { participant: SELF, displayName: "Alice", tagline: "" },
    },
    statusByKey: status === undefined ? {} : { "human:human-a": status },
    capabilities: {
      status: true,
      replyLater: true,
      reactions: true,
      notifications: true,
    },
    setStatus,
  });
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(Date.parse("2026-08-18T09:00:00Z"));
});

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
  vi.useRealTimers();
  useMessaging.setState({ setStatus: realSetStatus });
});

describe("StatusMenu", () => {
  it("期限を選ぶ前に、その期限が切れたときどこへ戻るかを見せる", () => {
    setSelfStatus({
      participant: SELF,
      status: "away",
      note: "在宅です",
      expiresAt: null,
      baseStatus: null,
      baseNote: "",
    });
    render(<StatusMenu />);

    fireEvent.click(
      screen.getByRole("button", { name: /^Alice(?!のプロフィール)/ }),
    );
    fireEvent.click(screen.getByRole("button", { name: /取り込み中/ }));

    expect(screen.getByText("期限が来たら「離席中」に戻ります")).toBeVisible();

    fireEvent.click(screen.getByRole("menuitem", { name: "1時間" }));

    expect(setStatus).toHaveBeenCalledWith(
      "busy",
      "在宅です",
      Date.parse("2026-08-18T10:00:00Z"),
    );
  });

  it("戻る先が無いときは、期限で宣言そのものが終わると言う", () => {
    setSelfStatus(undefined);
    render(<StatusMenu />);

    fireEvent.click(
      screen.getByRole("button", { name: /^Alice(?!のプロフィール)/ }),
    );
    fireEvent.click(screen.getByRole("button", { name: /離席中/ }));

    expect(
      screen.getByText("期限が来たら申告そのものが解除されます"),
    ).toBeVisible();

    // 「解除するまで」は期限なし。
    fireEvent.click(screen.getByRole("menuitem", { name: "解除するまで" }));
    expect(setStatus).toHaveBeenCalledWith("away", "", null);
  });

  it("ひとことだけ書き替えたときに、いまの期限を黙って外さない", () => {
    const until = Date.parse("2026-08-18T10:30:00Z");
    setSelfStatus({
      participant: SELF,
      status: "busy",
      note: "会議中",
      expiresAt: until,
      baseStatus: "away",
      baseNote: "在宅です",
    });
    render(<StatusMenu />);

    // アカウント行は、いま出ている申告と期限をそのまま読める形で見せる。
    expect(screen.getByText(/取り込み中 — 会議中/)).toBeVisible();

    fireEvent.click(
      screen.getByRole("button", { name: /^Alice(?!のプロフィール)/ }),
    );
    const note = screen.getByRole("textbox");
    fireEvent.change(note, { target: { value: "電話中" } });
    fireEvent.keyDown(note, { key: "Enter" });

    expect(setStatus).toHaveBeenCalledWith("busy", "電話中", until);
  });

  it("IME変換確定のEnterでは宣言しない", () => {
    setSelfStatus(undefined);
    render(<StatusMenu />);

    fireEvent.click(
      screen.getByRole("button", { name: /^Alice(?!のプロフィール)/ }),
    );
    const note = screen.getByRole("textbox");
    fireEvent.change(note, { target: { value: "会議" } });
    fireEvent.keyDown(note, { key: "Enter", isComposing: true });
    fireEvent.keyDown(note, { key: "Enter", keyCode: 229 });

    expect(setStatus).not.toHaveBeenCalled();
  });
});
