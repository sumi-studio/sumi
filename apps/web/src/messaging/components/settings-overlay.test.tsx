// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { MemberProfile, ParticipantRef } from "../model";
import { participantKey } from "../model";
import { useMessaging } from "../store";
import { SettingsOverlay, useSettingsOverlay } from "./settings-overlay";

const human: ParticipantRef = { kind: "human", humanId: "h1" };
const humanKey = participantKey(human);

const self: MemberProfile = {
  participant: human,
  displayName: "余白",
  tagline: "創業・デザイン",
};

const updateProfile = vi.fn();
const uploadAttachment = vi.fn();

beforeEach(() => {
  updateProfile.mockResolvedValue(undefined);
  uploadAttachment.mockResolvedValue({
    attachmentId: "att-1",
    filename: "face.png",
    mime: "image/png",
    size: 12,
    url: "blob:face",
  });
  useMessaging.setState({
    ready: true,
    self: human,
    selfKey: humanKey,
    membersByKey: { [humanKey]: self },
    updateProfile,
    uploadAttachment,
  });
  useSettingsOverlay.setState({ open: true, section: "profile" });
});

afterEach(() => {
  cleanup();
  useSettingsOverlay.setState({ open: false, section: "profile" });
  vi.clearAllMocks();
});

describe("SettingsOverlay", () => {
  it("閉じているときは何も描かない", () => {
    useSettingsOverlay.setState({ open: false });
    render(<SettingsOverlay />);

    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("現在の名乗りを出し、変えたぶんだけ保存する", async () => {
    render(<SettingsOverlay />);

    const name = screen.getByDisplayValue("余白");
    // 変更前は保存する理由がないので押せない。
    expect(screen.getByRole("button", { name: "保存" })).toBeDisabled();

    fireEvent.change(name, { target: { value: "余白（改）" } });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));

    await vi.waitFor(() => {
      expect(updateProfile).toHaveBeenCalledWith({
        displayName: "余白（改）",
        tagline: "創業・デザイン",
        avatarAttachmentId: "",
        bannerAttachmentId: "",
      });
    });
  });

  it("表示名を空にしたままでは保存させない", () => {
    render(<SettingsOverlay />);

    fireEvent.change(screen.getByDisplayValue("余白"), {
      target: { value: "   " },
    });

    expect(screen.getByRole("button", { name: "保存" })).toBeDisabled();
    expect(updateProfile).not.toHaveBeenCalled();
  });

  it("保存に失敗したら伝えて、入力を捨てない", async () => {
    updateProfile.mockRejectedValue(new Error("boom"));
    render(<SettingsOverlay />);

    fireEvent.change(screen.getByDisplayValue("創業・デザイン"), {
      target: { value: "設計" },
    });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));

    await screen.findByText(/保存できませんでした/);
    expect(screen.getByDisplayValue("設計")).toBeInTheDocument();
  });

  it("画像は送信前の添付と同じ経路で預け、保存でプロフィールに結びつける", async () => {
    render(<SettingsOverlay />);

    const file = new File([new Uint8Array([1, 2, 3])], "face.png", {
      type: "image/png",
    });
    fireEvent.change(screen.getByLabelText("アバター"), {
      target: { files: [file] },
    });

    await vi.waitFor(() => {
      expect(uploadAttachment).toHaveBeenCalledWith(file);
      expect(screen.getByRole("button", { name: "保存" })).toBeEnabled();
    });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));

    await vi.waitFor(() => {
      expect(updateProfile).toHaveBeenCalledWith(
        expect.objectContaining({ avatarAttachmentId: "att-1" }),
      );
    });
  });

  it("画像でないファイルは預けずに断る", async () => {
    render(<SettingsOverlay />);

    fireEvent.change(screen.getByLabelText("アバター"), {
      target: {
        files: [new File(["notes"], "notes.txt", { type: "text/plain" })],
      },
    });

    await screen.findByText("画像ファイルを選んでください");
    expect(uploadAttachment).not.toHaveBeenCalled();
  });

  it("参加者IDを確認できるが、種別を文字として出さない", () => {
    useSettingsOverlay.setState({ section: "account" });
    render(<SettingsOverlay />);

    expect(screen.getByText("h1")).toBeInTheDocument();
    expect(document.body.textContent).not.toMatch(
      /personality_agent|bot|ボット/i,
    );
  });

  it("Escで閉じる", () => {
    render(<SettingsOverlay />);

    fireEvent.keyDown(window, { key: "Escape" });

    expect(useSettingsOverlay.getState().open).toBe(false);
  });
});
