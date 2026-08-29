// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { copyText } from "./clipboard";

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("copyText", () => {
  it("clipboard APIが使えるならそれで書き、成功を返す", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    vi.stubGlobal("navigator", { ...navigator, clipboard: { writeText } });
    await expect(copyText("https://example.test/x")).resolves.toBe(true);
    expect(writeText).toHaveBeenCalledWith("https://example.test/x");
  });

  it("clipboard APIが無い環境では選択経由へ落ちる", async () => {
    vi.stubGlobal("navigator", { ...navigator, clipboard: undefined });
    const exec = vi.fn().mockReturnValue(true);
    Object.defineProperty(document, "execCommand", {
      value: exec,
      configurable: true,
      writable: true,
    });
    await expect(copyText("落ちる経路")).resolves.toBe(true);
    expect(exec).toHaveBeenCalledWith("copy");
    // 一時要素は必ず片付ける。
    expect(document.querySelectorAll("textarea").length).toBe(0);
  });

  it("clipboard APIが拒否したら代替経路の結果をそのまま返す", async () => {
    const writeText = vi.fn().mockRejectedValue(new Error("denied"));
    vi.stubGlobal("navigator", { ...navigator, clipboard: { writeText } });
    Object.defineProperty(document, "execCommand", {
      value: vi.fn().mockReturnValue(false),
      configurable: true,
      writable: true,
    });
    await expect(copyText("だめな場合")).resolves.toBe(false);
  });
});
