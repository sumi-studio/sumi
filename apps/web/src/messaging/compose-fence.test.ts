import { describe, expect, it } from "vitest";
import { isInsideUnclosedCodeFence } from "./compose-fence";

describe("isInsideUnclosedCodeFence", () => {
  it("is false for plain text", () => {
    expect(isInsideUnclosedCodeFence("hello", 5)).toBe(false);
  });

  it("is true right after opening a fence", () => {
    const value = "```ts";
    expect(isInsideUnclosedCodeFence(value, value.length)).toBe(true);
  });

  it("is true while typing inside an open fence", () => {
    const value = "```ts\nconst a = 1;";
    expect(isInsideUnclosedCodeFence(value, value.length)).toBe(true);
  });

  it("is false once the fence is closed", () => {
    const value = "```ts\nconst a = 1;\n```";
    expect(isInsideUnclosedCodeFence(value, value.length)).toBe(false);
  });

  it("uses the caret position, not the whole value", () => {
    const value = "```ts\nconst a = 1;\n```";
    expect(isInsideUnclosedCodeFence(value, "```ts\nconst".length)).toBe(true);
  });

  it("ignores backticks that are not at line start", () => {
    const value = "code: ```inline";
    expect(isInsideUnclosedCodeFence(value, value.length)).toBe(false);
  });

  it("supports tilde fences", () => {
    const value = "~~~\ncode";
    expect(isInsideUnclosedCodeFence(value, value.length)).toBe(true);
  });
});
