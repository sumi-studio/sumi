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

  it("closes a tilde fence only with a tilde fence", () => {
    const closed = "~~~\ncode\n~~~";
    const mismatched = "~~~\ncode\n```";
    expect(isInsideUnclosedCodeFence(closed, closed.length)).toBe(false);
    expect(isInsideUnclosedCodeFence(mismatched, mismatched.length)).toBe(true);
  });

  it("does not let a tilde line close a backtick fence", () => {
    const value = "```\ncode\n~~~";
    expect(isInsideUnclosedCodeFence(value, value.length)).toBe(true);
  });

  it("requires a closing fence at least as long as its opener", () => {
    const short = "````\ncode\n```";
    const long = "```\ncode\n````";
    expect(isInsideUnclosedCodeFence(short, short.length)).toBe(true);
    expect(isInsideUnclosedCodeFence(long, long.length)).toBe(false);
  });

  it("does not treat a fence with an info string as closing", () => {
    const value = "```\ncode\n```ts";
    expect(isInsideUnclosedCodeFence(value, value.length)).toBe(true);
  });

  it("allows whitespace and up to three-space indent on closing fences", () => {
    const value = "```ts\ncode\n   ```   ";
    expect(isInsideUnclosedCodeFence(value, value.length)).toBe(false);
  });

  it("reopens after a closed fence", () => {
    const value = "```\na\n```\ntext\n```";
    expect(isInsideUnclosedCodeFence(value, value.length)).toBe(true);
  });

  it("ignores a backtick opener whose info string contains a backtick", () => {
    const value = "```a`b\ncode";
    expect(isInsideUnclosedCodeFence(value, value.length)).toBe(false);
  });
});
