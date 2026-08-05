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

  it("is false once a tilde fence is closed by a tilde fence", () => {
    const value = "~~~\ncode\n~~~";
    expect(isInsideUnclosedCodeFence(value, value.length)).toBe(false);
  });
});

describe("isInsideUnclosedCodeFence closing fence rules", () => {
  it("does not let a tilde line close a backtick fence", () => {
    const value = "```\ncode\n~~~";
    expect(isInsideUnclosedCodeFence(value, value.length)).toBe(true);
  });

  it("does not let a backtick line close a tilde fence", () => {
    const value = "~~~\ncode\n```";
    expect(isInsideUnclosedCodeFence(value, value.length)).toBe(true);
  });

  it("does not let a shorter fence close a longer opener", () => {
    const value = "````\ncode\n```";
    expect(isInsideUnclosedCodeFence(value, value.length)).toBe(true);
  });

  it("lets a longer fence close a shorter opener", () => {
    const value = "```\ncode\n````";
    expect(isInsideUnclosedCodeFence(value, value.length)).toBe(false);
  });

  it("does not treat a fence with an info string as closing", () => {
    const value = "```\ncode\n```ts";
    expect(isInsideUnclosedCodeFence(value, value.length)).toBe(true);
  });

  it("allows trailing whitespace on the closing fence", () => {
    const value = "```ts\ncode\n```   ";
    expect(isInsideUnclosedCodeFence(value, value.length)).toBe(false);
  });

  it("allows up to three spaces of indent on the closing fence", () => {
    const value = "```ts\ncode\n   ```";
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
