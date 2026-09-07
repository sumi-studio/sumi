import { describe, expect, it } from "vitest";
import { isInsideUnclosedCodeFence } from "./compose-fence";

describe("isInsideUnclosedCodeFence", () => {
  it.each([
    ["is false for plain text", "hello", false],
    ["is true right after opening a fence", "```ts", true],
    ["is true while typing inside an open fence", "```ts\nconst a = 1;", true],
    ["is false once the fence is closed", "```ts\nconst a = 1;\n```", false],
    ["ignores backticks that are not at line start", "code: ```inline", false],
    ["supports tilde fences", "~~~\ncode", true],
    [
      "is false once a tilde fence is closed by a tilde fence",
      "~~~\ncode\n~~~",
      false,
    ],
    [
      "does not let a tilde line close a backtick fence",
      "```\ncode\n~~~",
      true,
    ],
    [
      "does not let a backtick line close a tilde fence",
      "~~~\ncode\n```",
      true,
    ],
    [
      "does not let a shorter fence close a longer opener",
      "````\ncode\n```",
      true,
    ],
    ["lets a longer fence close a shorter opener", "```\ncode\n````", false],
    [
      "does not treat a fence with an info string as closing",
      "```\ncode\n```ts",
      true,
    ],
    [
      "allows trailing whitespace on the closing fence",
      "```ts\ncode\n```   ",
      false,
    ],
    [
      "allows up to three spaces of indent on the closing fence",
      "```ts\ncode\n   ```",
      false,
    ],
    ["reopens after a closed fence", "```\na\n```\ntext\n```", true],
    [
      "ignores a backtick opener whose info string contains a backtick",
      "```a`b\ncode",
      false,
    ],
  ])("%s", (_description, value, expected) => {
    expect(isInsideUnclosedCodeFence(value, value.length)).toBe(expected);
  });

  it("uses the caret position, not the whole value", () => {
    const value = "```ts\nconst a = 1;\n```";
    expect(isInsideUnclosedCodeFence(value, "```ts\nconst".length)).toBe(true);
  });
});
