import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const GLOBAL_STYLES = new URL(
  "../../../packages/ui/src/styles/globals.css",
  import.meta.url,
);

test("interaction highlights expose Tailwind accent color tokens", async () => {
  const css = await readFile(GLOBAL_STYLES, "utf8");

  assert.match(css, /--color-accent:\s*var\(--interactive-hover\);/);
  assert.match(css, /--color-accent-foreground:\s*var\(--foreground\);/);
});
