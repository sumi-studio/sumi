import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const GLOBAL_STYLES = new URL(
  "../../../packages/ui/src/styles/globals.css",
  import.meta.url,
);

// bg-accent / text-accent-foreground は messaging の hover・選択面の全体で
// 使われている。@theme にトークンが無いとTailwindがそのクラスを生成せず、
// ビルドは通ったままハイライトだけが消えるので、契約としてここで縛る。
test("interaction highlights expose Tailwind accent color tokens", async () => {
  const css = await readFile(GLOBAL_STYLES, "utf8");

  assert.match(css, /--color-accent:\s*var\(--interactive-hover\);/);
  assert.match(css, /--color-accent-foreground:\s*var\(--foreground\);/);
});
