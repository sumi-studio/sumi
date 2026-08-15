import { beforeEach, describe, expect, it, vi } from "vitest";
import { rehypeCompactKatex } from "../../../../packages/ui/src/ai-elements/compact-message-math";

const katexTransform = vi.fn();

interface TestNode {
  type: string;
  tagName?: string;
  properties?: Record<string, unknown>;
  value?: string;
  children?: TestNode[];
}

function inlineMath(source: string): TestNode {
  return {
    type: "element",
    tagName: "code",
    properties: { className: ["language-math", "math-inline"] },
    children: [{ type: "text", value: source }],
  };
}

function text(value: string): TestNode {
  return { type: "text", value };
}

function transform(tree: TestNode) {
  const transformer = rehypeCompactKatex(katexTransform);
  transformer(tree, {});
}

function textContent(node: TestNode): string {
  if (node.type === "text") return node.value ?? "";
  return (node.children ?? []).map(textContent).join("");
}

function sourceModeMarkers(node: TestNode): TestNode[] {
  const own =
    node.properties?.["data-math-source-mode"] === "budget" ? [node] : [];
  return [...own, ...(node.children ?? []).flatMap(sourceModeMarkers)];
}

describe("compact math pre-render guard", () => {
  beforeEach(() => {
    katexTransform.mockClear();
  });

  it("keeps an ordinary formula on the KaTeX path", () => {
    const tree: TestNode = { type: "root", children: [inlineMath("x")] };

    transform(tree);

    expect(katexTransform).toHaveBeenCalledTimes(1);
  });

  it("rejects the exact 512-square-root reproducer before KaTeX", () => {
    const source = String.raw`\sqrt{x}`.repeat(512);
    const tree: TestNode = { type: "root", children: [inlineMath(source)] };

    transform(tree);

    expect(source).toHaveLength(4_096);
    expect(katexTransform).not.toHaveBeenCalled();
    expect(textContent(tree)).toBe(`$${source}$`);
    expect(sourceModeMarkers(tree)).toHaveLength(1);
    expect(sourceModeMarkers(tree)[0]?.properties?.title).toBe(
      "数式の量が多いため原文のまま表示しています",
    );
  });

  it("rejects a rendered prefix plus square roots before the first KaTeX call", () => {
    const children: TestNode[] = [];
    for (let index = 0; index < 250; index += 1) {
      if (index > 0) children.push(text(" "));
      children.push(inlineMath("x"));
    }
    const roots = String.raw`\sqrt{x}`.repeat(50);
    children.push(text(" "), inlineMath(roots));
    const tree: TestNode = {
      type: "root",
      children: [{ type: "element", tagName: "p", children }],
    };

    transform(tree);

    expect(katexTransform).not.toHaveBeenCalled();
    expect(tree.children?.[0]?.children).toHaveLength(1);
    expect(textContent(tree)).toBe(
      `${Array.from({ length: 250 }, () => "$x$").join(" ")} $${roots}$`,
    );
    expect(sourceModeMarkers(tree)).toHaveLength(1);
  });

  it.each([
    ["fractions", String.raw`\frac{1}{2}`.repeat(292)],
    ["scripts", "x^{y}_{z}".repeat(227)],
    ["text", String.raw`\text{x}`.repeat(512)],
    ["accents", String.raw`\hat{x}`.repeat(512)],
    ["overbraces", String.raw`\overbrace{x}`.repeat(315)],
    [
      "arrays",
      `${String.raw`\begin{matrix}`}${Array.from({ length: 200 }, () => "x&x&x&x").join(String.raw`\\`)}${String.raw`\end{matrix}`}`,
    ],
  ])("rejects adversarial built-in %s input before KaTeX", (_name, source) => {
    const tree: TestNode = { type: "root", children: [inlineMath(source)] };

    transform(tree);

    expect(source.length).toBeLessThanOrEqual(4_096);
    expect(katexTransform).not.toHaveBeenCalled();
    expect(textContent(tree)).toBe(`$${source}$`);
    expect(sourceModeMarkers(tree)).toHaveLength(1);
  });
});
