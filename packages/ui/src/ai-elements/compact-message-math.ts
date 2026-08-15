import rehypeKatex from "rehype-katex";
import remarkMath from "remark-math";

interface MdNode {
  type: string;
  value?: string;
  children?: MdNode[];
  data?: {
    hName?: string;
    hProperties?: Record<string, unknown>;
    hChildren?: HastNode[];
  };
  position?: {
    start?: { offset?: number };
    end?: { offset?: number };
  };
}

interface HastNode {
  type: string;
  tagName?: string;
  properties?: Record<string, unknown>;
  value?: string;
  children?: HastNode[];
}

interface MathScope {
  display: boolean;
  source: string;
}

const MAX_TEX_SOURCE_LENGTH = 4_096;
const MAX_TEX_NESTING_DEPTH = 64;
const MAX_TEX_TOKENS = 2_048;

// KaTeX supports these author-controlled assignment primitives. Even with a
// finite maxExpand, a short definition body can be repeated enough times to
// allocate a much larger render tree than the source limits imply.
const AUTHOR_MACRO_PRIMITIVES = new Set([
  "\\def",
  "\\gdef",
  "\\edef",
  "\\xdef",
  "\\let",
  "\\futurelet",
  "\\newcommand",
  "\\renewcommand",
  "\\providecommand",
]);

export const REMARK_MATH_OPTIONS = { singleDollarTextMath: false } as const;

function dollarRun(raw: string): number {
  let count = 0;
  while (count < raw.length && raw[count] === "$") count += 1;
  return count;
}

function rawSource(source: string, node: MdNode): string | null {
  const start = node.position?.start?.offset;
  const end = node.position?.end?.offset;
  if (typeof start !== "number" || typeof end !== "number") return null;
  const raw = source.slice(start, end);
  return raw.startsWith("$") && raw.endsWith("$") ? raw : null;
}

function displayMathNode(value: string): MdNode {
  return {
    type: "math",
    value,
    data: {
      hName: "pre",
      hChildren: [
        {
          type: "element",
          tagName: "code",
          properties: { className: ["language-math", "math-display"] },
          children: [{ type: "text", value }],
        },
      ],
    },
  };
}

/**
 * remark-math treats one-line $$...$$ as inline math. In chat, a line made
 * only of double-dollar math is display intent, while surrounding text on the
 * same line keeps it inline.
 */
function splitDisplayMathLines(
  paragraph: MdNode,
  source: string,
): MdNode[] | null {
  const children = paragraph.children ?? [];
  const output: MdNode[] = [];
  let line: MdNode[] = [];
  let promoted = false;

  const flush = () => {
    while (line[0]?.type === "break") line.shift();
    while (line.at(-1)?.type === "break") line.pop();
    if (line.length > 0) output.push({ type: "paragraph", children: line });
    line = [];
  };

  for (const [index, child] of children.entries()) {
    const previous = children[index - 1];
    const next = children[index + 1];
    const aloneOnLine =
      (previous === undefined || previous.type === "break") &&
      (next === undefined || next.type === "break");
    const raw = child.type === "inlineMath" ? rawSource(source, child) : null;
    if (aloneOnLine && raw !== null && dollarRun(raw) >= 2) {
      flush();
      output.push(displayMathNode(child.value ?? ""));
      promoted = true;
      continue;
    }
    line.push(child);
  }
  flush();
  return promoted ? output : null;
}

/** Promote standalone one-line double-dollar math to display layout. */
export function remarkCompactMath() {
  return (tree: MdNode, file: { value?: unknown }) => {
    const source = typeof file.value === "string" ? file.value : "";
    if (source === "") return;

    const promoteDisplays = (node: MdNode) => {
      if (!node.children) return;
      const next: MdNode[] = [];
      let changed = false;
      for (const child of node.children) {
        if (child.type === "paragraph") {
          const split = splitDisplayMathLines(child, source);
          if (split) {
            next.push(...split);
            changed = true;
            continue;
          }
        }
        promoteDisplays(child);
        next.push(child);
      }
      if (changed) node.children = next;
    };
    promoteDisplays(tree);
  };
}

function hastClassNames(node: HastNode): string[] {
  const value = node.properties?.className;
  if (Array.isArray(value)) return value.map(String);
  if (typeof value === "string") return value.split(/\s+/);
  return [];
}

function hastText(node: HastNode): string {
  if (node.type === "text") return node.value ?? "";
  return (node.children ?? []).map(hastText).join("");
}

function mathScope(node: HastNode): MathScope | null {
  if (node.type !== "element") return null;
  const classes = hastClassNames(node);
  if (
    node.tagName === "code" &&
    (classes.includes("math-inline") || classes.includes("math-display"))
  ) {
    return {
      display: classes.includes("math-display"),
      source: hastText(node),
    };
  }
  if (node.tagName !== "pre") return null;
  const code = node.children?.find(
    (child) =>
      child.type === "element" &&
      child.tagName === "code" &&
      hastClassNames(child).includes("language-math"),
  );
  return code ? { display: true, source: hastText(code) } : null;
}

type MathLimit = "length" | "depth" | "tokens" | "macro";

function texLimit(source: string): MathLimit | null {
  if (source.length > MAX_TEX_SOURCE_LENGTH) return "length";
  let depth = 0;
  let tokens = 0;
  for (let index = 0; index < source.length; index += 1) {
    const character = source.charAt(index);
    if (/\s/.test(character)) continue;
    if (character === "%") {
      while (
        index + 1 < source.length &&
        !/[\n\r]/.test(source.charAt(index + 1))
      ) {
        index += 1;
      }
      continue;
    }
    tokens += 1;
    if (tokens > MAX_TEX_TOKENS) return "tokens";
    if (character === "\\") {
      const commandStart = index;
      if (/[A-Za-z@]/.test(source[index + 1] ?? "")) {
        while (/[A-Za-z@]/.test(source[index + 1] ?? "")) index += 1;
      } else if (source[index + 1] !== undefined) {
        index += 1;
      }
      if (AUTHOR_MACRO_PRIMITIVES.has(source.slice(commandStart, index + 1))) {
        return "macro";
      }
      continue;
    }
    if (character === "{") {
      depth += 1;
      if (depth > MAX_TEX_NESTING_DEPTH) return "depth";
    } else if (character === "}") {
      depth = Math.max(0, depth - 1);
    }
  }
  return null;
}

function mathFallback(
  source: string,
  display: boolean,
  reason: MathLimit | "render",
): HastNode {
  const error: HastNode = {
    type: "element",
    tagName: "span",
    properties: {
      className: ["katex-error"],
      style: "color:var(--destructive)",
      title:
        reason === "render"
          ? "数式を描画できませんでした"
          : reason === "macro"
            ? "数式内でのコマンド定義は使用できません"
            : "数式が表示上限を超えています",
      "data-math-fallback": reason,
    },
    children: [{ type: "text", value: source }],
  };
  return {
    type: "element",
    tagName: display ? "div" : "span",
    properties: display
      ? {
          className: "my-1 max-w-full overflow-x-auto",
          "data-math-display": "",
        }
      : {
          className: "inline-block max-w-full overflow-x-auto align-baseline",
          "data-math-inline": "",
        },
    children: [error],
  };
}

function replaceMath(
  tree: HastNode,
  replacement: (scope: MathScope) => HastNode | null,
) {
  const walk = (node: HastNode) => {
    const children = node.children;
    if (!children) return;
    for (const [index, child] of children.entries()) {
      const scope = mathScope(child);
      const next = scope ? replacement(scope) : null;
      if (next) {
        children[index] = next;
        continue;
      }
      walk(child);
    }
  };
  walk(tree);
}

const KATEX_OPTIONS = {
  strict: "ignore" as const,
  errorColor: "var(--destructive)",
  trust: false,
  maxExpand: 1_000,
  maxSize: 20,
};

/**
 * Bound author-controlled TeX before KaTeX allocates layout nodes. KaTeX's
 * own parser errors stay visible, while unexpected plugin errors replace every
 * still-unrendered math scope with its exact source.
 */
export function rehypeCompactKatex() {
  const render = rehypeKatex(KATEX_OPTIONS) as unknown as (
    tree: HastNode,
    file: unknown,
  ) => void;
  return (tree: HastNode, file: unknown) => {
    replaceMath(tree, (scope) => {
      const limit = texLimit(scope.source);
      return limit ? mathFallback(scope.source, scope.display, limit) : null;
    });
    try {
      render(tree, file);
    } catch {
      replaceMath(tree, (scope) =>
        mathFallback(scope.source, scope.display, "render"),
      );
    }
  };
}

/** Contain both display and long inline formulae inside their message row. */
export function rehypeCompactMathLayout() {
  return (tree: HastNode) => {
    const walk = (node: HastNode) => {
      const children = node.children;
      if (!children) return;
      for (const [index, child] of children.entries()) {
        if (
          child.type === "element" &&
          hastClassNames(child).includes("katex-display")
        ) {
          child.properties = {
            ...child.properties,
            className: [...hastClassNames(child), "my-0!"],
          };
          children[index] = {
            type: "element",
            tagName: "div",
            properties: {
              className: "my-1 max-w-full overflow-x-auto",
              "data-math-display": "",
            },
            children: [child],
          };
          continue;
        }
        if (
          child.type === "element" &&
          hastClassNames(child).includes("katex")
        ) {
          children[index] = {
            type: "element",
            tagName: "span",
            properties: {
              className:
                "inline-block max-w-full overflow-x-auto align-baseline",
              "data-math-inline": "",
            },
            children: [child],
          };
          continue;
        }
        walk(child);
      }
    };
    walk(tree);
  };
}

export { remarkMath };
