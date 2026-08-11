import rehypeKatex from "rehype-katex";
import remarkMath from "remark-math";

interface MdNode {
  type: string;
  value?: string;
  children?: MdNode[];
  data?: {
    hName?: string;
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

/**
 * Keep single-dollar inline math without turning ordinary prices into math.
 * These are the same delimiter constraints Pandoc applies: non-blank content,
 * no whitespace just inside the delimiters, and no digit immediately after
 * the closing delimiter.
 */
function isCurrencyLikeMath(raw: string, source: string, end: number): boolean {
  const inner = raw.slice(1, -1);
  if (inner.trim() === "") return true;
  if (/^\s|\s$/.test(inner)) return true;
  return /\d/.test(source.charAt(end));
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

/** Correct remark-math's chat-specific delimiter ambiguities before mentions. */
export function remarkCompactMath() {
  return (tree: MdNode, file: { value?: unknown }) => {
    const source = typeof file.value === "string" ? file.value : "";
    if (source === "") return;

    const demoteCurrency = (node: MdNode) => {
      if (!node.children) return;
      const next: MdNode[] = [];
      let changed = false;
      for (const child of node.children) {
        if (child.type === "inlineMath") {
          const raw = rawSource(source, child);
          const end = child.position?.end?.offset ?? 0;
          if (
            raw !== null &&
            dollarRun(raw) === 1 &&
            isCurrencyLikeMath(raw, source, end)
          ) {
            next.push({ type: "text", value: raw });
            changed = true;
            continue;
          }
        }
        demoteCurrency(child);
        next.push(child);
      }
      if (changed) node.children = next;
    };
    demoteCurrency(tree);

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

/** Remove KaTeX's large display margins and contain long formulae locally. */
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
        walk(child);
      }
    };
    walk(tree);
  };
}

const KATEX_OPTIONS = {
  strict: "ignore" as const,
  errorColor: "var(--destructive)",
  trust: false,
  throwOnError: false,
};

/** KaTeX failures stay visible as their TeX source instead of aborting chat. */
export function rehypeCompactKatex() {
  return rehypeKatex(KATEX_OPTIONS);
}

export { remarkMath };
