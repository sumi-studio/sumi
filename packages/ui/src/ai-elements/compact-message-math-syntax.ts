interface Position {
  start?: { offset?: number };
  end?: { offset?: number };
}

interface MdNode {
  type: string;
  value?: string;
  position?: Position;
  children?: MdNode[];
  data?: {
    hName: string;
    hProperties: { className: string[] };
    hChildren: Array<{ type: "text"; value: string }>;
  };
}

interface DollarToken {
  escaped: boolean;
}

function markdownWhitespace(character: string | undefined): boolean {
  return character !== undefined && /[\t\n\r ]/.test(character);
}

function digit(character: string | undefined): boolean {
  return character !== undefined && /[0-9]/.test(character);
}

/**
 * Match decoded dollars to their source spelling. Markdown escapes and
 * character references become literal text, but must never become delimiters.
 */
function sourceDollarTokens(raw: string): DollarToken[] {
  const tokens: DollarToken[] = [];
  for (let index = 0; index < raw.length; index += 1) {
    if (raw[index] === "\\" && raw[index + 1] === "$") {
      tokens.push({ escaped: true });
      index += 1;
      continue;
    }
    if (raw[index] === "$") {
      tokens.push({ escaped: false });
      continue;
    }
    const reference =
      raw[index] === "&"
        ? raw.slice(index, index + 12).match(/^&(?:#0*36|#x0*24|dollar);/i)?.[0]
        : undefined;
    if (reference) {
      tokens.push({ escaped: true });
      index += reference.length - 1;
    }
  }
  return tokens;
}

function eligibleDollars(value: string, raw: string): Set<number> {
  const sourceTokens = sourceDollarTokens(raw);
  const eligible = new Set<number>();
  let tokenIndex = 0;
  for (let index = 0; index < value.length; index += 1) {
    if (value[index] !== "$") continue;
    const token = sourceTokens[tokenIndex];
    if (token && !token.escaped) eligible.add(index);
    tokenIndex += 1;
  }
  return eligible;
}

function splitSingleDollarMath(value: string, raw: string): MdNode[] | null {
  const dollars = eligibleDollars(value, raw);
  const output: MdNode[] = [];
  let textStart = 0;
  let opener = value.indexOf("$");
  let changed = false;

  while (opener !== -1) {
    if (
      !dollars.has(opener) ||
      markdownWhitespace(value[opener + 1]) ||
      value[opener + 1] === undefined
    ) {
      opener = value.indexOf("$", opener + 1);
      continue;
    }

    let closer = value.indexOf("$", opener + 1);
    let retryAt = -1;
    while (closer !== -1) {
      if (!dollars.has(closer)) {
        closer = value.indexOf("$", closer + 1);
        continue;
      }
      if (!markdownWhitespace(value[closer - 1]) && !digit(value[closer + 1])) {
        break;
      }
      if (
        value[closer + 1] !== undefined &&
        !markdownWhitespace(value[closer + 1])
      ) {
        retryAt = closer;
        break;
      }
      closer = value.indexOf("$", closer + 1);
    }

    if (retryAt !== -1) {
      opener = retryAt;
      continue;
    }
    if (closer === -1) break;
    if (opener > textStart) {
      output.push({ type: "text", value: value.slice(textStart, opener) });
    }
    const math = value.slice(opener + 1, closer);
    output.push({
      type: "inlineMath",
      value: math,
      data: {
        hName: "code",
        hProperties: { className: ["language-math", "math-inline"] },
        hChildren: [{ type: "text", value: math }],
      },
    });
    changed = true;
    textStart = closer + 1;
    opener = value.indexOf("$", textStart);
  }

  if (!changed) return null;
  if (textStart < value.length) {
    output.push({ type: "text", value: value.slice(textStart) });
  }
  return output;
}

/**
 * Parse safe single-dollar math only inside Markdown's already-established
 * plain-text nodes. Native code, links/autolinks, escapes, and formatting are
 * therefore barriers rather than source that a math tokenizer can consume.
 */
export function remarkSafeSingleDollar() {
  return (tree: MdNode, file: { value?: unknown }) => {
    const source = typeof file.value === "string" ? file.value : "";
    const walk = (node: MdNode) => {
      if (
        !node.children ||
        node.type === "link" ||
        node.type === "linkReference"
      ) {
        return;
      }
      const next: MdNode[] = [];
      for (const child of node.children) {
        if (child.type !== "text" || child.value === undefined) {
          walk(child);
          next.push(child);
          continue;
        }
        const start = child.position?.start?.offset;
        const end = child.position?.end?.offset;
        const raw =
          start === undefined || end === undefined
            ? child.value
            : source.slice(start, end);
        next.push(...(splitSingleDollarMath(child.value, raw) ?? [child]));
      }
      node.children = next;
    };
    walk(tree);
  };
}
