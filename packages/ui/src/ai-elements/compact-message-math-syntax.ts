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
    hProperties: Record<string, unknown>;
    hChildren: Array<{ type: "text"; value: string }>;
  };
}

interface DollarToken {
  escaped: boolean;
  rawStart: number;
  rawEnd: number;
}

const UNICODE_WHITESPACE = /(?:\p{White_Space}|\uFEFF)/u;

function markdownWhitespace(character: string | undefined): boolean {
  return character !== undefined && UNICODE_WHITESPACE.test(character);
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
    if (raw[index] === "$") {
      let precedingBackslashes = 0;
      for (
        let cursor = index - 1;
        cursor >= 0 && raw[cursor] === "\\";
        cursor -= 1
      ) {
        precedingBackslashes += 1;
      }
      tokens.push({
        escaped: precedingBackslashes % 2 === 1,
        rawStart: index,
        rawEnd: index + 1,
      });
      continue;
    }
    const reference =
      raw[index] === "&"
        ? raw.slice(index, index + 12).match(/^&(?:#0*36|#x0*24|dollar);/i)?.[0]
        : undefined;
    if (reference) {
      tokens.push({
        escaped: true,
        rawStart: index,
        rawEnd: index + reference.length,
      });
      index += reference.length - 1;
    }
  }
  return tokens;
}

function dollarTokens(
  value: string,
  raw: string,
): Map<number, DollarToken> | null {
  const sourceTokens = sourceDollarTokens(raw);
  const decodedDollarIndexes: number[] = [];
  for (let index = 0; index < value.length; index += 1) {
    if (value[index] === "$") decodedDollarIndexes.push(index);
  }
  if (decodedDollarIndexes.length !== sourceTokens.length) return null;

  const tokens = new Map<number, DollarToken>();
  let tokenIndex = 0;
  for (const index of decodedDollarIndexes) {
    const token = sourceTokens[tokenIndex];
    if (!token) return null;
    tokens.set(index, token);
    tokenIndex += 1;
  }
  return tokens;
}

function canOpen(
  value: string,
  dollars: Map<number, DollarToken>,
  index: number,
): boolean {
  return (
    dollars.get(index)?.escaped === false &&
    value[index + 1] !== undefined &&
    !markdownWhitespace(value[index + 1])
  );
}

function canClose(
  value: string,
  dollars: Map<number, DollarToken>,
  index: number,
): boolean {
  return (
    dollars.get(index)?.escaped === false &&
    value[index - 1] !== undefined &&
    !markdownWhitespace(value[index - 1]) &&
    !digit(value[index + 1])
  );
}

function hasLaterCloser(
  value: string,
  dollars: Map<number, DollarToken>,
  opener: number,
): boolean {
  let closer = value.indexOf("$", opener + 1);
  while (closer !== -1) {
    if (canClose(value, dollars, closer)) return true;
    closer = value.indexOf("$", closer + 1);
  }
  return false;
}

/**
 * One dollar can be both the closer of a numeric expression and the opener of
 * later math. Re-synchronize only across a structured prose label, retain
 * concrete TeX syntax, and leave the whole text node inert when neither is
 * established.
 */
type SharedNumericDelimiter = "formula" | "currency" | "ambiguous";

const CURRENCY_AMOUNT = /^(?:[0-9]+(?:[.,][0-9]+)*|[.,][0-9]+)/u;
const CURRENCY_UNIT =
  /^(?:\p{White_Space}|\uFEFF)*(?:(?:円|ドル|ユーロ|ポンド|元|ウォン|セント|USD|JPY|EUR|GBP|CNY|KRW|CAD|AUD)|(?:[/／](?:個|人|件|本|枚|台|回|時間|時|日|週|月|年|kg|g)))/iu;

function currencyTransition(suffix: string): boolean {
  const englishLabel =
    /^[,.;!?]\s*[A-Za-z]+(?:\s+[A-Za-z]+)*\s*[:：]\s*$/u.test(suffix);
  const japaneseLabel =
    /^[、。，．：；！？]\s*[\p{Script=Han}\p{Script=Hiragana}\p{Script=Katakana}]+(?:は|が|を|に|へ|で|と|も|の|[:：])\s*$/u.test(
      suffix,
    );
  return englishLabel || japaneseLabel;
}

function numericFormula(suffix: string): boolean {
  // A shared delimiter is not enough evidence when its complete left-hand
  // candidate contains prose. Keep ambiguous currency/unit text inert rather
  // than letting one operator (notably `/個`) turn the prose into TeX.
  if (/[\p{Script=Han}\p{Script=Hiragana}\p{Script=Katakana}]/u.test(suffix)) {
    return false;
  }
  const withoutCommands = suffix.replace(/\\(?:[A-Za-z@]+|[^A-Za-z\s])/gu, "");
  if (
    !/^(?:\p{White_Space}|\uFEFF|[A-Za-z\u0370-\u03ff0-9+\-*/=^_<>{}()[\]|,.±∓×÷·⋅≤≥≈≠]|\\(?:[A-Za-z@]+|[^A-Za-z\s]))*$/u.test(
      suffix,
    ) ||
    /[A-Za-z\u0370-\u03ff](?:\p{White_Space}|\uFEFF)+[A-Za-z\u0370-\u03ff]/u.test(
      withoutCommands,
    )
  ) {
    return false;
  }
  if (/\\(?:[A-Za-z@]+|[^A-Za-z\s])/u.test(suffix)) return true;
  if (/[+\-*/=^_<>{}()[\]|±∓×÷·⋅≤≥≈≠]/u.test(suffix)) return true;

  const compactVariable =
    /^[\p{L}][\p{L}\p{N}]*$/u.test(suffix) ||
    /^(?:\p{White_Space}|\uFEFF)*[A-Za-z\u0370-\u03ff](?:\p{White_Space}|\uFEFF)*$/u.test(
      suffix,
    );
  const commaSeparatedVariable =
    /^[,;](?:\p{White_Space}|\uFEFF)*[A-Za-z\u0370-\u03ff](?:\p{White_Space}|\uFEFF)*$/u.test(
      suffix,
    );
  return compactVariable || commaSeparatedVariable;
}

function currencyCandidate(body: string): boolean {
  const amount = body.match(CURRENCY_AMOUNT)?.[0];
  if (!amount) return false;
  const suffix = body.slice(amount.length);
  if (currencyTransition(suffix)) return true;
  const unit = suffix.match(CURRENCY_UNIT)?.[0];
  return unit !== undefined && currencyTransition(suffix.slice(unit.length));
}

function currencyAmountStart(value: string, opener: number): boolean {
  const first = value[opener + 1];
  return (
    digit(first) ||
    ((first === "." || first === ",") && digit(value[opener + 2]))
  );
}

function sharedNumericDelimiter(
  value: string,
  opener: number,
  candidate: number,
): SharedNumericDelimiter {
  const body = value.slice(opener + 1, candidate);
  if (currencyCandidate(body)) return "currency";
  const amount = body.match(CURRENCY_AMOUNT)?.[0] ?? "";
  if (!amount) return "ambiguous";
  const suffix = body.slice(amount.length);
  if (numericFormula(suffix)) return "formula";
  return "ambiguous";
}

function splitSingleDollarMath(value: string, raw: string): MdNode[] | null {
  const dollars = dollarTokens(value, raw);
  if (!dollars) return null;
  const output: MdNode[] = [];
  let textStart = 0;
  let opener = value.indexOf("$");
  let changed = false;

  while (opener !== -1) {
    if (!canOpen(value, dollars, opener)) {
      opener = value.indexOf("$", opener + 1);
      continue;
    }

    let closer = value.indexOf("$", opener + 1);
    let retryAt = -1;
    while (closer !== -1) {
      if (dollars.get(closer)?.escaped !== false) {
        closer = value.indexOf("$", closer + 1);
        continue;
      }
      if (canClose(value, dollars, closer)) {
        if (
          canOpen(value, dollars, closer) &&
          currencyAmountStart(value, opener) &&
          hasLaterCloser(value, dollars, closer)
        ) {
          const decision = sharedNumericDelimiter(value, opener, closer);
          if (decision === "currency") retryAt = closer;
          else if (decision === "ambiguous") return null;
        }
        break;
      }
      if (canOpen(value, dollars, closer)) {
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
    const openerToken = dollars.get(opener);
    const closerToken = dollars.get(closer);
    if (!openerToken || !closerToken) return null;
    const math = raw.slice(openerToken.rawEnd, closerToken.rawStart);
    const mathRaw = raw.slice(openerToken.rawStart, closerToken.rawEnd);
    output.push({
      type: "inlineMath",
      value: math,
      data: {
        hName: "code",
        hProperties: {
          className: ["language-math", "math-inline"],
          "data-math-raw": mathRaw,
        },
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
        if (start === undefined || end === undefined) {
          next.push(child);
          continue;
        }
        const raw = source.slice(start, end);
        next.push(...(splitSingleDollarMath(child.value, raw) ?? [child]));
      }
      node.children = next;
    };
    walk(tree);
  };
}
