import "katex/dist/katex.min.css";
import { Check, Copy } from "lucide-react";
import {
  Children,
  isValidElement,
  memo,
  type ReactNode,
  useMemo,
  useState,
} from "react";
import ReactMarkdown, { type Components } from "react-markdown";
import rehypeKatex from "rehype-katex";
import remarkBreaks from "remark-breaks";
import remarkCjkFriendly from "remark-cjk-friendly";
import remarkCjkFriendlyGfmStrikethrough from "remark-cjk-friendly-gfm-strikethrough";
import remarkGfm from "remark-gfm";
import remarkMath from "remark-math";
import { displayMentionPattern } from "../mention";
import type { MemberProfile, ParticipantKey } from "../model";
import { participantKey } from "../model";

/**
 * メッセージ本文のMarkdown描画。
 *
 * 契約上contentはmarkdown（docs/messaging-contracts-draft.md）。開発チームの
 * 会話はコード片だらけなので、コードブロック・インラインコード・リスト・
 * 引用・強調をDiscord相当の控えめな組版で描く。
 *
 * 安全性: rehype-rawを入れないため本文由来の生HTMLは描画されない
 * （react-markdownのデフォルトで生HTMLはテキスト扱い）。リンクは
 * rel="noreferrer noopener" target="_blank"。
 *
 * mention装飾はremarkプラグインとしてAST上のtextノードを分割する。
 * code / inlineCode は値がtextノードにならないため、コードの内側は
 * Discordと同様に装飾されない。link内も装飾しない（URLを壊さない）。
 *
 * 数式はremark-math + rehype-katex（KaTeX）。CSSはCDNではなくローカル
 * import（バンドル同梱）。単一$の誤爆と$$の扱いはremarkMathGuardで補正する。
 */

const MENTION_SELF_CLASS =
  "rounded bg-amber-500/15 px-0.5 font-medium text-amber-700 dark:text-amber-400";
const MENTION_OTHER_CLASS =
  "rounded bg-primary/10 px-0.5 font-medium text-primary";

/** remark/mdastノードの最小型。@types/mdastへ依存せず必要な形だけを持つ。 */
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

interface MentionTarget {
  displayName: string;
  isSelf: boolean;
}

/** この中ではmention装飾をしない（コードは値がtextにならないため対象外）。 */
const MENTION_SKIP_PARENTS = new Set(["link", "linkReference"]);

function mentionNode(label: string, isSelf: boolean): MdNode {
  return {
    type: "strong",
    data: {
      hName: "span",
      hProperties: {
        className: isSelf ? MENTION_SELF_CLASS : MENTION_OTHER_CLASS,
        "data-mention": isSelf ? "self" : "other",
      },
    },
    children: [{ type: "text", value: label }],
  };
}

function splitMentions(
  text: string,
  targets: MentionTarget[],
): MdNode[] | null {
  const pattern = displayMentionPattern(targets.map((t) => t.displayName));
  const parts: MdNode[] = [];
  let cursor = 0;
  for (const match of text.matchAll(pattern)) {
    const start = match.index ?? 0;
    if (start > cursor) {
      parts.push({ type: "text", value: text.slice(cursor, start) });
    }
    const target = targets.find((t) => t.displayName === match[1]);
    parts.push(mentionNode(match[0], target?.isSelf ?? false));
    cursor = start + match[0].length;
  }
  if (parts.length === 0) return null;
  if (cursor < text.length) {
    parts.push({ type: "text", value: text.slice(cursor) });
  }
  return parts;
}

/** mdastを歩き、textノード中の @表示名 をハイライト用spanノードへ置き換える。 */
function remarkMentions(targets: MentionTarget[]) {
  return () => (tree: MdNode) => {
    if (targets.length === 0) return;
    const walk = (node: MdNode) => {
      if (!node.children) return;
      if (MENTION_SKIP_PARENTS.has(node.type)) return;
      const next: MdNode[] = [];
      let changed = false;
      for (const child of node.children) {
        if (child.type === "text" && typeof child.value === "string") {
          const replaced = splitMentions(child.value, targets);
          if (replaced) {
            next.push(...replaced);
            changed = true;
            continue;
          }
        }
        walk(child);
        next.push(child);
      }
      if (changed) node.children = next;
    };
    walk(tree);
  };
}

/**
 * remark-mathの単一$を素直に使うと「ランチは $5 と $10」のような通貨表記が
 * 数式になってしまう（$5 と $ が数式として閉じる）。単一$の数式は残したまま、
 * pandoc相当の規則で誤爆だけを元のテキストへ戻す。
 *
 * 戻す条件（単一$のときだけ判定する）:
 * - 中身が空、または前後が空白（開き$の直後・閉じ$の直前が空白）
 * - 閉じ$の直後が数字（"$5 と $10" の 10 のような続き）
 */
function isCurrencyLikeMath(raw: string, source: string, end: number): boolean {
  const inner = raw.slice(1, -1);
  if (inner.trim() === "") return true;
  if (/^\s|\s$/.test(inner)) return true;
  return /\d/.test(source.charAt(end));
}

/** 先頭に連続する $ の数。$$…$$ の判別に使う。 */
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

/** remark-mathのブロック数式（$$を独立行で囲む形）と同じmdastノードを作る。 */
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
 * 段落の中で「その行がまるごと $$…$$」になっているインライン数式を
 * ブロック数式へ昇格させる。remark-mathは1行に閉じた $$…$$ をインライン
 * 扱いにするが、チャットでは $$ は別行組みの意図で書かれるため。
 * remark-breaksにより行の区切りはbreakノードとして残っている。
 */
function splitDisplayMathLines(
  paragraph: MdNode,
  source: string,
): MdNode[] | null {
  const children = paragraph.children ?? [];
  const out: MdNode[] = [];
  let line: MdNode[] = [];
  let promoted = false;
  const flush = () => {
    while (line.length > 0 && line[0].type === "break") line.shift();
    while (line.length > 0 && line[line.length - 1].type === "break")
      line.pop();
    if (line.length > 0) out.push({ type: "paragraph", children: line });
    line = [];
  };
  for (const [index, child] of children.entries()) {
    const previous = children[index - 1];
    const next = children[index + 1];
    const alone =
      (previous === undefined || previous.type === "break") &&
      (next === undefined || next.type === "break");
    const raw = child.type === "inlineMath" ? rawSource(source, child) : null;
    if (alone && raw !== null && dollarRun(raw) >= 2) {
      flush();
      out.push(displayMathNode(child.value ?? ""));
      promoted = true;
      continue;
    }
    line.push(child);
  }
  flush();
  return promoted ? out : null;
}

/** remark-mathの結果を補正する。誤爆の巻き戻し → $$行のブロック昇格の順。 */
function remarkMathGuard() {
  return (tree: MdNode, file: { value?: unknown }) => {
    const source = typeof file.value === "string" ? file.value : "";
    if (source === "") return;

    const demote = (node: MdNode) => {
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
        demote(child);
        next.push(child);
      }
      if (changed) node.children = next;
    };
    demote(tree);

    const promote = (node: MdNode) => {
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
        promote(child);
        next.push(child);
      }
      if (changed) node.children = next;
    };
    promote(tree);
  };
}

/** hastノードの最小型（rehypeプラグイン用）。 */
interface HastNode {
  type: string;
  tagName?: string;
  properties?: Record<string, unknown>;
  value?: string;
  children?: HastNode[];
}

function hastClassNames(node: HastNode): string[] {
  const value = node.properties?.className;
  if (Array.isArray(value)) return value.map(String);
  if (typeof value === "string") return value.split(/\s+/);
  return [];
}

/**
 * KaTeXのブロック数式は既定で上下1emの余白を取り、長い式は横にはみ出す。
 * チャットの行間に合わせて余白を詰め、横スクロール可能な器で包む。
 * rehype-katexが元の要素ごと差し替えるため、この整形はKaTeXの後に行う。
 */
function rehypeKatexLayout() {
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
            properties: { className: "my-1 max-w-full overflow-x-auto" },
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

export interface ContentTrailer {
  text: string;
  title?: string;
}

/**
 * 「(編集済み)」のような後置ラベルを最終段落の内側へ差し込む。
 * Markdown化で本文がブロック要素になっても、Discord同様に本文末尾へ
 * インラインで続けるための道具。最後がコードブロック等ならルート末尾に置く。
 */
function rehypeTrailer(trailer: ContentTrailer) {
  const span: HastNode = {
    type: "element",
    tagName: "span",
    properties: {
      className: "ml-1 text-[10px] text-muted-foreground",
      title: trailer.title,
      "data-trailer": "",
    },
    children: [{ type: "text", value: trailer.text }],
  };
  return () => (tree: HastNode) => {
    const children = tree.children ?? [];
    const last = [...children]
      .reverse()
      .find((child) => child.type === "element");
    if (last?.tagName === "p" && last.children) {
      last.children.push(span);
    } else {
      children.push(span);
      tree.children = children;
    }
  };
}

function nodeToText(children: ReactNode): string {
  let out = "";
  for (const child of Children.toArray(children)) {
    if (typeof child === "string" || typeof child === "number") {
      out += String(child);
    } else if (isValidElement<{ children?: ReactNode }>(child)) {
      out += nodeToText(child.props.children);
    }
  }
  return out;
}

function CodeBlock({ children }: { children?: ReactNode }) {
  const [copied, setCopied] = useState(false);
  const codeElement = Children.toArray(children).find((child) =>
    isValidElement<{ className?: string; children?: ReactNode }>(child),
  ) as
    | React.ReactElement<{ className?: string; children?: ReactNode }>
    | undefined;
  const language =
    codeElement?.props.className?.match(/language-([\w+-]+)/)?.[1] ?? "";
  const code = nodeToText(children).replace(/\n$/, "");

  const copy = () => {
    void navigator.clipboard?.writeText(code).then(() => {
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1_500);
    });
  };

  return (
    <div className="group/code my-1 max-w-full overflow-hidden rounded-lg border border-border bg-muted/40">
      <div className="flex items-center justify-between border-border/60 border-b px-3 py-1">
        <span className="text-[10px] text-muted-foreground uppercase tracking-wide">
          {language || "code"}
        </span>
        <button
          type="button"
          onClick={copy}
          title="コードをコピー"
          aria-label="コードをコピー"
          className="flex items-center gap-1 rounded px-1 py-0.5 text-[10px] text-muted-foreground opacity-0 transition-opacity hover:text-foreground focus-visible:opacity-100 group-hover/code:opacity-100"
        >
          {copied ? (
            <>
              <Check className="size-3" />
              コピーしました
            </>
          ) : (
            <>
              <Copy className="size-3" />
              コピー
            </>
          )}
        </button>
      </div>
      <pre className="overflow-x-auto whitespace-pre px-3 py-2 font-mono text-[12.5px] leading-5">
        {codeElement?.props.children ?? children}
      </pre>
    </div>
  );
}

/**
 * チャット向けの控えめな組版。見出しはDiscord同様に本文よりわずかに
 * 大きい程度へ抑える。段落間は狭く、リストはインデントのみ。
 */
const markdownComponents: Components = {
  pre: ({ children }) => <CodeBlock>{children}</CodeBlock>,
  code: ({ children }) => (
    <code className="rounded bg-muted px-1 py-px font-mono text-[12.5px]">
      {children}
    </code>
  ),
  a: ({ href, children }) => (
    <a
      href={href}
      target="_blank"
      rel="noreferrer noopener"
      className="break-all text-primary underline decoration-primary/40 underline-offset-2 hover:decoration-primary"
    >
      {children}
    </a>
  ),
  p: ({ children }) => <p className="my-0 [p+&]:mt-2">{children}</p>,
  blockquote: ({ children }) => (
    <blockquote className="my-1 border-border border-l-2 pl-3 text-muted-foreground">
      {children}
    </blockquote>
  ),
  ul: ({ children }) => (
    <ul className="my-0.5 list-disc pl-5 marker:text-muted-foreground">
      {children}
    </ul>
  ),
  ol: ({ children }) => (
    <ol className="my-0.5 list-decimal pl-5 marker:text-muted-foreground">
      {children}
    </ol>
  ),
  li: ({ children }) => <li className="my-0">{children}</li>,
  h1: ({ children }) => (
    <p className="my-0.5 font-semibold text-[15px] [p+&]:mt-2">{children}</p>
  ),
  h2: ({ children }) => (
    <p className="my-0.5 font-semibold text-[14px] [p+&]:mt-2">{children}</p>
  ),
  h3: ({ children }) => (
    <p className="my-0.5 font-semibold [p+&]:mt-2">{children}</p>
  ),
  h4: ({ children }) => (
    <p className="my-0.5 font-semibold [p+&]:mt-2">{children}</p>
  ),
  h5: ({ children }) => (
    <p className="my-0.5 font-semibold [p+&]:mt-2">{children}</p>
  ),
  h6: ({ children }) => (
    <p className="my-0.5 font-semibold [p+&]:mt-2">{children}</p>
  ),
  hr: () => <hr className="my-2 border-border" />,
  table: ({ children }) => (
    <div className="my-1 max-w-full overflow-x-auto">
      <table className="border-collapse text-[12.5px]">{children}</table>
    </div>
  ),
  th: ({ children }) => (
    <th className="border border-border px-2 py-0.5 text-left font-semibold">
      {children}
    </th>
  ),
  td: ({ children }) => (
    <td className="border border-border px-2 py-0.5">{children}</td>
  ),
};

/**
 * strict:"ignore" は日本語混じりの数式（$長さ = 3$ 等）で出る警告を止めるため。
 * 失敗した式は例外を投げず、赤字＋title付きで元のソースを見せる（KaTeX既定）。
 */
const KATEX_OPTIONS = {
  strict: "ignore" as const,
  errorColor: "var(--destructive)",
  trust: false,
};

/** オプション付きKaTeX。既存のrehypeTrailerと同じくattacherの形で渡す。 */
function rehypeKatexWithOptions() {
  return rehypeKatex(KATEX_OPTIONS);
}

export interface MessageContentProps {
  content: string;
  members: Record<ParticipantKey, MemberProfile>;
  selfKey: ParticipantKey;
  /** 本文末尾へインラインで続ける後置ラベル（例:「(編集済み)」）。 */
  trailer?: ContentTrailer;
}

export const MessageContent = memo(function MessageContent({
  content,
  members,
  selfKey,
  trailer,
}: MessageContentProps) {
  const remarkPlugins = useMemo(() => {
    const targets: MentionTarget[] = Object.values(members).map((member) => ({
      displayName: member.displayName,
      isSelf: participantKey(member.participant) === selfKey,
    }));
    return [
      remarkGfm,
      remarkCjkFriendly,
      remarkCjkFriendlyGfmStrikethrough,
      remarkBreaks,
      remarkMath,
      // 誤爆の巻き戻しはmention装飾より前。戻したテキストの@名前も装飾したい。
      remarkMathGuard,
      remarkMentions(targets),
    ];
  }, [members, selfKey]);

  const rehypePlugins = useMemo(
    () => [
      rehypeKatexWithOptions,
      rehypeKatexLayout,
      ...(trailer ? [rehypeTrailer(trailer)] : []),
    ],
    [trailer],
  );

  return (
    <ReactMarkdown
      remarkPlugins={remarkPlugins}
      rehypePlugins={rehypePlugins}
      components={markdownComponents}
    >
      {content}
    </ReactMarkdown>
  );
});
