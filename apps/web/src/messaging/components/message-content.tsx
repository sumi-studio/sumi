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
import remarkBreaks from "remark-breaks";
import remarkCjkFriendly from "remark-cjk-friendly";
import remarkCjkFriendlyGfmStrikethrough from "remark-cjk-friendly-gfm-strikethrough";
import remarkGfm from "remark-gfm";
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

/** hastノードの最小型（rehypeプラグイン用）。 */
interface HastNode {
  type: string;
  tagName?: string;
  properties?: Record<string, unknown>;
  value?: string;
  children?: HastNode[];
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
      remarkMentions(targets),
    ];
  }, [members, selfKey]);

  const rehypePlugins = useMemo(
    () => (trailer ? [rehypeTrailer(trailer)] : []),
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
