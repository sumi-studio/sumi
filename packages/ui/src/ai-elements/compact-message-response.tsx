import { Check, Copy, Image as ImageIcon } from "lucide-react";
import {
  Children,
  type ClipboardEvent,
  isValidElement,
  type ReactElement,
  type ReactNode,
  useMemo,
  useState,
} from "react";
import ReactMarkdown, {
  type Components,
  defaultUrlTransform,
  type Options,
} from "react-markdown";
import remarkBreaks from "remark-breaks";
import remarkCjkFriendly from "remark-cjk-friendly";
import remarkCjkFriendlyGfmStrikethrough from "remark-cjk-friendly-gfm-strikethrough";
import remarkGfm from "remark-gfm";
import {
  REMARK_MATH_OPTIONS,
  rehypeCompactKatex,
  rehypeCompactMathLayout,
  remarkCompactMath,
  remarkMath,
} from "./compact-message-math";
import { remarkSafeSingleDollar } from "./compact-message-math-syntax";
import "katex/dist/katex.min.css";

const LINK_CLASS =
  "break-all text-primary underline decoration-primary/40 underline-offset-2 hover:decoration-primary";

function elementForNode(node: Node | null): Element | null {
  if (node instanceof Element) return node;
  return node?.parentElement ?? null;
}

function katexSource(element: ParentNode): string | null {
  return (
    element.querySelector('annotation[encoding="application/x-tex"]')
      ?.textContent ?? null
  );
}

const BLOCK_TEXT_ELEMENTS = new Set([
  "ADDRESS",
  "ARTICLE",
  "ASIDE",
  "BLOCKQUOTE",
  "DIV",
  "DL",
  "DT",
  "DD",
  "FIGCAPTION",
  "FIGURE",
  "FOOTER",
  "FORM",
  "H1",
  "H2",
  "H3",
  "H4",
  "H5",
  "H6",
  "HEADER",
  "HR",
  "LI",
  "MAIN",
  "NAV",
  "OL",
  "P",
  "PRE",
  "SECTION",
  "TABLE",
  "TR",
  "UL",
]);

function fragmentPlainText(fragment: DocumentFragment): string {
  let output = "";
  const lineBreak = () => {
    if (output !== "" && !output.endsWith("\n")) output += "\n";
  };
  const serialize = (node: Node) => {
    if (node.nodeType === Node.TEXT_NODE) {
      let value = node.nodeValue ?? "";
      // React's HTML shape includes a source newline after `<br>` and between
      // block elements. The structural branch already emitted that break.
      if (output.endsWith("\n")) value = value.replace(/^\r?\n/, "");
      output += value;
      return;
    }
    if (!(node instanceof Element)) {
      for (const child of node.childNodes) serialize(child);
      return;
    }
    if (node.tagName === "BR") {
      output += "\n";
      return;
    }
    const block = BLOCK_TEXT_ELEMENTS.has(node.tagName);
    if (block) lineBreak();
    for (const child of node.childNodes) serialize(child);
    if (node.tagName === "TD" || node.tagName === "TH") output += "\t";
    if (block) lineBreak();
  };
  serialize(fragment);
  return output.replace(/^\n+|[\t\n]+$/g, "").replace(/\t+\n/g, "\n");
}

/** Replace KaTeX's visual+MathML duplicate selection with one TeX source. */
function normalizedMathSelection(
  selection: Selection,
  boundary: HTMLElement,
): string | null {
  if (selection.isCollapsed || selection.rangeCount !== 1) return null;
  const anchor = elementForNode(selection.anchorNode);
  const focus = elementForNode(selection.focusNode);
  if (
    !anchor ||
    !focus ||
    !boundary.contains(anchor) ||
    !boundary.contains(focus)
  ) {
    return null;
  }

  const range = selection.getRangeAt(0).cloneRange();
  const startMath = elementForNode(range.startContainer)?.closest(".katex");
  const endMath = elementForNode(range.endContainer)?.closest(".katex");
  if (startMath && boundary.contains(startMath))
    range.setStartBefore(startMath);
  if (endMath && boundary.contains(endMath)) range.setEndAfter(endMath);

  const liveFormulae = [...boundary.querySelectorAll(".katex")].filter(
    (formula) => range.intersectsNode(formula),
  );
  if (liveFormulae.length === 0) return null;
  const sources = liveFormulae.map(katexSource);
  if (sources.some((source) => source === null)) return null;

  const fragment = range.cloneContents();
  const clonedFormulae = [...fragment.querySelectorAll(".katex")];
  if (clonedFormulae.length !== sources.length) return null;
  for (const [index, formula] of clonedFormulae.entries()) {
    formula.replaceWith(document.createTextNode(sources[index] ?? ""));
  }
  return fragmentPlainText(fragment);
}

function copyMathAsTex(event: ClipboardEvent<HTMLDivElement>) {
  const selection = window.getSelection();
  if (!selection) return;
  const text = normalizedMathSelection(selection, event.currentTarget);
  if (text === null) return;
  event.preventDefault();
  event.clipboardData.setData("text/plain", text);
}

export interface CompactMessageTrailer {
  text: string;
  title?: string;
}

/** Minimal hast shape used by the trusted trailer transform. */
interface HastNode {
  type: string;
  tagName?: string;
  properties?: Record<string, unknown>;
  value?: string;
  children?: HastNode[];
}

/** Append labels such as “edited” inside the final paragraph when possible. */
function rehypeTrailer(trailer: CompactMessageTrailer) {
  return () => (tree: HastNode) => {
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
  ) as ReactElement<{ className?: string; children?: ReactNode }> | undefined;
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
 * Compact chat typography and security policy are intentionally one closed
 * component map. Consumers cannot replace links, images, or parser policy on a
 * screen-by-screen basis.
 */
const COMPONENTS: Components = {
  // Markdown images never become <img>. Rendering one would fetch an arbitrary
  // author-controlled URL as soon as a reader opened the conversation.
  img: ({ src, alt, title }) => {
    const href = typeof src === "string" && src !== "" ? src : undefined;
    const label = alt?.trim() || href || "画像";
    if (!href) {
      return (
        <span className="inline-flex items-baseline gap-1 text-muted-foreground">
          <ImageIcon className="size-3 self-center" aria-hidden="true" />
          {label}
        </span>
      );
    }
    return (
      <a
        href={href}
        target="_blank"
        rel="noreferrer noopener"
        title={title ?? href}
        data-image-link=""
        className={`inline-flex items-baseline gap-1 ${LINK_CLASS}`}
      >
        <ImageIcon className="size-3 self-center" aria-hidden="true" />
        {label}
      </a>
    );
  },
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
      className={LINK_CLASS}
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

const STANDARD_REMARK_PLUGINS: NonNullable<Options["remarkPlugins"]> = [
  remarkGfm,
  remarkCjkFriendly,
  remarkCjkFriendlyGfmStrikethrough,
  [remarkMath, REMARK_MATH_OPTIONS],
  // Single-dollar math is admitted only after native Markdown constructs have
  // established safe plain-text boundaries, but before line-break transforms
  // discard the source positions needed to distinguish escaped dollars.
  remarkSafeSingleDollar,
  remarkBreaks,
  remarkCompactMath,
];

export interface CompactMessageResponseProps {
  children: string;
  /** Trusted domain transforms, such as membership-bound mention rendering. */
  extraRemarkPlugins?: Options["remarkPlugins"];
  trailer?: CompactMessageTrailer;
  /** Optional class for the copy-normalizing renderer boundary. */
  className?: string;
}

/**
 * Shared static Markdown renderer for compact person-authored chat messages.
 *
 * It owns parser defaults, compact presentation, and the security invariants
 * that must not drift between consumers: raw HTML is ignored, URL schemes use
 * react-markdown's allowlist, links open without opener access, and image
 * syntax is inert until the reader explicitly follows its link. The only
 * extension point is a trusted remark transform; arbitrary components, URL
 * transforms, and rehype plugins are deliberately not exposed.
 */
export function CompactMessageResponse({
  children,
  extraRemarkPlugins,
  trailer,
  className,
}: CompactMessageResponseProps) {
  const remarkPlugins = useMemo(
    () => [...STANDARD_REMARK_PLUGINS, ...(extraRemarkPlugins ?? [])],
    [extraRemarkPlugins],
  );
  const rehypePlugins = useMemo(
    () => [
      rehypeCompactKatex,
      rehypeCompactMathLayout,
      ...(trailer ? [rehypeTrailer(trailer)] : []),
    ],
    [trailer],
  );
  const rendered = (
    <ReactMarkdown
      remarkPlugins={remarkPlugins}
      rehypePlugins={rehypePlugins}
      components={COMPONENTS}
      skipHtml
      urlTransform={defaultUrlTransform}
    >
      {children}
    </ReactMarkdown>
  );
  return (
    <div
      className={className}
      data-compact-message-response=""
      onCopy={copyMathAsTex}
    >
      {rendered}
    </div>
  );
}
