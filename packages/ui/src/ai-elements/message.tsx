import { cjk } from "@streamdown/cjk";
import { code } from "@streamdown/code";
import { createMathPlugin } from "@streamdown/math";
import { CheckIcon, CopyIcon, PencilIcon } from "lucide-react";
import type { ComponentProps, HTMLAttributes } from "react";
import { memo, useEffect, useRef, useState } from "react";
import { type DiagramPlugin, Streamdown } from "streamdown";
import { Button } from "../components/button";
import { Tooltip, TooltipContent, TooltipTrigger } from "../components/tooltip";
import { cn } from "../lib/utils";
import "katex/dist/katex.min.css";

export type MessageProps = ComponentProps<"div"> & {
  from: "user" | "assistant";
};

/** AI ElementsのMessage。ユーザーだけ右寄せのsecondary面を持つ。 */
export function Message({ className, from, ...props }: MessageProps) {
  return (
    <div
      data-role={from}
      className={cn(
        "group/message flex w-full max-w-[95%] flex-col gap-2",
        from === "user"
          ? "is-user ml-auto items-end justify-end"
          : "is-assistant",
        className,
      )}
      {...props}
    />
  );
}

export type MessageContentProps = HTMLAttributes<HTMLDivElement>;

export function MessageContent({ className, ...props }: MessageContentProps) {
  return (
    <div
      className={cn(
        "flex w-fit min-w-0 max-w-full flex-col gap-2 overflow-hidden text-sm",
        "group-[.is-user]/message:ml-auto group-[.is-user]/message:rounded-lg group-[.is-user]/message:bg-secondary group-[.is-user]/message:px-4 group-[.is-user]/message:py-3 group-[.is-user]/message:text-foreground",
        "group-[.is-assistant]/message:w-full group-[.is-assistant]/message:text-foreground",
        className,
      )}
      {...props}
    />
  );
}

export type MessageActionsProps = HTMLAttributes<HTMLDivElement>;

export function MessageActions({ className, ...props }: MessageActionsProps) {
  return (
    <div className={cn("flex items-center gap-1", className)} {...props} />
  );
}

export type MessageActionProps = ComponentProps<typeof Button> & {
  tooltip?: string;
  label: string;
};

export function MessageAction({
  tooltip,
  label,
  variant = "ghost",
  size = "icon-sm",
  children,
  ...props
}: MessageActionProps) {
  return tooltip ? (
    <Tooltip>
      <TooltipTrigger
        render={
          <Button type="button" variant={variant} size={size} {...props} />
        }
      >
        {children}
        <span className="sr-only">{label}</span>
      </TooltipTrigger>
      <TooltipContent>{tooltip}</TooltipContent>
    </Tooltip>
  ) : (
    <Button type="button" variant={variant} size={size} {...props}>
      {children}
      <span className="sr-only">{label}</span>
    </Button>
  );
}

const math = createMathPlugin({ singleDollarTextMath: true });
const mermaid: DiagramPlugin = {
  name: "mermaid",
  type: "diagram",
  language: "mermaid",
  getMermaid(config) {
    let activeConfig = config;
    return {
      initialize(nextConfig) {
        activeConfig = nextConfig;
      },
      async render(id, source) {
        const module = await import("@streamdown/mermaid");
        return module.mermaid.getMermaid(activeConfig).render(id, source);
      },
    };
  },
};
const streamdownPlugins = { cjk, code, math, mermaid };
const streamdownControls = {
  code: true,
  table: true,
  mermaid: {
    copy: true,
    download: true,
    fullscreen: true,
    panZoom: false,
  },
} as const;

export type MessageResponseProps = ComponentProps<typeof Streamdown> & {
  onRenderSettled?: () => void;
};

/** AI Elements標準のStreamdown組版。図・コードの判定もStreamdownへ委譲する。 */
export const MessageResponse = memo(
  ({
    children,
    className,
    onRenderSettled,
    ...props
  }: MessageResponseProps) => {
    const rootRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
      if (!onRenderSettled) {
        return;
      }
      const root = rootRef.current;
      const markdown = typeof children === "string" ? children : "";
      const expected = deferredFenceCounts(markdown);
      let settled = false;
      let frame = 0;
      const finish = () => {
        if (settled) {
          return;
        }
        settled = true;
        frame = window.requestAnimationFrame(onRenderSettled);
      };
      const check = () => {
        if (!root || !richMarkdownSettled(root, expected)) {
          return;
        }
        finish();
      };
      check();
      if (settled) {
        return () => window.cancelAnimationFrame(frame);
      }
      const observer = new MutationObserver(check);
      if (root) {
        observer.observe(root, {
          attributes: true,
          childList: true,
          subtree: true,
        });
      }
      return () => {
        observer.disconnect();
        window.cancelAnimationFrame(frame);
      };
    }, [children, onRenderSettled]);

    return (
      <div
        ref={rootRef}
        className={cn("message-markdown size-full", className)}
      >
        <Streamdown
          className="size-full [&>*:first-child]:mt-0 [&>*:last-child]:mb-0"
          controls={streamdownControls}
          plugins={streamdownPlugins}
          allowedTags={{ kbd: [], sub: [], sup: [] }}
          {...props}
        >
          {children}
        </Streamdown>
      </div>
    );
  },
  (previous, next) =>
    previous.children === next.children &&
    previous.className === next.className &&
    previous.isAnimating === next.isAnimating &&
    previous.mode === next.mode &&
    previous.onRenderSettled === next.onRenderSettled,
);

MessageResponse.displayName = "MessageResponse";

interface DeferredFenceCounts {
  code: number;
  mermaid: number;
}

function deferredFenceCounts(markdown: string): DeferredFenceCounts {
  const counts = { code: 0, mermaid: 0 };
  for (const match of markdown.matchAll(
    /^(?: {0,3})(?:`{3,}|~{3,})([^\n]*)$/gm,
  )) {
    const language = match[1]?.trim().split(/\s+/, 1)[0]?.toLowerCase();
    if (!language) {
      continue;
    }
    if (language === "mermaid") {
      counts.mermaid += 1;
    } else if (code.supportsLanguage(language as never)) {
      counts.code += 1;
    }
  }
  return counts;
}

function richMarkdownSettled(
  root: HTMLElement,
  expected: DeferredFenceCounts,
): boolean {
  if (outsideScrollViewport(root)) {
    return true;
  }

  const codeBlocks = root.querySelectorAll(
    '[data-streamdown="code-block-body"]',
  );
  const highlightedCodeBlocks = [...codeBlocks].filter((block) => {
    const language = block.getAttribute("data-language")?.toLowerCase();
    return language && code.supportsLanguage(language as never);
  });
  if (highlightedCodeBlocks.length < expected.code) {
    return false;
  }

  const mermaidBlocks = root.querySelectorAll(
    '[data-streamdown="mermaid-block"]',
  );
  return (
    mermaidBlocks.length >= expected.mermaid &&
    [...mermaidBlocks].every(
      (block) =>
        block.querySelector('[data-streamdown="mermaid"]') ||
        block.querySelector(".bg-red-50"),
    )
  );
}

function outsideScrollViewport(element: HTMLElement): boolean {
  const viewport = element.closest<HTMLElement>(
    '[data-slot="message-scroller-viewport"]',
  );
  if (!viewport) {
    return false;
  }
  const elementRect = element.getBoundingClientRect();
  const viewportRect = viewport.getBoundingClientRect();
  const overscan = 64;
  return (
    elementRect.bottom < viewportRect.top - overscan ||
    elementRect.top > viewportRect.bottom + overscan
  );
}

interface MessageMetadataProps {
  timestamp: string | null;
  copyText: string;
  align?: "left" | "right";
  copyFirst?: boolean;
  copyAlwaysVisible?: boolean;
  revealed?: boolean;
  onEdit?: () => void;
  className?: string;
}

/** MessageActionsを使ったアプリ固有の時刻・コピー・編集行。 */
export function MessageMetadata({
  timestamp,
  copyText,
  align = "left",
  copyFirst = false,
  copyAlwaysVisible = false,
  revealed = false,
  onEdit,
  className,
}: MessageMetadataProps) {
  const [copied, setCopied] = useState(false);
  const hoverVisibility =
    "opacity-0 transition-opacity duration-150 group-focus-within/message:opacity-100 group-hover/message:opacity-100 pointer-coarse:opacity-100";

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(copyText);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    } catch {
      // Permission denial or an unavailable clipboard leaves the UI unchanged.
    }
  };

  const time = timestamp ? (
    <time
      dateTime={timestamp}
      className={revealed ? "opacity-100" : hoverVisibility}
    >
      {new Date(timestamp).toLocaleTimeString("ja-JP", {
        hour: "2-digit",
        minute: "2-digit",
      })}
    </time>
  ) : null;

  const copyButton = (
    <MessageAction
      label="コピー"
      tooltip={copied ? "コピーしました" : "コピー"}
      onClick={copy}
      size="icon-xs"
      className={cn(
        "size-5",
        copyAlwaysVisible || revealed ? "opacity-100" : hoverVisibility,
      )}
    >
      {copied ? <CheckIcon /> : <CopyIcon />}
    </MessageAction>
  );

  return (
    <MessageActions
      className={cn(
        "text-muted-foreground text-xs",
        align === "right" && "justify-end",
        className,
      )}
    >
      {copyFirst ? (
        <>
          {copyButton}
          {time}
        </>
      ) : (
        <>
          {time}
          {copyButton}
        </>
      )}
      {onEdit && (
        <MessageAction
          label="編集して再送"
          tooltip="編集して再送"
          onClick={onEdit}
          size="icon-xs"
          className={cn("size-5", revealed ? "opacity-100" : hoverVisibility)}
        >
          <PencilIcon />
        </MessageAction>
      )}
    </MessageActions>
  );
}
