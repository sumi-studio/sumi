import { CheckIcon, CopyIcon } from "lucide-react";
import type {
  ComponentProps,
  CSSProperties,
  HTMLAttributes,
  ReactNode,
} from "react";
import {
  createContext,
  memo,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import type { ThemedToken } from "shiki";
import { createHighlighterCore, type LanguageInput } from "shiki/core";
import { createJavaScriptRegexEngine } from "shiki/engine/javascript";
import { Button } from "../components/button";
import { cn } from "../lib/utils";

interface TokenizedCode {
  tokens: ThemedToken[][];
  fg: string;
  bg: string;
}

export type HighlightLanguage =
  | "css"
  | "go"
  | "html"
  | "javascript"
  | "json"
  | "jsx"
  | "markdown"
  | "python"
  | "rust"
  | "shellscript"
  | "sql"
  | "tsx"
  | "typescript"
  | "yaml";
export type CodeLanguage = HighlightLanguage | "text";

interface CodeBlockContextValue {
  code: string;
}

const CodeBlockContext = createContext<CodeBlockContextValue>({ code: "" });

type CoreHighlighter = Awaited<ReturnType<typeof createHighlighterCore>>;

const highlighterCache = new Map<string, Promise<CoreHighlighter>>();
const tokenCache = new Map<string, TokenizedCode>();
const MAX_TOKEN_CACHE_ENTRIES = 100;

function readTokenCache(key: string): TokenizedCode | undefined {
  const cached = tokenCache.get(key);
  if (cached) {
    // Mapの挿入順をLRU順として使う。
    tokenCache.delete(key);
    tokenCache.set(key, cached);
  }
  return cached;
}

function writeTokenCache(key: string, value: TokenizedCode): void {
  tokenCache.delete(key);
  tokenCache.set(key, value);
  if (tokenCache.size > MAX_TOKEN_CACHE_ENTRIES) {
    const oldestKey = tokenCache.keys().next().value;
    if (oldestKey !== undefined) {
      tokenCache.delete(oldestKey);
    }
  }
}

const languageLoaders: Record<HighlightLanguage, () => Promise<LanguageInput>> =
  {
    css: () => import("shiki/langs/css.mjs").then((module) => module.default),
    go: () => import("shiki/langs/go.mjs").then((module) => module.default),
    html: () => import("shiki/langs/html.mjs").then((module) => module.default),
    javascript: () =>
      import("shiki/langs/javascript.mjs").then((module) => module.default),
    json: () => import("shiki/langs/json.mjs").then((module) => module.default),
    jsx: () => import("shiki/langs/jsx.mjs").then((module) => module.default),
    markdown: () =>
      import("shiki/langs/markdown.mjs").then((module) => module.default),
    python: () =>
      import("shiki/langs/python.mjs").then((module) => module.default),
    rust: () => import("shiki/langs/rust.mjs").then((module) => module.default),
    shellscript: () =>
      import("shiki/langs/shellscript.mjs").then((module) => module.default),
    sql: () => import("shiki/langs/sql.mjs").then((module) => module.default),
    tsx: () => import("shiki/langs/tsx.mjs").then((module) => module.default),
    typescript: () =>
      import("shiki/langs/typescript.mjs").then((module) => module.default),
    yaml: () => import("shiki/langs/yaml.mjs").then((module) => module.default),
  };

function getHighlighter(language: HighlightLanguage): Promise<CoreHighlighter> {
  const cached = highlighterCache.get(language);
  if (cached) {
    return cached;
  }
  const highlighter = Promise.all([
    languageLoaders[language](),
    import("shiki/themes/github-light.mjs").then((module) => module.default),
    import("shiki/themes/github-dark.mjs").then((module) => module.default),
  ]).then(([languageDefinition, lightTheme, darkTheme]) =>
    createHighlighterCore({
      engine: createJavaScriptRegexEngine(),
      langs: [languageDefinition],
      themes: [lightTheme, darkTheme],
    }),
  );
  highlighterCache.set(language, highlighter);
  return highlighter;
}

function cacheKey(code: string, language: CodeLanguage): string {
  return `${language}:${code}`;
}

function rawTokens(code: string): TokenizedCode {
  return {
    bg: "transparent",
    fg: "inherit",
    tokens: code
      .split("\n")
      .map((line) =>
        line ? [{ color: "inherit", content: line } as ThemedToken] : [],
      ),
  };
}

function useHighlightedCode(
  code: string,
  language: CodeLanguage,
): TokenizedCode {
  const key = cacheKey(code, language);
  const [result, setResult] = useState<{
    key: string;
    value: TokenizedCode;
  } | null>(() => {
    const cached = readTokenCache(key);
    return cached ? { key, value: cached } : null;
  });
  const fallback = useMemo(() => rawTokens(code), [code]);

  useEffect(() => {
    if (language === "text") {
      return;
    }
    const cached = readTokenCache(key);
    if (cached) {
      setResult({ key, value: cached });
      return;
    }
    let cancelled = false;
    void getHighlighter(language)
      .then((highlighter) => {
        const highlighted = highlighter.codeToTokens(code, {
          lang: language,
          themes: { light: "github-light", dark: "github-dark" },
        });
        const value: TokenizedCode = {
          bg: highlighted.bg ?? "transparent",
          fg: highlighted.fg ?? "inherit",
          tokens: highlighted.tokens,
        };
        writeTokenCache(key, value);
        if (!cancelled) {
          setResult({ key, value });
        }
      })
      .catch(() => {
        // 未対応言語でもプレーンテキスト表示は維持する。
      });
    return () => {
      cancelled = true;
    };
  }, [code, key, language]);

  return result?.key === key ? result.value : fallback;
}

function tokenStyle(token: ThemedToken): CSSProperties {
  const fontStyle = token.fontStyle ?? 0;
  return {
    backgroundColor: token.bgColor,
    color: token.color,
    fontStyle: fontStyle & 1 ? "italic" : undefined,
    fontWeight: fontStyle & 2 ? "bold" : undefined,
    textDecoration: fontStyle & 4 ? "underline" : undefined,
    ...token.htmlStyle,
  } as CSSProperties;
}

const CodeBlockBody = memo(function CodeBlockBody({
  tokenized,
  showLineNumbers,
}: {
  tokenized: TokenizedCode;
  showLineNumbers: boolean;
}) {
  return (
    <pre
      className="m-0 overflow-x-auto p-4 text-sm dark:!bg-[var(--shiki-dark-bg)] dark:!text-[var(--shiki-dark)]"
      style={{ backgroundColor: tokenized.bg, color: tokenized.fg }}
    >
      <code
        className={cn(
          "font-mono text-sm",
          showLineNumbers && "[counter-increment:line_0] [counter-reset:line]",
        )}
      >
        {tokenized.tokens.map((line, lineIndex) => (
          <span
            // Shikiのトークン列は位置が識別子になる。
            // biome-ignore lint/suspicious/noArrayIndexKey: コード行の位置自体が安定した識別子
            key={lineIndex}
            className={cn(
              "block",
              showLineNumbers &&
                "before:mr-4 before:inline-block before:w-8 before:select-none before:text-right before:font-mono before:text-muted-foreground/50 before:[counter-increment:line] before:content-[counter(line)]",
            )}
          >
            {line.length === 0
              ? "\n"
              : line.map((token, tokenIndex) => (
                  <span
                    // biome-ignore lint/suspicious/noArrayIndexKey: Shikiトークンの位置自体が安定した識別子
                    key={tokenIndex}
                    className="dark:!bg-[var(--shiki-dark-bg)] dark:!text-[var(--shiki-dark)]"
                    style={tokenStyle(token)}
                  >
                    {token.content}
                  </span>
                ))}
          </span>
        ))}
      </code>
    </pre>
  );
});

export type CodeBlockProps = HTMLAttributes<HTMLDivElement> & {
  code: string;
  language: CodeLanguage;
  showLineNumbers?: boolean;
};

export function CodeBlockContainer({
  className,
  language,
  style,
  ...props
}: HTMLAttributes<HTMLDivElement> & { language: string }) {
  return (
    <div
      data-language={language}
      className={cn(
        "group not-prose relative w-full overflow-hidden rounded-md border bg-background text-foreground",
        className,
      )}
      style={{
        containIntrinsicSize: "auto 200px",
        contentVisibility: "auto",
        ...style,
      }}
      {...props}
    />
  );
}

export function CodeBlockHeader({
  className,
  ...props
}: HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={cn(
        "flex items-center justify-between border-b bg-muted/80 px-3 py-2 text-muted-foreground text-xs",
        className,
      )}
      {...props}
    />
  );
}

export function CodeBlockTitle({
  className,
  ...props
}: HTMLAttributes<HTMLDivElement>) {
  return (
    <div className={cn("flex items-center gap-2", className)} {...props} />
  );
}

export function CodeBlockFilename({
  className,
  ...props
}: HTMLAttributes<HTMLSpanElement>) {
  return <span className={cn("font-mono", className)} {...props} />;
}

export function CodeBlockActions({
  className,
  ...props
}: HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={cn("-my-1 -mr-1 flex items-center gap-2", className)}
      {...props}
    />
  );
}

export function CodeBlockContent({
  code,
  language,
  showLineNumbers = false,
}: {
  code: string;
  language: CodeLanguage;
  showLineNumbers?: boolean;
}) {
  const tokenized = useHighlightedCode(code, language);
  return (
    <div className="relative overflow-auto">
      <CodeBlockBody tokenized={tokenized} showLineNumbers={showLineNumbers} />
    </div>
  );
}

export function CodeBlock({
  code,
  language,
  showLineNumbers = false,
  className,
  children,
  ...props
}: CodeBlockProps) {
  const context = useMemo(() => ({ code }), [code]);
  return (
    <CodeBlockContext.Provider value={context}>
      <CodeBlockContainer className={className} language={language} {...props}>
        {children}
        <CodeBlockContent
          code={code}
          language={language}
          showLineNumbers={showLineNumbers}
        />
      </CodeBlockContainer>
    </CodeBlockContext.Provider>
  );
}

export type CodeBlockCopyButtonProps = ComponentProps<typeof Button> & {
  onCopy?: () => void;
  onError?: (error: Error) => void;
  timeout?: number;
  children?: ReactNode;
};

export function CodeBlockCopyButton({
  onCopy,
  onError,
  timeout = 2000,
  children,
  className,
  ...props
}: CodeBlockCopyButtonProps) {
  const [copied, setCopied] = useState(false);
  const timeoutRef = useRef<number>(0);
  const { code } = useContext(CodeBlockContext);

  const copy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(code);
      setCopied(true);
      onCopy?.();
      window.clearTimeout(timeoutRef.current);
      timeoutRef.current = window.setTimeout(() => setCopied(false), timeout);
    } catch (error) {
      onError?.(error as Error);
    }
  }, [code, onCopy, onError, timeout]);

  useEffect(
    () => () => {
      window.clearTimeout(timeoutRef.current);
    },
    [],
  );

  return (
    <Button
      type="button"
      variant="ghost"
      size="icon-sm"
      aria-label={copied ? "コピーしました" : "コードをコピー"}
      className={cn("shrink-0", className)}
      onClick={copy}
      {...props}
    >
      {children ?? (copied ? <CheckIcon size={14} /> : <CopyIcon size={14} />)}
    </Button>
  );
}
