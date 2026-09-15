import { Image as ImageIcon } from "lucide-react";

export const markdownLinkClass =
  "break-all text-primary underline decoration-primary/40 underline-offset-2 hover:decoration-primary";

/**
 * Markdown images never become <img>. Rendering one would fetch an arbitrary
 * author-controlled URL as soon as a reader opened the conversation, so the
 * shared policy draws image syntax as a link the reader explicitly opens.
 * Both renderers feed hast-derived props; the loose signature accepts either.
 */
export function MarkdownImageLink(props: {
  src?: unknown;
  alt?: unknown;
  title?: unknown;
  node?: unknown;
}) {
  const src = props.src;
  const alt = typeof props.alt === "string" ? props.alt : undefined;
  const title = typeof props.title === "string" ? props.title : undefined;
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
      className={`inline-flex items-baseline gap-1 ${markdownLinkClass}`}
    >
      <ImageIcon className="size-3 self-center" aria-hidden="true" />
      {label}
    </a>
  );
}
