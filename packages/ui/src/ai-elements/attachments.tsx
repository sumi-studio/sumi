import { FileTextIcon, ImageIcon, PaperclipIcon, XIcon } from "lucide-react";
import type { ComponentProps, HTMLAttributes, ReactNode } from "react";
import { createContext, useContext, useMemo } from "react";
import {
  AttachmentAction,
  AttachmentContent,
  AttachmentGroup,
  AttachmentMedia,
  Attachment as AttachmentPrimitive,
  AttachmentTitle,
} from "../components/attachment";
import { cn } from "../lib/utils";

export interface AttachmentData {
  id: string;
  filename: string;
  mediaType?: string;
  url?: string;
}

export type AttachmentVariant = "grid" | "inline" | "list";

const AttachmentsContext = createContext<AttachmentVariant>("inline");
const AttachmentContext = createContext<{
  data: AttachmentData;
  onRemove?: () => void;
  variant: AttachmentVariant;
} | null>(null);

function useAttachment() {
  const context = useContext(AttachmentContext);
  if (!context) {
    throw new Error("Attachment parts must be used inside Attachment");
  }
  return context;
}

export type AttachmentsProps = ComponentProps<typeof AttachmentGroup> & {
  variant?: AttachmentVariant;
};

export function Attachments({
  variant = "inline",
  className,
  ...props
}: AttachmentsProps) {
  return (
    <AttachmentsContext.Provider value={variant}>
      <AttachmentGroup
        data-variant={variant}
        className={cn(
          variant === "grid" && "ml-auto w-fit",
          variant === "list" && "flex-col flex-nowrap",
          className,
        )}
        {...props}
      />
    </AttachmentsContext.Provider>
  );
}

export type AttachmentProps = Omit<
  ComponentProps<typeof AttachmentPrimitive>,
  "children"
> & {
  data: AttachmentData;
  onRemove?: () => void;
  children: ReactNode;
};

export function Attachment({
  data,
  onRemove,
  className,
  children,
  ...props
}: AttachmentProps) {
  const variant = useContext(AttachmentsContext);
  const context = useMemo(
    () => ({ data, onRemove, variant }),
    [data, onRemove, variant],
  );
  return (
    <AttachmentContext.Provider value={context}>
      <AttachmentPrimitive
        size="sm"
        orientation={variant === "grid" ? "vertical" : "horizontal"}
        className={cn(variant === "list" && "w-full", className)}
        {...props}
      >
        {children}
      </AttachmentPrimitive>
    </AttachmentContext.Provider>
  );
}

export function AttachmentPreview({
  className,
  ...props
}: HTMLAttributes<HTMLDivElement>) {
  const { data } = useAttachment();
  const isImage = data.mediaType?.startsWith("image/") && data.url;
  const Icon = data.mediaType?.startsWith("image/") ? ImageIcon : FileTextIcon;
  return (
    <AttachmentMedia className={className} {...props}>
      {isImage ? <img src={data.url} alt={data.filename} /> : <Icon />}
    </AttachmentMedia>
  );
}

export function AttachmentInfo({
  className,
  showMediaType = false,
  ...props
}: HTMLAttributes<HTMLDivElement> & { showMediaType?: boolean }) {
  const { data } = useAttachment();
  return (
    <AttachmentContent className={className} {...props}>
      <AttachmentTitle>{data.filename}</AttachmentTitle>
      {showMediaType && data.mediaType && (
        <span className="block truncate text-muted-foreground text-xs">
          {data.mediaType}
        </span>
      )}
    </AttachmentContent>
  );
}

export function AttachmentRemove({
  label = "添付を削除",
  className,
  ...props
}: ComponentProps<typeof AttachmentAction> & { label?: string }) {
  const { onRemove } = useAttachment();
  if (!onRemove) {
    return null;
  }
  return (
    <AttachmentAction
      type="button"
      aria-label={label}
      onClick={(event) => {
        event.stopPropagation();
        onRemove();
      }}
      className={cn("mr-1 rounded-full", className)}
      {...props}
    >
      <XIcon />
    </AttachmentAction>
  );
}

export function AttachmentEmpty({
  className,
  children = "添付はありません",
  ...props
}: HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={cn(
        "flex items-center justify-center gap-2 p-4 text-muted-foreground text-sm",
        className,
      )}
      {...props}
    >
      <PaperclipIcon className="size-4" />
      {children}
    </div>
  );
}
