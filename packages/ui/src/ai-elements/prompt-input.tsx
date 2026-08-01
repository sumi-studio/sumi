import type { ComponentProps, HTMLAttributes } from "react";
import { Button } from "../components/button";
import { Textarea } from "../components/textarea";
import { Tooltip, TooltipContent, TooltipTrigger } from "../components/tooltip";
import { cn } from "../lib/utils";

/** AI Elements PromptInputのcomposableなフォーム外枠。 */
export function PromptInput({ className, ...props }: ComponentProps<"form">) {
  return (
    <form
      className={cn(
        "rounded-[1.375rem] border border-input bg-background shadow-xs focus-within:border-ring",
        className,
      )}
      {...props}
    />
  );
}

export function PromptInputBody({
  className,
  ...props
}: HTMLAttributes<HTMLDivElement>) {
  return <div className={cn("contents", className)} {...props} />;
}

export function PromptInputTextarea({
  className,
  ...props
}: ComponentProps<typeof Textarea>) {
  return (
    <Textarea
      name="message"
      className={cn(
        "prompt-input-textarea scrollbar-ui mx-2 min-h-0 w-[calc(100%-1rem)] resize-none rounded-none border-0 bg-transparent px-2 pt-3.5 pb-1 text-[15px] leading-6 shadow-none focus-visible:border-transparent focus-visible:ring-0",
        className,
      )}
      {...props}
    />
  );
}

export function PromptInputFooter({
  className,
  ...props
}: HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={cn("flex items-center gap-1 px-2.5 pb-2.5", className)}
      {...props}
    />
  );
}

export function PromptInputTools({
  className,
  ...props
}: HTMLAttributes<HTMLDivElement>) {
  return (
    <div className={cn("flex items-center gap-1", className)} {...props} />
  );
}

export type PromptInputButtonProps = ComponentProps<typeof Button> & {
  tooltip?: string;
  label: string;
};

export function PromptInputButton({
  tooltip,
  label,
  variant = "ghost",
  size = "icon",
  className,
  children,
  ...props
}: PromptInputButtonProps) {
  const button = (
    <Button
      type="button"
      variant={variant}
      size={size}
      className={cn("rounded-full", className)}
      {...props}
    />
  );
  return tooltip ? (
    <Tooltip>
      <TooltipTrigger render={button}>
        {children}
        <span className="sr-only">{label}</span>
      </TooltipTrigger>
      <TooltipContent>{tooltip}</TooltipContent>
    </Tooltip>
  ) : (
    <Button
      type="button"
      variant={variant}
      size={size}
      className={cn("rounded-full", className)}
      {...props}
    >
      {children}
      <span className="sr-only">{label}</span>
    </Button>
  );
}
