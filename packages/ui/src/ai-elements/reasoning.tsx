import { cn } from "../lib/utils";

interface ReasoningContentProps {
  children: string;
  streaming?: boolean;
  className?: string;
}

/** AI Elements ReasoningContent相当。開閉の所有権は親のToolに置く。 */
export function ReasoningContent({
  children,
  streaming = false,
  className,
}: ReasoningContentProps) {
  return (
    <p
      className={cn(
        "whitespace-pre-wrap text-muted-foreground leading-6",
        className,
      )}
    >
      {children}
      {streaming && <span className="animate-pulse">▍</span>}
    </p>
  );
}
