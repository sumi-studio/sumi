import { ReasoningContent } from "@sumi/ui/ai-elements/reasoning";
import { Button } from "@sumi/ui/components/button";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@sumi/ui/components/collapsible";
import { Marker } from "@sumi/ui/components/marker";
import { cn } from "@sumi/ui/lib/utils";
import {
  Box,
  ChevronRight,
  CircleAlert,
  FileSearch,
  Loader2,
  Pencil,
  ShieldAlert,
  ShieldCheck,
  ShieldX,
  SquareTerminal,
  Wrench,
} from "lucide-react";
import { type ReactNode, useState } from "react";
import type { AgentTraceEvent } from "../agent/model";
import {
  type AgentRun,
  describeAgentRun,
  getInspectableTrace,
} from "../agent/work-summary";

interface WorkSummaryProps {
  run: AgentRun;
  onOpenChange?: (open: boolean) => void;
}

const TOOL_ICONS = {
  read_file: FileSearch,
  edit_file: Pencil,
  bash: SquareTerminal,
} as const;

export function WorkSummary({ run, onOpenChange }: WorkSummaryProps) {
  const [open, setOpen] = useState(false);
  const trace = getInspectableTrace(run.trace);

  if (trace.length === 0) {
    return null;
  }

  return (
    <div className="pt-3 pb-1">
      <Collapsible
        open={open}
        onOpenChange={(nextOpen) => {
          setOpen(nextOpen);
          onOpenChange?.(nextOpen);
        }}
        className="text-sm"
      >
        <Marker variant="border" className="text-[13px]">
          <CollapsibleTrigger
            render={
              <Button
                variant="ghost"
                size="xs"
                className="h-auto px-0 text-neutral-400 hover:bg-transparent hover:text-neutral-600"
              />
            }
          >
            <LiveRunDescription run={run} />
            <ChevronRight
              className={cn(
                "size-3.5 transition-transform duration-150",
                open && "rotate-90",
              )}
            />
          </CollapsibleTrigger>
        </Marker>

        <CollapsibleContent className="h-(--collapsible-panel-height) overflow-hidden opacity-100 outline-none transition-[height,opacity] duration-200 ease-out data-ending-style:h-0 data-ending-style:opacity-0 data-starting-style:h-0 data-starting-style:opacity-0 motion-reduce:transition-none">
          <div className="mt-2 space-y-3 border-neutral-200 border-l-2 pl-4">
            {trace.map((event) => (
              <TraceRow key={event.id} event={event} />
            ))}
          </div>
        </CollapsibleContent>
      </Collapsible>
    </div>
  );
}

function LiveRunDescription({ run }: { run: AgentRun }) {
  return <span>{describeAgentRun(run)}</span>;
}

function TraceRow({ event }: { event: AgentTraceEvent }) {
  switch (event.type) {
    case "reasoning":
      return (
        <ReasoningContent streaming={event.status === "streaming"}>
          {event.text}
        </ReasoningContent>
      );
    case "tool":
      return <ToolTraceRow event={event} />;
    case "approval": {
      const Icon =
        event.status === "pending"
          ? ShieldAlert
          : event.status === "denied"
            ? ShieldX
            : event.status === "cancelled"
              ? ShieldX
              : ShieldCheck;
      const status =
        event.status === "pending"
          ? "承認待ち"
          : event.status === "denied"
            ? "拒否"
            : event.status === "cancelled"
              ? "キャンセル"
              : event.decision?.type === "approve_always"
                ? "常に許可"
                : "許可";
      return (
        <TraceLine
          icon={Icon}
          tone={event.status === "denied" ? "error" : "default"}
          muted={event.status === "cancelled"}
        >
          <span>{event.summary}</span>
          <span className="shrink-0 text-neutral-400 text-xs">{status}</span>
        </TraceLine>
      );
    }
    case "artifact":
      return <TraceLine icon={Box}>{event.label}</TraceLine>;
    case "error":
      return (
        <TraceLine icon={CircleAlert} tone="error">
          {event.message}
        </TraceLine>
      );
  }
}

function ToolTraceRow({
  event,
}: {
  event: Extract<AgentTraceEvent, { type: "tool" }>;
}) {
  const Icon = TOOL_ICONS[event.name as keyof typeof TOOL_ICONS] ?? Wrench;
  const result = asRecord(event.result);
  const detail = pickString(event.args.path) ?? pickString(event.args.command);
  const additions = pickNumber(result.additions);
  const deletions = pickNumber(result.deletions);
  return (
    <TraceLine
      icon={event.status === "running" ? Loader2 : Icon}
      iconClassName={event.status === "running" ? "animate-spin" : undefined}
      tone={event.status === "error" ? "error" : "default"}
      muted={event.status === "cancelled"}
    >
      <span>{event.label}</span>
      {detail && (
        <code className="max-w-full truncate rounded bg-neutral-100 px-1.5 py-0.5 font-mono text-neutral-700 text-xs">
          {detail}
        </code>
      )}
      {additions !== undefined && (
        <span className="font-mono text-emerald-600 text-xs">+{additions}</span>
      )}
      {deletions !== undefined && (
        <span className="font-mono text-red-500 text-xs">-{deletions}</span>
      )}
    </TraceLine>
  );
}

function TraceLine({
  icon: Icon,
  children,
  iconClassName,
  tone = "default",
  muted = false,
}: {
  icon: typeof Wrench;
  children: ReactNode;
  iconClassName?: string;
  tone?: "default" | "error";
  muted?: boolean;
}) {
  return (
    <div
      className={cn(
        "flex min-w-0 items-center gap-2 text-neutral-600",
        tone === "error" && "text-red-600",
        muted && "text-neutral-400",
      )}
    >
      <Icon className={cn("size-4 shrink-0 text-neutral-400", iconClassName)} />
      {children}
    </div>
  );
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object"
    ? (value as Record<string, unknown>)
    : {};
}

function pickString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function pickNumber(value: unknown): number | undefined {
  return typeof value === "number" ? value : undefined;
}
