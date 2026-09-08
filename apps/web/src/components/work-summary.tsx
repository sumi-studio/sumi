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
  headingOnly?: boolean;
  run: AgentRun;
  onOpenChange?: (open: boolean) => void;
}

const TOOL_ICONS = {
  read_file: FileSearch,
  edit_file: Pencil,
  bash: SquareTerminal,
} as const;

export function WorkSummary({
  run,
  onOpenChange,
  headingOnly = false,
}: WorkSummaryProps) {
  // Completion updates the same visible work. Only the reader closes it.
  const [open, setOpen] = useState(true);
  const trace = getInspectableTrace(run.trace);

  if (trace.length === 0) {
    return null;
  }

  if (headingOnly)
    return (
      <div className="flex items-center gap-2 pt-5 pb-2 text-muted-foreground text-xs">
        <span
          className={cn(
            "size-1.5 rounded-full bg-current",
            run.status === "running" && "animate-pulse",
          )}
        />
        {run.audience === "secretary"
          ? "外部の出来事からの活動"
          : "診断用の活動"}
        <span>· {describeAgentRun(run)}</span>
      </div>
    );
  return (
    <div className="pt-3 pb-1">
      <Collapsible
        open={open}
        onOpenChange={(nextOpen) => {
          setOpen(nextOpen);
          onOpenChange?.(nextOpen);
        }}
        className="text-[14px] leading-6"
      >
        <Marker variant="border" className="text-[13px]">
          <CollapsibleTrigger
            render={
              <Button
                variant="ghost"
                size="xs"
                className="h-auto px-0 text-muted-foreground hover:bg-transparent hover:text-foreground"
              />
            }
          >
            <LiveRunDescription run={run} />
            {run.trace.some(
              (item) =>
                item.type === "error" ||
                (item.type === "tool" && item.status === "error"),
            ) && <span className="text-red-600">エラーあり</span>}
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

export function TraceRow({
  event,
  phase = "result",
  open,
  onOpenChange,
}: {
  event: AgentTraceEvent;
  phase?: "activity" | "result";
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
}) {
  switch (event.type) {
    case "reasoning":
      return (
        <div className="space-y-1 border-border border-l-2 pl-4">
          <p className="text-muted-foreground text-xs">
            思考の要約
            {event.status === "streaming"
              ? " · 更新中"
              : event.status === "incomplete"
                ? " · 途中までの記録"
                : ""}
          </p>
          <ReasoningContent streaming={event.status === "streaming"}>
            {event.text}
          </ReasoningContent>
        </div>
      );
    case "tool":
      return (
        <ToolTraceRow
          event={event}
          phase={phase}
          open={open}
          onOpenChange={onOpenChange}
        />
      );
    case "approval": {
      const Icon =
        event.status === "pending"
          ? ShieldAlert
          : event.status === "denied" || event.status === "rejected"
            ? ShieldX
            : event.status === "cancelled"
              ? ShieldX
              : ShieldCheck;
      const status =
        event.status === "pending"
          ? "承認待ち"
          : event.status === "denied"
            ? "拒否"
            : event.status === "rejected"
              ? "実行されず"
              : event.status === "cancelled"
                ? "キャンセル"
                : "許可";
      return (
        <TraceLine
          icon={Icon}
          tone={
            event.status === "denied" || event.status === "rejected"
              ? "error"
              : "default"
          }
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
  phase,
  open,
  onOpenChange,
}: {
  event: Extract<AgentTraceEvent, { type: "tool" }>;
  phase: "activity" | "result";
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
}) {
  const Icon = TOOL_ICONS[event.name as keyof typeof TOOL_ICONS] ?? Wrench;
  const detail = pickString(event.args.path) ?? pickString(event.args.command);
  const status = {
    pending: "待機中",
    running: "実行中",
    done: "完了",
    error: "失敗",
    cancelled: "中止",
  }[event.status];
  const resultText =
    phase === "activity"
      ? event.progress === undefined
        ? null
        : displayValue(event.progress)
      : event.result === undefined
        ? null
        : displayValue(event.result);
  const previewText =
    phase === "result" ? previewValue(event.result) : resultText;
  const preview = previewText?.slice(0, 240);
  return (
    <details
      open={open}
      onToggle={
        onOpenChange
          ? (event) => onOpenChange(event.currentTarget.open)
          : undefined
      }
      className={cn(
        "group/tool min-w-0 bg-background open:pb-3",
        phase === "result" ? "border-border border-l-2 pl-1" : "",
      )}
    >
      <summary className="flex cursor-pointer list-none flex-wrap items-center gap-x-2 gap-y-1 px-3 py-1.5 [&::-webkit-details-marker]:hidden">
        <Icon
          className={cn(
            "size-4 shrink-0 text-muted-foreground",
            event.status === "running" && "animate-pulse",
          )}
        />
        <span className="min-w-0 flex-1 break-words font-medium text-foreground">
          {phase === "activity" ? event.name : event.label}
        </span>
        <span
          className={cn(
            "text-xs",
            phase === "result" && event.status === "error"
              ? "text-red-600"
              : "text-muted-foreground",
          )}
        >
          {phase === "activity" &&
          (event.status === "done" ||
            event.status === "error" ||
            event.status === "cancelled")
            ? "操作"
            : status}
        </span>
        <ChevronRight className="size-3.5 text-muted-foreground transition-transform group-open/tool:rotate-90 motion-reduce:transition-none" />
        {detail && (
          <code className="w-full truncate pl-6 font-mono text-muted-foreground text-xs">
            {detail}
          </code>
        )}
        {preview && (
          <p
            className={cn(
              "line-clamp-3 w-full whitespace-pre-wrap break-words pl-6 text-[13px] group-open/tool:hidden",
              event.status === "error"
                ? "text-red-600"
                : "text-muted-foreground",
            )}
          >
            {preview}
            {previewText && previewText.length > 240 ? "…" : ""}
          </p>
        )}
      </summary>
      <div className="space-y-3 px-3">
        <Payload label="入力" text={displayValue(event.args)} />
        {phase === "activity" && resultText !== null && (
          <Payload label="進行状況" text={resultText} />
        )}
        {phase === "result" && (
          <Payload
            label="結果"
            text={
              resultText ??
              (event.status === "running" || event.status === "pending"
                ? "結果を待っています"
                : "結果の本文は記録されていません")
            }
          />
        )}
      </div>
    </details>
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

function previewValue(value: unknown): string | null {
  if (value === undefined) return null;
  if (value && typeof value === "object" && !Array.isArray(value)) {
    const record = value as Record<string, unknown>;
    for (const key of ["error", "content", "output", "message", "stdout"]) {
      if (typeof record[key] === "string" && record[key]) return record[key];
    }
  }
  return displayValue(value);
}

function displayValue(value: unknown): string {
  return typeof value === "string"
    ? value
    : (JSON.stringify(value, null, 2) ?? "");
}

function Payload({ label, text }: { label: string; text: string }) {
  return (
    <section className="min-w-0" aria-label={label}>
      <h3 className="mb-1 text-muted-foreground text-xs">{label}</h3>
      <pre
        // biome-ignore lint/a11y/noNoninteractiveTabindex: bounded output must be keyboard-scrollable
        tabIndex={0}
        className="max-h-72 overflow-auto whitespace-pre-wrap break-words rounded-md bg-secondary/50 p-3 font-mono text-[12px] leading-5 text-foreground"
      >
        {text}
      </pre>
    </section>
  );
}

function pickString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}
