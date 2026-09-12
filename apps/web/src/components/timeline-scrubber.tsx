import { Button } from "@sumi/ui/components/button";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from "@sumi/ui/components/sheet";
import { cn } from "@sumi/ui/lib/utils";
import { FileText } from "lucide-react";
import { useLayoutEffect, useState } from "react";
import type { ChatItem } from "../agent/model";

export interface ScrubberTick {
  id: string;
  /** ユーザー発話 (カードの見出し) */
  title: string;
  /** 直後のアシスタント応答の抜粋 (カードの本文) */
  preview?: string;
  /** 添付ファイル名など (カード下部のチップ) */
  chip?: string;
}

export interface ConversationTimeline {
  ticks: ScrubberTick[];
  messageIds: string[];
  visibleRange: [number, number] | null;
}

interface TimelineScrubberProps {
  ticks: ScrubberTick[];
  /** 現在ビューポートに見えている往復の範囲 [先頭, 末尾] */
  visibleRange: [number, number] | null;
  onJump: (index: number) => void;
  className?: string;
}

/** 目盛りの間隔。狭めに保ち、収まらなくなったら圧縮する */
const MAX_SPACING = 14;
const MIN_SPACING = 7;
/** 目盛りの長さ (px)。既定は全目盛り同じ。ホバー時のみ山なりに伸びる */
const BASE_WIDTH = 10;
const PEAK_WIDTH = 22;
/** 山の裾野の広さ (ホバー位置から何目盛りで平常に戻るか) */
const PEAK_FALLOFF = 4;

/**
 * シングルスレッドの長い会話を俯瞰するタイムライン。1 目盛り = 1 往復。
 * 既定はごく控えめ (全目盛り同サイズ、ビュー内は色だけ濃く)。ホバーすると
 * 触れた目盛りを頂点に山なりに伸び、他は一様に薄くなる。
 */
export function TimelineScrubber({
  ticks,
  visibleRange,
  onJump,
  className,
}: TimelineScrubberProps) {
  const [hoveredId, setHoveredId] = useState<string | null>(null);
  const hoveredIndex = ticks.findIndex((tick) => tick.id === hoveredId);
  const hovered = hoveredIndex < 0 ? null : hoveredIndex;
  const [container, setContainer] = useState<HTMLElement | null>(null);
  const [containerHeight, setContainerHeight] = useState(0);

  useLayoutEffect(() => {
    const el = container;
    if (!el) {
      return;
    }
    const observer = new ResizeObserver(() =>
      setContainerHeight(el.clientHeight),
    );
    observer.observe(el);
    setContainerHeight(el.clientHeight);
    return () => observer.disconnect();
  }, [container]);

  if (ticks.length === 0) {
    return null;
  }

  const spacing =
    containerHeight > 0
      ? Math.max(
          MIN_SPACING,
          Math.min(MAX_SPACING, containerHeight / ticks.length),
        )
      : MAX_SPACING;

  const capacity = Math.max(2, Math.floor(containerHeight / MIN_SPACING));
  const dense = ticks.length > capacity;
  const selected = hovered ?? visibleRange?.[0] ?? 0;
  const indexAt = (clientY: number) => {
    const rect = container?.getBoundingClientRect();
    if (!rect || rect.height <= 0) return selected;
    return Math.round(
      Math.max(0, Math.min(1, (clientY - rect.top) / rect.height)) *
        (ticks.length - 1),
    );
  };
  const marks = dense
    ? Array.from({ length: capacity }, (_, index) =>
        Math.round((index * (ticks.length - 1)) / (capacity - 1)),
      )
    : [];

  const widthOf = (i: number): number => {
    if (hovered === null) {
      return BASE_WIDTH;
    }
    const falloff = Math.max(
      0,
      1 -
        Math.abs(i - hovered) /
          (PEAK_FALLOFF * (dense ? (ticks.length - 1) / (capacity - 1) : 1)),
    );
    return BASE_WIDTH + (PEAK_WIDTH - BASE_WIDTH) * falloff;
  };

  const colorOf = (i: number): string => {
    if (hovered !== null) {
      // ホバー中: 触れている目盛りだけ濃く、他は問答無用で薄く
      return i === hovered ? "bg-neutral-800" : "bg-neutral-200";
    }
    const inView =
      visibleRange !== null && i >= visibleRange[0] && i <= visibleRange[1];
    return inView ? "bg-neutral-500" : "bg-neutral-300";
  };

  return (
    <nav
      ref={setContainer}
      className={cn(
        "flex h-full w-6 flex-col items-start justify-center",
        className,
      )}
      onMouseLeave={() => setHoveredId(null)}
      aria-label="会話タイムライン"
    >
      {dense ? (
        <div
          role="slider"
          tabIndex={0}
          aria-label="会話の位置"
          aria-orientation="vertical"
          aria-valuemin={1}
          aria-valuemax={ticks.length}
          aria-valuenow={selected + 1}
          aria-valuetext={`${selected + 1} / ${ticks.length} — ${ticks[selected]?.title || "添付のみ"}`}
          className="relative h-full w-6 outline-none focus-visible:ring-2 focus-visible:ring-neutral-300"
          onPointerMove={(event) =>
            setHoveredId(ticks[indexAt(event.clientY)].id)
          }
          onFocus={() => setHoveredId(ticks[selected].id)}
          onBlur={() => setHoveredId(null)}
          onClick={(event) => {
            const index = indexAt(event.clientY);
            setHoveredId(ticks[index].id);
            onJump(index);
          }}
          onKeyDown={(event) => {
            const next =
              event.key === "Home"
                ? 0
                : event.key === "End"
                  ? ticks.length - 1
                  : event.key === "ArrowDown" || event.key === "ArrowRight"
                    ? Math.min(ticks.length - 1, selected + 1)
                    : event.key === "ArrowUp" || event.key === "ArrowLeft"
                      ? Math.max(0, selected - 1)
                      : event.key === "PageDown"
                        ? Math.min(ticks.length - 1, selected + 10)
                        : event.key === "PageUp"
                          ? Math.max(0, selected - 10)
                          : event.key === "Enter" || event.key === " "
                            ? selected
                            : null;
            if (next === null) return;
            event.preventDefault();
            setHoveredId(ticks[next].id);
            onJump(next);
          }}
        >
          {marks.map((index) => (
            <span
              key={ticks[index].id}
              aria-hidden="true"
              className={cn(
                "pointer-events-none absolute left-0 h-[2px] -translate-y-1/2 rounded-full transition-[width,background-color] duration-150",
                colorOf(index),
              )}
              style={{
                top: `${(index / (ticks.length - 1)) * 100}%`,
                width: widthOf(index),
              }}
            />
          ))}
          {hovered !== null && (
            <span
              aria-hidden="true"
              className="pointer-events-none absolute left-0 h-[2px] -translate-y-1/2 rounded-full bg-neutral-800"
              style={{
                top: `${(hovered / (ticks.length - 1)) * 100}%`,
                width: PEAK_WIDTH,
              }}
            />
          )}
          {visibleRange && hovered === null && (
            <span
              aria-hidden="true"
              className="pointer-events-none absolute left-0 min-h-[2px] rounded-full bg-neutral-500"
              style={{
                top: `${(visibleRange[0] / (ticks.length - 1)) * 100}%`,
                height: `${((visibleRange[1] - visibleRange[0]) / (ticks.length - 1)) * 100}%`,
                width: BASE_WIDTH,
              }}
            />
          )}
          {hovered !== null && (
            <div
              className="pointer-events-none absolute left-8 z-10 w-72 -translate-y-1/2 rounded-xl border border-neutral-200 bg-white p-3.5 shadow-[0_4px_24px_rgba(0,0,0,0.08)]"
              style={{ top: `${(hovered / (ticks.length - 1)) * 100}%` }}
            >
              <p className="line-clamp-2 font-medium text-[13px] text-neutral-900 leading-5">
                {ticks[hovered].title || "(添付のみ)"}
              </p>
              {ticks[hovered].preview && (
                <p className="mt-1.5 line-clamp-3 text-[13px] text-neutral-500 leading-5">
                  {ticks[hovered].preview}
                </p>
              )}
              <p className="mt-1.5 text-[11px] text-neutral-400">
                {hovered + 1} / {ticks.length}
              </p>
            </div>
          )}
        </div>
      ) : (
        ticks.map((tick, i) => (
          <div
            key={tick.id}
            className="relative flex items-center"
            style={{ height: spacing }}
          >
            <Button
              variant="ghost"
              aria-label={`「${tick.title.slice(0, 20)}」へ移動`}
              onMouseEnter={() => setHoveredId(tick.id)}
              onFocus={() => setHoveredId(tick.id)}
              onClick={() => onJump(i)}
              className="h-full w-6 justify-start rounded-none p-0 hover:bg-transparent"
            >
              <span
                className={cn(
                  "h-[2px] rounded-full transition-[width,background-color] duration-150",
                  colorOf(i),
                )}
                style={{ width: widthOf(i) }}
              />
            </Button>

            {hovered === i && (
              <div className="-translate-y-1/2 pointer-events-none absolute top-1/2 left-8 z-10 w-72 rounded-xl border border-neutral-200 bg-white p-3.5 shadow-[0_4px_24px_rgba(0,0,0,0.08)]">
                <p className="line-clamp-2 font-medium text-[13px] text-neutral-900 leading-5">
                  {tick.title || "(添付のみ)"}
                </p>
                {tick.preview && (
                  <p className="mt-1.5 line-clamp-3 text-[13px] text-neutral-500 leading-5">
                    {tick.preview}
                  </p>
                )}
                {tick.chip && (
                  <p className="mt-2.5 flex items-center gap-1.5 text-[13px] text-neutral-600">
                    <FileText className="size-3.5 shrink-0 text-neutral-400" />
                    <span className="truncate">{tick.chip}</span>
                  </p>
                )}
              </div>
            )}
          </div>
        ))
      )}
    </nav>
  );
}

export function MobileTimelineSheet({
  open,
  onOpenChange,
  ticks,
  onJump,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  ticks: ScrubberTick[];
  onJump: (index: number) => void;
}) {
  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent
        side="bottom"
        className="max-h-[70dvh] pb-[max(1rem,env(safe-area-inset-bottom))] md:hidden"
      >
        <SheetHeader className="sticky top-0 z-10 bg-white px-5 pt-4 pb-2">
          <SheetTitle>会話タイムライン</SheetTitle>
        </SheetHeader>
        <ul className="overflow-y-auto">
          {ticks.map((tick, index) => (
            <li key={tick.id}>
              <Button
                variant="ghost"
                onClick={() => onJump(index)}
                className="h-auto w-full justify-start rounded-none px-5 py-3 text-left"
              >
                <span className="min-w-0">
                  <span className="block line-clamp-1 font-medium text-[14px] text-neutral-900">
                    {tick.title}
                  </span>
                  {tick.preview && (
                    <span className="mt-0.5 block line-clamp-2 whitespace-normal text-[13px] text-neutral-500 leading-5">
                      {tick.preview}
                    </span>
                  )}
                </span>
              </Button>
            </li>
          ))}
        </ul>
      </SheetContent>
    </Sheet>
  );
}

interface Exchange {
  startIndex: number;
  endIndex: number;
  tick: ScrubberTick;
}

/** チャット項目列を、タイムライン表示に必要な1往復単位へ変換する。 */
export function createConversationTimeline(
  items: ChatItem[],
  visibleMessageIds: string[],
  historyIndex?: readonly ScrubberTick[],
): ConversationTimeline {
  const exchanges: Exchange[] = [];
  const itemIndexById = new Map<string, number>();
  items.forEach((item, index) => {
    itemIndexById.set(item.id, index);
    if (item.kind === "user") {
      const previous = exchanges.at(-1);
      if (previous) {
        previous.endIndex = index - 1;
      }
      exchanges.push({
        startIndex: index,
        endIndex: items.length - 1,
        tick: {
          id: item.id,
          title: item.text,
        },
      });
      return;
    }

    if (item.kind === "prose") {
      const current = exchanges.at(-1);
      if (current && !current.tick.preview) {
        current.tick.preview = toExcerpt(item.text);
      }
    }
  });

  const visibleIndexes = visibleMessageIds
    .flatMap((id) => {
      const index = itemIndexById.get(id);
      return index === undefined ? [] : [index];
    })
    .sort((a, b) => a - b);

  const localRange = computeVisibleRange(
    exchanges,
    visibleIndexes[0],
    visibleIndexes.at(-1),
  );
  if (historyIndex?.length) {
    // Navigation represents the conversation, not the currently loaded body
    // window. Keep server index entries stable when an older page arrives.
    const known = new Set(historyIndex.map((tick) => tick.id));
    const ticks = [
      ...historyIndex,
      ...exchanges
        .filter(({ tick }) => !known.has(tick.id))
        .map(({ tick }) => tick),
    ];
    const positions = new Map(ticks.map((tick, index) => [tick.id, index]));
    const start = localRange && positions.get(exchanges[localRange[0]].tick.id);
    const end = localRange && positions.get(exchanges[localRange[1]].tick.id);
    return {
      ticks,
      messageIds: ticks.map((tick) => tick.id),
      visibleRange: start != null && end != null ? [start, end] : null,
    };
  }
  return {
    ticks: exchanges.map((exchange) => exchange.tick),
    messageIds: exchanges.map((exchange) => exchange.tick.id),
    visibleRange: localRange,
  };
}

function toExcerpt(text: string): string {
  return text
    .replace(/```[\s\S]*?```/g, " (コード) ")
    .replace(/\$\$[\s\S]*?\$\$/g, " (数式) ")
    .replace(/[#*`>|$_-]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 140);
}

function computeVisibleRange(
  exchanges: Exchange[],
  firstVisible: number | undefined,
  lastVisible: number | undefined,
): [number, number] | null {
  if (firstVisible === undefined || lastVisible === undefined) {
    return null;
  }

  let start: number | null = null;
  let end: number | null = null;
  exchanges.forEach((exchange, index) => {
    if (
      exchange.startIndex <= lastVisible &&
      exchange.endIndex >= firstVisible
    ) {
      start ??= index;
      end = index;
    }
  });
  return start !== null && end !== null ? [start, end] : null;
}
