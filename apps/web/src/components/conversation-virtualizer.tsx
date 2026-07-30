import { useVirtualizer } from "@tanstack/react-virtual";
import {
  type ReactNode,
  type Ref,
  useCallback,
  useImperativeHandle,
  useLayoutEffect,
  useRef,
} from "react";

export interface ConversationVirtualizerItem {
  id: string;
}

export interface ConversationScrollOptions {
  align?: "start" | "center" | "end" | "auto";
  behavior?: "auto" | "smooth" | "instant";
}

export interface ConversationVirtualizerHandle {
  isAtEnd: () => boolean;
  scrollToEnd: (options?: Pick<ConversationScrollOptions, "behavior">) => void;
  scrollToMessage: (id: string, options?: ConversationScrollOptions) => boolean;
}

export interface ConversationVirtualizerProps<
  TItem extends ConversationVirtualizerItem,
> {
  ref?: Ref<ConversationVirtualizerHandle>;
  items: readonly TItem[];
  renderItem: (item: TItem, index: number) => ReactNode;
  estimateSize?: (item: TItem, index: number) => number;
  overscan?: number;
  scrollEndThreshold?: number;
  busy?: boolean;
  ariaLabel?: string;
  className?: string;
  contentClassName?: string;
  onAtEndChange?: (atEnd: boolean) => void;
  onVisibleMessageIdsChange?: (ids: string[]) => void;
}

const DEFAULT_ESTIMATED_ITEM_SIZE = 96;
const DEFAULT_OVERSCAN = 6;
const DEFAULT_SCROLL_END_THRESHOLD = 80;

export function ConversationVirtualizer<
  TItem extends ConversationVirtualizerItem,
>({
  ref,
  items,
  renderItem,
  estimateSize,
  overscan = DEFAULT_OVERSCAN,
  scrollEndThreshold = DEFAULT_SCROLL_END_THRESHOLD,
  busy = false,
  ariaLabel = "Sumiとの会話",
  className,
  contentClassName,
  onAtEndChange,
  onVisibleMessageIdsChange,
}: ConversationVirtualizerProps<TItem>) {
  const viewportRef = useRef<HTMLDivElement>(null);
  const itemsRef = useRef(items);
  const estimateSizeRef = useRef(estimateSize);
  const didInitialEndAnchorRef = useRef(false);
  const previousAtEndRef = useRef<boolean | null>(null);
  const previousVisibleIdsRef = useRef<readonly string[] | null>(null);

  itemsRef.current = items;
  estimateSizeRef.current = estimateSize;

  const getItemKey = useCallback(
    (index: number) => itemsRef.current[index]?.id ?? `missing-item-${index}`,
    [],
  );
  const getEstimatedSize = useCallback((index: number) => {
    const item = itemsRef.current[index];
    return item && estimateSizeRef.current
      ? estimateSizeRef.current(item, index)
      : DEFAULT_ESTIMATED_ITEM_SIZE;
  }, []);

  const virtualizer = useVirtualizer<HTMLDivElement, HTMLDivElement>({
    count: items.length,
    getScrollElement: () => viewportRef.current,
    estimateSize: getEstimatedSize,
    getItemKey,
    anchorTo: "end",
    followOnAppend: true,
    scrollEndThreshold,
    overscan,
    useFlushSync: false,
  });

  const scrollToEnd = useCallback(
    (options?: Pick<ConversationScrollOptions, "behavior">) => {
      virtualizer.scrollToEnd(options);
    },
    [virtualizer],
  );
  const scrollToMessage = useCallback(
    (id: string, options?: ConversationScrollOptions) => {
      const index = itemsRef.current.findIndex((item) => item.id === id);
      if (index < 0) return false;
      virtualizer.scrollToIndex(index, options);
      return true;
    },
    [virtualizer],
  );

  useImperativeHandle(
    ref,
    () => ({
      isAtEnd: () => itemsRef.current.length === 0 || virtualizer.isAtEnd(),
      scrollToEnd,
      scrollToMessage,
    }),
    [scrollToEnd, scrollToMessage, virtualizer],
  );

  useLayoutEffect(() => {
    if (didInitialEndAnchorRef.current || items.length === 0) return;
    didInitialEndAnchorRef.current = true;
    virtualizer.scrollToEnd();
  }, [items.length, virtualizer]);

  const virtualItems = virtualizer.getVirtualItems();
  const visibleMessageIds = getVisibleMessageIds(items, virtualizer.range);
  const atEnd = items.length === 0 || virtualizer.isAtEnd();

  useLayoutEffect(() => {
    if (previousAtEndRef.current === atEnd) return;
    previousAtEndRef.current = atEnd;
    onAtEndChange?.(atEnd);
  }, [atEnd, onAtEndChange]);

  useLayoutEffect(() => {
    if (sameIds(previousVisibleIdsRef.current, visibleMessageIds)) return;
    previousVisibleIdsRef.current = visibleMessageIds;
    onVisibleMessageIdsChange?.(visibleMessageIds);
  }, [onVisibleMessageIdsChange, visibleMessageIds]);

  return (
    <div
      ref={viewportRef}
      role="log"
      aria-label={ariaLabel}
      aria-busy={busy}
      className={className}
      style={{
        height: "100%",
        overflowX: "hidden",
        overflowY: "auto",
      }}
    >
      <div
        className={contentClassName}
        style={{
          height: virtualizer.getTotalSize(),
          position: "relative",
          width: "100%",
        }}
      >
        {virtualItems.map((virtualItem) => {
          const item = items[virtualItem.index];
          if (!item) return null;

          return (
            <div
              key={virtualItem.key}
              ref={virtualizer.measureElement}
              data-index={virtualItem.index}
              data-message-id={item.id}
              style={{
                left: 0,
                position: "absolute",
                top: 0,
                transform: `translateY(${virtualItem.start}px)`,
                width: "100%",
              }}
            >
              {renderItem(item, virtualItem.index)}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function sameIds(
  previous: readonly string[] | null,
  current: readonly string[],
) {
  return (
    previous !== null &&
    previous.length === current.length &&
    previous.every((id, index) => id === current[index])
  );
}

function getVisibleMessageIds<TItem extends ConversationVirtualizerItem>(
  items: readonly TItem[],
  range: { startIndex: number; endIndex: number } | null,
) {
  if (!range) return [];

  const ids: string[] = [];
  for (let index = range.startIndex; index <= range.endIndex; index += 1) {
    const id = items[index]?.id;
    if (id) ids.push(id);
  }
  return ids;
}
