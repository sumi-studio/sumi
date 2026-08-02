import { useVirtualizer } from "@tanstack/react-virtual";
import {
  type KeyboardEvent as ReactKeyboardEvent,
  type ReactNode,
  type Ref,
  useCallback,
  useEffect,
  useImperativeHandle,
  useInsertionEffect,
  useLayoutEffect,
  useRef,
  useState,
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
  /** Rendered only while the user has explicitly opened the full transcript. */
  renderTranscriptItem?: (item: TItem, index: number) => ReactNode;
  estimateSize?: (item: TItem, index: number) => number;
  overscan?: number;
  scrollEndThreshold?: number;
  /**
   * Space kept below the last row, inside the scroll range. Prefer this over
   * a trailing spacer item: a constant trailing key hides appends from the
   * virtualizer's end-follow detection.
   */
  paddingEnd?: number;
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
const SCROLL_KEYS = new Set([
  "ArrowUp",
  "ArrowDown",
  "PageUp",
  "PageDown",
  "Home",
  "End",
  " ",
]);

export function ConversationVirtualizer<
  TItem extends ConversationVirtualizerItem,
>({
  ref,
  items,
  renderItem,
  renderTranscriptItem,
  estimateSize,
  overscan = DEFAULT_OVERSCAN,
  scrollEndThreshold = DEFAULT_SCROLL_END_THRESHOLD,
  paddingEnd = 0,
  busy = false,
  ariaLabel = "Sumiとの会話",
  className,
  contentClassName,
  onAtEndChange,
  onVisibleMessageIdsChange,
}: ConversationVirtualizerProps<TItem>) {
  const viewportRef = useRef<HTMLElement>(null);
  const itemsRef = useRef(items);
  const estimateSizeRef = useRef(estimateSize);
  const didInitialEndAnchorRef = useRef(false);
  const previousAtEndRef = useRef<boolean | null>(null);
  const previousVisibleIdsRef = useRef<readonly string[] | null>(null);
  const transcriptTriggerRef = useRef<HTMLButtonElement>(null);
  const transcriptDialogRef = useRef<HTMLDivElement>(null);
  const transcriptCloseRef = useRef<HTMLButtonElement>(null);
  const transcriptWasOpenRef = useRef(false);
  const flightRef = useRef({ active: false, startedAt: 0 });
  const followRef = useRef(true);
  const lastGestureAtRef = useRef(0);
  const [transcriptOpen, setTranscriptOpen] = useState(false);

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

  const virtualizer = useVirtualizer<HTMLElement, HTMLDivElement>({
    count: items.length,
    getScrollElement: () => viewportRef.current,
    estimateSize: getEstimatedSize,
    getItemKey,
    anchorTo: "end",
    followOnAppend: true,
    scrollEndThreshold,
    paddingEnd,
    overscan,
    useFlushSync: false,
  });

  const scrollToEnd = useCallback(
    (options?: Pick<ConversationScrollOptions, "behavior">) => {
      followRef.current = true;
      flightRef.current = { active: true, startedAt: performance.now() };
      virtualizer.scrollToEnd(options);
    },
    [virtualizer],
  );
  const scrollToMessage = useCallback(
    (id: string, options?: ConversationScrollOptions) => {
      const index = itemsRef.current.findIndex((item) => item.id === id);
      if (index < 0) return false;
      followRef.current = false;
      flightRef.current = { active: true, startedAt: performance.now() };
      virtualizer.scrollToIndex(index, options);
      return true;
    },
    [virtualizer],
  );
  // A real user gesture must win over an in-flight programmatic scroll.
  // The virtualizer keeps reconciling toward its last target for seconds,
  // and any counter-write of ours would race the gesture, so the flight is
  // cancelled by clearing the reconcile target itself. Writes are never
  // dropped: dropped writes desynchronize the virtualizer's internal offset
  // from the real one, which later teleports the viewport.
  const interruptProgrammaticScroll = useCallback(() => {
    followRef.current = false;
    lastGestureAtRef.current = performance.now();
    flightRef.current = { active: false, startedAt: 0 };
    // virtual-core@3.17 exposes no public cancel; scrollState is the
    // documented-by-source reconcile target and clearing it ends the flight.
    (virtualizer as unknown as { scrollState: unknown }).scrollState = null;
  }, [virtualizer]);

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
    scrollToEnd();
  }, [items.length, scrollToEnd]);

  // Follow-mode backstop. The virtualizer's own end-follow reacts to size
  // changes only within its threshold and loses the end during fast
  // streaming growth. While the user hasn't scrolled away, re-pin to the
  // end whenever a render leaves the viewport behind it. Skipped during a
  // programmatic flight so a smooth scroll isn't degraded to a snap; the
  // flight ends on arrival, on a user gesture, or after a stale timeout.
  useLayoutEffect(() => {
    if (items.length === 0) return;
    if (virtualizer.isAtEnd()) {
      // A gesture event fires before its scroll movement lands, so being at
      // the end right now doesn't mean the user wants to stay there. Only
      // re-arm following once the gestures have gone quiet.
      if (performance.now() - lastGestureAtRef.current > 300) {
        followRef.current = true;
      }
      flightRef.current = { active: false, startedAt: 0 };
      return;
    }
    const flightIsActive =
      flightRef.current.active &&
      performance.now() - flightRef.current.startedAt < 1_600;
    if (followRef.current && !flightIsActive) {
      flightRef.current = { active: true, startedAt: performance.now() };
      virtualizer.scrollToEnd({ behavior: "auto" });
    }
  });

  const virtualItems = virtualizer.getVirtualItems();
  const virtualItemIds = virtualItems.map(
    (virtualItem) => items[virtualItem.index]?.id,
  );
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

  useInsertionEffect(() => {
    const active = document.activeElement;
    if (!(active instanceof HTMLElement)) return;
    const focusedRow = active.closest<HTMLElement>("[data-message-id]");
    const focusedMessageId = focusedRow?.dataset.messageId;
    if (
      !focusedRow ||
      !viewportRef.current?.contains(focusedRow) ||
      (focusedMessageId !== undefined &&
        virtualItemIds.includes(focusedMessageId))
    ) {
      return;
    }
    // React removes the row after insertion effects. Move focus while the
    // focused control still exists so it never falls back to document.body.
    viewportRef.current.focus({ preventScroll: true });
  }, [virtualItemIds]);

  useEffect(() => {
    if (transcriptOpen) {
      transcriptWasOpenRef.current = true;
      transcriptDialogRef.current?.focus();
      return;
    }
    if (transcriptWasOpenRef.current) {
      transcriptWasOpenRef.current = false;
      transcriptTriggerRef.current?.focus();
    }
  }, [transcriptOpen]);

  const handleViewportScrollCapture = () => {
    const viewport = viewportRef.current;
    if (!viewport) return;
    const active = document.activeElement;
    const focusedRow =
      active instanceof HTMLElement
        ? active.closest<HTMLElement>("[data-message-id]")
        : null;
    if (focusedRow && viewport.contains(focusedRow)) {
      // Scrolling can change the virtual window during this event. Move focus
      // before that rerender rather than letting React remove its control.
      viewport.focus({ preventScroll: true });
    }
  };

  const handleViewportKeyDownCapture = (
    event: ReactKeyboardEvent<HTMLElement>,
  ) => {
    if (SCROLL_KEYS.has(event.key)) interruptProgrammaticScroll();
  };

  return (
    <>
      <button
        ref={transcriptTriggerRef}
        type="button"
        className="sr-only focus:not-sr-only focus:absolute focus:top-2 focus:left-2 focus:z-10 focus:rounded focus:bg-background focus:px-3 focus:py-2 focus:shadow"
        aria-haspopup="dialog"
        onClick={() => setTranscriptOpen(true)}
      >
        会話の全文を開く
      </button>
      <section
        ref={viewportRef}
        data-slot="conversation-viewport"
        tabIndex={-1}
        aria-label={`${ariaLabel}（表示中）`}
        aria-busy={busy}
        onScrollCapture={handleViewportScrollCapture}
        onWheelCapture={interruptProgrammaticScroll}
        onTouchStartCapture={interruptProgrammaticScroll}
        onPointerDownCapture={interruptProgrammaticScroll}
        onKeyDownCapture={handleViewportKeyDownCapture}
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
      </section>
      {transcriptOpen && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4"
          role="presentation"
        >
          <div
            ref={transcriptDialogRef}
            role="dialog"
            aria-modal="true"
            aria-label={`${ariaLabel}の全文`}
            tabIndex={-1}
            onKeyDown={(event) => {
              if (event.key === "Escape") {
                setTranscriptOpen(false);
                return;
              }
              if (event.key === "Tab") {
                // The plain transcript deliberately contains no interactive
                // messages, so its close control is the complete tab stop.
                event.preventDefault();
                transcriptCloseRef.current?.focus();
              }
            }}
            className="flex max-h-full w-full max-w-3xl flex-col rounded-xl bg-background p-4 shadow-xl"
          >
            <div className="mb-3 flex items-center justify-between gap-4">
              <h2 className="font-semibold">会話の全文</h2>
              <button
                ref={transcriptCloseRef}
                type="button"
                onClick={() => setTranscriptOpen(false)}
              >
                閉じる
              </button>
            </div>
            <div
              role="log"
              aria-label={`${ariaLabel}の全文`}
              className="overflow-y-auto"
            >
              {items.map((item, index) => {
                const transcriptItem =
                  renderTranscriptItem?.(item, index) ?? item.id;
                return transcriptItem === null ? null : (
                  <div key={item.id} className="border-b py-3 last:border-0">
                    {transcriptItem}
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      )}
    </>
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
