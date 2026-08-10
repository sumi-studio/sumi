import { StrictMode, useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  ConversationVirtualizer,
  type ConversationVirtualizerHandle,
} from "../src/components/conversation-virtualizer";

/**
 * Measurement harness: drives ConversationVirtualizer through the same
 * sequence the real chat screen performs right after the user sends a
 * message, while logging every scroll write against the viewport.
 */

interface HarnessRow {
  id: string;
  h: number;
}

interface ScrollLogEntry {
  t: number;
  type: string;
  detail: Record<string, unknown>;
}

const log: ScrollLogEntry[] = [];
(window as unknown as Record<string, unknown>).__scrollLog = log;

function isViewport(element: unknown): element is HTMLElement {
  return (
    element instanceof HTMLElement &&
    element.getAttribute("data-slot") === "conversation-viewport"
  );
}

const origScrollTo = Element.prototype.scrollTo;
Element.prototype.scrollTo = function patchedScrollTo(
  ...args: [ScrollToOptions?] | [number, number]
) {
  if (isViewport(this)) {
    const options = typeof args[0] === "object" ? args[0] : null;
    log.push({
      t: performance.now(),
      type: "scrollTo",
      detail: {
        top: options?.top ?? args[1],
        behavior: options?.behavior,
        scrollTopBefore: this.scrollTop,
        scrollHeight: this.scrollHeight,
      },
    });
  }
  return origScrollTo.apply(
    this,
    args as unknown as Parameters<Element["scrollTo"]>,
  );
};

window.addEventListener(
  "scroll",
  (event) => {
    if (isViewport(event.target)) {
      log.push({
        t: performance.now(),
        type: "scroll-event",
        detail: {
          scrollTop: event.target.scrollTop,
          scrollHeight: event.target.scrollHeight,
        },
      });
    }
  },
  { capture: true, passive: true },
);

function initialRows(): HarnessRow[] {
  const rows: HarnessRow[] = [];
  for (let index = 0; index < 25; index += 1) {
    rows.push({ id: `m${index}`, h: 48 + ((index * 37) % 180) });
  }
  return rows;
}

function App() {
  const [rows, setRows] = useState<HarnessRow[]>(initialRows);
  const [atEnd, setAtEnd] = useState(true);
  const handleRef = useRef<ConversationVirtualizerHandle>(null);

  useEffect(() => {
    const harness = {
      append(row: HarnessRow) {
        log.push({ t: performance.now(), type: "append", detail: { ...row } });
        setRows((previous) => [...previous, row]);
      },
      remove(id: string) {
        log.push({ t: performance.now(), type: "remove", detail: { id } });
        setRows((previous) => previous.filter((row) => row.id !== id));
      },
      resize(id: string, h: number) {
        log.push({ t: performance.now(), type: "resize", detail: { id, h } });
        setRows((previous) =>
          previous.map((row) => (row.id === id ? { ...row, h } : row)),
        );
      },
      scrollToEnd(behavior: "smooth" | "auto" = "smooth") {
        log.push({
          t: performance.now(),
          type: "scrollToEnd-call",
          detail: { behavior },
        });
        requestAnimationFrame(() => {
          handleRef.current?.scrollToEnd({ behavior });
        });
      },
      isAtEnd() {
        return handleRef.current?.isAtEnd() ?? false;
      },
      snapshot() {
        const viewport = document.querySelector<HTMLElement>(
          '[data-slot="conversation-viewport"]',
        );
        if (!viewport) return null;
        return {
          scrollTop: viewport.scrollTop,
          scrollHeight: viewport.scrollHeight,
          clientHeight: viewport.clientHeight,
          distanceFromEnd:
            viewport.scrollHeight - viewport.clientHeight - viewport.scrollTop,
          atEnd: handleRef.current?.isAtEnd() ?? false,
          firstVisibleId: firstVisibleRowId(viewport),
        };
      },
    };
    (window as unknown as Record<string, unknown>).__harness = harness;
    (window as unknown as Record<string, unknown>).__harnessReady = true;
  }, []);

  return (
    <div style={{ height: "600px", width: "700px" }}>
      <ConversationVirtualizer
        ref={handleRef}
        items={rows}
        paddingEnd={24}
        onAtEndChange={setAtEnd}
        renderItem={(item) => (
          <div
            data-harness-row={item.id}
            style={{
              height: item.h,
              boxSizing: "border-box",
              borderBottom: "1px solid #ddd",
              padding: "4px",
            }}
          >
            {item.id}
          </div>
        )}
      />
      <div data-harness-at-end={String(atEnd)} />
    </div>
  );
}

function firstVisibleRowId(viewport: HTMLElement): string | null {
  const rows = [
    ...viewport.querySelectorAll<HTMLElement>("[data-harness-row]"),
  ];
  let best: { id: string; top: number } | null = null;
  const viewportTop = viewport.getBoundingClientRect().top;
  for (const row of rows) {
    const rect = row.getBoundingClientRect();
    if (rect.bottom <= viewportTop) continue;
    if (best === null || rect.top < best.top) {
      best = { id: row.dataset.harnessRow ?? "", top: rect.top };
    }
  }
  return best?.id ?? null;
}

const root = document.getElementById("root");
if (!root) throw new Error("missing #root");
createRoot(root).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
