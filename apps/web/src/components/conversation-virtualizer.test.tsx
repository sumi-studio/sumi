// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { createRef } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  ConversationVirtualizer,
  type ConversationVirtualizerHandle,
} from "./conversation-virtualizer";

const VIEWPORT_HEIGHT = 240;

interface TestMessage {
  id: string;
  text: string;
}

beforeEach(() => {
  Object.defineProperty(HTMLElement.prototype, "offsetHeight", {
    configurable: true,
    get() {
      if (this.dataset.slot === "conversation-viewport") {
        return VIEWPORT_HEIGHT;
      }
      if (this.dataset.index !== undefined) {
        return Number(this.dataset.index) % 2 === 0 ? 72 : 36;
      }
      return 0;
    },
  });
  Object.defineProperty(HTMLElement.prototype, "offsetWidth", {
    configurable: true,
    get() {
      return 800;
    },
  });
  Object.defineProperty(HTMLElement.prototype, "clientHeight", {
    configurable: true,
    get() {
      return this.dataset.slot === "conversation-viewport"
        ? VIEWPORT_HEIGHT
        : 0;
    },
  });
  Object.defineProperty(HTMLElement.prototype, "clientWidth", {
    configurable: true,
    get() {
      return this.getAttribute("role") === "log" ? 800 : 0;
    },
  });
  Object.defineProperty(HTMLElement.prototype, "scrollHeight", {
    configurable: true,
    get() {
      if (this.dataset.slot !== "conversation-viewport") return 0;
      return Number.parseFloat(
        (this.firstElementChild as HTMLElement | null)?.style.height ?? "0",
      );
    },
  });
  Object.defineProperty(HTMLElement.prototype, "scrollWidth", {
    configurable: true,
    get() {
      return this.dataset.slot === "conversation-viewport" ? 800 : 0;
    },
  });
  Object.defineProperty(HTMLElement.prototype, "scrollTo", {
    configurable: true,
    value(this: HTMLElement, options: ScrollToOptions) {
      if (typeof options.top === "number") this.scrollTop = options.top;
      this.dispatchEvent(new Event("scroll"));
    },
  });
});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("ConversationVirtualizer", () => {
  it("renders a bounded variable-height window for a large conversation", async () => {
    const messages = makeMessages(1_000);

    render(
      <ConversationVirtualizer
        items={messages}
        estimateSize={() => 64}
        busy
        ariaLabel="Test conversation"
        renderItem={(message) => <p>{message.text}</p>}
      />,
    );

    const region = screen.getByRole("region", {
      name: "Test conversation（表示中）",
    });
    expect(region).toHaveAttribute("aria-busy", "true");

    await waitFor(() => {
      expect(
        document.querySelector('[data-message-id="message-999"]'),
      ).toBeInTheDocument();
    });

    const renderedRows = document.querySelectorAll("[data-message-id]");
    expect(renderedRows.length).toBeGreaterThan(0);
    expect(renderedRows.length).toBeLessThan(30);
    expect(
      document.querySelector('[data-message-id="message-0"]'),
    ).not.toBeInTheDocument();
    expect(
      Array.from(renderedRows).every((row) => row.hasAttribute("data-index")),
    ).toBe(true);
  });

  it("pins the footer overlay inside the scroll viewport so the wheel keeps reaching it", () => {
    render(
      <ConversationVirtualizer
        items={makeMessages(20)}
        estimateSize={() => 64}
        ariaLabel="Test conversation"
        renderItem={(message) => <p>{message.text}</p>}
        footerOverlay={
          <button type="button" className="absolute right-4 bottom-3">
            jump to latest
          </button>
        }
      />,
    );

    const viewport = document.querySelector(
      '[data-slot="conversation-viewport"]',
    );
    const jump = screen.getByRole("button", { name: "jump to latest" });
    // A control floating outside the scroll container swallows wheel events:
    // the browser scrolls the nearest scrollable ancestor of whatever sits
    // under the pointer, and none of its ancestors would scroll.
    expect(viewport?.contains(jump)).toBe(true);
    const footer = jump.closest(
      '[data-slot="conversation-viewport-footer"]',
    ) as HTMLElement | null;
    expect(footer?.style.position).toBe("sticky");
  });

  it("scrolls to a stable message id and reports the visible ids", async () => {
    const messages = makeMessages(500);
    const handle = createRef<ConversationVirtualizerHandle>();
    const onVisibleMessageIdsChange = vi.fn();

    render(
      <ConversationVirtualizer
        ref={handle}
        items={messages}
        estimateSize={() => 60}
        onVisibleMessageIdsChange={onVisibleMessageIdsChange}
        renderItem={(message) => <p>{message.text}</p>}
      />,
    );

    await waitFor(() => {
      expect(handle.current?.isAtEnd()).toBe(true);
    });

    let found = false;
    act(() => {
      found =
        handle.current?.scrollToMessage("message-240", {
          align: "start",
        }) ?? false;
    });

    expect(found).toBe(true);
    expect(handle.current?.scrollToMessage("missing-message")).toBe(false);
    await waitFor(() => {
      expect(
        document.querySelector('[data-message-id="message-240"]'),
      ).toBeInTheDocument();
      expect(latestCall(onVisibleMessageIdsChange)).toContain("message-240");
    });

    const visibleIds = latestCall(onVisibleMessageIdsChange);
    expect(visibleIds.length).toBeGreaterThan(0);
    expect(visibleIds.length).toBeLessThan(messages.length);
  });

  it("follows appended messages only while the viewport is at the end", async () => {
    const handle = createRef<ConversationVirtualizerHandle>();
    const onAtEndChange = vi.fn();
    let messages = makeMessages(100);

    const view = render(
      <ConversationVirtualizer
        ref={handle}
        items={messages}
        estimateSize={() => 60}
        onAtEndChange={onAtEndChange}
        renderItem={(message) => <p>{message.text}</p>}
      />,
    );
    const viewport = screen.getByRole("region");

    await waitFor(() => {
      expect(handle.current?.isAtEnd()).toBe(true);
    });
    await settleProgrammaticScroll();

    fireEvent.wheel(viewport, { deltaY: -300 });
    viewport.scrollTop = 300;
    fireEvent.scroll(viewport);
    await waitFor(() => {
      expect(handle.current?.isAtEnd()).toBe(false);
      expect(onAtEndChange).toHaveBeenLastCalledWith(false);
    });

    messages = [...messages, { id: "message-100", text: "Message 100" }];
    view.rerender(
      <ConversationVirtualizer
        ref={handle}
        items={messages}
        estimateSize={() => 60}
        onAtEndChange={onAtEndChange}
        renderItem={(message) => <p>{message.text}</p>}
      />,
    );
    await waitFor(() => {
      expect(handle.current?.isAtEnd()).toBe(false);
      expect(viewport.scrollTop).toBeLessThan(1_000);
    });

    act(() => handle.current?.scrollToEnd());
    await waitFor(() => {
      expect(handle.current?.isAtEnd()).toBe(true);
    });
    const previousEndOffset = viewport.scrollTop;

    messages = [...messages, { id: "message-101", text: "Message 101" }];
    view.rerender(
      <ConversationVirtualizer
        ref={handle}
        items={messages}
        estimateSize={() => 60}
        onAtEndChange={onAtEndChange}
        renderItem={(message) => <p>{message.text}</p>}
      />,
    );

    await waitFor(() => {
      expect(viewport.scrollTop).toBeGreaterThan(previousEndOffset);
      expect(handle.current?.isAtEnd()).toBe(true);
    });
  });

  it("offers a complete transcript only on demand and restores focus after closing", async () => {
    const messages = makeMessages(1_000);
    render(
      <ConversationVirtualizer
        items={messages}
        estimateSize={() => 64}
        renderItem={(message) => <p>{message.text}</p>}
        renderTranscriptItem={(message) => <p>{message.text}</p>}
      />,
    );

    expect(document.querySelectorAll("[data-message-id]").length).toBeLessThan(
      30,
    );
    const trigger = screen.getByRole("button", { name: "会話の全文を開く" });
    fireEvent.click(trigger);

    const dialog = screen.getByRole("dialog", { name: "Sumiとの会話の全文" });
    expect(dialog).toHaveFocus();
    const transcript = screen.getByRole("log", { name: "Sumiとの会話の全文" });
    expect(transcript).toHaveTextContent("Message 0");
    expect(transcript).toHaveTextContent("Message 999");
    fireEvent.click(screen.getByRole("button", { name: "閉じる" }));
    await waitFor(() => expect(trigger).toHaveFocus());
    expect(document.querySelectorAll("[data-message-id]").length).toBeLessThan(
      30,
    );
  });

  it("moves focus to the viewport before a focused row is unmounted", async () => {
    const messages = makeMessages(500);
    const handle = createRef<ConversationVirtualizerHandle>();
    render(
      <ConversationVirtualizer
        ref={handle}
        items={messages}
        estimateSize={() => 60}
        renderItem={(message) => <button type="button">{message.text}</button>}
      />,
    );
    await waitFor(() =>
      expect(
        document.querySelector('[data-message-id="message-499"]'),
      ).toBeInTheDocument(),
    );
    screen.getByRole("button", { name: "Message 499" }).focus();
    act(() => handle.current?.scrollToMessage("message-0", { align: "start" }));
    await waitFor(() => {
      expect(
        document.querySelector('[data-message-id="message-499"]'),
      ).not.toBeInTheDocument();
      expect(screen.getByRole("region")).toHaveFocus();
    });
  });

  it("cancels a stale programmatic end reconciliation after divergent user scrolling", async () => {
    let messages = makeMessages(100);
    const view = render(
      <ConversationVirtualizer
        items={messages}
        estimateSize={() => 60}
        renderItem={(message) => <p>{message.text}</p>}
      />,
    );
    const viewport = screen.getByRole("region");
    await settleProgrammaticScroll();
    fireEvent.wheel(viewport, { deltaY: -300 });
    viewport.scrollTop = 120;
    fireEvent.scroll(viewport);
    messages = [...messages, { id: "message-100", text: "Message 100" }];
    view.rerender(
      <ConversationVirtualizer
        items={messages}
        estimateSize={() => 60}
        renderItem={(message) => <p>{message.text}</p>}
      />,
    );
    await settleProgrammaticScroll();
    // The reading position may shift by a few pixels as rows above it
    // re-measure, but it must never be pulled back toward the end.
    expect(Math.abs(viewport.scrollTop - 120)).toBeLessThan(60);
  });

  it("keeps follow mode armed for non-scrolling controls inside rows", async () => {
    const handle = createRef<ConversationVirtualizerHandle>();
    const messages = makeMessages(100);
    render(
      <ConversationVirtualizer
        ref={handle}
        items={messages}
        estimateSize={() => 60}
        renderItem={(message) => <button type="button">{message.text}</button>}
      />,
    );
    const viewport = screen.getByRole("region");

    await waitFor(() => {
      expect(handle.current?.isAtEnd()).toBe(true);
    });
    await settleProgrammaticScroll();

    const control = screen.getByRole("button", { name: "Message 99" });
    fireEvent.pointerDown(control);
    fireEvent.touchStart(control);
    fireEvent.keyDown(control, { key: " " });

    viewport.scrollTop = 120;
    fireEvent.scroll(viewport);
    await waitFor(() => {
      expect(handle.current?.isAtEnd()).toBe(true);
    });

    fireEvent.keyDown(viewport, { key: "PageUp" });
    viewport.scrollTop = 120;
    fireEvent.scroll(viewport);
    await waitFor(() => {
      expect(handle.current?.isAtEnd()).toBe(false);
    });
  });
});

function makeMessages(count: number): TestMessage[] {
  return Array.from({ length: count }, (_, index) => ({
    id: `message-${index}`,
    text: `Message ${index}`,
  }));
}

function latestCall(mock: ReturnType<typeof vi.fn>): string[] {
  return mock.mock.calls.at(-1)?.[0] ?? [];
}

async function settleProgrammaticScroll() {
  await act(
    () =>
      new Promise<void>((resolve) => {
        requestAnimationFrame(() => resolve());
      }),
  );
}
