// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { MessageResponse } from "@sumi/ui/ai-elements/message";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";

vi.mock("@streamdown/mermaid", () => ({
  mermaid: {
    name: "mermaid",
    type: "diagram",
    language: "mermaid",
    getMermaid: () => ({
      render: async () => ({
        svg: '<svg xmlns="http://www.w3.org/2000/svg"><text>Rendered</text></svg>',
      }),
    }),
  },
}));

beforeAll(() => {
  class ImmediateIntersectionObserver implements IntersectionObserver {
    readonly root = null;
    readonly rootMargin = "0px";
    readonly scrollMargin = "0px";
    readonly thresholds = [0];
    private readonly callback: IntersectionObserverCallback;

    disconnect() {}
    observe(target: Element) {
      this.callback(
        [
          {
            boundingClientRect: target.getBoundingClientRect(),
            intersectionRatio: 1,
            intersectionRect: target.getBoundingClientRect(),
            isIntersecting: true,
            rootBounds: null,
            target,
            time: 0,
          },
        ],
        this,
      );
    }
    takeRecords() {
      return [];
    }
    unobserve() {}

    constructor(
      callback: IntersectionObserverCallback,
      _options?: IntersectionObserverInit,
    ) {
      this.callback = callback;
    }
  }
  vi.stubGlobal("IntersectionObserver", ImmediateIntersectionObserver);
});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("MessageResponse", () => {
  it("applies syntax highlighting to a typed code fence", async () => {
    const { container } = render(
      <MessageResponse mode="static">
        {"```typescript\nconst answer: number = 42;\n```"}
      </MessageResponse>,
    );

    await waitFor(
      () => {
        expect(
          container.querySelector(
            '[data-streamdown="code-block-body"] span[style*="--sdm-c"]',
          ),
        ).toBeInTheDocument();
      },
      { timeout: 5000 },
    );
  });

  it("renders a mermaid fence as a diagram instead of a code block", async () => {
    render(
      <MessageResponse mode="static">
        {"```mermaid\nflowchart LR\n  A --> B\n```"}
      </MessageResponse>,
    );

    await waitFor(() => {
      expect(
        screen.queryByRole("img", { name: "Mermaid chart" }) ??
          screen.queryByText(/Mermaid Error:/),
      ).toBeInTheDocument();
    });
    expect(screen.queryByText("text")).not.toBeInTheDocument();
  });

  it("settles only after code and Mermaid reach their final render state", async () => {
    const onRenderSettled = vi.fn();
    const { container } = render(
      <MessageResponse mode="static" onRenderSettled={onRenderSettled}>{`\
\`\`\`made-up-language
plain text
\`\`\`

\`\`\`typescript
const answer: number = 42;
\`\`\`

\`\`\`mermaid
flowchart LR
  A --> B
\`\`\``}</MessageResponse>,
    );

    await waitFor(
      () => {
        expect(onRenderSettled).toHaveBeenCalledOnce();
      },
      { timeout: 8000 },
    );
    expect(
      container.querySelector(
        '[data-streamdown="code-block-body"] span[style*="--sdm-c"]',
      ),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole("img", { name: "Mermaid chart" }) ??
        screen.queryByText(/Mermaid Error:/),
    ).toBeInTheDocument();
  }, 10_000);

  it("does not wait for rich content outside the chat viewport", async () => {
    vi.spyOn(HTMLElement.prototype, "getBoundingClientRect").mockImplementation(
      function (this: HTMLElement) {
        if (this.dataset.slot === "message-scroller-viewport") {
          return rect(0, 600);
        }
        if (this.classList.contains("message-markdown")) {
          return rect(1200, 1400);
        }
        return rect(0, 0);
      },
    );
    const onRenderSettled = vi.fn();

    render(
      <div data-slot="message-scroller-viewport">
        <MessageResponse mode="static" onRenderSettled={onRenderSettled}>
          {"```mermaid\nflowchart LR\n  A --> B\n```"}
        </MessageResponse>
      </div>,
    );

    await waitFor(() => {
      expect(onRenderSettled).toHaveBeenCalledOnce();
    });
  });
});

function rect(top: number, bottom: number): DOMRect {
  return {
    x: 0,
    y: top,
    top,
    bottom,
    left: 0,
    right: 800,
    width: 800,
    height: bottom - top,
    toJSON: () => ({}),
  };
}
