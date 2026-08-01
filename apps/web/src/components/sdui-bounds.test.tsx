// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  MAX_SDUI_ACTIONS,
  MAX_SDUI_LIST_ITEMS,
  type SduiNode,
  SduiView,
} from "@sumi/sdui";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

afterEach(cleanup);

describe("bounded SDUI renderer", () => {
  it("rejects ignored recursive children without overflowing the stack", () => {
    let node: Record<string, unknown> = {
      type: "list",
      props: { items: [] },
    };
    for (let depth = 0; depth < 10_000; depth += 1) {
      node = { type: "list", props: { items: [] }, children: [node] };
    }

    expect(() =>
      render(<SduiView node={node as unknown as SduiNode} />),
    ).not.toThrow();
    expect(screen.getByText(/invalid declaration/)).toBeInTheDocument();
  });

  it("does not render over-limit list items or actions", () => {
    const list = render(
      <SduiView
        node={{
          type: "list",
          props: {
            items: Array.from(
              { length: MAX_SDUI_LIST_ITEMS + 1 },
              (_, index) => ({ text: `item-${index}` }),
            ),
          },
        }}
      />,
    );
    expect(screen.queryAllByRole("listitem")).toHaveLength(0);
    expect(screen.getByText(/invalid declaration/)).toBeInTheDocument();

    list.rerender(
      <SduiView
        node={{
          type: "reminder",
          props: {
            title: "Reminder",
            at: "2026-08-01T09:00:00Z",
            actions: Array.from(
              { length: MAX_SDUI_ACTIONS + 1 },
              (_, index) => ({
                label: `action-${index}`,
                action: `action:${index}`,
              }),
            ),
          },
        }}
      />,
    );
    expect(screen.queryAllByRole("button")).toHaveLength(0);
    expect(screen.getByText(/invalid declaration/)).toBeInTheDocument();
  });
});
