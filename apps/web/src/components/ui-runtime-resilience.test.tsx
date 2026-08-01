// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { MessageMetadata } from "@sumi/ui/ai-elements/message";
import { TooltipProvider } from "@sumi/ui/components/tooltip";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("shared UI runtime resilience", () => {
  it("keeps copy state unchanged when clipboard permission is denied", async () => {
    const writeText = vi
      .fn()
      .mockRejectedValue(new DOMException("denied", "NotAllowedError"));
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText },
    });

    render(
      <TooltipProvider>
        <MessageMetadata timestamp={null} copyText="private text" />
      </TooltipProvider>,
    );
    fireEvent.click(screen.getByRole("button", { name: "コピー" }));

    await waitFor(() => {
      expect(writeText).toHaveBeenCalledWith("private text");
    });
    expect(screen.getByRole("button", { name: "コピー" })).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "コピーしました" }),
    ).not.toBeInTheDocument();
  });
});
