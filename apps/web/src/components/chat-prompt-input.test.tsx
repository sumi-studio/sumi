// @vitest-environment jsdom

import { TooltipProvider } from "@sumi/ui/components/tooltip";
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { ComponentProps } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ChatPromptInput } from "./chat-prompt-input";

afterEach(cleanup);

function renderInput(
  props: Partial<ComponentProps<typeof ChatPromptInput>> = {},
) {
  const onSend = vi.fn();
  const onAbort = vi.fn();
  render(
    <TooltipProvider>
      <ChatPromptInput
        value=""
        onValueChange={vi.fn()}
        onSend={onSend}
        onAbort={onAbort}
        streaming={false}
        {...props}
      />
    </TooltipProvider>,
  );
  return { onAbort, onSend };
}

describe("ChatPromptInput", () => {
  it("does not send an empty message", () => {
    const { onSend } = renderInput();
    const send = screen.getByRole("button", { name: "送信" });
    expect(send).toBeDisabled();
    fireEvent.click(send);
    expect(onSend).not.toHaveBeenCalled();
  });

  it("gates input while the direct-chat target is unavailable", () => {
    renderInput({ disabled: true, value: "hello" });
    expect(screen.getByRole("textbox")).toBeDisabled();
    expect(screen.getByRole("button", { name: "送信" })).toBeDisabled();
  });

  it("keeps stop available while a steering message is entered", () => {
    const { onAbort, onSend } = renderInput({
      streaming: true,
      value: "追加の指示",
    });
    fireEvent.click(screen.getByRole("button", { name: "停止" }));
    fireEvent.click(screen.getByRole("button", { name: "割り込んで送信" }));
    expect(onAbort).toHaveBeenCalledOnce();
    expect(onSend).toHaveBeenCalledOnce();
  });
});
