// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { useState } from "react";
import { afterEach, describe, expect, it } from "vitest";
import { useOverlayPanel } from "./overlay";

function Menu({ name, panelStyle }: { name: string; panelStyle?: object }) {
  const [open, setOpen] = useState(false);
  const overlay = useOverlayPanel<HTMLButtonElement>({
    open,
    onOpenChange: setOpen,
  });
  return (
    <div>
      <button type="button" {...overlay.triggerProps}>
        {name}
      </button>
      {open ? (
        <div
          {...overlay.panelProps}
          data-testid={`${name}-panel`}
          style={panelStyle}
        >
          <button type="button">{name}の項目</button>
        </div>
      ) : null}
    </div>
  );
}

function Harness({ panelStyle }: { panelStyle?: object }) {
  return (
    <div>
      <div data-slot="conversation-viewport" data-testid="viewport">
        一覧
      </div>
      <Menu name="通知" panelStyle={panelStyle} />
      <Menu name="後で返信" />
      <button type="button">外側のボタン</button>
    </div>
  );
}

function fakeScroller(
  element: HTMLElement,
  { scrollHeight = 0, clientHeight = 0 } = {},
) {
  let top = 0;
  Object.defineProperty(element, "scrollTop", {
    configurable: true,
    get: () => top,
    set: (value: number) => {
      top = value;
    },
  });
  Object.defineProperty(element, "scrollHeight", {
    configurable: true,
    get: () => scrollHeight,
  });
  Object.defineProperty(element, "clientHeight", {
    configurable: true,
    get: () => clientHeight,
  });
  return () => top;
}

function fakeTwoAxisScroller(
  element: HTMLElement,
  { clientHeight, clientWidth }: { clientHeight: number; clientWidth: number },
) {
  let top = 0;
  let left = 0;
  Object.defineProperties(element, {
    scrollTop: {
      configurable: true,
      get: () => top,
      set: (value: number) => {
        top = value;
      },
    },
    scrollLeft: {
      configurable: true,
      get: () => left,
      set: (value: number) => {
        left = value;
      },
    },
    clientHeight: { configurable: true, get: () => clientHeight },
    clientWidth: { configurable: true, get: () => clientWidth },
  });
  return { readLeft: () => left, readTop: () => top };
}

afterEach(cleanup);

describe("useOverlayPanel", () => {
  it("パネル内のpointerdownでは閉じず、外側で閉じる", () => {
    render(<Harness />);
    fireEvent.click(screen.getByRole("button", { name: "通知" }));

    fireEvent.pointerDown(screen.getByRole("button", { name: "通知の項目" }));
    expect(screen.getByTestId("通知-panel")).toBeInTheDocument();

    fireEvent.pointerDown(screen.getByRole("button", { name: "外側のボタン" }));
    expect(screen.queryByTestId("通知-panel")).not.toBeInTheDocument();
  });

  it("外側を押したときは押した先からフォーカスを奪わない", () => {
    render(<Harness />);
    fireEvent.click(screen.getByRole("button", { name: "通知" }));
    const outside = screen.getByRole("button", { name: "外側のボタン" });

    fireEvent.pointerDown(outside);
    outside.focus();

    expect(document.activeElement).toBe(outside);
  });

  it("Escapeで閉じ、フォーカスをトリガーへ戻す", () => {
    render(<Harness />);
    const trigger = screen.getByRole("button", { name: "通知" });
    fireEvent.click(trigger);
    screen.getByRole("button", { name: "通知の項目" }).focus();

    fireEvent.keyDown(document, { key: "Escape" });

    expect(screen.queryByTestId("通知-panel")).not.toBeInTheDocument();
    expect(document.activeElement).toBe(trigger);
  });

  it("IME変換中のEscapeでは閉じない", () => {
    render(<Harness />);
    fireEvent.click(screen.getByRole("button", { name: "通知" }));

    fireEvent.keyDown(document, { key: "Escape", isComposing: true });
    fireEvent.keyDown(document, { key: "Escape", keyCode: 229 });

    expect(screen.getByTestId("通知-panel")).toBeInTheDocument();
  });

  it("別のオーバーレイを開くと先のものを閉じる", () => {
    render(<Harness />);
    fireEvent.click(screen.getByRole("button", { name: "通知" }));
    fireEvent.click(screen.getByRole("button", { name: "後で返信" }));

    expect(screen.queryByTestId("通知-panel")).not.toBeInTheDocument();
    expect(screen.getByTestId("後で返信-panel")).toBeInTheDocument();
  });

  it("開いているトリガーのpointerdownとclickで閉じたままにする", () => {
    render(<Harness />);
    const trigger = screen.getByRole("button", { name: "通知" });
    fireEvent.click(trigger);

    fireEvent.pointerDown(trigger);
    fireEvent.click(trigger);

    expect(screen.queryByTestId("通知-panel")).not.toBeInTheDocument();
  });

  it("パネル上のpixelとline単位のホイールを一覧へ渡す", () => {
    render(<Harness />);
    const readScrollTop = fakeScroller(screen.getByTestId("viewport"));
    fireEvent.click(screen.getByRole("button", { name: "通知" }));
    const panel = screen.getByTestId("通知-panel");

    const pixelWheel = new WheelEvent("wheel", {
      bubbles: true,
      cancelable: true,
      deltaY: 120,
    });
    panel.dispatchEvent(pixelWheel);
    expect(pixelWheel.defaultPrevented).toBe(true);
    expect(readScrollTop()).toBe(120);

    const lineWheel = new WheelEvent("wheel", {
      bubbles: true,
      cancelable: true,
      deltaMode: WheelEvent.DOM_DELTA_LINE,
      deltaY: 3,
    });
    panel.dispatchEvent(lineWheel);
    expect(lineWheel.defaultPrevented).toBe(true);
    expect(readScrollTop()).toBe(168);
  });

  it("page単位のホイールを対象viewportの幅と高さで換算する", () => {
    render(<Harness />);
    const viewport = screen.getByTestId("viewport");
    const scroll = fakeTwoAxisScroller(viewport, {
      clientHeight: 240,
      clientWidth: 640,
    });
    fireEvent.click(screen.getByRole("button", { name: "通知" }));

    const wheel = new WheelEvent("wheel", {
      bubbles: true,
      cancelable: true,
      deltaMode: WheelEvent.DOM_DELTA_PAGE,
      deltaX: 1,
      deltaY: -1,
    });
    screen.getByTestId("通知-panel").dispatchEvent(wheel);

    expect(wheel.defaultPrevented).toBe(true);
    expect(scroll.readLeft()).toBe(640);
    expect(scroll.readTop()).toBe(-240);
  });

  it("パネル内がスクロールできるときはそちらを優先する", () => {
    render(<Harness panelStyle={{ overflowY: "auto" }} />);
    const readScrollTop = fakeScroller(screen.getByTestId("viewport"));
    fireEvent.click(screen.getByRole("button", { name: "通知" }));
    const panel = screen.getByTestId("通知-panel");
    fakeScroller(panel, { scrollHeight: 400, clientHeight: 100 });

    const wheel = new WheelEvent("wheel", {
      bubbles: true,
      cancelable: true,
      deltaY: 120,
    });
    panel.dispatchEvent(wheel);

    expect(wheel.defaultPrevented).toBe(false);
    expect(readScrollTop()).toBe(0);
  });
});
