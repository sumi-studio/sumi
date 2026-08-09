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

/** jsdomはレイアウトを持たない。スクロール量だけ手で用意する。 */
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

afterEach(cleanup);

describe("useOverlayPanel", () => {
  it("パネルの外を押したら閉じる", () => {
    render(<Harness />);
    fireEvent.click(screen.getByRole("button", { name: "通知" }));
    expect(screen.getByTestId("通知-panel")).toBeInTheDocument();

    fireEvent.pointerDown(screen.getByRole("button", { name: "外側のボタン" }));

    expect(screen.queryByTestId("通知-panel")).not.toBeInTheDocument();
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

    expect(screen.getByTestId("通知-panel")).toBeInTheDocument();
  });

  it("別のオーバーレイを開くと先に開いていたものが閉じる", () => {
    render(<Harness />);
    fireEvent.click(screen.getByRole("button", { name: "通知" }));
    fireEvent.click(screen.getByRole("button", { name: "後で返信" }));

    expect(screen.queryByTestId("通知-panel")).not.toBeInTheDocument();
    expect(screen.getByTestId("後で返信-panel")).toBeInTheDocument();
  });

  it("開いているトリガーを押したら閉じたままで、開き直さない", () => {
    render(<Harness />);
    const trigger = screen.getByRole("button", { name: "通知" });
    fireEvent.click(trigger);

    // 実機と同じ順序（pointerdown → click）で押し直す。
    fireEvent.pointerDown(trigger);
    fireEvent.click(trigger);

    expect(screen.queryByTestId("通知-panel")).not.toBeInTheDocument();
  });

  it("パネル上のホイールをメッセージ一覧へ渡す", () => {
    render(<Harness />);
    const readScrollTop = fakeScroller(screen.getByTestId("viewport"));
    fireEvent.click(screen.getByRole("button", { name: "通知" }));

    const wheel = new WheelEvent("wheel", {
      bubbles: true,
      cancelable: true,
      deltaY: 120,
    });
    screen.getByTestId("通知-panel").dispatchEvent(wheel);

    expect(wheel.defaultPrevented).toBe(true);
    expect(readScrollTop()).toBe(120);
  });

  it("行単位のホイール量をピクセルへ換算して一覧へ渡す", () => {
    render(<Harness />);
    const readScrollTop = fakeScroller(screen.getByTestId("viewport"));
    fireEvent.click(screen.getByRole("button", { name: "通知" }));

    const wheel = new WheelEvent("wheel", {
      bubbles: true,
      cancelable: true,
      deltaMode: WheelEvent.DOM_DELTA_LINE,
      deltaY: 3,
    });
    screen.getByTestId("通知-panel").dispatchEvent(wheel);

    expect(wheel.defaultPrevented).toBe(true);
    expect(readScrollTop()).toBe(48);
  });

  it("パネル自身がスクロールできるならホイールを奪わない", () => {
    render(<Harness panelStyle={{ overflowY: "auto" }} />);
    const readScrollTop = fakeScroller(screen.getByTestId("viewport"));
    fireEvent.click(screen.getByRole("button", { name: "通知" }));
    fakeScroller(screen.getByTestId("通知-panel"), {
      scrollHeight: 400,
      clientHeight: 100,
    });

    const wheel = new WheelEvent("wheel", {
      bubbles: true,
      cancelable: true,
      deltaY: 120,
    });
    screen.getByTestId("通知-panel").dispatchEvent(wheel);

    expect(wheel.defaultPrevented).toBe(false);
    expect(readScrollTop()).toBe(0);
  });
});
