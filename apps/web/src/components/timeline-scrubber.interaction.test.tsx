// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { TimelineScrubber } from "./timeline-scrubber";
const observed = vi.fn();
const ticks = Array.from({ length: 1000 }, (_, i) => ({
  id: `message-${i}`,
  title: `Topic ${i}`,
  preview: `Excerpt ${i}`,
}));
beforeEach(() => {
  vi.stubGlobal("PointerEvent", MouseEvent);
  vi.stubGlobal(
    "ResizeObserver",
    class {
      callback: () => void;
      constructor(callback: () => void) {
        this.callback = callback;
      }
      observe() {
        observed();
        this.callback();
      }
      disconnect() {}
    },
  );
  vi.spyOn(HTMLElement.prototype, "clientHeight", "get").mockReturnValue(350);
  vi.spyOn(HTMLElement.prototype, "getBoundingClientRect").mockReturnValue({
    top: 0,
    bottom: 350,
    height: 350,
    left: 0,
    right: 24,
    width: 24,
    x: 0,
    y: 0,
    toJSON() {},
  });
});
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  observed.mockClear();
});
describe("large conversation scrubber", () => {
  it("measures after asynchronous history arrival and bounds marks", () => {
    const onJump = vi.fn();
    const view = render(
      <TimelineScrubber ticks={[]} visibleRange={null} onJump={onJump} />,
    );
    expect(observed).not.toHaveBeenCalled();
    view.rerender(
      <TimelineScrubber ticks={ticks} visibleRange={null} onJump={onJump} />,
    );
    expect(observed).toHaveBeenCalledTimes(1);
    const slider = screen.getByRole("slider");
    expect(slider.querySelectorAll("span[aria-hidden]")).toHaveLength(50);
    expect(screen.queryAllByRole("button")).toHaveLength(0);
    fireEvent.pointerMove(slider, { clientY: 175 });
    expect(screen.getByText("Excerpt 500")).toBeTruthy();
    expect(onJump).not.toHaveBeenCalled();
    fireEvent.click(slider, { clientY: 175 });
    expect(onJump).toHaveBeenLastCalledWith(500);
  });
  it("supports exact keyboard entries and stable positions across body changes", () => {
    const onJump = vi.fn();
    const view = render(
      <TimelineScrubber
        ticks={ticks}
        visibleRange={[800, 800]}
        onJump={onJump}
      />,
    );
    const slider = screen.getByRole("slider");
    const positions = () =>
      [...slider.querySelectorAll("span[aria-hidden]")]
        .slice(0, 50)
        .map((node) => (node as HTMLElement).style.top);
    const before = positions();
    fireEvent.keyDown(slider, { key: "Home" });
    expect(onJump).toHaveBeenLastCalledWith(0);
    fireEvent.keyDown(slider, { key: "ArrowDown" });
    expect(onJump).toHaveBeenLastCalledWith(1);
    fireEvent.keyDown(slider, { key: "End" });
    expect(onJump).toHaveBeenLastCalledWith(999);
    fireEvent.keyDown(slider, { key: "ArrowUp" });
    expect(onJump).toHaveBeenLastCalledWith(998);
    view.rerender(
      <TimelineScrubber
        ticks={[...ticks]}
        visibleRange={[10, 11]}
        onJump={onJump}
      />,
    );
    expect(positions()).toEqual(before);
    expect(slider.getAttribute("aria-valuenow")).toBe("999");
  });
});
