import { describe, expect, it } from "vitest";
import { initialViewerState, phaseText, recoveryText, reduceViewer } from "./model";
import { forwardsKey, modifierBits, toOverlay, toRemotePoint } from "./screen-input";

describe("viewer state", () => {
  it("adopts the hello snapshot and tracks control handoffs", () => {
    let s = reduceViewer(initialViewerState, {
      type: "hello",
      phase: "live",
      viewport: { width: 1280, height: 800 },
      control: { mode: "agent" },
      tabs: [{ id: "t1", url: "http://app.sumi-fixture.test/", title: "Fixture", active: true, loading: false }],
      shared: [{ tab: "t1", allowActions: true }],
      unsupported: ["file_upload"],
    });
    expect(s.phase).toBe("live");
    expect(s.tabs[0]?.active).toBe(true);
    s = reduceViewer(s, { type: "agent", phase: "start", tab: "t1", kind: "click", label: "Next", bounds: { x: 1, y: 2, width: 3, height: 4 } });
    expect(s.agent?.label).toBe("Next");
    s = reduceViewer(s, { type: "control", mode: "human", goalStopped: true, inFlight: { kind: "click", label: "Next" } });
    expect(s.control).toBe("human");
    expect(s.takeover).toEqual({ goalStopped: true, inFlight: { kind: "click", label: "Next" } });
    s = reduceViewer(s, { type: "control", mode: "agent" });
    expect(s.takeover).toBeUndefined();
  });

  it("keeps at most three notices", () => {
    let s = initialViewerState;
    for (const code of ["a", "b", "c", "d"]) s = reduceViewer(s, { type: "notice", code });
    expect(s.notices.map((n) => n.code)).toEqual(["b", "c", "d"]);
  });

  it("explains restoration and never promises an uncertain effect", () => {
    const lines = recoveryText({
      kind: "restored",
      at: 1,
      checkpointAt: "2026-09-27T01:02:00Z",
      uncertain: { kind: "click", label: "Place order" },
    });
    expect(lines[0]).toContain("復元しました");
    expect(lines[0]).toContain("未送信の入力");
    expect(lines[1]).toContain("届いたかは確認できません");
    expect(lines[1]).toContain("自動では再実行しません");
    expect(recoveryText({ kind: "reconnected", at: 1 })[0]).toContain("そのまま");
    expect(phaseText({ phase: "unavailable", detail: "browser_limit" })).toContain("利用上限");
  });
});

describe("screen input mapping", () => {
  const box = { left: 100, top: 50, width: 640, height: 400 };
  const viewport = { width: 1280, height: 800 };
  it("maps a scaled frame back to remote CSS pixels", () => {
    expect(toRemotePoint(100, 50, box, viewport)).toEqual({ x: 0, y: 0 });
    expect(toRemotePoint(420, 250, box, viewport)).toEqual({ x: 640, y: 400 });
    expect(toRemotePoint(99, 50, box, viewport)).toBeNull();
    expect(toOverlay({ x: 640, y: 400, width: 128, height: 80 }, viewport)).toEqual({
      left: "50%",
      top: "50%",
      width: "10%",
      height: "10%",
    });
  });
  it("forwards keys but leaves paste to the paste event", () => {
    const k = (key: string, ctrlKey = false) => ({ key, ctrlKey, metaKey: false });
    expect(forwardsKey(k("a"))).toBe(true);
    expect(forwardsKey(k("Enter"))).toBe(true);
    expect(forwardsKey(k("v", true))).toBe(false);
    expect(forwardsKey(k("Shift"))).toBe(false);
    expect(modifierBits({ altKey: false, ctrlKey: true, metaKey: false, shiftKey: true })).toBe(10);
  });
});
