import { describe, expect, it } from "vitest";
import {
  controlHoldText,
  controlReturnedText,
  initialViewerState,
  NOTICE_TEXT,
  notSavedText,
  phaseText,
  recoveryText,
  reduceViewer,
} from "./model";
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

describe("review follow-ups", () => {
  it("F9: keeps what the checkpoint could not save and says so", () => {
    let s = reduceViewer(initialViewerState, { type: "hello", phase: "live", notSaved: { nonJsonIdbValues: 3, sessionStorageKeys: 0, skippedOrigins: [] } });
    expect(s.notSaved?.nonJsonIdbValues).toBe(3);
    s = reduceViewer(s, {
      type: "checkpoint",
      at: "2026-09-28T01:02:00Z",
      notSaved: { nonJsonIdbValues: 2, sessionStorageKeys: 4, skippedOrigins: ["https://slow.example"], oversizedOrigins: ["https://heavy.example"] },
    });
    expect(s.checkpointAt).toBe("2026-09-28T01:02:00Z");
    expect(s.notSaved).toEqual({ nonJsonIdbValues: 2, sessionStorageKeys: 4, skippedOrigins: ["https://slow.example"], oversizedOrigins: ["https://heavy.example"] });
    const text = notSavedText(s.notSaved) ?? "";
    expect(text).toContain("heavy.example");
    expect(text).toContain("slow.example");
    expect(text).toContain("2 件");
    expect(text).toContain("sessionStorage 4 件");
    expect(notSavedText({ nonJsonIdbValues: 0, sessionStorageKeys: 0, skippedOrigins: [] })).toBeUndefined();
  });

  it("F5: skipped sites are not described as over the limit when they were not", () => {
    const lines = recoveryText({ kind: "restored", at: 1, checkpointAt: "2026-09-27T01:02:00Z", skippedOrigins: ["https://a.example"], failedRecords: 2 });
    expect(lines[1]).toContain("a.example");
    expect(lines[1]).toContain("保存または復元できなかった");
    expect(lines[1]).not.toContain("上限を超えた");
    expect(lines[2]).toContain("2 件");
    expect(NOTICE_TEXT.checkpoint_too_large).toContain("Cookie と開いているタブだけで");
  });

  it("F7: a refused input on a switched tab is explained", () => {
    const s = reduceViewer(initialViewerState, { type: "notice", code: "tab_changed", tab: "slot-b" });
    expect(NOTICE_TEXT[s.notices[0]?.code ?? ""]).toContain("タブが切り替わった");
  });

  it("F10: a reconnecting page keeps the person's control and explains the hold; an absence return is shown", () => {
    let s = reduceViewer(initialViewerState, { type: "hello", phase: "live", control: { mode: "human", holdMs: 120_000 } });
    expect(s.control).toBe("human");
    expect(s.controlHoldMs).toBe(120_000);
    expect(controlHoldText(s.controlHoldMs)).toContain("2 分間はあなたの操作のまま");
    s = reduceViewer(s, { type: "hello", phase: "live", control: { mode: "agent", holdMs: 120_000, returned: { reason: "viewer_absent", at: 1_000 } } });
    expect(s.control).toBe("agent");
    expect(s.controlReturned).toEqual({ reason: "viewer_absent", at: 1_000 });
    expect(controlReturnedText(s.controlReturned as { at: number }, s.controlHoldMs)).toContain("再開していません");
    s = reduceViewer(s, { type: "control", mode: "human", goalStopped: false });
    expect(s.controlReturned).toBeUndefined();
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
