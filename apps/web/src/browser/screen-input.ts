/** Pure helpers for mapping the person's input onto the remote viewport. */

export interface Viewport {
  width: number;
  height: number;
}

export interface Box {
  left: number;
  top: number;
  width: number;
  height: number;
}

/** Client coordinates on the displayed frame -> remote CSS pixels. The frame
 * element keeps the viewport's aspect ratio, so one scale applies. */
export function toRemotePoint(clientX: number, clientY: number, box: Box, viewport: Viewport): { x: number; y: number } | null {
  if (box.width <= 0 || box.height <= 0) return null;
  const x = ((clientX - box.left) * viewport.width) / box.width;
  const y = ((clientY - box.top) * viewport.height) / box.height;
  if (x < 0 || y < 0 || x > viewport.width || y > viewport.height) return null;
  return { x: Math.round(x * 10) / 10, y: Math.round(y * 10) / 10 };
}

/** Remote CSS pixels -> percentage box on the displayed frame (overlays). */
export function toOverlay(bounds: { x: number; y: number; width: number; height: number }, viewport: Viewport) {
  return {
    left: `${(bounds.x / viewport.width) * 100}%`,
    top: `${(bounds.y / viewport.height) * 100}%`,
    width: `${(bounds.width / viewport.width) * 100}%`,
    height: `${(bounds.height / viewport.height) * 100}%`,
  };
}

/** CDP modifier bits: Alt 1, Ctrl 2, Meta 4, Shift 8. */
export function modifierBits(event: { altKey: boolean; ctrlKey: boolean; metaKey: boolean; shiftKey: boolean }): number {
  return (event.altKey ? 1 : 0) | (event.ctrlKey ? 2 : 0) | (event.metaKey ? 4 : 0) | (event.shiftKey ? 8 : 0);
}

const SPECIAL = new Set([
  "Enter", "Tab", "Backspace", "Escape", "Delete", "Insert", "ArrowLeft", "ArrowUp", "ArrowRight", "ArrowDown",
  "Home", "End", "PageUp", "PageDown", "F5",
]);

/** Whether a keydown/keyup goes to the remote page as a key event. Plain
 * printable keys do too (typed by the page); paste is left to the paste
 * event so its text arrives as one insertion. */
export function forwardsKey(event: { key: string; ctrlKey: boolean; metaKey: boolean }): boolean {
  if (SPECIAL.has(event.key)) return true;
  if ([...event.key].length !== 1) return false;
  if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "v") return false;
  return true;
}

export const MAX_TEXT = 2_000;
