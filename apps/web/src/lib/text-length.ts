/** Unicode code points, matching Go's range / RuneCountInString semantics. */
export function codePointLength(value: string): number {
  return Array.from(value).length;
}

/** Keep the first max Unicode code points without splitting a surrogate pair. */
export function clampCodePoints(value: string, max: number): string {
  if (codePointLength(value) <= max) return value;
  return Array.from(value).slice(0, max).join("");
}
