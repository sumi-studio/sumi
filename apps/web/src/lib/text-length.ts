/** Unicode code points, matching Go's range / RuneCountInString semantics. */
export function codePointLength(value: string): number {
  return Array.from(value).length;
}

/** Keep the first max Unicode code points without splitting a surrogate pair. */
export function clampCodePoints(value: string, max: number): string {
  if (codePointLength(value) <= max) return value;
  return Array.from(value).slice(0, max).join("");
}

/**
 * Whether a sender-controlled display string uses only the characters accepted
 * by the API: no controls or line/paragraph separators, and no format controls
 * except the ZWNJ/ZWJ joiners needed by scripts and emoji sequences.
 */
export function hasSafeDisplayCharacters(value: string): boolean {
  for (const character of value) {
    if (character === "\u200c" || character === "\u200d") continue;
    if (/^[\p{Cc}\p{Cf}\p{Zl}\p{Zp}]$/u.test(character)) return false;
  }
  return true;
}
