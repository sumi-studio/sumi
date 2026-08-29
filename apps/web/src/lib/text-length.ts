export function codePointLength(value: string): number {
  return Array.from(value).length;
}

export function clampCodePoints(value: string, max: number): string {
  if (codePointLength(value) <= max) return value;
  return Array.from(value).slice(0, max).join("");
}

/** Match the API's safe single-line profile text boundary. */
export function hasSafeDisplayCharacters(value: string): boolean {
  if (value.length === 0) return true;
  let visible = false;
  for (const character of value) {
    if (character === "\u200c" || character === "\u200d") continue;
    if (/^[\p{Cc}\p{Cf}\p{Zl}\p{Zp}]$/u.test(character)) return false;
    if (!/^[\p{White_Space}\p{M}]$/u.test(character)) visible = true;
  }
  return visible;
}
