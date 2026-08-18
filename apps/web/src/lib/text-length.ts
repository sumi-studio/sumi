export function codePointLength(value: string): number {
  return Array.from(value).length;
}

export function clampCodePoints(value: string, max: number): string {
  if (codePointLength(value) <= max) return value;
  return Array.from(value).slice(0, max).join("");
}
