/** Structural JSON equality, insensitive to object key order. */
export function jsonEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (Array.isArray(a) && Array.isArray(b)) {
    return a.length === b.length && a.every((v, i) => jsonEqual(v, b[i]));
  }
  if (a && b && typeof a === "object" && typeof b === "object") {
    const ka = Object.keys(a).sort();
    const kb = Object.keys(b).sort();
    return (
      ka.length === kb.length &&
      ka.every(
        (k, i) =>
          k === kb[i] &&
          jsonEqual(
            (a as Record<string, unknown>)[k],
            (b as Record<string, unknown>)[k],
          ),
      )
    );
  }
  return false;
}

/** Replace NUL with U+FFFD recursively — jsonb can never hold 0x00. */
export function scrubJson(v: unknown): unknown {
  if (typeof v === "string") return v.replaceAll("\u0000", "\uFFFD");
  if (Array.isArray(v)) return v.map(scrubJson);
  if (v !== null && typeof v === "object") {
    return Object.fromEntries(
      Object.entries(v).map(([k, x]) => [k, scrubJson(x)]),
    );
  }
  return v;
}

const TRUNC_MARK = "…[truncated]";

/**
 * Bound a string's UTF-8 encoding, cutting only at a code-point boundary
 * so multibyte and control characters can never push the result past
 * maxBytes. Truncation is explicit — the marker is part of the record.
 */
export function truncateText(s: string, maxBytes: number): string {
  const enc = new TextEncoder().encode(s);
  if (enc.length <= maxBytes) return s;
  const markLen = new TextEncoder().encode(TRUNC_MARK).length;
  let end = Math.max(0, maxBytes - markLen);
  while (end > 0 && ((enc[end] ?? 0) & 0xc0) === 0x80) end--;
  return new TextDecoder().decode(enc.subarray(0, end)) + TRUNC_MARK;
}
