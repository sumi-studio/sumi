import { z } from "zod";

export const MAX_SDUI_PAYLOAD_BYTES = 64 * 1024;
export const MAX_SDUI_DEPTH = 8;
export const MAX_SDUI_VALUES = 512;
export const MAX_SDUI_COLLECTION_ITEMS = 128;
export const MAX_SDUI_OBJECT_KEYS = 64;
export const MAX_SDUI_STRING_BYTES = 4 * 1024;
export const MAX_SDUI_TYPE_LENGTH = 64;

// Declarative UI node. The current catalog renders one card at a time, so
// children are deliberately unsupported instead of accepting an ignored,
// recursively unbounded tree.
export interface SduiNode {
  type: string;
  props?: Record<string, unknown>;
}

export const sduiNodeSchema: z.ZodType<SduiNode> = z
  .object({
    type: z.string().min(1).max(MAX_SDUI_TYPE_LENGTH),
    props: z.record(z.string(), z.unknown()).optional(),
  })
  .strict();

/**
 * Validates an untrusted SDUI payload without recursively walking it on the JS
 * call stack. This boundary runs before Zod or React sees the value.
 */
export function parseSduiNode(value: unknown): SduiNode | null {
  try {
    if (!isBoundedJsonValue(value)) {
      return null;
    }
    const parsed = sduiNodeSchema.safeParse(value);
    return parsed.success ? parsed.data : null;
  } catch {
    return null;
  }
}

function isBoundedJsonValue(root: unknown): boolean {
  const encoder = new TextEncoder();
  const seen = new WeakSet<object>();
  const stack: Array<{ value: unknown; depth: number }> = [
    { value: root, depth: 0 },
  ];
  let values = 0;
  let bytes = 0;

  const consumeBytes = (amount: number) => {
    bytes += amount;
    return bytes <= MAX_SDUI_PAYLOAD_BYTES;
  };

  while (stack.length > 0) {
    const current = stack.pop();
    if (!current || current.depth > MAX_SDUI_DEPTH) {
      return false;
    }
    values += 1;
    if (values > MAX_SDUI_VALUES) {
      return false;
    }

    const item = current.value;
    if (item === null) {
      if (!consumeBytes(4)) return false;
      continue;
    }
    if (typeof item === "string") {
      const rawBytes = encoder.encode(item).byteLength;
      if (
        rawBytes > MAX_SDUI_STRING_BYTES ||
        !consumeBytes(encoder.encode(JSON.stringify(item)).byteLength)
      ) {
        return false;
      }
      continue;
    }
    if (typeof item === "boolean") {
      if (!consumeBytes(item ? 4 : 5)) return false;
      continue;
    }
    if (typeof item === "number") {
      if (!Number.isFinite(item) || !consumeBytes(String(item).length)) {
        return false;
      }
      continue;
    }
    if (typeof item !== "object") {
      return false;
    }
    if (seen.has(item)) {
      return false;
    }
    seen.add(item);

    if (Array.isArray(item)) {
      if (
        item.length > MAX_SDUI_COLLECTION_ITEMS ||
        !consumeBytes(2 + Math.max(0, item.length - 1))
      ) {
        return false;
      }
      for (let index = item.length - 1; index >= 0; index -= 1) {
        stack.push({ value: item[index], depth: current.depth + 1 });
      }
      continue;
    }

    const entries = Object.entries(item);
    if (
      entries.length > MAX_SDUI_OBJECT_KEYS ||
      !consumeBytes(2 + Math.max(0, entries.length - 1))
    ) {
      return false;
    }
    for (let index = entries.length - 1; index >= 0; index -= 1) {
      const [key, child] = entries[index] ?? [];
      if (key === undefined) return false;
      const keyBytes = encoder.encode(key).byteLength;
      if (
        keyBytes > MAX_SDUI_STRING_BYTES ||
        !consumeBytes(encoder.encode(JSON.stringify(key)).byteLength + 1)
      ) {
        return false;
      }
      stack.push({ value: child, depth: current.depth + 1 });
    }
  }

  return true;
}
