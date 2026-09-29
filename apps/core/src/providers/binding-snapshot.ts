import { type ModelBindingSnapshot, ModelError } from "../provider.ts";

/** Hash settings in a stable order; never persist the source or credentials. */
export async function snapshotFor(
  provider: string,
  settings: unknown,
  model?: string,
  reasoningEffort?: string,
): Promise<ModelBindingSnapshot> {
  const stable = (value: unknown): unknown => {
    if (Array.isArray(value)) return value.map(stable);
    if (value !== null && typeof value === "object") {
      return Object.fromEntries(
        Object.entries(value)
          .filter(([, v]) => v !== undefined)
          .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
          .map(([k, v]) => [k, stable(v)]),
      );
    }
    return value;
  };
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(JSON.stringify(stable({ provider, settings }))),
  );
  return {
    version: 1,
    fingerprint: Array.from(new Uint8Array(digest), (n) =>
      n.toString(16).padStart(2, "0"),
    ).join(""),
    provider,
    ...(model ? { model } : {}),
    ...(reasoningEffort ? { reasoningEffort } : {}),
  };
}

export function assertBindingSnapshot(
  expected: ModelBindingSnapshot | undefined,
  actual: ModelBindingSnapshot,
): void {
  if (
    expected !== undefined &&
    (expected.version !== actual.version ||
      expected.fingerprint !== actual.fingerprint ||
      expected.provider !== actual.provider ||
      expected.model !== actual.model ||
      expected.reasoningEffort !== actual.reasoningEffort)
  ) {
    throw new ModelError(
      "model_binding_changed: the saved branch's model connection or configuration changed; no model request was sent",
      { retryable: false, unavailable: true, cause: "model_binding_changed" },
    );
  }
}
