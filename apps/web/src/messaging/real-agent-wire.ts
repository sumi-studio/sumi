function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isCanonicalUUIDv7(value: unknown): value is string {
  return (
    typeof value === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(
      value,
    )
  );
}

/**
 * The real-agent E2E deliberately fail-closes on the complete message wire.
 * This attachment scenario has ordinary messages, whose poll field is null.
 * Keep its wire requirements in a small, fixture-testable validator so a
 * future REST field addition cannot silently turn into a false E2E timeout.
 */
export function hasExactOpenMessageWireShape(value: unknown): boolean {
  return (
    isRecord(value) &&
    Object.keys(value).sort().join("\0") ===
      "attachments\0author\0client_nonce\0content\0created_at\0deleted\0edited_at\0mentions\0message_id\0place\0poll\0reactions\0reply_to\0revision\0seq\0urgency" &&
    isCanonicalUUIDv7(value.message_id) &&
    typeof value.content === "string" &&
    value.poll === null &&
    typeof value.revision === "number" &&
    Number.isSafeInteger(value.revision) &&
    value.revision >= 1
  );
}
