/**
 * Characters that must not appear in sender-controlled attachment display
 * text. Keep this list aligned with the API and agent gates: C0/C1 controls
 * (including NEL), Unicode line/paragraph separators, bidi controls, and
 * zero-width format controls all make a one-line label ambiguous.
 */
const FORBIDDEN_ATTACHMENT_DISPLAY_CHARACTERS =
  /[\p{Cc}\u{180E}\u{200B}-\u{200F}\u{2028}\u{2029}\u{202A}-\u{202E}\u{2060}\u{2066}-\u{2069}\u{FEFF}]/gu;

export function sanitizeAttachmentDisplayText(value: string): string {
  return value.replace(FORBIDDEN_ATTACHMENT_DISPLAY_CHARACTERS, " ");
}

export function sanitizeAttachmentFilenameForDisplay(value: string): string {
  return sanitizeAttachmentDisplayText(value).trim() || "file";
}

export function hasForbiddenAttachmentDisplayCharacter(value: string): boolean {
  FORBIDDEN_ATTACHMENT_DISPLAY_CHARACTERS.lastIndex = 0;
  return FORBIDDEN_ATTACHMENT_DISPLAY_CHARACTERS.test(value);
}
