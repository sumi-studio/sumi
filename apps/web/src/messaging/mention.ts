const DISPLAY_MENTION_BOUNDARY = String.raw`(?=$|[\s.,!?、。！？:：;；()（）\[\]{}「」『』])`;

export function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export function hasDisplayMention(
  content: string,
  displayName: string,
): boolean {
  return new RegExp(
    `@${escapeRegExp(displayName)}${DISPLAY_MENTION_BOUNDARY}`,
    "u",
  ).test(content);
}

export function displayMentionPattern(displayNames: readonly string[]): RegExp {
  const alternatives = [...displayNames]
    .sort((a, b) => b.length - a.length)
    .map(escapeRegExp)
    .join("|");
  return new RegExp(`@(${alternatives})${DISPLAY_MENTION_BOUNDARY}`, "gu");
}
