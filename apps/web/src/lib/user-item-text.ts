import type { ChatItem } from "../agent/model";

/** Display projection only; source events remain separate from authored text. */
export function userItemText(
  item: Extract<ChatItem, { kind: "user" }>,
): string {
  const source = item.source?.source;
  if (source?.kind !== "messaging_poll_vote") return item.text;
  const vote = source.poll_vote;
  return `${vote.question}\n${vote.selected_options.length ? `選択：${vote.selected_options.map((option) => option.text).join("、")}` : "回答を撤回しました"}`;
}
