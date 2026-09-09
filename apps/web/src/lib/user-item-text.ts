import type { ChatItem } from "../agent/model";

/** Display projection only; source events remain separate from authored text. */
export function userItemText(
  item: Extract<ChatItem, { kind: "user" }>,
): string {
  const source = item.source?.source;
  if (source?.surface === "workspace_operation") {
    const state = {
      succeeded: "完了",
      failed: "失敗",
      cancelled: "中止",
      indeterminate: "終了状態不明",
    }[source.result.state];
    return `処理：${state}${source.result.exit_code === null ? "" : `（終了コード ${source.result.exit_code}）`}\n処理ID：${source.operation_id}`;
  }
  if (source?.kind !== "messaging_poll_vote") return item.text;
  const vote = source.poll_vote;
  return `${vote.question}\n${vote.selected_options.length ? `選択：${vote.selected_options.map((option) => option.text).join("、")}` : "回答を撤回しました"}`;
}

export function userItemSourceLabel(
  item: Extract<ChatItem, { kind: "user" }>,
): string {
  if (!item.source) return "あなた";
  const source = item.source.source;
  if (source.surface === "workspace_operation") return "ワークスペース処理";
  return `${source.place.kind === "dm" ? "DM" : source.place.kind === "group_dm" ? "グループDM" : "Messaging"} · ${source.place.name} · ${item.source.actor.display_name || item.source.actor.principal_id}${source.kind === "messaging_poll_vote" ? " · 投票" : source.kind === "reply_later_due" ? " · リマインダー" : ""}`;
}
