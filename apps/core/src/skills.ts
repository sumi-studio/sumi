/**
 * First-party usage guides. Only these short entries are sent with the tool
 * definition; the state service embeds and returns a guide when it is read.
 * A guide describes an app, not a grant to use tools absent from the request.
 */
export const SKILL_CATALOG = [
  {
    name: "messaging",
    description:
      "Messaging の場所や会話を探す、返信先を選ぶ、添付ファイルを読んだり送ったりするときの使い方。",
  },
  {
    name: "calls",
    description:
      "Messaging の通話に参加する、発話の再生状態を調べる、通話から退出するときの使い方。",
  },
  {
    name: "terminal-files",
    description:
      "共有ターミナルの既存セッションや入力結果を調べる、対話入力をする、ファイルを扱うときの使い方。",
  },
] as const;

export function renderSkillCatalog(): string {
  return SKILL_CATALOG.map(
    ({ name, description }) => `${name}: ${description}`,
  ).join("\n");
}
