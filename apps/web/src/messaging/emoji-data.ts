/**
 * リアクション用の絵文字一覧。
 *
 * 完全なUnicode絵文字データベースは数MBあり、リアクションのために
 * 常時読み込む価値がない。会話のリアクションとして実際に使われる範囲に
 * 絞った手選びの集合を持ち、検索語は日本語と英語の両方を付ける
 * （日本語入力のまま "thanks" と打つ人がいるため）。
 */

export interface EmojiEntry {
  emoji: string;
  /** 日本語の呼び名。プレビューと読み上げに使う。 */
  name: string;
  /** 検索語。nameも暗黙に検索対象。 */
  keywords: string[];
}

export interface EmojiCategory {
  id: string;
  label: string;
  /** カテゴリ列に出す代表絵文字。 */
  icon: string;
  entries: EmojiEntry[];
}

function entry(emoji: string, name: string, keywords: string): EmojiEntry {
  return { emoji, name, keywords: keywords.split(" ") };
}

export const EMOJI_CATEGORIES: EmojiCategory[] = [
  {
    id: "reaction",
    label: "よく使う",
    icon: "👍",
    entries: [
      entry("👍", "いいね", "good yes ok thumbsup sansei 賛成 了解"),
      entry("👎", "よくない", "bad no thumbsdown 反対"),
      entry("✅", "完了", "check done ok 済み 完了 チェック"),
      entry("❌", "だめ", "cross no ng だめ 却下"),
      entry("👀", "見てる", "eyes look watch 確認 見た"),
      entry("🙏", "お願い", "pray thanks please ありがとう お願い 感謝"),
      entry("🎉", "祝う", "tada party congrats おめでとう 完成 リリース"),
      entry("🔥", "熱い", "fire hot 最高 burning"),
      entry("💯", "満点", "hundred perfect 完璧 100"),
      entry("🚀", "進める", "rocket ship launch リリース デプロイ 出荷"),
      entry("⚡", "速い", "zap fast lightning 高速 急ぎ"),
      entry("💡", "ひらめき", "bulb idea 思いつき 案"),
    ],
  },
  {
    id: "face",
    label: "顔・気持ち",
    icon: "😄",
    entries: [
      entry("😀", "にっこり", "grin smile 笑"),
      entry("😄", "笑顔", "smile happy 嬉しい 楽しい"),
      entry("😅", "苦笑い", "sweat smile 汗 あせり"),
      entry("😂", "大笑い", "joy lol laugh 涙 爆笑"),
      entry("🙂", "ほほえみ", "slight smile 微笑"),
      entry("😉", "ウインク", "wink"),
      entry("😊", "照れ", "blush 嬉しい"),
      entry("😍", "大好き", "heart eyes love 好き"),
      entry("🤩", "感動", "star struck すごい"),
      entry("🤔", "考え中", "thinking hmm 検討 悩む"),
      entry("🤨", "いぶかしむ", "raised eyebrow 疑問"),
      entry("😐", "無表情", "neutral"),
      entry("😑", "無言", "expressionless"),
      entry("😴", "寝てる", "sleep zzz 眠い"),
      entry("😭", "泣く", "cry sob 悲しい"),
      entry("😱", "驚き", "scream shock びっくり"),
      entry("😳", "動揺", "flushed 焦り"),
      entry("🥺", "お願い顔", "pleading 頼む"),
      entry("😤", "気合い", "triumph 意気込み"),
      entry("🤯", "衝撃", "mind blown 爆発 すごい"),
      entry("😇", "天使", "innocent"),
      entry("🥳", "お祝い", "partying congrats 祝"),
      entry("🫠", "とける", "melting 限界"),
      entry("😵‍💫", "目が回る", "dizzy 混乱"),
    ],
  },
  {
    id: "hand",
    label: "手・人",
    icon: "🙌",
    entries: [
      entry("👏", "拍手", "clap applause すばらしい"),
      entry("🙌", "やった", "raised hands hooray 万歳"),
      entry("🤝", "握手", "handshake deal 合意"),
      entry("💪", "がんばる", "muscle strong 力"),
      entry("👋", "やあ", "wave hello hi bye 挨拶"),
      entry("🫡", "了解", "salute roger 承知"),
      entry("👌", "オーケー", "ok perfect"),
      entry("✌️", "ピース", "victory peace"),
      entry("🤞", "祈る", "crossed fingers 願う"),
      entry("👇", "下", "point down 下記"),
      entry("👆", "上", "point up 上記"),
      entry("👉", "こちら", "point right"),
      entry("✍️", "書く", "writing 記録 メモ"),
      entry("🧠", "頭脳", "brain 考える 知恵"),
      entry("👤", "人", "person 参加者"),
      entry("👥", "人たち", "people チーム"),
    ],
  },
  {
    id: "object",
    label: "もの・仕事",
    icon: "🛠️",
    entries: [
      entry("🛠️", "直す", "tools fix 修理 実装"),
      entry("🔧", "工具", "wrench 調整"),
      entry("🐛", "バグ", "bug 不具合 虫"),
      entry("🧪", "テスト", "test experiment 実験 検証"),
      entry("📦", "荷物", "package 配布 依存"),
      entry("📝", "メモ", "memo note 書く ドキュメント"),
      entry("📄", "書類", "document page ファイル"),
      entry("📌", "ピン", "pin 固定 重要"),
      entry("📎", "クリップ", "paperclip 添付"),
      entry("🔗", "リンク", "link url"),
      entry("🔍", "調べる", "search magnify 検索 調査"),
      entry("🗑️", "捨てる", "trash delete 削除"),
      entry("⏰", "時間", "alarm clock 締切 期限"),
      entry("📅", "予定", "calendar schedule 日程"),
      entry("💻", "パソコン", "laptop computer 開発"),
      entry("📱", "スマホ", "phone mobile"),
      entry("🔒", "鍵", "lock secure 権限 認証"),
      entry("🔑", "キー", "key 鍵 資格情報"),
      entry("⚙️", "設定", "gear config 歯車"),
      entry("📊", "グラフ", "chart data 集計 分析"),
      entry("🧭", "方角", "compass 方針 指針"),
      entry("🪄", "魔法", "magic wand 自動"),
    ],
  },
  {
    id: "symbol",
    label: "記号",
    icon: "⭐",
    entries: [
      entry("⭐", "星", "star お気に入り"),
      entry("✨", "きらめき", "sparkles new 新機能 きれい"),
      entry("❤️", "ハート", "heart love 好き"),
      entry("🧡", "オレンジハート", "orange heart"),
      entry("💛", "黄ハート", "yellow heart"),
      entry("💚", "緑ハート", "green heart"),
      entry("💙", "青ハート", "blue heart"),
      entry("💜", "紫ハート", "purple heart"),
      entry("🖤", "黒ハート", "black heart"),
      entry("💔", "こわれた心", "broken heart 失敗"),
      entry("⚠️", "注意", "warning caution 警告"),
      entry("🚨", "緊急", "siren alert 警報 急ぎ"),
      entry("🛑", "止める", "stop 停止 中止"),
      entry("❓", "疑問", "question 質問"),
      entry("❗", "重要", "exclamation 注目"),
      entry("➕", "追加", "plus add"),
      entry("➖", "削る", "minus remove"),
      entry("🔁", "繰り返し", "repeat loop 再試行"),
      entry("🆗", "OK", "ok"),
      entry("🆕", "新規", "new 新しい"),
      entry("♻️", "再利用", "recycle リファクタ"),
      entry("💤", "休み", "zzz sleep 保留"),
    ],
  },
  {
    id: "nature",
    label: "自然・食べ物",
    icon: "🌱",
    entries: [
      entry("🌱", "芽", "seedling growth 成長 新規"),
      entry("🌸", "桜", "cherry blossom 春"),
      entry("🌊", "波", "wave 海"),
      entry("🌙", "月", "moon night 夜"),
      entry("☀️", "太陽", "sun 晴れ 朝"),
      entry("🌈", "虹", "rainbow"),
      entry("❄️", "雪", "snow cold 冷える"),
      entry("🐈", "猫", "cat ねこ"),
      entry("🐕", "犬", "dog いぬ"),
      entry("🐢", "亀", "turtle slow 遅い"),
      entry("🍵", "お茶", "tea 休憩"),
      entry("☕", "コーヒー", "coffee 休憩 朝"),
      entry("🍺", "ビール", "beer 乾杯 打ち上げ"),
      entry("🍰", "ケーキ", "cake 祝い"),
      entry("🍜", "ラーメン", "ramen noodle 昼"),
      entry("🍙", "おにぎり", "rice ball 昼"),
      entry("🍫", "チョコ", "chocolate 甘い"),
      entry("🧊", "氷", "ice 冷える 凍結"),
    ],
  },
];

/** 直近が空のときに操作チップへ出す既定の3つ。 */
export const DEFAULT_RECENT_EMOJIS = ["👍", "✅", "🎉"];

const ALL_ENTRIES = EMOJI_CATEGORIES.flatMap((category) => category.entries);

const BY_EMOJI = new Map(ALL_ENTRIES.map((item) => [item.emoji, item]));

/** 一覧に無い絵文字（サーバー由来のリアクション等）も呼び名だけは返す。 */
export function emojiName(emoji: string): string {
  return BY_EMOJI.get(emoji)?.name ?? emoji;
}

export function searchEmojis(query: string, limit = 60): EmojiEntry[] {
  const trimmed = query.trim().toLowerCase();
  if (!trimmed) return [];
  const starts: EmojiEntry[] = [];
  const contains: EmojiEntry[] = [];
  for (const item of ALL_ENTRIES) {
    if (item.emoji === trimmed) {
      starts.push(item);
      continue;
    }
    const terms = [item.name.toLowerCase(), ...item.keywords];
    if (terms.some((term) => term.startsWith(trimmed))) starts.push(item);
    else if (terms.some((term) => term.includes(trimmed))) contains.push(item);
  }
  return [...starts, ...contains].slice(0, limit);
}
