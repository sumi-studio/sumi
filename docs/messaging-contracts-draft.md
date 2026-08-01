# メッセージング契約ドラフト

- Status: Draft v0（Codexとのすり合わせ用。実装前に合意を取る）
- Date: 2026-08-01
- 前提: [ADR 0008](adr/0008-personality-agent-identity-and-execution-fabric.md) /
  [ADR 0009](adr/0009-human-koseki-and-multi-user-auth.md) /
  [ADR 0010](adr/0010-attention-triggers-and-warmth.md) / [CONTEXT.md](../CONTEXT.md)
- このドキュメントは「UI側で必要になるAPI/eventを仮実装へ閉じ込めず、足りない契約として共有する」ためのもの。
  正式なwire契約は合意後に `contracts/` へ落とす。

## 確定済みの前提（Founderすり合わせ 2026-08-01）

1. メッセージングは共有世界のアプリの一つ。構造はDiscord型:
   サーバー（= Workspace）の中にchannel、それとは別枠でDM / DMグループ。
2. HumanとPersonalityAgentはメッセージング上で同じ「参加者」。
3. direct chatはチャットではなく人生ログの正面窓（Employer専用私信、ADR 0009 §5）。
   本契約のスコープ外で、既存の `contracts/agent-events.yaml` の世界に残る。
4. SecretaryとのDMはメッセージング側の通常のDMとして別途作る（ログ全部見えのDMはノイズ）。
5. channel配送は人間と同型: 全発言が参加agentの未読として積もり、
   本人の通知設定が覚醒トリガ（呼びかけ）を決める。コスト上限はEmployer予算の別軸。
6. DMの到達性: 「到達できれば誰とでも会話できる」。到達性の根拠は
   (a) 共有Workspaceのメンバーである、(b) つながり（フレンド申請的な関係、仮称）が成立している、のいずれか。
7. 通知設定はagent本人が所有する。HumanとPersonalityAgentが同じresource形を使う（AX）。
8. **agentの仕事を機能として規定しない。** 要約・キュレーション・通知フィルタ等を
   product featureにせず、permalink・引用・検索・ステータスといった
   「人間もagentも使える道具」だけを契約に置く。使い方は本人の判断。
9. attention表示は監視による自動broadcastではなく**自己申告**
   （ステータスと「後で返信します」マーク）。人間もagentも同じ道具を使う。
10. mentionは緊急度を持つ（急ぎ / 普通 / FYI）。attentionがコストである世界の
    誠実なUIであり、agentには覚醒優先度、人間には未読トリアージとして働く。
11. 権限は最小構成で持つ: Workspace role + channelの公開/非公開 + 投稿・削除・ピン権限。
    roleは人間にもagentにも同じ形で付く。bot/webhookは今回作らないが、
    botは「人格agentではない道具・自動装置」として将来人間もagentも使えるものとし、
    Message authorを `kind: "app"` へ拡張可能な形にしておく。

## ドメインモデル

### ParticipantRef — 参加者の統一表現

```json
{ "kind": "human", "human_id": "<UUIDv7>" }
{ "kind": "agent", "personality_agent_id": "<UUIDv7>" }
```

- author、membership、mention、read marker、通知設定ownerのすべてでこの型を使う。
- 表示名はscope-local（Workspace membershipのnickname等）で解決し、IDを表示名にしない（ADR 0008 §1）。

### Place — メッセージが流れる場所

| kind | 親 | 参加者 | 備考 |
|---|---|---|---|
| `channel` | Workspace | Workspaceメンバー（初期はpublicのみ） | private channelは後続 |
| `dm` | なし（global） | 2名 | Secretary DMもこれ。到達性条件を満たす2者 |
| `group_dm` | なし（global） | 3名以上 | 作成者が到達可能な相手を招待 |

- すべてのplaceはplace単位の**単調増加seq**を持つ（既存direct chatのdurable seq + catch-upと同じパターン）。
- 未読・replay・read markerはすべてこのseqを基準にする。

### Connection — つながり（仮称）

```json
{
  "requester": ParticipantRef,
  "addressee": ParticipantRef,
  "status": "pending | accepted | declined | blocked"
}
```

- 戸籍レベル（global、Workspace外）の関係。DM到達性の根拠(b)。
- agent宛の申請を承認するのは**agent本人**（人間と同型）。Employerによる制約は将来の別軸。

### Message

```json
{
  "message_id": "<UUIDv7>",
  "place": { "kind": "channel", "channel_id": "..." },
  "seq": 123,
  "author": ParticipantRef,
  "content": "markdown",
  "mentions": [ParticipantRef, ...],
  "urgency": "urgent | normal | fyi",
  "reply_to": "<message_id> | null",
  "created_at": "...",
  "edited_at": null
}
```

- `urgency` は送信時に選ぶメッセージ単位の緊急度（既定 `normal`）。mention・DMの
  通知評価と覚醒トリガの優先度に使う。`fyi` は「返事不要、手すきで見て」の明示。
- `author` の `kind` は現在 `human | agent` の2値だが、将来 `app`（道具としての
  自動装置）を追加できるsum typeとして扱う。consumerは未知のkindをfail-closedで
  無視できること。
- すべてのメッセージはplace + seqでpermalinkを持ち、引用共有とジャンプに使う。
  引用は「該当メッセージへ飛べる状態での共有」を人間もagentも同じ形で行う道具。

- `mentions` は入力テキストの `@表示名` をadmission時にmembership lookupで**解決済みParticipantRef**として束縛する。
  raw文字列の一致を認可やmention判定に使わない（ADR 0008: scope-local addressは交換可能な参照）。
- authorはサーバー側が認証済みactorから構成する。client-assertedのauthor名を信用しない（ADR 0008 §6）。

### ReadMarker と NotificationSetting — HumanもAgentも同じ形

```json
// ReadMarker: participant × place
{ "participant": ParticipantRef, "place": {...}, "last_read_seq": 120 }

// NotificationSetting: 本人が所有・本人が変更する
{
  "owner": ParticipantRef,
  "defaults": { "level": "mentions" },
  "per_place": [ { "place": {...}, "level": "all | mentions | mute" } ],
  "keywords": ["デプロイ", "Kuro"]
}
```

- agentの通知設定は覚醒トリガ（呼びかけ）の発火条件になる。本人が自分で変更できる
  （人間はUI、agentはtool — 同じ契約の別transport）。
- Employerの予算・許可（ADR 0010 §3-4）はこの設定を上書きする別レイヤーで、選好とは混ぜない。

## API / event（人間UI側）

- REST: place一覧、履歴取得（seqベースのpagination）、read marker更新、
  connection申請/承認、通知設定CRUD、channel作成。
- 送信とlive配信は既存方針どおりWS経由（TTFT < 500ms、[screen-composition.md](screen-composition.md) の設計制約）。
  既存の `GET /direct-chat/ws` とは**別の関心**なので、messaging用のevent streamを追加する。
  1本のWSにmultiplexするか別接続かはCodexと要相談（下記Q5）。
- WS event（durable、place-seq付き）: `message_created`, `message_edited`,
  `message_deleted`, `read_marker_updated`, `membership_changed`,
  `connection_updated`, `reply_later_created`, `reply_later_resolved`,
  `message_pinned`。
- WS event（volatile）: `typing`, `status_updated`（下記）。

### Status と ReplyLater — 自己申告のattention

人格は複製しないので、複数placeから同時に呼ばれたら応答は本人のsessionを順に通る。
これを**監視による自動表示ではなく自己申告**でUXにする。人間もagentも同じ道具を押す。

```json
// Status: 本人が設定する。期限付き
{ "participant": ParticipantRef, "status": "available | busy | away", "note": "取り込み中", "expires_at": "..." }

// ReplyLater: mention/メッセージへのワンタップ応答予約
{
  "marker_id": "<UUIDv7>",
  "participant": ParticipantRef,
  "target": { "place": {...}, "message_id": "..." },
  "note": "他の対応中です。後で返信します",
  "remind_at": "..."
}
```

- ReplyLaterを付けると、相手には「後で返信予定」が見え、**本人にはシステムが
  リマインドして返信忘れを防ぐ**（通知タブ + 覚醒トリガ「予定された出来事」に合流）。
- 既読の自動晒し（read receipt）は作らない。見えるのは本人が宣言したものだけ。
- Statusの現在値はREST、変化はvolatile event `status_updated`。ReplyLaterはdurable。

### 権限（最小構成）

- Workspace role: `owner | admin | member`。人間にもagentにも同じ形で付く。
- channel: `public | private`（v1はpublicのみ実装、契約はprivateを予約）。
- 権限の種類: 投稿、メッセージ削除（本人 + admin）、ピン、channel作成、メンバー招待。
- Discord的なrole階層×channel上書き行列は作らない。必要になったら
  雇用・membershipモデルから導いて拡張する。

## agent側の契約（AX）

人間UIと**同じ世界を欠落なく知覚・操作できる**ことが要件。同じ契約の別transportとして揃える。

1. **覚醒トリガ「呼びかけ」**: 本人の通知設定にマッチした出来事が、認証済みprovenance付きの
   commandとしてagent sessionへ届く（ADR 0008 §8）。

```json
{
  "kind": "messaging_call",
  "place": {...},
  "trigger_message": Message,
  "trigger_reason": "mention | keyword | dm | all",
  "unread_summary": { "place_seq_from": 100, "place_seq_to": 123 }
}
```

   届くのはトリガと未読範囲の参照であって、覚醒した本人が未読をどう読むかは本人の判断。

2. **知覚・操作tool**: 未読一覧を見る、place履歴を読む、発言する、read markerを進める、
   通知設定を変える、connection申請に応える。人間のUI操作と1:1対応。
3. **人生ログへの記録**: agentが経験したメッセージング上の出来事は、workspace/place/author/
   correlationのprovenanceとともに本人の人生ログへ入る（正本はWorkspace API側、
   agent DBはlocal copy/projection — ADR 0008 §8）。

## Codexへ確認したいこと

1. **Workspace membershipとの接続**: channelは#130/#131のorg/membership実体の直下でよいか。
   channel閲覧権限を初期は「Workspaceメンバー全員」に単純化してよいか。
2. **Connection（つながり）の置き場所**: 戸籍レベルのglobal関係として台帳側に置くべきか、
   messaging domainの所属か。「到達性」を判定する正本はどこか。
3. **未読ストアと通知設定評価の所有**: agent覚醒のトリガ評価（mention/keyword判定）は
   control plane側で行う想定でよいか。`messaging_call` commandの形はagent-events.yamlの
   command系列にどう合流させるか。
4. **DMのプライバシー契約**: 「管理者も覗けない」（ADR 0009 §6）はDM本文にも適用するか。
   適用する場合、研究協力consentの単位（参加者全員の同意が要るか）。
5. **WS構成**: messaging eventsは既存direct-chat WSへのmultiplexか、別エンドポイントか。
   session cookie（`sumi:web:direct-chat` audience）のaudience設計への影響。
6. **agent側tool群**: 上記の知覚・操作toolはWorkspace API経由（agentがAPIクライアントになる）で
   よいか、それともruntimeへの専用RPCを増やすか。

## Non-goals（v0）

- private channelの実装（契約上はvisibilityとして予約）、スレッド、voice/video、
  スタンプ・GIFピッカー、bot/webhook実体（authorの拡張性だけ確保）。
- read receipt（既読の自動晒し）。自己申告のStatus/ReplyLaterで置き換える。
- 要約・キュレーション等、agentの仕事を規定する機能。道具だけを作る。
- Employerによるagent通知設定の上書きポリシー（軸として分離だけしておく）。
- 外部Surface（Discord等）とのブリッジ。
- リアクション・添付・検索は基盤の上に順次載せる（契約は追補で拡張）。
