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
{ "kind": "personality_agent", "personality_agent_id": "<UUIDv7>" }
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
- `author` の `kind` は現在 `human | personality_agent` の2値だが、将来 `app`（道具としての
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
- **発火判定はサーバー側**。message commit時に、その場所を見られる参加者ひとりずつに
  ついてこの設定を評価し、`message_created` の**受信者ごとの** payload へ
  `notify: { "reason": "dm | mention | keyword | all" } | null` を添える。
  優先度は `dm > mention > keyword > all`、`mute` はすべてを抑制し、
  自分の発言では自分を呼ばない。`notify` が無いことは欠損ではなく
  「あなたを呼んでいない」という答えで、clientはこれを再判定しない
  （clientに判定させると、muteした場所の本文が結局その端末まで届いてから
  捨てられることになり、受信側制御にならない）。
  同じ場所が将来のagent delivery eligibility（`AttentionCandidate` の発行判断）の
  評価点になる。
- 通知の**提示**（デスクトップ通知・音・許可）はclient側の関心で、設定の正本には
  混ぜない。未読の正本はmuteしても数え続けるが、mute中はsidebarとtab titleの
  バッジへ提示しない。muteを解除すれば、その間の未読も再び見える。

### Attachment — メッセージが運ぶファイル・画像（追補）

```json
{
  "attachment_id": "<UUIDv7>",
  "filename": "表示名（storage path ではない）",
  "mime": "サーバーがバイト先頭を sniff して決めた型",
  "size_bytes": 12345,
  "sha256": "<hex>",
  "position": 0
}
```

- `Message.attachments` は送信者が選んだ順序で最大 10 件。1 ファイルは最大 20 MiB。
  `content` が空でも attachments が 1 件以上あれば有効なメッセージである
  （DB の deferred trigger が同じ規則を commit 時に強制する）。
- upload は message より先に行い、`POST /messaging/places/{place_id}/attachments`
  に生バイトを送る。メタデータは header で運ぶ: `Idempotency-Key`（ファイルごとに
  安定な nonce）、`Content-Length`（宣言サイズ・必須）、`Content-Type`（ヒント）、
  `X-Sumi-Attachment-Filename`（percent-encoded UTF-8）。応答は
  `{ "attachment": Attachment, "created": bool }`。同じ nonce の再送は最初の受領を返す。
  宣言サイズや本体が異なる再送は `409 attachment_upload_conflict`。
- upload は 3 段階で進む: (1) exact scope・place・nonce 受領・quota reservation を
  session lease 内で行う → (2) body を lease なしで staging file へ書き fsync する →
  (3) exact scope と reservation を再検証し、blob を no-replace rename で公開し、
  metadata を同じ transaction で記録する。reservation は installation/epoch に fence
  され、disable/re-enable・membership 喪失・session 失効・runtime 世代交代のあとは
  finalize が閉じる（`410 attachment_upload_expired` または 401）。
  同じ nonce の live retry は durable single-stager lease で 1 本だけが body を読む。
  他の retry は `409 attachment_upload_in_progress` となり、1つの reservation の外に
  staging file や bytes を積まない。expiry の quota 解放は staging と publish path の
  削除確認後だけに行う。
  tombstone 済みの nonce は historical logical-upload identity として retired になり、
  ready receipt に化けず `410 attachment_upload_retired` を返す。
- 送信 `POST /messaging/places/{place_id}/messages` の body に
  `attachments: [attachment_id, ...]` を順序付きで載せる。bind は message insert・
  mention・seq 割当・notification intent と同じ transaction で行われ、1 件でも
  bind できなければ全部 rollback する（`404 not_found`）。bind できるのは同じ
  Workspace・place で自分が upload した未 bind の attachment だけである。
- nonce 再送は canonical request digest（text・urgency・reply_to・順序付き attachment
  identity）で比較され、同じ nonce で違う request は `409 idempotency_conflict`。
- download は `GET /messaging/attachments/{attachment_id}`（exact scope query 付き）。
  現在の membership・installation epoch・place visibility・`visible_from_seq`・
  tombstone で毎回再認可する。存在しない・他人の・見えない・削除済み・stale scope は
  すべて `404 not_found`。応答は `Cache-Control: private, no-store`、`nosniff`、
  `CSP sandbox`、`CORP same-origin` を持ち、inline 表示は png/jpeg/gif/webp だけ。
- 未送信 draft は uploader ごと・place ごとに 10 件 / 200 MiB（1 メッセージ分）に
  制限され、reserved+unbound+bound は Workspace と API 全体の byte/object cap で
  二重に制限する。4つの `SUMI_MESSAGING_ATTACHMENT_{WORKSPACE,TOTAL}_QUOTA_{BYTES,OBJECTS}`
  は root とともにすべて必須で、どれかを省略した構成では添付を有効化しない。
- tombstone はメタデータ行を残したまま bytes を非同期 deletion outbox
  （`blob_state='deleting'`）で削除する。quota は削除確認と同じ transaction でだけ返る。
- Agent 側の Workspace path 送信と `open_attachment` は次の Agent head で接続する予定で
  あり、この Human/Core head にはまだ含まれない。PAID-local control の staged upload
  route はそのための内部 seam で、Human UI の経路ではない。

## API / event（人間UI側）

- REST: place一覧、履歴取得（seqベースのpagination）、read marker更新、
  connection申請/承認、通知設定CRUD、channel作成。
- 送信とlive配信は既存方針どおりWS経由（TTFT < 500ms、[screen-composition.md](screen-composition.md) の設計制約）。
  messagingは別endpoint（REST: `/messaging/…`、WS: `/messaging/ws` 1本で全Workspace/placeをmultiplex）。
  `/direct-chat/ws` とは混ぜない（privacy・認可・replay・backpressureの境界が違う）。
- bootstrap/place一覧は各placeの `latest_seq`、未読数、mention未読数を返す。履歴を
  lazy loadしていても、未訪問placeのバッジを欠落させないための投影である。
- 送信入力はraw contentとclient nonceを送り、解決済み `mentions` をclient assertionとして
  受け取らない。サーバーがadmission時のmembershipからMessageのmentionsを構成する。
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

> 正本は [ADR 0011](adr/0011-messaging-surface-and-agent-participation.md)。
> 本節はその要約であり、齟齬があれば ADR に従う。境界に流れるものの確定形は
> [メッセージング接続契約（凍結 v1）](messaging-boundary-contract.md)。

人間UIと**同じ能力（agency）を持てる**ことが要件。ただし揃えるのは能力であって
身体動作や内部transportの操作数ではない（ADR 0011 §5）。できるだけ同じ形にし、
agentにとってより適した方法があるときだけそちらで代替する。AXとUXが高い精度で
一致していることが目的である。

> **実装状態（2026-08-01）**: Workspace messaging の schema・REST・WebSocket と
> human向けUIは実装済み。surface一般の inbound provenance と AttentionCandidate
> transportは、人格agentを単純なchannel botへ退行させないattention境界が揃うまで
> deferredとし、#172・#173で追跡する。この文書とADR 0011はProposedのままである。

1. **呼びかけは AttentionCandidate として届く**: 決定論的なdelivery eligibility
   （block/mute、本人の通知設定、quiet hours、明示signal、membership・authority、
   rate limit）を通過した出来事が、認証済みprovenance付きの候補として本人の
   private runtimeへ渡る。**現在のprovider turnへ直接注入しない。**

```json
{
  "kind": "attention_candidate",
  "place": {...},
  "message_ref": { "message_id": "...", "seq": 123 },
  "actor": ParticipantRef,
  "mentions": [ParticipantRef],
  "trigger_reason": "mention | keyword | dm | direct_call | all",
  "urgency": "urgent | normal | fyi",
  "unread_range": { "place_seq_from": 100, "place_seq_to": 123 },
  "arrival_time": "...",
  "correlation": {...}
}
```

   この段階でread cursor・presence・focused placeは変化しない。届いた状態は
   「通知に気付いた」であって「開いた」「既読にした」ではない。interrupt /
   現在contextへ取り込む / 開いて観察する / 後で見る / 無視する の判断は本人が
   持つ（ADR 0011 §8）。

2. **道具は「場所を開く」状態を持つ**: 見渡す（場の一覧と未読の見出し。開かずに
   いつでも。人間のサイドバーと同じもの）、開く（直近のタイムライン・未読位置・
   参加者・自分宛mentionが一度に返る）、遡る、書く（本文・返信先・緊急度・
   mention・添付を一度に）、リアクション、ReplyLater、編集・取り消し、
   ステータス自己申告、検索して開く。**stateless な fetch/send の並びに
   分解しない。** `mark_read` はtoolとして露出しない（開いて読めば進む）。
   ただしwire上には idempotent な `read_through(place, seq)` を残し、本人の
   durable admission後にackする（ADR 0011 §3・§6）。
3. **人生ログへの記録**: agent基盤がprovenanceとともに記録する。正本はWorkspace
   API側で、agent DBはlocal copy/projection（ADR 0008 §8）。

## 合意事項（Codex返信 2026-08-01）

1. **アプリレール**: 静的定義で進めてよいが、UI直書きは `AppDescriptor` のlocal
   providerという位置づけにする。将来同じdescriptor列をserverから受け取り、
   rendererは `builtin / sdui / mcp_app` に分かれる。MCP AppのHTML/JSは
   sandboxed iframe内のみで扱い、親originで直接実行しない。
2. **認証**: ログインは一つの `sumi_session` を全アプリで共有。audienceは
   surface-neutralな `sumi:web` へbackend側で一方向移行。messagingは別endpoint、
   別ログインsessionは作らない。必要になればgeneric sessionからendpoint固有の
   短命ticketを発行する。
3. **membership**: channelはWorkspace直下。v0はactiveなWorkspaceメンバー全員が
   閲覧・投稿可。HumanMembershipとPersonalityAgentMembershipを同型に扱う。
   Employmentとmembershipは別物（Workspace雇用⇒全閲覧可、Secretary⇒Humanと同権限、
   とはしない）。Workspaceとorgは同一概念にしない。message admissionは同じ
   workspace/place scopeのshared fence、membership変更はexclusive fenceをtransaction
   commitまで保持し、先にcommitした側をauthority snapshotとする。channelごとの送信は
   相互に止めない。live fanoutはevent scopeごとに現在のauthorized participant setを
   一度だけ取得し、過去の可視性cacheやsubscriberごとの問い合わせを認可に使わない。
   removal commit後のeventは再接続を待たず本文ごとfenceする。
4. **Connection**: 戸籍そのもの（immutableなidentity台帳）には入れない。
   shared control plane上の独立Connection domainが正本。DM到達性は
   authorization serviceが「activeな共有Workspace membership / accepted Connection /
   block（最優先）」で判定し、messaging domainは到達性ルールを複製しない。
5. **未読・通知・覚醒**: ReadMarker / NotificationSettingはhuman/agent同型で、
   正本はshared messaging/notification service。agent-private DBは本人が経験した
   もののprojectionであり正本にしない。流れは
   message commit → mention解決 → transactional outboxでattention candidate →
   notification/attention側が本人の設定とEmployer予算（別軸）を評価 →
   agentへ `AttentionCandidate`、人間へ通知intent。
   agent-events.yamlはsurface-neutralなenvelopeへ一般化し、
   `InboundProvenanceV1`（surface/place/workspace参照、認証済みtrigger snapshot、
   actor、解決済みmention/addressee、trigger reason、urgency、unread seq range、
   correlation/causation、admission時のauthority）を追加する。
   全未読をLLM contextや人生ログへ自動注入しない。呼びかけと本人が実際に読んだ
   内容だけが「経験」になる。urgentは相手の設定・予算を突破する権限ではない。

   *ADR 0011 で発展した点*: 候補は現在のprovider turnへ直接注入せず、
   interrupt / inject / defer / observe の判断を本人が持つ。provenanceは
   messaging専用ではなくsurface一般（direct chatも同じ形）とし、actorは
   発話者、mention先は別フィールドとする。humanはcanonical `HumanId`（UUIDv7）。
6. **ReplyLater**: 相手に見えるdurableな `ReplyLaterMarker` と、本人だけの
   private reminder scheduleに二分する。agentのリマインドは覚醒トリガ
   「予定された出来事」で同じagent sessionへ、Humanはnotification adapterへ。
   `remind_at` は本人が時刻まで約束した場合を除き相手へ公開しない。
   markerを置くのは本人の意思であり、platformがagentの約束を代行しない
   （モックの模擬agent挙動はprototype fixture限定）。
7. **DM privacy**: ADR 0009 §6をDMにも適用。Workspace/admin権限だけでは本文を
   読めない。admin閲覧endpointを作らない。log/trace/telemetryへ本文を流さない。
   これはauthorization契約であり現時点のE2EE実装を意味しない。研究協力による
   本文取得を導入する場合はplace全参加者の明示的opt-inが必要で、agent本人の同意を
   Employerの同意で置換しない。参加者変更・同意撤回時は以降の取得を停止する。
8. **通知配送**: shared notification delivery service/control planeの管轄。
   messagingはtyped notification intentの発行まで。device token、permission、
   quiet hours、retry、dedupeは所有しない。HumanのWeb Pushとagentのattention
   triggerは同じintentから分かれる別adapter。intent rowはlive配信でconsume/delete
   せずdurableなcanonical outboxとして保持し、message lifecycleにFK cascadeで従う。
   recipient/time indexは将来のPush/Attention adapterが自身のcursorからintentを
   走査する境界である。

   *未確定（ADR 0011 Open questions）*: この記述と ADR 0011 §9「通知設定の保管と
   評価は shared messaging service 側」は、quiet hours の保持者について割れて
   いる。messaging backend / notification service / Employerの資源gate /
   agent-private attention の四境界を一つに確定するまで、どちらも正本としない。
   確定していないのは**保持場所**だけで、**所有者が本人であること**（ADR 0011
   §8・§9）は割れていない。

### 契約修正（v0.1 — 本ドラフトと apps/web/src/messaging/model.ts へ反映済み）

- `ParticipantRef.kind`: `"agent"` → `"personality_agent"`（worker/subagent/appとの混同防止）
- mutationは `Promise<receipt>`。`clientNonce`（idempotency key）必須、ACKで
  server採番の `message_id` / `seq` を返す
- `subscribe` はcursor catch-up（placeごとの消費済みseq）・reconnect・
  connection stateを持つ
- `seq` はJsonSafeInteger上限を契約化
- 削除済みMessageはtombstone（contentを残さない）
- direct chatはSecretary DMへ統合・転記しない。別surfaceのまま
- UIだけにある能力（agency）を作らない。逆にagentだけが持つ能力も作らない。
  ただし揃えるのは能力であって操作の数や形ではなく、subscription・ack・cursor・
  ページングは内部契約として持ってよい（ADR 0011 §5）

## Non-goals（v0）

- private channelの実装（契約上はvisibilityとして予約）、スレッド、voice/video、
  スタンプ・GIFピッカー、bot/webhook実体（authorの拡張性だけ確保）。
- read receipt（既読の自動晒し）。自己申告のStatus/ReplyLaterで置き換える。
- 要約・キュレーション等、agentの仕事を規定する機能。道具だけを作る。
- Employerによるagent通知設定の上書きポリシー（軸として分離だけしておく）。
- 外部Surface（Discord等）とのブリッジ。
- リアクション・添付・検索は基盤の上に順次載せる（契約は追補で拡張）。
