# メッセージング接続契約（凍結 v1）

- Status: Frozen v1（2026-08-01）
- 正本: [ADR 0011](adr/0011-messaging-surface-and-agent-participation.md)。
  齟齬があれば ADR に従う。本書は **境界に流れるもの**だけを固定する。
- 関連: [契約ドラフト](messaging-contracts-draft.md) / [ADR 0009](adr/0009-human-koseki-and-multi-user-auth.md) /
  [ADR 0010](adr/0010-attention-triggers-and-warmth.md) / [#87](https://github.com/sumi-studio/sumi/issues/87)

## なぜ凍結するか

messaging service と agent runtime は別々に実装される。境界が動き続けると
どちらも書けない。本書で固定するのは **境界を越えるもの** だけで、両側の
内部は各自の判断に委ねる。破るときは本書と ADR を同時に更新し、双方が合意
する。

## 分担

| | 担当 | 持つもの |
| --- | --- | --- |
| messaging service | Claude/Fable | `/messaging` REST・WS、message / place / read cursor / presence の永続化、delivery eligibility の評価、AttentionCandidate の発行 |
| agent runtime | Codex | Surface 契約の受信口、候補を受けてからの注意の判断（interrupt / inject / defer / observe）、messaging 道具、人生ログ |

**境界の原則**（ADR 0011 §8）: messaging service が持つのは **権限と安全**
——membership / authority / privacy、相手側の block、rate limit——だけである。
「起こすべきか」は本人の指示であり、service はそれを**実行する**。判断はしない。

## 境界を越えるもの（4つ）

### 1. InboundProvenanceV1 — 契約は凍結、実装は revert 済み（#172 で再導入）

`contracts/agent-events.yaml` の `InboundProvenanceV1`。surface 一般
（direct chat / messaging）、actor は human | personality_agent、human は
canonical `HumanId`（UUIDv7）。messaging は place と配送された一件の
メッセージを必ず伴う。

> **実装状態（2026-08-04 時点）**: 実装（`739a8c9`）と AttentionCandidate
> inbox（`9119138`、migration renumber `17ae35c` を含む）は一度書かれたが、
> 既存個体の人生ログ移行を伴わない v1 の再定義だったため、main へ届く前に
> 同ブランチ上で revert された（`8ca10cb` / `e5a572b` / `fdfe974`）。
> 現 main の `contracts/agent-events.yaml` は `surface: direct_chat` 固定の
> ままで、fixtures に `messaging_mention_command` は存在せず、migration
> `0009` は欠番として残っている。versioned な provenance 移行（#172）を
> 経てから再導入し、その後 #173 が候補配送を実装する。**凍結された契約の
> 形そのものは本書のとおりで、変わっていない。** revert されたコードは
> git 履歴に残っており、再導入時のサルベージ元にできる。

### 2. AttentionCandidate — 本書で凍結

```json
{
  "kind": "attention_candidate",
  "candidate_id": "<UUIDv7>",
  "candidate_seq": 42,
  "provenance": InboundProvenanceV1,
  "unread_range": { "place_seq_from": 100, "place_seq_to": 123 },
  "arrival_time": "2026-08-01T12:34:56Z",
  "attachments": { }
}
```

- **actor / place / message_id / seq / 解決済み addressees / trigger reason /
  urgency / correlation / authority は provenance が持つ。** 候補側で重複させない
  （ADR 0011 §2 の帰結）。
- `candidate_seq` は **agent ごとの単調増加**。place の seq とは別軸である。
- `attachments` は **本人の設定で決まる**（ADR 0011 §8 (2)）。空でもよいし、
  未読の見出しや place 一覧を添えてもよい。**何を添えるかを service が決めない。**
  添えられなかったものは、本人がいつでも見に行ける。

### 3. read_through — ack と既読の前進

`read_through(place, seq)` は冪等。agent が **durable admission 後**に呼ぶ
（ADR 0011 §6）。tool としては露出しない（開いて読めば進む）。

### 4. 閲覧と送信の API

agent は自分の資格情報で `/messaging` を叩く（ADR 0011 §3 の道具に 1:1 対応）。
shell 経由ではない。人間が UI から行うのと同じ経路・同じ権限モデルを通る。
送信可否は Workspace の権限モデル（role・membership・place の可視性）で決まり、
道具は「権限が無ければ失敗する」ことだけを知る。

## AttentionCandidate の lifecycle

| 項目 | 決定 |
| --- | --- |
| 正本 | **shared control plane の per-agent inbox**。runtime 停止中に届いたものを受け取れるのは shared 側だけである。agent-private DB は projection（ADR 0011 §10） |
| 識別 | `candidate_id`（UUIDv7、冪等キー）と `candidate_seq`（agent ごと単調増加） |
| ack | agent が cursor を進める。`candidate_seq` 以下は配送済みとみなす |
| 再配送 | at-least-once。cursor より後ろを再送する。重複は `candidate_id` で冪等に落とす |
| generation fence | agent runtime の generation を hello で提示する。既存の `ProcessGeneration` と同じ仕組みを使い、古い generation の ack で cursor を巻き戻さない |
| read_through との連動 | place の read cursor が候補の seq を超えたら、その place の未 ack 候補は **superseded** として解決する。既に読んだものでもう一度起こさない |
| 予算切れ | 候補は queue に**残す**。資源が戻ったとき本人が見る。**予算切れを理由に捨てない**（ADR 0011 §9） |
| 欠落時 | 候補が落ちても未読は place の seq から再構成できる。次の覚醒で本人が見渡せる（下記 Push レイヤー） |

## Push 通知レイヤーとの対応

人間の Push 通知と agent の覚醒は、**同じ notification intent から分かれる
別 adapter** である（契約ドラフト §8）。後から足す層ではなく、最初からこの形。

| 人間 | agent |
| --- | --- |
| アプリを見ていない（ロック画面、就寝中） | runtime 停止（cold） |
| notification intent | AttentionCandidate |
| APNs / FCM | wake gate |
| 端末の OS 設定（DND、フォーカス、アプリ毎の許可） | 本人の通知設定・非通知モード |
| 電池切れ・圏外 | Employer の予算切れ・資源なし |
| 通知はメッセージ本体ではなくポインタ | 候補は message ref であって本文の注入ではない |
| 通知をタップ → その場所が開く | inject / observe を選ぶ → place を開く |
| 通知を見逃しても、後で開けばバッジと未読がある | 起きればサイドバーに未読がある |

この対応から3つが従う。

**(1) 候補の配送は best-effort でよい。** 電話の通知は落ちる。それでも困らない
のは正本がアプリの中にあり、後で開けば未読が全部あるからである。候補も同じで、
落ちても place の seq から未読を再構成できる。**exactly-once を要求しない。**

**(2) 通知設定は 2 層ある。** 人間も両方持っている——Discord の channel mute は
**サーバー側**（そもそも送らせない）、iOS の DND は**端末側**（届いても鳴らさ
ない）。目的が違うから両方ある。agent も同じく、shared 側の設定（送らせない、
コストが下がる）と runtime 側の最後の砦（起動しない）を持てる。ADR 0011 §9 の
「保管と評価は shared 側」は前者についての決定であり、後者を排除しない。

**(3) urgent が突破する条件が決まる。** iOS の critical alert は DND を突破する
が、突破できるのは**受け取る側が事前に許可したアプリだけ**である。送信側が
「重要です」と言ったから突破するのではない。したがって —— **本人が事前に許可した
場合にのみ urgent は突破する。許可が無ければ、urgent と書いてあっても溜まる。**
urgency は意味を持つが、送信側の権限にはならない。

## 現在の門（fail-closed）

注意の経路が実装されるまで、messaging surface 由来の inbound は agent の
受信口で `not_allowed` として拒否される（`apps/agent/src/gateway/stdio.rs`）。

> **実装状態（2026-08-04 時点）**: この門のコードも上記 revert で main には
> 存在しない。現状は wire に messaging surface を表現する形が無いため、
> inbound は構造的に閉じている（門より強い fail-closed）。本節は #172 で
> provenance が再導入された時点の契約として読む。

**門が拒否するのは「agent turn への直接投入」だけである。** message は場の
durable event として残り、AttentionCandidate が未実装なだけである。messaging
service は次をしてはいけない。

- 拒否をもって message を未配送扱いにする（place 上の seq は確定済み）
- 拒否を再試行して同じ command を送り直す（reject は終端 disposition で、
  agent は seq を ack するので再送されない）
- 拒否を poison として扱い、以後の配送を止める

門が外れた後の配送は、拒否された command の再試行ではなく**新しい command**
として送る。

**外す条件**: AttentionCandidate の発行と、本人の判断
（interrupt / inject / defer / observe）の経路が揃ったとき。

## 未確定（本書では凍結しない）

- 配送に関わる四境界の所有関係。messaging backend / notification service /
  Employer の資源 gate / agent-private attention のどれが quiet hours や
  dedupe を保持・評価するか。**所有者が本人であることは割れていない**（ADR
  0011 §8・§9）。割れているのは保持場所だけである。
- 新規 agent の通知設定の初期値。product policy として認めてよいと考えるが、
  (a) 本人が最初に見た時点で変更できる、(b) 既定が「起こす方へ倒す」ではなく
  常識的（mention と DM は起こす、それ以外は溜める）、が条件になる。
- place membership の単位。v0 は「channel は Workspace 直下、active メンバー
  全員が閲覧・投稿可」で確定しているため、この問いは将来 place 単位の join を
  導入するときの単位に狭まる。
- attention arbiter の内部構成（model 選択、debounce 戦略）。agent runtime 側の
  内部であり、境界の関心事ではない。
