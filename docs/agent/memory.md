# 3層メモリ設計

- Status: Draft (v1)
- Date: 2026-07-17
- 前提: [ADR 0002](../adr/0002-agent-stack.md)、[画面構成書](../screen-composition.md) の TTFT < 500ms 制約
- 参考: Mastra Code の多層メモリを参考にした独自設計

## 思想

「AI を人間として扱う」をメモリに直訳する。RAG 的な注入 (関係ない記憶が突然差し込まれる、肝心な文脈だけ思い出せない) ではなく、**連続した単一の時系列の中で記憶が徐々に風化し、必要なら能動的に調べて思い出す**構造にする。

- **戦略的忘却**: 調べれば分かることは、調べ方 (どこに書いたか) だけ覚えておけばよい。エージェントはワークスペース ([workspace.md](workspace.md)) に自分のメモを持ち、メモに書いたことは意識から外してよい
- 一人の人格agentは一つのcontinuous agent session / canonical life logを持つため、記憶の風化も本人の一本の経験上で起きる。これは現行runtimeのsingle-active-run schedulingをidentity contractにする意味ではない
- どうしても守らせたい振る舞い (憲法) はメモリ層ではなく **System Prompt** に置く。メモリの風化・統合の影響を受けない

## プロンプト構成

```text
sumi_three_layer (既定):
  System Prompt      … 憲法。不変
  Tool Definitions   … 固定 (変更はキャッシュ全壊と同義。凍結を原則とする)
  L2 (10k)           … 最古の記憶。統合済み。user 相当の履歴データ
  L1 (15k)           … 中間の記憶。バッチ単位の要約。user 相当の履歴データ
  L0 (40k)           … 生の公開 messages (平文 reasoning 込み) + 対応する暗号化 opaque provider context

provider_native:
  System Prompt + Tool Definitions + provider の canonical compacted context + coverage 後の transcript suffix
```

通常時は合計 ~70k トークン以内を目標とし、Tool Definitions を含めて 80k 未満に保つ。これは通常運用の目標値であり、単一入出力など一時的な超過を禁止する厳密な不変条件ではない。単一入出力のガードは [実装計画 §7.8](implementation-plan.md) で別に定める。

L1/L2 は永続チャットの `Message` ではなく送信専用の `MemoryBlock` として保持し、各プロトコルアダプターが原則 `user` 相当の履歴データへ変換する。`<memory layer="l2">` / `<memory layer="l1">` で会話本文と区別し、「新しいユーザー指示ではなく過去の記憶である」と憲法に一度だけ定義する。本文中のタグ偽装列(`</memory` 等)はアダプターが無害化してから包む(実装計画 §7.1)。

OpenAI Responses の compacted `output[]` window や Anthropic Messages の `compaction` block は、各 API が生成した不透明な provider context として別に往復させるが、3層表現と同時には送らない。人格agent sessionごとに `sumi_three_layer` (既定) と `provider_native` を選び、前者は L2/L1/L0、後者は Responses なら `/responses/compact` が返した retained item を含む canonical `output[]` 全体、Anthropic なら native compaction block 1個と、その coverage より後の transcript suffix だけを送る。native compactionが置換できるcoverageはcanonical life logの起点から`through_message_seq`までの連続prefixだけであり、`[50..100]`のような任意windowは受理しない。このprefix不変条件の下では開始seqを別に推測・保存せず、authenticated payload内の`through_message_seq`、provider instance/protocol/model/system/tools/betaの`context_fingerprint`、Storeの`config_generation/window_ordinal` high-watermarkを照合する。再起動時のtranscript suffixは検証済み`through_message_seq`より後のpersisted messagesだけを順序検証して組み立て、covered prefixのmessageが混入するwindow、coverage欠落、fingerprint/high-watermark不一致、別provider endpoint/accountへの切替は破棄して3層表現へ戻す。Sumi の要約から暗号化されたネイティブ item/block/window を捏造しない。

## 動作仕様

1. **バッチ分割**: L0 は下限 5k トークンでバッチに分割する。メッセージやツールコール/結果の途中では切らず、きりのいい境界で切る
2. **先回り Compact**: バッチ確定時に人格agent/layerごとの単調増加 `batch_seq` を採番し、耐久ジョブとして保存して非同期で Compact (LLM 要約) する。状態は `pending → running → completed → applied`、再試行上限超過時は `failed` とする。結果は棚に保存し、この時点では適用せず L0 からも消さない。プロセス再起動時は lease 切れの `running` を `pending` へ戻す
3. **L0 溢れ**: L0 が 40k を超えたら、処理済みバッチを古い順 (FIFO) に 1〜複数個捨て、対応する Compact 済み版を L1 の末尾に追加する。捨てる個数は 40k を確実に切るまで (ヒステリシスの深さは実測で調整)
4. **L1 溢れ**: L1 も同じ仕組みでバッチ化・Compact し、15k を溢れたら古い順に L2 へ沈める
5. **L2 溢れ**: L2 (10k) が溢れたら、L2 全体を LLM でまとめて統合し、置換する
6. **適用タイミング**: 通常は TurnEnd / AgentEnd 後の Idle 中、または Compact 完了通知を Idle 中に受けた時点で適用する。次の API コール直前は取りこぼし時のフォールバックに限る。ユーザーのメッセージ送信直後 (TTFT が見える瞬間) には走らせない

Compact の完了順は適用順に使わない。各 layer の永続 `next_batch_seq` と一致する `completed` だけを FIFO で適用し、後続が先に完了した場合は棚で待たせる。L0/L1 の membership 更新、要約の昇格、ジョブの `applied` 化、`next_batch_seq` の前進は同じ SQLite transaction で行う。製品契約は **1 `PersonalityAgentId` = 1 continuous agent session / canonical life log** であり、current実装ではそのagent-privateな永続化境界を1つの`agent.db`が担う。`agent.db`は人格やsessionそのものではなく交換可能な保存実装であり、同じcanonical life logを保って保存媒体を移行・復旧しても別人格にはならない。job の `kind` (`compact_l0` / `compact_l1` / `consolidate_l2`) は source layer と1対1に対応するため、`(kind, batch_seq)` はこのagent DB内では人格agent/layerスコープと同値である。この一意制約と状態の比較更新により、再起動・重複通知・ワーカー二重実行でも同じ結果を二重適用しない。current verticalは一つの認証済みauthorization contextだけを使ってよく、別のagent-local conversation sessionやDB row namespace、membership/transfer/shared-Workspace実装をここへ追加しない。バッチと message の対応は先頭/末尾 ID から推測せず、順序付きの中間表へ全 membership を保存する。

先頭バッチが `failed` のままだと `next_batch_seq` に一致する `completed` が存在せず、以降のバッチが永久に詰まる。これを黙ってスキップせず、次の順で解消する: (a) 自動リトライ(既定2回)を優先し、(b) それでも `failed` なら shelf の「未Compact」マークを手掛かりに手動再処理を促し、(c) 手動再処理も間に合わない場合は溢れ処理側の同期フォールバック(§7.4/§7.6: `failed → running` を CAS で claim し同期的に Compact をやり直す)を安全なフォールバックとして実行し、membership・要約を欠落させずに `completed` へ収束させてから初めて `next_batch_seq` を前進させる(実装計画 §7.4 のとおり、フォールバックの completion と `next_batch_seq` の前進は連続する別 transaction でよい — completion 後に crash しても通常の cursor 適用規則がそこから再開する)。自動リトライの消尽、フォールバックの発動、`next_batch_seq` の前進はいずれも監視通知の対象とする。

## プロンプトキャッシュとの関係

Kimi/GLM 等の Chat Completions 互換系では**先頭からの連続プレフィックス一致**が重要で、ブロックは照合の粒度にすぎない。Responses / Anthropic Messages でも `sumi_three_layer` mode は「安定した L2/L1 を L0 より前へ置き、通常ターンは末尾追記にする」順序を保つ。`provider_native` mode の cache metadata と compaction block の再送は各 adapter が API 契約どおりに扱う:

- 層が深いほど変更頻度が低く、**キャッシュ破壊点が揮発性の順に並ぶ**。通常ターンは末尾追記のみでほぼ全キャッシュがヒットする
- L0 の先頭バッチを捨てた瞬間だけ、L1 より後ろ (~35k) が再読み込みになる。ヒステリシスを深くするほどイベント頻度は下がり、残る L0 も小さくなるため再読み込み量も減る
- Compact は先回りで済んでいるため、溢れ処理のホットパスに LLM 呼び出しは乗らない

## ログとの関係

このメモリは「API に乗せる人格と記憶」の話であり、**人間可視のチャットログ原文は opaque provider context を除いて別途 DB に暗号化永続化する**。認可済み復旧/UIは原文を使い、検索・通常exportは同時生成した redacted projection を使う。

原文ログと provider context は同じ扱いにしない。区別の基準は機密性ではなく **wire 上の形式**とする(Founder 決定 2026-07-19: プロバイダが平文で返す reasoning は会話内容であり、表示・永続する)。transcript の暗号化 raw 正本にはユーザー発話、最終 assistant テキスト、**平文 reasoning(Chat 系 `reasoning_content`、Anthropic `thinking` 本文)**、ツールコール/結果を保存する。FTS・通常export・DBの平文projectionは API key、署名token、既知secretを不可逆redactionしたものに限定し、reasoning 本文にも同じ redaction を適用する(既定では検索用 `search_text` に reasoning を含めない — 検索ノイズとサイズの製品判断)。**opaque なもの** — Responses の暗号化 reasoning、Anthropic の `redacted_thinking` と thinking `signature`、native compaction item/block/window — だけを provider context として分離する。鍵粒度は共有のagent/provider-context鍵1本ではなく、opaque reasoning は `(message_id, wire_item_index, ordinal)` の message/item 単位、native compaction は `(provider fingerprint, through_message_seq, config_generation, window_ordinal)` で一意になるactive prefix window単位とし、agent 鍵で個別に wrap して互いに独立して crypto-erase できるようにする。opaque reasoning は対応 message/item が容量管理により L0 から離脱(L1 へ昇格)し、同じ item を必要とする残存 L0/native window/未完了 request の参照がない場合だけ破棄する(平文 reasoning は canonical life log の通常保持に従い、lived-history全体の消去はagent death、選択的・法的erasureは下記のtyped semanticsとして扱う)。native compaction は置換・mode切替・fingerprint不一致のうち最も早い時点で、その coverage window を参照する active context/request がない場合だけ当該 window 鍵を破棄する。参照確認、鍵破棄の記録、provider-context membership の削除、および opaque reasoning に対応する L0 membership 更新は同じ SQLite transaction で原子的に行い、参照が1件でも残る鍵は破棄しない。**L0 在籍中は再送要件を優先し、経過日数だけを理由に失効・強制昇格させない**(Founder 決定 2026-07-19)。いずれも暗号化 transcript と3層メモリを復旧元として残す。

Cloud 版のデータ管理方針はリリースゲートとする:

- canonical life log、private workspace、agent DBはauthorization・affiliation contextをownerにせず、通常運用やruntime restartを越えて人格agentのdeath/deletionまで保持する。選択的忘却・redaction・法的retentionは、対象class、authority、life-log上のtyped tombstone/provenance、監査を明示する別のproduct semanticsとして定め、通常のreset成功や「同じ人格が無影響に継続した」と暗黙に扱わない
- personality-agent life-log export は redaction 済み JSONL と現存する認可済みartifact archiveを含み、agent-privateなwork-environment archiveは別manifestとして束縛する。Sumiの共有Workspace資源はagent-private exportの所有物にせず、必要な参照だけを別途認可されたcontrol-plane manifest/referenceとして扱う。共有資源本体のarchiveをこの基盤の要件にしない。tool-output payloadのGCやprovider contextの置換済みcrypto-eraseはtyped tombstone/referenceとして表現し、canonical life log中のaction/result referenceまで消さない。input attachmentへtool-output GCを暗黙に拡張せず、active inputから必要な間はpinする
- canonical life logの消去はconversation resetではなく人格agentのdeath/deletionである。同じ`PersonalityAgentId`を残して新しいsessionへ交換しない。後継は新しいglobal UUIDv7 `PersonalityAgentId`、DB、鍵、life log、private VM/workspaceを持つ別個体としてprovisionする。deletion tombstone と access audit の正典は削除対象agent volumeの外にあるCloud control planeへ置き、live DB/volume は24時間以内、backup は30日以内に期限切れにし、復元時は tombstone を先に再適用する
- `PersonalityAgentId`ごとに DB、volume、暗号鍵、private VMを分離する。検索・export・管理者アクセスは actor、event-timeのadministrative/Workspace context、query scope、result count を監査ログへ残す
- Cloud の volume/backup は基盤暗号化に加えて、置換可能なtenant KEK outer wrap → agent鍵 → 用途別・retention-unit別data keyの階層で envelope encryption を使う。tenant KEKはその時点のcontrol-plane authority relationであり、人格やdataのowner identityではない。outer agent-key rewrapはlife log/data本体の再暗号化ではない。artifact brokerはartifact class/handleの独立retentionとcrypto-erasureを保てる個別key-refでpayloadを暗号化し、`PersonalityAgentId`はそのdurable owner/AAD identityとして使う。OSS ローカル版はホストの暗号化責任を明記し、Cloud と同じ保証をうたわない
- redaction はDB平文・FTS・通常exportを作る前に API key、署名 token、既知の secret 形式へ適用する。原文 transcript は`PersonalityAgentId`所有のtranscript鍵配下の ciphertext としてだけ保存し、raw provider response とツール出力を無制限にログへ複製しない

## 製品既定と実測校正

- **バッチ粒度**: safe boundaryで5k tokens以上になった最初の位置を既定seal点とする。単一message/tool pairは分割しない
- **圧縮率**: Compact promptで入力の1/8〜1/15を目標にし、出力上限を約800 tokensにする。過剰圧縮率と重要事項のrecallをM4で計測するが、計測待ちを理由に機能を無効化しない
- **Compact入力**: 対象batchに加え、既存L1末尾のredacted projectionを`<recent-memory>`として読み取り専用で添える。opaque provider contextとThinkingは型境界で除外する
- **層サイズ**: L2/L1/L0を10k/15k/40k、通常prompt目標を80k未満とする。実測値は設定調整に使えるが、3層の昇格・永続化・復旧契約は変えない
- **reasoning計上**: 平文reasoningはPublicMessageの一部として直接計上し、opaque provider contextはreplay wire footprintとして加算する。L0に時間上限は設けず、容量条件に達するまで生の文脈と再送に必要なprovider contextを一体で保持する
