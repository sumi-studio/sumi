# エージェント基盤 実装タスク分解 (Codex用)

- 正典: [docs/agent/implementation-plan.md](../../docs/agent/implementation-plan.md) (以下「計画書」。PR #6 マージ済み最終版。§n は章番号、#n は第12章の移植リスト番号、Mn ゲートは §13)
- pi 参照ソース: `/home/yohaku/pi-reference/packages/` (計画書の `pi:` パスはここを指す)
- 作業ディレクトリ: `apps/agent`(ブランチは各PRで明示する)
- このファイルの各タスクをそのまま Codex に貼る。**冒頭に必ず「共通ルール」も一緒に貼ること**
- 縮退方針は「縮退しない」(計画書§14.3)。詰まったらスコープを黙って削らず停止して報告する

---

## 共通ルール (全タスクの前提。毎回プロンプト先頭に貼る)

```text
リポジトリ /home/yohaku/sumi、対象 apps/agent (Rust, edition 2024)。作業開始前に対象ブランチを確認すること。
設計の正典は docs/agent/implementation-plan.md (PR #6 マージ済み版)。担当範囲の章と、§12 の該当移植項目、§13 の該当マイルストーンゲートを必ず読むこと。
pi のソースは /home/yohaku/pi-reference/packages/ にある。移植項目は該当ファイルを実読してから書く(計画書の表は索引であってコードの代替ではない)。
⚠ 計画書の pi 行番号にはズレがある(§12冒頭の注意書き参照)。正典は「ファイルパス+関数/挙動の記述」。行番号が合わなくても挙動記述を頼りに該当箇所を自分で特定すること。
規律:
- 依存クレートは Cargo.toml にあるものだけ使う(§2.1 の全依存は導入済み)。新規追加禁止。唯一の例外は M5 の tokio-tungstenite(T24 に明記)
- タスクで指定されたファイル以外は変更しない(mod 宣言の追記のみ例外)
- cargo build && cargo clippy --all-targets -- -D warnings && cargo fmt && cargo test を全部通してから終了
- コメントは「コードから読めない制約」のみ。pi の参照は移植元として価値があるので `// pi: ai/src/api/openai-completions.ts:362-366` 形式で残す
- panic 禁止方針: プロバイダ層は「stream は決して panic/Err しない」契約(§3.2)。unwrap はテスト内のみ
- inbound_commands の run_phase / status / run owner に触れる場合は、先に計画書付録Cの正典表と整合を確認する(付録B-5)
- 迷ったら付録Bの3点自己レビュー: 正常形クローズ / キャッシュプレフィックス / ホットパス同期I/O
- 完了したら指定のコミットメッセージで git commit (このタスクの変更ファイルのみ add)
```

---

## 進捗と全体地図

**完了済み (CP1、本ブランチにコミット済み)**:

- T1: M0 足場 — Cargo.toml 全依存 / モジュール骨格 / config / stdio ゲートウェイ(CommandEnvelope/OutboundFrame/ACK、attachmentsは必須の空配列、1MiB payload拒否、外形不正と4MiB transport超過はepoch終了)/ echo
- T2: `provider/types.rs` — §3 の provider-facing subset(PublicMessage、ProviderContextItem、ValidatedToolArguments、ProviderEventStream の fuse/EOF 合成契約)。ModelSpec は T6、Tool 境界は T13、AgentEvent/PublicStreamEvent は T15
- T3: `provider/partial_json.rs` — pi json-parse.ts 忠実移植(UI preview 専用、確定値への変換 API なし)
- T4: `provider/sse.rs` — protocol-neutral SSE framing(**T8 で計画書どおり `transport.rs` へ改名する**)

**マイルストーン順序 (§13 が正典)**:

```text
M1  (T5〜T8):   共通 provider core + Chat Completions adapter
M1P (T9〜T10):  Responses + Anthropic adapters (M1後、M2〜M5と並行可。release必須)
M2  (T11〜T16): durability foundation → ループ+ツール+ステア
M3  (T17〜T18): 永続化拡張 + contracts 凍結
M4  (T19〜T21): 3層メモリ
M5  (T22〜T24): 権限承認 + WS ゲートウェイ
Cloud release (T25〜T29): M1P・M0〜M5 と依存が満たされた範囲で並行、全項目必須 (§13 末尾)
```

依存の要点:

- T5 と T7 は T2 完了済みなので**並列可**。T6 は T5 の公開境界を確定してから受信adapterを結線し、T8 が合流点(M1 ゲート)
- **M2 は「durability foundation (T11→T12) が先」**。hard steer / abort / 承認待ち / ツール実行開始は foundation の commit を通るまで有効化しない(§13 M2)。T13(tools 純関数部)と T14(transform)は T12 と並列可
- **M2 の steer/abort/tool 副作用と M3 の Store 作業は並行不可**。T12 で EventWriter の transaction 契約を凍結してから積み増す
- T9/T10 (M1P) は T8 後いつでも。ただし暗号化 provider context の durable round-trip ゲートは T17 (M3) 完了後に結合
- contracts 凍結 (T18) は M3 完了時点で先出しし、api/web 担当へ渡す
- Codex 2セッション並列なら: レーンA = T5→T6→T8→T11→T12→T15→T16、レーンB = T7→T13→T14→(T9/T10)。T6 は T5 の公開API確定を待ち、Session/steer まわり(T15〜T16)は1人で書くこと

---

## M1: 共通 provider core + Chat Completions

### T5: provider/assembler.rs (正規化event→メッセージ組立) 【T7と並列可】

- 読む: §3.2、§4.3 全体、#2、#13、pi: `ai/src/api/openai-completions.ts:229-511` **必ず実読**
- 作る: `src/provider/assembler.rs`
- やること: `MessageAssembler` 構造体を **正規化済み `ProviderEvent` 列→`AssistantMessage`** の protocol-neutral な純関数として実装し、プロバイダ層とループで共有する。content_index ごとの text/thinking/tool block、`ThinkingStart.signature_field`、flatten済みwire slot→`wire_item_index`のchecked変換、terminal後のscratch破棄、途中状態のcloneを同乗させない契約を持つ。加えて adapter 共通の `ToolArgumentAccumulator` を置き、deltaごとにraw bufferへ追記してコピーだけを `parse_streaming` へ渡し、終端では蓄積raw全体をstrict parse+top-level object+凍結schema(`jsonschema` 0.48、制約込み)で検証する。成功時だけ`ToolCallEnd`、失敗時はraw引数を捨て`ToolCallRejected { rejected, synthetic_result }`を返し、承認・実行へ進めない。Length停止は`IncompleteResponse`で一括拒否する。**raw provider chunk、usage配置、reasoning field、finish_reasonの解釈は受け取らない**(T6/T9/T10のadapter責務)
- 累積`raw_args`はtool callごとに4MiBで制限し、超過時はrawを即時破棄して`TooLarge`の拒否対で閉じる。加えてrequestのoutput token予算から§4.3の`ResponseBudget`をchecked導出し、assemblerが正規化eventから永続contentへ入る累積byteを独立に制限する。`ToolCallEnd`では永続id/name/final argumentsを必ず一度だけ数え、Rejectedではid/nameだけを数えて破棄rawを除外する。SSE event/tool単体の上限を複数deltaで迂回させない
- 受け入れ: 「正規化ProviderEvent列→途中content+最終AssistantMessage」のテーブルテスト(text/thinking/toolcall/terminal/error scratch)。ToolArgumentAccumulatorはrepairならpreview可能だがstrictでは落ちる入力、non-object、schema違反、正常objectを固定。response content budgetの一致/1byte超過、Kimi K3最大output予算の表現可能性、checked overflow拒否も固定。clippy 緑
- コミット: `agent: provider event組立とtool引数strict検証`

### T6: adapters/chat_completions.rs + config プリセット 【要T5公開境界。T7と並列可】

- 読む: §4.1〜§4.3 全体、#2〜#12、#30、pi: `ai/src/api/openai-completions.ts:229-511,575-1150`、`moonshotai.models.ts`、`zai.models.ts:79-98`
- ⚠ **旧タスク文の compat 記述は誤りだった。正典は §4.1 プリセット**: **Kimi K3 = `max_completion_tokens` + `thinking_format=OpenAIEffort`**(top-level `reasoning_effort`。K2.x の `thinking:{"type":"enabled"}` は送らない。`temperature`/`top_p`/`seed` も送らない)、`supports_strict_mode=true`(省略時 true なので、pinned Moonshot walleに照らして MFJS 意味論の保持を保守的に証明できないschemaにだけ明示 `strict:false`。walleが`encoding/json`でnumberを`float64`へdecodeするため、enum/minimum/maximumはbinary64へのexact変換も必須)、`requires_reasoning_content_on_assistant=true`。**GLM 直API = `max_tokens`** + `thinking_format=Zai` + `zai_tool_stream=true`、base_url は直APIの `https://api.z.ai/api/paas/v4`(pi のコーディングプラン用値を流用しない)
- 作る: `src/provider/adapters/chat_completions.rs`(+ `adapters/mod.rs`)、`src/config.rs` への ModelSpec/ChatCompat プリセット追加(kimi-k3 / glm-5.2 / umans / **opencode-zen-go**。base_url/model は環境変数で上書き可、TOML 例をコメントで残す)。`provider_instance_id` の生成規則(§4.1)も実装
- OpenCode Zen (Go): base_url `https://opencode.ai/zen/go/v1`、`Authorization: Bearer $OPENCODE_GO_API_KEY`。**検証用の当面の既定**(Founder 決定 2026-07-17)。実体はゲートウェイなので方言・Compat は直結先の値を流用せず **M1 ライブ fixture で個別に固定**する。2026-07-20のproduction body A/Bでは文字列`tool_choice:"required"`だけがHTTP 400 `invalid_request_error`、同一tool requestの省略時は2ターンtool/reasoningが成功したため、OpenCode presetはこの値だけを送信前拒否する
- やること(送信): PromptContext→Chat Completions JSON。§4.2 の12項目を全部(assistant はプレーン文字列。複数Text blockはwire順のまま`"\n\n"`で境界を保ってChat送信viewへ射影、永続`ProviderOrigin(provider_instance_id/protocol/model)`完全一致時だけthinkingのsignature_fieldを書き戻し、不一致時は送信viewから**常に除外**、`reasoning_content:""` 補完、ツール画像の user 追送、空 assistant スキップ、履歴にツールコールがあれば `"tools": []`、stream_options)。L2/L1 は `<memory layer="...">` の user 相当メッセージ+タグ偽装列の無害化(§7.1)。`max_output_tokens`(物理上限)と`default_output_tokens`(通常16k)を分離し、overrideは範囲検証する
- やること(受信): Chat SSE payloadをraw chunk型へdecodeし、`choices[].delta`をT5のprotocol-neutral境界へ正規化する`ChatReceiveState`を実装する。ここでtool_callsのindex/id二重引き(#2)、Moonshot `choices[0].usage` fallback(#3)、reasoning 3フィールドの最初の非空採用+`signature_field`(#4)、usage変換(#5)、finish_reason→StopReason+machine-readable `provider_code`(#6)、finish_reason無し終端エラー(#7)を扱う。pre-launchではlegacy `delta.function_call`をmodern tool callへ合成せず明示拒否する。tool argsはT5のAccumulatorを使い、strict成功/拒否だけをProviderEventへ出す。§4.3 budgetに従い新規deltaだけのbounded overlayでcontent/event/tool slot/partial-JSON preview work/response ID/modelをpreflightし、全成功後にsemantic stateを一括commitする。finishもevent枠reserve後にdrainし、失敗時はsemantic state/counter不変とする。usageはsidebandとして分離し、同じchunkのprovider error、abort、finish検証失敗でも最後の値をterminalへ保持する
- 受け入れ: 送信変換の**完全request body**スナップショット(通常/ツール往復/thinking再送/クロスモデル切替でthinking marker が body に現れない/interrupted部分応答/画像/OpenCode実captureに使ったrequest)に加え、**raw Chat chunk列→全正規化ProviderEvent列+最終メッセージ**のfixture(text/thinking/toolcall/usage/固有finish_reason/finish無し/strict拒否)。budgetの各counter一致/1超過、MFJS numberの`2^53`/`2^53+1`/exact・inexact decimal境界、error/finish検証失敗のusage保持を固定。モデルがTOML/envのランタイム設定で切替可能(再コンパイル不要)であることを確認。clippy 緑
- コミット: `agent: Chat Completions送受信adapterとモデルCompatプリセット`

### T7: provider/retry.rs + provider/overflow.rs 【T5/T6と並列可】

- 読む: §4.4〜§4.5、#14〜#16、pi: `ai/src/utils/retry.ts` 全文、`ai/src/utils/overflow.ts` 全文(165行)、`coding-agent/src/core/agent-session.ts` のリトライポリシー部(grep で特定)
- 作る: `src/provider/retry.rs`、`src/provider/overflow.rs`
- やること: 正規表現パターン集の移植(non-retryable 先判定→retryable、pi コメントの issue 番号も残す)。**リトライは最大3回=計4 attempt**(§4.4 の用語統一に注意。2s/4s/8s を使い切る)。バックオフは「待機時間を返す純関数+Cancellation 対応 sleep ヘルパ」で実施はループ側(T15)とし、cancelをbiased先頭に置いて同時readyでも追加retryしない。HTTP/SSEのnumeric・`http_*` machine codeは本文なしでも直接分類する。overflow は **provider_code/finish_reason の直接分類を正規表現より先に**(z.ai `model_context_window_exceeded`/`network_error`/`sensitive`)、Kimi/汎用パターン、`input+cache_read+cache_write`のusage判定(#16)、非溢れ除外(rate limit)先行。`ImmediateRecovery`(provider error、Length+output=0+99%以上)と`DeferredApply`(Stop成功+window超過)を区別して T15/T21 で使う分類を返す
- 受け入れ: パターン判定のテーブルテスト(pi のパターンから30ケース以上+provider_code 直判定)。clippy 緑
- コミット: `agent: リトライ/コンテキスト溢れ判定 (piパターン集移植)`

### T8: provider 統合 + transport 改名 + フィクスチャ再生テスト 【要T5,T6,T7】

- 読む: §3.2(stream 契約全文、特に fuse と EOF 合成の分類)、§4.3 末尾(transport 仕様)、#17、§13 M1 ゲート
- 作る: `src/provider/mod.rs`(`stream(spec, ctx, opts, cancel) -> ProviderEventStream`)、**`sse.rs` → `transport.rs` へ改名**(計画書のモジュール名に一致させる。中身は T4 実装を維持し、非2xx ボディ4000字切詰め `"{status}: {body}"`、connect 30s・response header待ち120s・headers後アイドル120s、各待機のCancellationToken優先、SSE framingを含むraw response byteのexact budgetを確認)、`tests/provider_fixtures.rs`、`tests/fixtures/*`(Kimi text / Kimi toolcall / Kimi reasoning / GLM tool_stream / GLM固有finish reason / 429 JSONエラー / 途中切断 / OpenCode live capture)。`compact_native`の型と口はResponses実装のT9で確定する
- やること: transport→adapter→assembler→イベント送出を繋ぎ、**どんな失敗も Error イベントで返す**(panic/Err 禁止契約)。`Start`はlane状態や即時失敗/cancelに関係なくstreamが必ず最初に返す。正常event用bounded laneと異常終端専用capacity-1 priority laneを分け、`Done`だけは正常laneの順序を維持する。cancel観測後は通常backlogをpollせず、partial blockをローカルで閉じ、既受信usageとproducer authoritative content snapshotを持つ`Aborted`をpriority受信して両laneをfuseする。consumer assemblerは既受信completed/scratch prefixとの非矛盾とreason/model/origin/budgetを検査してsnapshotへ収束し、`Done`は全ordered event完全一致を維持する。EOF 合成の分類(cancel 発火済みのみ Aborted、それ以外は retryable Error `"provider stream ended without a terminal event"`)と terminal 後 fuse は T2 実装を結合テストで検証。fixtureはsource kind/provenanceを明示してaxumモックサーバで再生し、**全イベント列と最終メッセージ**をアサート。T8では`request_sent→最初のText/Thinking delta` spanと上位span接続口を作り、`command受信→request_sent`接続・stdio表示・p95判定は実AgentLoopを持つT15で完成する
- 受け入れ: M1 ゲート1(strict/preview 分離、reasoning 分離、usage、z.ai 固有 finish_reason 込み)を cargo test で。OpenCode Zen Goはcurl raw captureをsanitization前SHA-256とmetadata付きで固定し、`[DONE]`後cost trailerも保持する。Moonshot/Z.ai/Umans directのraw captureとM1ゲート2(ライブ3プロバイダ、Kimi reasoning_content再送400なし)はcredential不在をskip完了とせず、**T25 provider releaseのrelease-blocking未完了条件**として引き継ぐ。adapter正規化だけのp95 smokeを置くが、command受信からrequest送出までを含む内部オーバーヘッド p95<30ms の正式判定は T15。通常channel飽和中でもabort 1s以内、partial+usage保持、terminal後fuse(M1-4)
- コミット: `agent: プロバイダ層統合とフィクスチャ再生テスト (M1)`

---

## M1P: Responses + Anthropic adapters 【T8後。M2〜M5と並行可。release必須】

### T9: adapters/responses.rs (OpenAI Responses)

- 読む: §4.2.1、§3.2(ProviderContextFragment / compact_native)、§13 M1P の OpenAI Responses ゲート
- 作る: `src/provider/adapters/responses.rs`、`src/provider/mod.rs`の`compact_native() -> NativeCompactionResult`型と入口
- やること: instructions=憲法、`sumi_three_layer` の input item 変換(L2/L1 user-role memory item、L0→message/function call/output item)、typed streaming event の正規化(`response.output_text.delta`→TextDelta 等)。**未知 event はログして無視、既知 item の未知 variant は fail-closed の Error**。reasoning summary は `ReasoningSummary*` へ、encrypted reasoning は `ProviderContextFragment` として terminal 収集(公開 transcript へ入れない)。`/v1/responses/compact` は `compact_native() -> NativeCompactionResult`: **ordered `output[]` 全体**+coverage を返す(compaction item だけに prune しない)。`store` 既定 false
- 受け入れ: M1P Responses ゲート1(公式 SSE fixture 正規化)。ゲート2・3(暗号化 durable round-trip、ライブ)は M3 (T17) 後に結合する旨をテストコメントに残す
- コミット: `agent: OpenAI Responses adapter`

### T10: adapters/anthropic.rs (Anthropic Messages)

- 読む: §4.2.2、§6.3 手順2(thinking の cancel 時規則)、§13 M1P の Anthropic ゲート
- 作る: `src/provider/adapters/anthropic.rs`
- やること: top-level `system`、user/assistant 交互 turn+隣接 user 結合、tool_use/tool_result、`message_start → content_block_* → message_delta → message_stop` の正規順検証、`input_json_delta`→partial parser、ping 無視、stream 内 error→Error。thinking 本文は `PublicAssistantContent::Thinking` として公開、`signature`/`redacted_thinking.data` だけを `EncryptedReasoning` fragment 収集(`signature_delta` は block 確定まで保持、欠落・改変・並べ替え fail-closed)。tool-use 継続では transcript thinking と保存済み signature を `wire_item_index` で合流し、直近 assistant turn の全 thinking block を値・順序とも不変で戻す。thinking 有効 turn では mode 途中変更禁止、`tool_choice` は auto/none のみ(強制 any/named は組立時拒否)。native compaction block+coverage
- 受け入れ: M1P Anthropic ゲート1・2(fixture)、ゲート4の組立時拒否部分。ゲート3・4の durable round-trip は M3 後結合。usage の `cache_creation_input_tokens` 経路(ゲート5)
- コミット: `agent: Anthropic Messages adapter`

---

## M2: durability foundation → ループ+ツール+ステア

### T11: store/ 最小 migration + 鍵/Redactor 基盤 【M2の最初。T13/T14と並列可】

- 読む: §10 冒頭(鍵階層・AAD 契約・versioned envelope)、§10.1 のうち `agent_scope` / `data_keys` / `messages` / `agent_events` / `inbound_commands` / `tool_executions` / `approval_log`、§13 M2 の「M2で data_keys を含める理由」
- 作る: `src/store/mod.rs`、`migrations/0001_init.sql`(上記7表+CHECK 制約+`one_live_run_owner` 部分 UNIQUE INDEX)、`src/store/crypto.rs`(仮名。KeyProvider 境界+AEAD)、`src/store/redactor.rs`
- やること: KeyProvider trait(Cloud=KMS、ローカルは環境変数テスト鍵。§10「鍵の供給」)、conversation データ鍵の AEAD wrap/unwrap(`data_keys` 表が wrap の正典、crypto-erase=`state='destroyed'`+wrapped_key 破棄)、暗号文の versioned envelope 形式 `version(1B)||nonce(24B)||ct+tag`(XChaCha20-Poly1305、OsRng)、**AAD は行の実位置から再構成**(tenant/agent/conversation/table/行id/purpose/schema version)。Redactor 基盤: 原文暗号化正本+redacted projection+`redaction_version` を**同時生成**する versioned 純関数(全 secret パターン網羅は M3、ここでは基盤+固定 fixture)
- 受け入れ: M2 ゲート9(`data_keys` の CHECK 違反 fixture 全拒否)、行スワップ(2行間の ciphertext/key_ref 入替)で復号が必ず拒否される fixture、M3 ゲート9の `inbound_commands`/`tool_executions` CHECK fixture の先行分
- コミット: `agent: store最小スキーマと鍵/Redactor基盤 (M2 foundation 1/2)`

### T12: EventWriter + command 受信/ACK + suffix 復旧骨格 【要T11】

- 読む: §10.2 **全体**(EventBatch/Projection/run owner)、§11.1.1 手順1〜5、§5.2 の bounded control window、付録C **全体**
- 作る: `src/store/event_writer.rs`(仮名)、`src/gateway/mod.rs` への `InboundCommand`/`CommandRejectReason` 拡張(T1 実装の置換)、`src/store/sizer.rs`(EventBatchSizer)
- やること: `EventBatch`/`EventWrite`/`Projection` 群と**1 SQLite transaction 適用**(seq は batch 内連番、途中 crash は全件なし)。`CommandReceived`(payload 暗号化+keyed HMAC、再送は seq/HMAC/payload 一致検証)、`CommandRejected`(外形正当な検証不能 command の terminal 拒否。oversized は本文非保存+digest 必須)、`CommandClassified`/`RunPhase`/`CommandApplied`/`CommandSuperseded`、`MessageEnd` 投影(messages INSERT と同一 transaction、`append_to_l0` フラグ)。ACK 規則(C.1): received commit 後=Received、finished/no-op commit 後=Applied、superseded/rejected は保存値から再構築。run owner 不変条件の事前/事後検査(C.3、owner 0件/2件拒否)。EventBatchSizer と `STEER_GROUP_MAX_COMMANDS=16`/`STEER_GROUP_MAX_BYTES=1MiB`/`EVENT_BATCH_MAX_BYTES=32MiB` 定数。起動時 suffix 復旧の骨格(§10.2 の分岐リスト。全 phase 完成は M3)
- 受け入れ: 各 transaction 境界の failpoint kill/restart で「全件なし/全件あり」に倒れること、command 再送の冪等性(同一 ACK 再送・本文差替えは HMAC 不一致で protocol violation)、oversized の Rejected 永続化と ACK 再構築。M3 ゲート9 fixture の該当分
- コミット: `agent: EventWriterとcommand配送保証 (M2 foundation 2/2)`

### T13: tools/ (切詰め・bash・fs・executor境界) 【T12と並列可】

- 読む: §8 **全体**、§3.4(Tool trait)、#21、#25〜#26、pi: `agent/src/harness/utils/truncate.ts` **全文**、`shell-output.ts` **全文**(テストも)
- 作る: `src/tools/mod.rs`(Tool trait/ToolRegistry/TypedTool/on_update ガード #21)、`truncate.rs`、`shell_capture.rs`、`fs.rs`、`bash.rs`、`executor.rs`(RPC 境界の骨格)
- やること: truncate は 2000行/50KB 二重上限・head/tail・部分行禁止・メタ注記をテストごと移植。shell_capture はローリングバッファ100KB(**バイト基準の仕様移植**)+50KB 超で「**保持済み prefix を一度だけ flush→逐次 append**」の全文退避+バイナリサニタイズ+**出力 quota 10MiB で execution 全体停止**(`ResourceLimit(OutputBytes)`)。fs は **workspace dirfd 起点の `openat2(RESOLVE_BENEATH|RESOLVE_NO_MAGICLINKS)` 相当**(canonicalize 後再 open の TOCTOU 禁止)、edit の old_string 一意性、grep 行長500字。bash は `bash -c`/`/workspace`/`env_clear`+最小許可リスト、タイムアウト120s、on_update ストリーミング。**中断は §8.3 の5段仕様**: Sumi product workspace/Cloud は Linux で execution cgroup/sandbox 一括停止が正、**開発用 Linux low-trust local は `process_group(0)`+`kill(-pgid,SIGKILL)` fallback**、OSS ローカル fallback は macOS 等の非 Linux Unix host で `child.kill()` fallback(low-trust であることを起動ログに残す)。native 非 Unix host は明示的に非対応として fail-closed にし、WSL/Linux を利用する(ADR 0004)。executor/artifact broker は同一バイナリの `--tool-executor`/`--artifact-broker` モードの骨格+JSON Lines RPC(generation/nonce 付き)まで。`read_file`/`grep` だけ `artifact://` handle を resolver で broker RPC へ route(他ツールは拒否)。フル sandbox(別UID/`network_mode=none` 等)は Cloud acceptance track 側
- 受け入れ: truncate/shell_capture は pi テスト移植で全緑+UTF-8 char boundary+多バイト文字の全文退避。bash は sleep/大量出力/中断の3テスト。M2 ゲート8 の mock mount/RPC テスト(0600/0700、broker の fchmod、symlink 拒否)
- コミット: `agent: ツール群とexecutor境界 (pi truncate/shell-output 移植)`

### T14: memory/transform.rs (履歴再送正規化) 【T12と並列可】

- 読む: §5.3、#11〜#12、#24、pi: `ai/src/api/transform-messages.ts` **全文**(全223行。計画書の旧行番号は誤り)
- 作る: `src/memory/transform.rs`(mod 宣言は memory/mod.rs に最小追記)
- やること: 純関数 `transform(&[ContextMessage], destination: &ProviderOrigin) -> Vec<ContextMessage>`。`destination` は選択済み `ModelSpec::origin()` から呼出し直前に導出し、保存済みmessageのoriginやcache値で代用しない。anchor identity を壊さない(§3.4 の Persisted/Synthetic)。孤児ツールコールへの合成結果挿入(user 分断位置・末尾未解決も)、Error/Aborted スキップ(**interrupted=true は保持**。未実行 ToolCall は §6.3 手順2が保存時に破棄済み)、**生成元とdestinationの`provider_instance_id/protocol/model`が完全一致するassistant flowではthinkingとtool call/result IDをbyte-preserve**する。3要素のいずれかが不一致ならthinkingだけを送信viewから除外する(pi の平文化分岐は移植しない)。tool call/result IDは、**origin不一致かつdestination protocolのwire制約に適合させる必要がある場合だけ**、同じassistant tool flow内のcall/result対へ同一のbounded mappingを適用して正規化する。mappingはそのflowのID数を上限として閉じ、次turnへ再利用しない。制約のないdestinationやsame-originのIDは書き換えない。**`RejectedToolCall`+is_error 対→protocol-neutral な user 相当診断1件へ変換**(実行可能 tool call へ復元しない)
- 受け入れ: テーブルテスト15ケース以上(abort直後/ステア分断/多重孤児/interrupted テキスト空/RejectedToolCall 対/anchor 保持)。加えてorigin完全一致でthinkingとcall/result IDがbyte一致、`provider_instance_id`のみ・`protocol`のみ・`model`のみの各不一致でthinking markerが消えること、cross-originかつ40字制約destinationで長いIDのcall/result対が同じ上限内IDへ写ること、連続turnでmapping stateを再利用しないこと、制約なしcross-originではIDを保持することを固定する
- コミット: `agent: 履歴再送の正規化 transform (pi transform-messages 移植+Sumi拡張)`

### T15: agent/ ループ+Session actor 【要T8,T12,T13,T14】

- 読む: §5.1〜§5.2 **全体**、#18〜#23、付録C、pi: `agent/src/agent-loop.ts:155-275` と `agent.ts` **実読**
- 作る: `src/agent/events.rs`(AgentEvent/PublicStreamEvent、§3.3)、`queue.rs`、`run.rs`、`mod.rs`(Session actor)。main.rs の echo を Session 起動に差替え
- やること: §5.1 疑似コードの忠実実装。Length 一括失敗(#19)+**同一 run 内連続2回で3回目へ進まない暴走ガード**、sequential 実行固定、steering ポーリング位置(#18)、one-at-a-time(#23)、run 失敗時の合成エラーで正常形クローズ(#22)。Session は**制御プレーンと run ワーカーを分離した actor**(RunCore move、二重 select 必須 — 単純 await 禁止、§5.2)。リトライ実施(T7 のポリシー+`MessageEnd(error)→RetryScheduled→次attempt` のイベント列、error assistant は `append_to_l0=false`)。control 直列化境界と **Abort cutoff**(§5.2: seq 順終端、後続 seq を先に ACK しない)、steer group の分類・snapshot・一括注入(bounded window、T12 の sizer)、run owner 移譲(付録C 行11)。T8の計測口へcommand受信時刻を接続し、`command受信→request_sent`/`request_sent→first Text|Thinking delta`を分離、stdio表示と内部overhead p95判定を完成する。承認は本物を作らず **fixture 専用 Pending action driver** で `ApprovalRequested+approval_log.pending`/解決/復旧 transaction だけ検証(§13 M2)。ツール実行開始は `ToolExecutionStart+prepared→running` の transaction を通す(§10.2)
- 受け入れ: stdio E2E(tests/loop_e2e.rs: モックプロバイダで user_message→イベント列アサート、異常系込み正常形クローズ)。M2 ゲート1・4・5・6c の該当部分(steer 前の soft 分岐は T16 と分担可)。実プロバイダなしで全緑
- コミット: `agent: エージェントループとSession actor (M2)`

### T16: steer.rs ハードステア + abort + supersede 【要T15】

- 読む: §6 **全体**(Sumi独自領域の核)、§6.3.1 イベント遷移表、§6.5、付録C 行4・9〜17・19・22・23、§10.2 run owner
- 作る: `src/agent/steer.rs` + Session への組込み
- やること: §6.2 の分岐(生成中=hard/ツール中=soft/retry 待機中=retry_steer)。ハードステアは **§6.3 手順0(旧 owner の `RunPhase(assistant_started→hard_steer_requested)` を分類と同一 transaction で commit してからだけ cancel 発火)** → 部分確定(Text は閉じる、thinking は「検証済み完全 block のみ保存・未署名 partial 破棄」を adapter 判定、**実行前 ToolCall は全破棄**、interrupted=true)→ L0 記録(**旧 owner 維持**)→ TurnEnd → Steered → TurnStart → user 注入(owner 原子移譲)→ 再開+中断マーカーテキスト。**§6.3.1 の表を厳守**(provider 終端は UI へ素通しせず Session が MessageEnd 発行、SteerPending/AbortRequested フラグ、hard steer は AgentEnd を出さず同一 run 継続)。abort(§6.4: cancel→ツール kill 伝播→部分確定→再開しない)+**未注入 command の supersede**(§6.5: `user_started` 境界、Superseded ACK、idle startup の正常形クローズ込み)
- 受け入れ: M2 ゲート2(ソフト/ハード両 E2E 自動判定)、ゲート3(Kimi reasoning のみ部分応答の受理確認、駄目なら回避策+コメント)、ゲート6・6b(kill/restart で hard steer 手順0前後の分岐、owner 継続、abort 各位置)、「MessageEnd 二重発行なし」「hard steer 後 AgentEnd なし」のイベント列アサート
- コミット: `agent: ハードステア/abort/supersede (M2完成)`

---

## M3: 永続化拡張

### T17: store/ 拡張 (provider_context・memory表・FTS・DeliveryPump・全phase復旧) 【要T16。M2と並行不可】

- 読む: §10.1 残り全部(`provider_context`+`provider_context_mutations`+`provider_context_replace_heads`、`memory_batches`/`memory_batch_messages`/`memory_jobs`/`memory_apply_cursors`、FTS、`kv`)、§10.2 残り全部(DeliveryPump、ProviderContextMutation の prepare→apply、復旧分岐リスト全体)、§13 M3 ゲート
- 作る: `src/store/transcript.rs`、`memory_state.rs`、`delivery.rs`(仮名)、migration 追補
- やること: FTS5 は **external content(`content='messages'`)+migration 内 INSERT/DELETE トリガ**(旧タスク文の contentless 案は廃止)。`provider_context` の MessageEnd transaction 内暗号化 INSERT+`(wire_item_index, ordinal)` 検証+eviction footprint 同時加算。`ProviderContextMutation`(Invalidate/Replace)の prepare→apply exactly-once、`replace_heads` の単調 CAS、起動時 `ProviderContextMutationRecovery`。**DeliveryPump 分離**(恒久=`agent_events` 正典で raw 復号送信/redaction-only は projection のみ、delta は Online 中だけ・catch-up 中破棄、epoch 付き bounded channel)。Redactor の全 secret パターン+FTS/export 除外を完成。全 phase の suffix 復旧(§10.2 分岐リスト完全実装: ToolUse 後の per-call 個別分類、tool/approval phase 中、retry group、cancel_requested、hard_steer_requested)。リトライの「state から除去・ログに保持」完成
- 受け入れ: M3 ゲート1〜9 全部(10ターン kill/restart、書込み遅延で順序不変、全 transaction 境界 kill、GatewayWriter 切断で durable 継続、MemoryTransition failpoint、`append_to_l0=false` membership、tool_executions の error_code 限定、CHECK/owner fixture)
- コミット: `agent: SQLite永続化拡張と全phase再起動復元 (M3)`

### T18: contracts/agent-events.yaml 凍結 + gateway/wire.rs 【要T17。M3完了時点で先出し】

- 読む: §11.2 全体、§3.3(message_id/UUIDv5 導出)
- 作る: `contracts/agent-events.yaml`(JSON Schema 2020-12)、`src/gateway/wire.rs`(wire DTO+内部 enum からの明示変換)
- やること: Envelope(durable は seq 必須/delta・Error は seq 禁止の if/then)、AgentEvent 全 variant、CommandEnvelope/CommandAck(rejected のみ reject_reason)、OutboundFrame、`attachments: maxItems 0`。**user message_id の UUIDv5 namespace 定数を contracts に正典として明記**。内部 `ProviderEvent` へ variant を足しても明示変換を更新しない限り wire に出ない構造(契約ファースト、§11.2 末尾)
- 受け入れ: Rust round-trip fixture(non-empty attachments 拒否含む)。**この時点で wire 形を凍結し、api/web 担当に contracts を渡す**(Go/TS 型生成と3言語 round-trip CI は api 側と合同)
- コミット: `agent: contracts凍結とwire DTO (M3同期ポイント)`

---

## M4: 3層メモリ

### T19: memory/ batch + estimate 【要T2。T17と並列可(純関数部)】

- 読む: §7.2〜§7.3、§7.5、#27〜#28、pi: `agent/src/harness/compaction/compaction.ts:118-303`
- 作る: `src/memory/batch.rs`、`estimate.rs`、`mod.rs` の状態モデル(§7.2 の構造体群と定数。`DecryptedMemorySummary` は zeroize)
- やること: seal 境界規則(**通常 seal は user メッセージ直前に限定**、toolResult 直前禁止、interrupted+steering 間禁止、tool loop 跨ぎ禁止。assistant 直前は**強制 seal(public est+footprint がバッチ強制上限超過)時のフォールバックのみ**)。est(ascii/4 + non_ascii/1.5)+EMA 校正の器。**eviction footprint の versioned 純関数**(`ReplayProbeV1` の2回 serialize 差分、`eviction_tokens_v1 = ceil(bytes/4)`、ratio は overflow 比較時に1回だけ)
- 受け入れ: 境界規則のテーブルテスト、見積の既知例テスト、M4 ゲート11 の golden fixture(estimator が全 adapter で同値)
- コミット: `agent: 3層メモリの状態モデル/バッチ分割/見積 (M4 1/3)`

### T20: memory/compactor.rs + memory_jobs 【要T17,T19】

- 読む: §7.4 **全体**、#29、pi: `compaction.ts:383-522`
- 作る: `src/memory/compactor.rs`
- やること: ワーカー1本(mpsc は wake-up のみ、**ジョブ正典は `memory_jobs`**)。`CompactionInput` 型境界(private field、`from_public_batch`(**Thinking 除去**)/`from_decrypted_summaries`(unredacted 正本入力)以外の constructor なし、provider context を型レベルで送信不能に)。**framing tag(`</conversation>` 等)の escape**(§7.4)。Compact プロンプト(pi 構造化チェックポイント形式の秘書ドメイン版、圧縮率 1/8〜1/15・上限 ~800tok 明示、D2=既定は会話と同モデル+trust domain 制約)。claim/CAS/completion(`MemoryProjectionBuilder` で暗号化正本+redacted projection 同時生成、source_versions CAS、`UNIQUE(kind,batch_seq)`)、失敗2リトライ→CompactFailed、lease 回収
- 受け入れ: M4 ゲート7(kill/restart で一度だけ適用)、ゲート9(全 provider-context variant+Thinking を紐付けても HTTP body に1byteも出ない+compile-fail テスト)、ゲート10(要約 secret の redaction/crypto-erase)、ゲート4後半(framing escape の adversarial fixture)
- コミット: `agent: 非同期Compactワーカーと耐久ジョブ (M4 2/3)`

### T21: memory/overflow.rs + ContextAssembler 【要T20】

- 読む: §7.6〜§7.8 **全体**、§13 M4 ゲート
- 作る: `src/memory/overflow.rs`、ContextAssembler 完成(T15 の暫定を置換)、`tools/` の `put_attachment` 経路接続
- やること: **経過時間は昇格条件にしない**(容量条件のみ)。`effective_l0 = ceil(Σ(est+footprint)×ratio)`。適用タイミングの優先順位: **Idle 直後/MaintenanceReady が主**、API 直前はフォールバック(ユーザー起点初回コールはスキップ=TTFT 保護、ハード上限 1.2 倍で無条件)。L0→L1 は FIFO+ヒステリシス(DROP_TO)+**昇格 transaction で provider-context 鍵/row 破棄と footprint 減算を同時に**。open バッチは絶対廃棄しない。L1→L2、L2 統合。50KB 超ユーザー入力の切詰めビュー(runtime-only)+broker `put_attachment`(message_id 決定論 ID、MessageEnd commit 後・assistant 再開前検証)。`sumi_three_layer`/`provider_native` の組立分岐(§7.7)
- 受け入れ: M4 ゲート1〜6・8(長会話シミュレータで全段昇格+80k 未満、キャッシュヒット率>0.8 実測枠、TTFT 非劣化 span 証明、adversarial probe、校正 ±15%、ツールなし会話の Idle 適用、50KB 貼り付け)
- コミット: `agent: 3層メモリ統合 (M4完成)`

---

## M5: 権限承認 + WS ゲートウェイ

### T22: approval/ action + policy 【要T16】

- 読む: §9.1〜§9.4 **全体**(Codex/Claude Code 調査結果含む)、#20、pi: `agent-loop.ts` の beforeToolCall 部
- 作る: `src/approval/mod.rs`(ApprovalBroker 骨格)、`action.rs`、`policy.rs`
- やること: `CanonicalAction`(runtime 内部正本。**wire にも DB にも書かない**)、shell 複合 command の segment 分解(`&&`/`||`/`;`/pipe/newline/subshell。heredoc・動的 eval・解析不能は NeedsApproval)、`Forbidden > NeedsApproval > Allow` で全 segment の最も厳しい結果。永続 rule は literal token prefix+制約(広域 prefix 拒否、ApproveAlways は仮適用→全 segment 再評価→競合なしのみ保存、**secret 検出時は永続化 fail-closed→ApproveOnce 降格**)。`SecretAwareActionProjector`(secret→kind+keyed digest の `SecretRef`、判定材料不足は `InsufficientEvidence`)。既定 fast path は D3(workspace read/write/edit=Allow、bash/network/domain mutation=NeedsApproval)
- 受け入れ: M5 ゲート1・2(shell fixture、広域 rule 拒否)、§9.4 の Authorization header 付き curl の ApproveAlways 拒否 fixture
- コミット: `agent: CanonicalActionと決定論的policy (M5 1/3)`

### T23: approval/ reviewer + broker 統合 【要T22】

- 読む: §9.2(状態機械)、§9.5〜§9.8 **全体**、D4/D6/D9
- 作る: `src/approval/reviewer.rs`、`prompt.rs`、ApprovalBroker 完成+Session 結線(T15 の fixture driver を置換。T12 で凍結した `approval_log` transaction 契約へ接続)
- やること: §9.2 状態機械(Pending=oneshot・タイムアウトなし、abort/soft steer で Cancelled、**soft steer 確定時は同一バッチの未開始ツールも Cancelled 確定** — D4)。ReviewerMode(User/AutoReview/StrictAutoReview)、reviewer trust-domain 制約(未許可は call せず manual/headless block)、bounded transcript(§9.6 の順序と上限。tool result 本文・Thinking・raw CanonicalAction を渡さない)、§9.7 プロンプト+strict JSON schema、3attempt/90s、fail-closed synthetic High/Unknown/Deny、circuit breaker、allow cache と invalidation。terminal/unknown request の no-op Applied(§9.8 末尾)
- 受け入れ: M5 ゲート3〜7(User/AutoReview E2E、secret 置換、fail-closed 各系、承認待ち中 steer、sandbox 非拡大)+ M2 fixture ゲートの実 broker 再実行(ゲート6)
- コミット: `agent: Audit reviewerと承認フロー統合 (M5 2/3)`

### T24: gateway/ws.rs + supervisor.rs 【要T17,T18】

- 読む: §11.1 **全体**(トレイト境界と supervisor 仕様)、§13 M5 ゲート9・10
- 作る: `src/gateway/ws.rs`、`src/gateway/supervisor.rs`。**このタスクに限り `tokio-tungstenite = { version = "0.24", features = ["rustls-tls-webpki-roots"] }` の依存追加を許可**(唯一の例外)
- やること: outbound WS+`Authorization: Bearer <short-lived-agent-token>`、`CredentialProvider` 抽象(接続 attempt ごとに fresh token)、hello(`agent_id/generation/last_sent_event_seq/last_received_command_seq/last_applied_command_seq` → `accepted_generation/last_received_event_seq/next_command_seq`)。ConnectionSupervisor: epoch 単位で reader/writer 一組、**片方の失敗で両 half join/drop してから次 epoch**、bounded backoff+jitter、認証拒否は credential refresh へ、claim 不一致は fatal。event/command 双方向 catch-up(`agent_events` 正典、durable 最新 seq 到達前の delta 破棄、完了後だけ Online)。stdio は `SingleConnectionConnector` として同 interface に整合
- 受け入れ: モック WS サーバで M5 ゲート10(reader EOF/writer timeout/token expiry/API 再起動の各個別発火)、ゲート9 の agent 側保険経路(oversized terminal Rejected)。M5 ゲート8(web→api→agent E2E)は api/web 側と合同
- コミット: `agent: WSゲートウェイとConnectionSupervisor (M5 3/3)`

---

## Cloud release 実装タスク (全て必須)

### T25: provider release 結合 【owner: agent/provider、要T9,T10,T17】

- 読む: §4.2.1〜§4.2.2、§10.1 provider_context、§13 M1P/Cloud track 1
- 変更対象: `apps/agent/src/provider/adapters/`、`apps/agent/src/store/`、provider fixture/live test
- やること: Responses encrypted reasoning/ordered compact outputとAnthropic signature/redacted thinking/compaction blockを、暗号化provider_contextへMessageEndと同一transactionで保存し、再起動後に同じprovider instance/protocol/model/fingerprintへ順序不変で再送する。異なるtrust domainへの送信はfail-closed。公式fixtureとライブ3経路を分離する。さらにT8から引き継いだMoonshot直API(Kimi text/tool/reasoning)、Z.ai直API(GLM tool stream/provider固有finish reason)、Umans(text/tool/reasoning)のcurl raw captureをsanitization前SHA-256・時刻・endpoint・command・sanitization操作付きで固定する
- 受け入れ: M1Pのdurable round-trip・tool-use継続・cache usage・native compaction全ゲート。kill/restartを挟んだ2ターン目が成功し、公開transcript/FTS/exportにopaque byteが出ない。`SUMI_LIVE_TEST=1`がdirect Moonshot/Z.ai/Umans release dispatcherを選んだ場合は、3経路の2ターン+tool 1往復を全て完走し、Kimi reasoning_content再送が400にならないことを必須とし、credentialの不足・空値はlive gate失敗にする。credential不要の通常テスト・公式fixtureテストはlive実行と分離し、live未選択時はskipできる。OpenCode gateway captureとsynthetic fixtureはこのdirect証拠の代替不可
- コミット: `agent: provider context durable round-trip (Cloud provider release)`

### T26: executor/artifact production deployment 【owner: platform/runtime、要T13,T17】

- 読む: §8.1〜§8.3、§13 Cloud track 2、`docs/agent/workspace.md`
- 変更対象: deployment supervisor、`deploy/agent/`、executor/artifact broker起動設定、fault-injection harness。配置先が別repoになる場合は対応issue/commitを本項へ記録する
- やること: runtime/executor/artifact brokerを別UID・別mount・別network namespaceで起動し、executorとbrokerのRPC generation/nonceをsupervisorが払い出す。executor/brokerは`network_mode=none`、runtimeはLLM通信を維持。artifactの0600/0700、fd-relative no-follow、conversation subtree限定resetをproduction構成で保証する
- 受け入れ: 各componentから禁止path/socket/networkが読めないこと、bash子がbrokerへ到達できないこと、file toolは同一executor UIDで継続できることを自動テスト。M2 mockだけでは代替しない
- コミット: `infra(agent): executor/artifact isolation deployment`

### T27: resource quota + generation recovery 【owner: platform/runtime、要T17,T26】

- 読む: §8.3、§10.2 tool_executions recovery、§13 Cloud track 3〜4、`docs/agent/workspace.md`
- 変更対象: deployment supervisor、quota policy、execution registry、fault-injection harness、運用metrics
- やること: disk/inode/PID/CPU throttle・CPU time/memory max/wall/outputの全制限を種別付き`ResourceLimit`へ収束させ、execution cgroup/sandboxをgeneration単位で登録・kill/reapする。runtime/provider/tool途中killでは旧generationと離脱descendantを回収し、`running`を`indeterminate`で一度だけ閉じて自動再実行しない
- 受け入れ: quotaを1種ずつ独立発火。`setsid`+stdio close後のdescendantも回収されworkspace変更不能。kill境界を跨いでもterminal event/toolResultが重複しない
- コミット: `infra(agent): resource quota and generation recovery`

### T28: WS production + 3言語E2E 【owner: api/web/agent、要T18,T24,T26】

- 読む: §11全体、§13 M5ゲート8〜10/Cloud track 5
- 変更対象: `apps/api`、`apps/web`、`apps/agent`、`contracts/agent-events.yaml`、Go/Rust/TS生成物とintegration harness
- やること: short-lived token検証、conversation/generation fence、hello cursor、command/ACK/event両方向catch-upをproduction境界で結線する。APIはoversized/attachmentsをseq採番前に拒否。contractsから3言語型を生成しround-trip CIを固定する
- 受け入れ: token無し/期限切れ/別conversation/旧generation、reader EOF、writer timeout、API再起動、command重複/欠番、ACK前後killを個別発火。chat UIでstream/tool/steer/approvalを含む代表journeyが完走する
- コミット: `feat(agent): production WS and cross-language E2E`

### T29: data lifecycle + KMS移行 【owner: store/api/platform、要T17,T21,T26】

- 読む: §10全体、§13 Cloud track 6
- 変更対象: store migration/lifecycle worker、API export/reset/delete、artifact tombstone、KMS KeyProvider、runbook/backup fixture
- やること: transcript/artifact export、conversation reset、agent deletion、provider context/memory summaryのcrypto-erase、検索監査を実装する。`deletion_tombstones`の状態機械とCHECK/CASを正典どおり固定し、backup復元時もtombstoneを再適用する。環境変数テスト鍵からtenant KEK/KMS階層へ移行しrotation/revocationを扱う
- 受け入れ: lifecycle各段階kill/restart、逆行/飛越し拒否、他conversation非破壊、backup再適用、KMS rotation/失効後拒否。delta分割secretとCompact結果を含むredaction fixtureでredaction-only接続・FTS・exportから復元不能
- コミット: `feat(agent): data lifecycle and KMS release gate`

---

## Cloud release acceptance track (M1P・M0〜M5と並行、全項目必須)

複数領域に跨る合同作業の最終照合表。実装責務はT25〜T29、ゲートの正典は§13。**どれかを省略した release candidate は存在しない**(§14.3):

1. **provider release (T25)**: M1P の Responses/Anthropic 全ゲート完了(T9/T10 + T17 後の durable round-trip 結合)
2. **executor/artifact deployment (T26)**: deployment supervisor、executor/broker sidecar の別UID・`network_mode=none`・volume mount 分離(§8.1)
3. **resource semantics (T27)**: disk/inode/PID/CPU/memory/wall/output の各 quota 経路が種別付き `ResourceLimit` へ収束(§8.3・workspace.md)
4. **generation recovery (T27)**: runtime kill 後の execution cgroup/sandbox 回収、`running`→`indeterminate`、自動再実行なし
5. **WS production (T28)**: token 検証・generation fence・fault-injection(T24 の production 検証)
6. **data lifecycle (T29)**: export/reset/deletion、crypto-erase、tombstone 再適用、redaction fixture(delta 分割 secret 含む)、KMS 移行、運用 runbook

---

## 補足

- **OpenCode Zen (Go)**: 検証用の当面の既定(D7 と §4 の表)。クォータはドル換算($12/5h、$30/週、$60/月)。疎通確認: `curl https://opencode.ai/zen/go/v1/chat/completions -H "Authorization: Bearer $OPENCODE_GO_API_KEY" -H "Content-Type: application/json" -d '{"model":"deepseek-v4-flash","messages":[{"role":"user","content":"Reply with OK"}]}'`。ゲートウェイ実体なので Compat は直結先の値を流用せず M1 ライブ fixture で個別固定
- ライブ検証はフィクスチャテストと完全分離し、キーが無くても通常テストは止めない(`SUMI_LIVE_TEST=1` オプトイン)。`SUMI_LIVE_TEST=1 cargo test --manifest-path apps/agent/Cargo.toml`ではnon-ignored T25 dispatcherがdirect Moonshot `kimi-k3`→direct Z.ai `glm-5.2`→Umansを順に実行し、credential不足/空を失敗にする。4つのprovider別ignored gateは明示的な開発実行用であり、OpenCodeはT25 direct証拠を代替しない。CI(GitHub Actions の agent パス)ではライブ以外を全部回す
- 各タスク後の状態は常に main 相当の品質(全テスト緑)を保つ。マイルストーンは日付ではなく品質ゲートで区切る
- 憲法プロンプト(人格)の執筆はスコープ外(Founder が書く)。実装はプレースホルダで進める(付録B-4)
