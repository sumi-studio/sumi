# エージェント基盤 実装タスク分解 (Codex用)

- 正典: [docs/agent/implementation-plan.md](../../docs/agent/implementation-plan.md) (以下「計画書」。§n は章番号、#n は第12章の移植リスト番号)
- pi 参照ソース: `/home/yohaku/pi-reference/packages/` (計画書の `pi:` パスはここを指す)
- 作業ブランチ: `feat/agent-scaffold` / 作業ディレクトリ: `apps/agent`
- このファイルの各タスクをそのまま Codex に貼る。**冒頭に必ず「共通ルール」も一緒に貼ること**

---

## 共通ルール (全タスクの前提。毎回プロンプト先頭に貼る)

```
リポジトリ /home/yohaku/sumi、ブランチ feat/agent-scaffold、対象 apps/agent (Rust, edition 2024)。
設計の正典は docs/agent/implementation-plan.md。担当範囲の章と、第12章の該当移植項目を必ず読むこと。
pi のソースは /home/yohaku/pi-reference/packages/ にある。移植項目は該当ファイルを実読してから書く(計画書の表は索引であってコードの代替ではない)。
⚠ 計画書の pi 行番号にはズレがある(計画書§12冒頭の注意書き参照)。正典は「ファイルパス+関数/挙動の記述」。行番号が合わなくても挙動記述を頼りに該当箇所を自分で特定すること。
規律:
- 依存クレートは Cargo.toml にあるものだけ使う。新規追加禁止(必要なら停止して報告)
- タスクで指定されたファイル以外は変更しない(mod 宣言の追記のみ例外)
- cargo build && cargo clippy --all-targets -- -D warnings && cargo fmt && cargo test を全部通してから終了
- コメントは「コードから読めない制約」のみ。pi の行番号参照は移植元として価値があるので `// pi: ai/src/api/openai-completions.ts:362-366` 形式で残す
- 完了したら指定のコミットメッセージで git commit (このタスクの変更ファイルのみ add)
- panic 禁止方針: プロバイダ層は「stream は決して panic/Err しない」契約(計画書§3.2)。unwrap は테스트内のみ
```

---

## 依存グラフと推奨順序

```
T1 ──▶ T2 ──▶ T3, T4 (並列可) ──▶ T5 ──▶ T8(統合)
        │              └─▶ T6, T7 (並列可) ─┘
        ├─▶ T9(tools) ──▶ T10(transform) 
        └─────────────────────▶ T11(loop) ──▶ T12(steer) ──▶ T13(store) ──▶ T16(approval)
                                     T14(memory純関数) ──▶ T15(memory統合) ──┘
                                                                T17(ws) は T13 後
チェックポイント: CP1 = T1〜T4 / CP2 = T5〜T8 (M1ゲート) / CP3 = T9〜T12 (M2ゲート)
```

各チェックポイントで全品質ゲートを満たしてから次へ進む(日付ではなく品質で区切る)。ライブ API 検証はフィクスチャテストと完全分離し、キーが無くても通常テストは止めない(`SUMI_LIVE_TEST=1` オプトイン)。

Codex を2セッション並列で回すなら: レーンA = T2→T4→T5→T6→T7→T8、レーンB = T3→T9→T10。T11 以降は合流後に直列推奨(Session まわりは1人で書くべき)。

---

## T1: M0 足場 (Cargo.toml 全依存 + モジュール骨格 + stdio エコー)

- 読む: 計画書 §2 全体、§11.1
- 作る: `Cargo.toml`(§2.1 の依存を**全部**この時点で追加。後続タスクの追加を防ぐ)、`src/main.rs`、`src/config.rs`、`src/gateway/mod.rs`、`src/gateway/stdio.rs`、他モジュールの空 `mod.rs` 群(§2.2 のツリー通り、中身は `//! doc comment` のみ)
- やること: config は環境変数+TOML(`SUMI_CONFIG` パス、モデルプリセットは T6 で入れるので構造体だけ)。gateway は Command/Envelope 型(§11.1)と stdio 実装(1行1JSON)。main は tracing 初期化(JSON ログ、`SUMI_LOG` フィルタ)→ stdio ゲートウェイ起動 → 受けた user_message に固定エコー(`AgentEvent` はまだ無いので仮の `{"type":"echo","text":...}` で可、T11 で置換)
- 受け入れ: `echo '{"type":"user_message","text":"hi"}' | cargo run` がエコー JSON を1行返して EOF で正常終了。clippy/fmt/test 緑
- コミット: `agent: M0 足場 (config/gateway骨格/stdioエコー)`

## T2: provider/types.rs (コアデータ型)

- 読む: 計画書 §3.1〜§3.5 全体(Rust 定義がほぼ書いてある)、pi: `ai/src/types.ts:301-476`
- 作る: `src/provider/types.rs`
- やること: §3.1 の Message/UserMessage/AssistantMessage/ToolResultMessage/AssistantContent/ToolCall/StopReason/Usage、§3.2 の ProviderEvent/ProviderEventStream、§3.4 の PromptContext/ToolDefinition をそのまま実装。serde 属性(tag 形式)は計画書の記載どおり。`Usage::from_raw`(#5: cached_tokens→cache_read、input=prompt−cached−write、Moonshot の choices[0].usage は T5 で扱う)も入れる
- 受け入れ: 各型の serde 往復テスト(JSON→型→JSON が安定)、tag 表現のスナップショット的アサート。clippy/fmt 緑
- コミット: `agent: provider コアデータ型 (計画書§3)`

## T3: provider/partial_json.rs (逐次JSONパース) 【T2と並列可】

- 読む: 計画書 #13、pi: `ai/src/utils/json-parse.ts` **全文とそのテスト**(同ディレクトリの `.test.ts` があれば)
- 作る: `src/provider/partial_json.rs`
- やること: 戦略チェーン「厳密→repair→partial補完→repair+partial→空Object」の忠実移植。repair は文字列内の生制御文字エスケープ・不正エスケープの二重化。partial は未閉鎖の文字列/配列/オブジェクトの補完と末尾不完全トークンの切除。公開 API: `parse_streaming(&str) -> serde_json::Value`(常に成功、フルチェーン)。**ToolCallEnd の確定パースも pi と同じくこのフルチェーン(best-effortサルベージ)を使う**(厳格化しない — 不完全引数のリスクは Length 一括失敗 #19 が受け持つ二段構え。計画書§4.3)
- 受け入れ: pi のテストケースを Rust に移植して全緑(最低20ケース)。依存は serde_json のみ
- コミット: `agent: 逐次JSONパース (pi json-parse.ts 移植)`

## T4: provider/sse.rs (SSE行パーサ) 【T3と並列可】

- 読む: 計画書 §4.3 末尾(SSE 層仕様)、#17、pi: `ai/src/utils/error-body.ts`
- 作る: `src/provider/sse.rs`
- やること: `bytes_stream` → 行バッファリング → `data: ` ペイロード yield、`data: [DONE]` 終端。非2xx はボディ4000字切詰めで `"{status}: {body}"` エラー化(#17)。チャンク間アイドルタイムアウト 120s(tokio::time::timeout)。CancellationToken で中断可能。reqwest 依存はこのモジュールに閉じる
- 受け入れ: ユニットテスト(行分割の境界: CRLF/複数data/分割チャンク/UTF-8境界またぎ/DONE/途中切断)。clippy 緑
- コミット: `agent: SSE行パーサ`

## T5: provider/assembler.rs (chunk→メッセージ組立) 【要T2,T3】

- 読む: 計画書 §4.3 全体、#2〜#7、pi: `ai/src/api/openai-completions.ts:229-511` **必ず実読**
- 作る: `src/provider/assembler.rs`
- やること: MessageAssembler 構造体(イベント列→AssistantMessage の純関数、§3.2「partial 同乗廃止」)。tool_calls の index/id 二重引き(#2)、Moonshot `choices[0].usage` フォールバック(#3)、reasoning 3フィールド検出+最初の非空だけ採用+signature_field 記録(#4)、finish_reason マップ(#6)、finish_reason 無し終端=エラー(#7)、エラー時 scratch 掃除、**ToolCallEnd 時の確定パースは parse_streaming と同じ best-effort フルチェーン**(T3 と統一、計画書§4.3)
- 受け入れ: 「生chunk JSON列 → 期待イベント列+最終メッセージ」形式のテーブルテスト(text/thinking/toolcall/usage各系統+異常系)。clippy 緑
- コミット: `agent: SSEチャンク→メッセージ組立 (pi openai-completions 移植)`

## T6: provider/request.rs + config プリセット 【要T2。T5と並列可】

- 読む: 計画書 §4.1〜§4.2 全体、#8〜#12、#30、pi: `ai/src/api/openai-completions.ts:575-1150`、`ai/src/providers/moonshotai.models.ts`、`zai.models.ts:79-98`
- ⚠ Compat の要注意点(計画書§4.1 修正済み): **GLM は `max_completion_tokens`**(pi の useMaxTokens 判定に z.ai は含まれない)、Kimi は `max_tokens`。GLM の base_url は pi の値(コーディングプラン用 `/api/coding/paas/v4`)ではなく**直APIの `https://api.z.ai/api/paas/v4`** を使う
- 作る: `src/provider/request.rs`、`src/config.rs` への ModelSpec/Compat プリセット追加(kimi-k3 / glm-5.2 / umans / **opencode-go**。base_url/model は環境変数で上書き可能にし、TOML例をコメントで残す)
- OpenCode Go の実値(2026-07-17調査、一次情報確認済み): base_url `https://opencode.ai/zen/go/v1`、認証 `Authorization: Bearer $OPENCODE_GO_API_KEY`(変数名はSumi側の定義)、モデルIDに **`kimi-k3` / `glm-5.2`**(本番候補と同一モデル!)、`kimi-k2.7-code`、`deepseek-v4-flash`(安価な疎通確認用)等。Compat は Kimi/GLM プリセットを流用しモデルIDで切替。ストリーミング/ツールコール対応は状況証拠のみなので **T8 のライブスモークで実測確認**すること
- やること: PromptContext→Chat Completions JSON。計画書§4.2 の12項目を全部(assistant はプレーン文字列送信、thinking の signature_field 書き戻し、reasoning_content:"" 補完、ツール画像の user 追送、空 assistant スキップ、ID 40字正規化 #12 等)
- 受け入れ: 変換のスナップショットテスト(通常/ツール往復/thinking再送/interrupted部分応答/画像)。clippy 緑。**モデルは TOML/env のランタイム設定で切替可能**(再コンパイル不要)であることをテストで確認
- コミット: `agent: リクエスト組立とモデルCompatプリセット`

## T7: provider/retry.rs + provider/overflow.rs 【要T2。並列可】

- 読む: 計画書 §4.4〜§4.5、#14〜#16、pi: `ai/src/utils/retry.ts` 全文、`ai/src/utils/overflow.ts` 全文(165行)、`coding-agent/src/core/agent-session.ts` のリトライポリシー部(行番号は目安、grep で特定)
- 作る: `src/provider/retry.rs`、`src/provider/overflow.rs`
- やること: 正規表現パターン集の移植(non-retryable 先判定→retryable、pi のコメントの issue 番号も残す)。バックオフ計画 (3回 2s/4s/8s) は「待機時間を返す純関数+Cancellation対応 sleep ヘルパ」で、実施はループ側(T11)。overflow は Kimi/汎用パターン+z.ai サイレント溢れの usage 判定(#16)
- 受け入れ: パターン判定のテーブルテスト(pi のパターンから30ケース以上)。clippy 緑
- コミット: `agent: リトライ/コンテキスト溢れ判定 (piパターン集移植)`

## T8: provider/mod.rs 統合 + フィクスチャ再生テスト 【要T4,T5,T6,T7】

- 読む: 計画書 §3.2(stream 契約)、§13 M1 ゲート
- 作る: `src/provider/mod.rs`(`stream(spec, ctx, opts, cancel) -> ProviderEventStream`)、`tests/provider_fixtures.rs`、`tests/fixtures/*.sse`(手書きで可: Kimi text / Kimi toolcall / Kimi reasoning / GLM tool_stream / 429エラー / 途中切断 の6本)
- やること: sse→assembler→イベント送出を繋ぎ、**どんな失敗も Error イベントで返す**(panic/Err 禁止契約)。フィクスチャは axum(dev-dep)のモックサーバで再生し、イベント列と最終メッセージをアサート。TTFT 計測 span(`request_sent`→`first_delta`)を tracing で入れる
- 受け入れ: `cargo test` 全緑。`SUMI_LIVE_TEST=1` 用のライブスモーク(env でキー注入、無ければ skip)の枠だけ用意
- コミット: `agent: プロバイダ層統合とフィクスチャ再生テスト (M1)`

## T9: tools/ (切詰め・bash・fs・トレイト) 【要T2。レーンB】

- 読む: 計画書 §8 全体、§3.4(Tool trait)、#25〜#26、pi: `agent/src/harness/utils/truncate.ts` **全文**、`shell-output.ts` **全文**(それぞれテストも)
- 作る: `src/tools/mod.rs`(Tool トレイト/ToolRegistry/TypedTool)、`truncate.rs`、`shell_capture.rs`、`fs.rs`(read/write/edit/list/glob/grep)、`bash.rs`
- やること: truncate は 2000行/50KB 二重上限・head/tail・部分行禁止・メタ注記をテストごと移植。shell_capture はローリングバッファ100KB+50KB超で全文テンポラリ退避+バイナリサニタイズ(**pi の上限は JS 文字数基準だが Rust ではバイト基準の仕様移植とする**。多バイト文字での退避テスト必須)。bash は tokio::process、**プロセスグループ kill は計画書§8.3 の5段仕様どおり**(spawn 時 `process_group(0)`、cancel 時 `libc::kill(-pgid, SIGKILL)`、wait で回収。**このタスクに限り `libc = "0.2"` の依存追加を許可**)、タイムアウト120s、on_update ストリーミング。fs の edit は old_string 一意性検査。grep は rg 呼び出し(無ければ grep フォールバック)、行長500字切詰め
- 受け入れ: truncate/shell_capture は pi テスト移植で全緑。bash は sleep/大量出力/中断の3テスト。UTF-8 char boundary のテスト必須(計画書§8.2 注意)
- コミット: `agent: ツール群 (fs/bash/切詰め、pi truncate/shell-output 移植)`

## T10: transform (履歴再送正規化) 【要T2。T9の後が楽】

- 読む: 計画書 §5.3、#11〜#12、#24、pi: `ai/src/api/transform-messages.ts` **全文**
- 作る: `src/memory/transform.rs`(mod 宣言は memory/mod.rs に最小追記)
- やること: 純関数 `transform(&[Message]) -> Vec<Message>`。孤児ツールコールへの合成結果挿入(user 分断位置・末尾未解決も)、Error/Aborted スキップ、**interrupted=true は保持しテキスト/thinking だけ残して未実行 ToolCall ブロックを落とす**(Sumi 拡張、§6.3)、クロスモデル thinking 降格、ID 40字正規化
- 受け入れ: テーブルテスト15ケース以上(abort直後/ステア分断/多重孤児/interrupted テキスト空)
- コミット: `agent: 履歴再送の正規化 transform (pi transform-messages 移植+interrupted拡張)`

## T11: agent/ ループ+Session 【要T8,T9,T10】

- 読む: 計画書 §5.1〜§5.2 全体、#18〜#23、pi: `agent/src/agent-loop.ts:155-275` と `agent.ts` **実読**
- 作る: `src/agent/events.rs`(AgentEvent、§3.3)、`queue.rs`、`run.rs`、`mod.rs`(Session actor)。main.rs の echo を Session 起動に差替え
- やること: §5.1 疑似コードのループ忠実実装(Length 一括失敗 #19、sequential 実行、steering ポーリング位置 #18、one-at-a-time #23)。Session は actor(コマンド mpsc 1本+イベント mpsc 1本)。run 失敗時の合成エラーで正常形クローズ(#22)。ツール実行に beforeToolCall フックポイント(T16 が挿す口)を用意し当面 Auto。メモリはまだ無いので「素の Vec<Message> に transform を適用して送る」暫定 ContextAssembler
- 受け入れ: stdio E2E スクリプトテスト(tests/loop_e2e.rs: モックプロバイダで user_message→イベント列アサート、正常形クローズを異常系込みで検証)。実プロバイダなしで全緑
- コミット: `agent: エージェントループとSession (M2前半)`

## T12: steer.rs ハードステア + abort 【要T11】

- 読む: 計画書 §6 **全体**(Sumi独自領域の核)、§5.2
- 作る: `src/agent/steer.rs` + Session への組込み
- やること: §6.2 の分岐(生成中=ハード/ツール中=ソフト)、§6.3 シーケンス(cancel→部分確定→interrupted=true→L0記録→Steered イベント→注入→再開)、**§6.3.1 のイベント遷移表を厳守**(provider の Done/Error 終端は UI に流さず Session が MessageEnd を発行、契機の区別は SteerPending/AbortRequested フラグ、ハードステアは AgentEnd を出さず同一 run 継続)、§6.4 abort(ツールも kill、再開しない)。中断マーカーテキストの付加
- 受け入れ追加: イベント列アサートに「MessageEnd が二重発行されないこと」「ハードステア後に AgentEnd が出ないこと」を含める
- 受け入れ: E2E テスト2本 — (a) モックプロバイダの遅延ストリーム中に user_message→部分応答が interrupted で確定し次ターンに注入される、(b) bash sleep 中の user_message→ソフトステア(完走後注入)。abort テスト1本
- コミット: `agent: ハードステア/abort (M2完成)`

## T13: store/ (SQLite永続化) 【要T11】

- 読む: 計画書 §10 全体、§13 M3 ゲート
- 作る: `src/store/mod.rs`、`transcript.rs`、`memory_state.rs`、`migrations/0001_init.sql`
- やること: §10.1 スキーマ(**FTS5 は contentless (content='') に変更済み**: StoreWriter が payload から抽出したテキストを rowid=messages.rowid で明示 INSERT。トリガ不要、検索は rowid JOIN。**`agent_events` 恒久イベントログ含む**)。StoreWriter タスク: §10.2 の**二階級イベント設計**に従い、恒久イベント(delta 以外)は seq 採番→ `agent_events` 追記→**その後** Gateway へ転送(保存してから送信を購読順序で保証)、delta 系は永続化せず直送。終了時 flush。起動時復元(L0 相当の会話 tail 復元。メモリ層の復元は T15 で拡張)。D1 決定に従い「イベントを後から api にミラーできる」ことだけ意識(実装は不要)
- 受け入れ: 10ターン会話→kill→再起動→履歴継続、FTS 検索が当たる、seq 単調継続、の統合テスト
- コミット: `agent: SQLite永続化と再起動復元 (M3)`

## T14: memory/ 純関数部 (batch + estimate) 【要T2。T13と並列可】

- 読む: 計画書 §7.2〜§7.3、§7.5、#27〜#28、pi: `agent/src/harness/compaction/compaction.ts:118-303`
- 作る: `src/memory/batch.rs`、`estimate.rs`、`mod.rs` の状態モデル(§7.2 の構造体群と定数)
- やること: カット境界規則(user/assistant 直前のみ、toolResult 直前禁止、interrupted+steering 間禁止)、トークン見積(ascii/4 + non_ascii/1.5)+EMA 校正の器
- 受け入れ: 境界規則のテーブルテスト、見積の既知例テスト
- コミット: `agent: 3層メモリの状態モデルとバッチ分割`

## T15: memory/ 統合 (compactor + overflow + ContextAssembler) 【要T8,T13,T14】

- 読む: 計画書 §7.4〜§7.7 **全体**、#29、pi: `compaction.ts:383-522`
- 作る: `src/memory/compactor.rs`、`overflow.rs`、ContextAssembler 完成(T11 の暫定を置換)、Store への層状態書込み
- やること: 先回り非同期 Compact(ワーカー+棚)、溢れ処理(FIFO+ヒステリシス、適用は API コール直前・ユーザー起点初回コールはスキップ、ハード上限 1.2 倍で無条件)、L1→L2、L2 統合、Compact プロンプト(§7.4 の秘書ドメイン版、D2 決定=会話と同モデル)、MemoryMaintenance イベント
- 受け入れ: 長会話シミュレータ(tests/ にスクリプト、モック Compact で 200k 相当投入)→ 全段昇格が発火しプロンプト総量が常に 80k 未満、ユーザー起点コール前に同期 Compact が走らないことを span で検証
- コミット: `agent: 3層メモリ統合 (M4)`

## T16: approval/ (権限承認) 【要T12】

- 読む: 計画書 §9 **全体**、#20、pi: `agent/src/agent-loop.ts:602-666`
- 作る: `src/approval/mod.rs`、`policy.rs`、Session/ループへの組込み、Command::ApprovalDecision 処理
- やること: §9.2 状態機械(Pending は oneshot、タイムアウト無し、abort/ハードステアで Cancelled)、§9.3 ポリシー(ReadOnly/Mutating=Auto、Exec=Ask、D3 決定済み)、AlwaysAllow ルールの SQLite 保存、承認待ち中の user_message はソフトステア(§9.4)
- 受け入れ: E2E — bash が ApprovalRequested を発行→承認で実行/拒否でエラー結果/AlwaysAllow が次回 Auto、承認待ち中ステアの統合テスト
- コミット: `agent: 権限承認フロー (M5前半)`

## T17: gateway/ws.rs + contracts ドラフト 【要T13】

- 読む: 計画書 §11 全体
- 作る: `src/gateway/ws.rs`(tokio-tungstenite を Cargo.toml に追加してよい。唯一の例外)、`contracts/agent-events.yaml`(ドラフト)
- やること: outbound WS、hello {conversation_id, last_sent_seq} ハンドシェイク、seq 差分再送(**`agent_events` テーブルから引く。恒久イベントのみ、delta は再送しない** — §10.2)、再接続バックオフ。contracts は Rust serde 出力を正としてスキーマ化(Envelope.seq は Option、delta 系は None)
- 受け入れ: モック WS サーバ相手の接続/切断/再送テスト。**この時点で Envelope/Command の JSON 形を凍結し、api 担当に contracts を渡す**
- コミット: `agent: WSゲートウェイと contracts ドラフト (M5後半)`

---

## 補足

- **OpenCode Go**: 接続情報は T6 に記載済み。クォータはドル換算($12/5h、$30/週、$60/月)。「Use with any agent」を公式に謳っており自作クライアントからの利用は設計意図どおり。疎通確認: `curl https://opencode.ai/zen/go/v1/chat/completions -H "Authorization: Bearer $OPENCODE_GO_API_KEY" -H "Content-Type: application/json" -d '{"model":"deepseek-v4-flash","messages":[{"role":"user","content":"Reply with OK"}]}'`
- T8 のライブスモークは接続情報が来たら `SUMI_LIVE_TEST=1` で随時実行
- 各タスク後の状態は常に main 相当の品質(全テスト緑)を保つ。Codex が迷ったら計画書§付録B の3点自己レビュー(正常形クローズ/キャッシュプレフィックス/ホットパス同期I/O)
