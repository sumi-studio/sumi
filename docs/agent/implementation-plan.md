# Sumi エージェント基盤 Rust 実装計画書

- Status: Draft v1
- Date: 2026-07-17
- 対象: `apps/agent`(Rust スキャフォールド済: tokio + anyhow + tracing、turbo に `@sumi/agent` として接続済み)
- 前提資料:
  - [ADR 0002 エージェント基盤の言語と実装方針](../adr/0002-agent-stack.md)
  - [3層メモリ設計](memory.md)
  - [ワークスペース設計](workspace.md)
  - [画面構成書](../screen-composition.md)
  - pi 調査レポート(2026-07-17)、モデルプロバイダ調査レポート(2026-07-17)
  - **pi ソースコード実読**: `github.com/earendil-works/pi` @ `216e672e` (2026-07-16)。本書で `pi:` で始まるパスは同リポジトリの `packages/` 配下を指す
- 締切: ハッカソン 2026-08-01(プレゼン 8/2)。「チャットUIからエージェントと会話でき、ストリーミング+ツール実行+ステアが見える」が最優先
- 凡例: 本文中 **[事実]** は pi ソースまたは一次資料の実読に基づく記述、**[推測]** は設計判断・未検証の見込み、**[要決定]** はユーザー(Founder)の判断が要る点

---

## 0. この計画書の使い方

この文書は「後続の AI セッションが人間の介入をほぼ受けずに実装を完遂できる」粒度を目指す。各章は独立して読めるように書かれ、第13章のマイルストーンが実装順序の正典。実装セッションは以下の順で読むこと:

1. 第13章で自分の担当マイルストーンを確認
2. 第2〜3章で全体構造とデータ型を頭に入れる
3. 担当コンポーネントの章(4〜11)を精読
4. 第12章の pi 移植リストで該当項目の pi ソースを**必ず実読**してから書く(pi は `/tmp` のスクラッチパッドに clone 済みだが消えている可能性がある。`git clone --depth 1 https://github.com/earendil-works/pi` で取り直せる)

**やらないことの明示**(スコープ外): マルチプロバイダ対応(OpenAI互換のみ)、MCP、サブエージェント、プランモード、音声、スケジューラ(リマインダー起動主体)、コンテナのライフサイクル管理、microVM化。これらは器(トレイト境界)だけ意識し、実装しない。

---

## 1. 要件の要約と全体アーキテクチャ

### 1.1 Sumi エージェントの性格

コーディングエージェントではなく、ユーザーの「メンバー」として振る舞う汎用秘書エージェント。

- 1エージェント = 1会話のシングルスレッド。人格が連続する
- 常時稼働・ステートフルな長命プロセス。ただし「エージェントの存在」と「プロセスの常駐」は分離(人格・記憶・会話は永続データ、コンテナは器)
- ユーザーごとの Linux ワークスペース(コンテナ)内で動き、ファイル・bash が自分の作業机
- ドメイン操作(ToDo、リマインダー等)は DB 直アクセス禁止。`contracts/openapi.yaml` 由来のクライアントで apps/api (Go) を叩く

### 1.2 接続トポロジ

```
web (React) ⇔ api (Go, WebSocketゲートウェイ) ⇔ agent (Rust, ユーザーごとのコンテナ)
                                                   ├── LLM プロバイダ (Kimi / GLM / Umans, OpenAI互換)
                                                   ├── ワークスペースFS + bash
                                                   └── ローカル SQLite (ログ・メモリ状態)
```

agent⇔api 間のイベントプロトコルは未定のため、**トレイト境界(`Gateway`)として切り**、contracts/ にイベントスキーマを置く前提の設計だけ提示する(第11章)。開発・デモ初期は同じトレイトの stdio (JSON Lines) 実装で CLI から直接会話できるようにし、api 側の進捗と切り離す。

### 1.3 プロセス内アーキテクチャ(データフロー)

```
                 ┌─────────────────────────────────────────────┐
 Gateway ──cmd──▶│ Session (会話の司令塔・状態機械)              │
 (stdio/WS)      │  ├─ steer/abort 制御 (CancellationToken)     │
   ▲             │  ├─ AgentLoop (ターン進行)                    │
   │             │  │   ├─ ContextAssembler (3層メモリ→prompt)  │
   └───event─────│  │   ├─ provider::stream (SSE→イベント)      │
                 │  │   └─ ToolRunner (承認フック+実行+切詰め)   │
                 │  ├─ MemoryMaintainer (非同期Compactワーカー)  │
                 │  └─ Store (SQLite: ログ全文+メモリ状態)       │
                 └─────────────────────────────────────────────┘
```

原則: **pi と同じく「イベントがすべての境界を流れる」**。プロバイダ層はストリーミングイベント(`ProviderEvent`)を吐き、エージェントループはそれを包んだライフサイクルイベント(`AgentEvent`)を吐き、Gateway はそれをシリアライズして外に流す。UI 状態・永続化・デバッグログすべてこのイベント列から導出する。**[事実]** pi のイベント体系(`pi:ai/src/types.ts:464-476` の `AssistantMessageEvent`、`pi:agent/src/types.ts:415-430` の `AgentEvent`)はこの二層構造であり、そのまま踏襲する。

### 1.4 pi に対する立ち位置(何を移し、何を変えるか)

| 領域 | pi | Sumi | 判断 |
|---|---|---|---|
| LLM配管 | 25+プロバイダ、10 API方言 | OpenAI互換 Chat Completions 1本 | 縮小移植 |
| ストリーミングイベント設計 | `AssistantMessageEvent` (contentIndex方式) | 同型を Rust enum で | **忠実移植** |
| エージェントループ | `agent-loop.ts` (steering/followUpキュー、フック) | 同型 + ハードステア追加 | 移植+拡張 |
| ステア | ターン境界注入のみ **[事実]** (`pi:agent/src/agent.ts:274-281` はキュー投入だけ) | abort+部分応答保持+再注入 | **自作**(第6章) |
| メモリ | 単発compaction(閾値超過時に同期要約) | 3層メモリ+非同期先回りCompact | **自作**(ただしバッチ境界・トークン見積・要約プロンプトの細部はpiから移植) |
| 永続化 | JSONLファイル | SQLite (sqlx) | 自作 |
| 権限 | 存在しない(公式に明言) | 承認フロー状態機械 | **自作**(pi の `beforeToolCall` block機構を土台に) |
| ツール | TypeBox スキーマ | serde + schemars | 同型 |
| ツール出力切詰め | 2000行/50KB、tail保持、全文テンポラリ退避 | 同値を移植 | **忠実移植** |
| リトライ/オーバーフロー判定 | 正規表現パターン集 | 同パターンを移植(Kimi/GLM分のみ+汎用) | 縮小移植 |

---

## 2. モジュール構成とクレート選定

### 2.1 クレート構成

単一バイナリクレート `sumi-agent` + 内部モジュール分割とする。**[推測]** ワークスペース分割(crates/)は M5 完了後にモジュール境界が安定してからで遅くない。ハッカソン中はコンパイル単位を1つに保ちビルドを単純にする。

`apps/agent/Cargo.toml` の依存(保守的定番のみ):

```toml
[dependencies]
anyhow = "1"                # main/バイナリ層のエラー
thiserror = "2"             # ライブラリ層の型付きエラー
tokio = { version = "1", features = ["rt-multi-thread", "macros", "signal", "process", "sync", "time", "io-util", "fs"] }
tokio-util = { version = "0.7", features = ["rt"] }   # CancellationToken
futures-util = "0.3"        # Stream 操作
reqwest = { version = "0.12", features = ["json", "stream", "rustls-tls"], default-features = false }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
toml = "0.9"                # 設定ファイル読込 (実装時に最新安定を確認)
schemars = "1"              # ツールパラメータの JSON Schema 導出 (TypeBox 相当)
sqlx = { version = "0.8", features = ["runtime-tokio", "sqlite", "migrate", "json", "chrono"] }
uuid = { version = "1", features = ["v7", "serde"] }  # v7: 時系列ソート可能ID
chrono = { version = "0.4", features = ["serde"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
async-trait = "0.1"
regex = "1"                 # retry/overflow パターン判定
# M5 で追加: tokio-tungstenite = "0.24" (WSゲートウェイ)
[dev-dependencies]
axum = "0.8"                # SSE フィクスチャ再生用モックサーバ (テスト専用)
```

**選定メモ**:
- SSE パーサは**自前実装**(~100行)。OpenAI 互換 SSE は `data: {json}\n\n` と `data: [DONE]` だけの単純形式で、`eventsource-stream` 等の外部クレートを足すより、reqwest の `bytes_stream()` の上に行バッファリングを書く方が制御しやすい(リトライ・abort・タイムアウトを一元管理できる)。**[推測]**
- partial JSON パーサ(ストリーミング中のツール引数の逐次パース)は既成クレートに定番がないため、pi の `parseStreamingJson` 戦略(`pi:ai/src/utils/json-parse.ts`)を自前移植する(第12章 #4)。
- トークナイザは**持たない**。pi 同様に文字数ヒューリスティック+API実測 usage による校正で賄う(第7.5節)。tiktoken系はKimi/GLMの語彙と一致せずどのみち不正確。**[事実]** pi も `estimateTokens`(chars/4)+直近 usage 実測で運用している(`pi:agent/src/harness/compaction/compaction.ts:169-197, 224-264`)。
- OpenAPI 生成クライアント: 現状 `contracts/openapi.yaml` は `/health` 1本のみ **[事実]**。当面は `apiclient` モジュールに reqwest の薄い手書きクライアントを置き、API が太り始めた時点で progenitor 等の導入を ADR 化する。**[要決定→第14章]**

### 2.2 モジュールツリー

```
apps/agent/src/
├── main.rs              # 起動、設定読込、Gateway選択、Session起動
├── config.rs            # 環境変数/設定ファイル (モデル、APIキー、ワークスペースパス、DB)
│
├── provider/            # ═══ LLM配管 (pi:ai の縮小移植) ═══
│   ├── mod.rs           # pub API: stream(model, context, opts) -> ProviderEventStream
│   ├── types.rs         # Message/ContentBlock/Usage/StopReason/ProviderEvent/ModelSpec/Compat
│   ├── request.rs       # Chat Completions リクエスト組立 (compat分岐、キャッシュ配慮)
│   ├── sse.rs           # SSE 行パーサ (bytes -> data ペイロード)
│   ├── assembler.rs     # chunk -> ContentBlock 組み立て (contentIndex管理、partialArgs)
│   ├── partial_json.rs  # 逐次JSONパース + repair (pi:json-parse.ts 移植)
│   ├── retry.rs         # リトライ可否判定 + 指数バックオフ
│   └── overflow.rs      # コンテキスト溢れ検出 (エラーパターン + usage判定)
│
├── agent/               # ═══ エージェントループ ═══
│   ├── mod.rs           # Session: 会話の司令塔 (状態機械、steer/abort、run管理)
│   ├── run.rs           # agent_loop: ターン進行 (pi:agent-loop.ts 移植)
│   ├── events.rs        # AgentEvent enum
│   ├── steer.rs         # ハードステア: abort→部分保持→再注入 (第6章)
│   └── queue.rs         # steering/followUp キュー (QueueMode)
│
├── memory/              # ═══ 3層メモリ (第7章) ═══
│   ├── mod.rs           # ThreeLayerMemory: 層状態 + ContextAssembler
│   ├── batch.rs         # L0バッチ分割 (境界規則)
│   ├── compactor.rs     # 非同期Compactワーカー (tokio task、棚への保存)
│   ├── overflow.rs      # 溢れ処理 (FIFO廃棄+昇格、ヒステリシス、適用タイミング)
│   └── estimate.rs      # トークン見積 + usage校正
│
├── tools/               # ═══ ツール ═══
│   ├── mod.rs           # Tool トレイト、ToolRegistry、TypedTool アダプタ
│   ├── fs.rs            # read/write/edit/ls/glob/grep
│   ├── bash.rs          # bash実行 (ストリーミング出力、打切り)
│   ├── truncate.rs      # head/tail切詰め (pi:truncate.ts 移植)
│   └── shell_capture.rs # ローリングバッファ+全文退避 (pi:shell-output.ts 移植)
│
├── approval/            # ═══ 権限承認 (第9章) ═══
│   ├── mod.rs           # ApprovalBroker: リクエスト発行/保留/裁定
│   └── policy.rs        # ツール別ポリシー (auto/ask/always-allow ルール)
│
├── store/               # ═══ 永続化 (第10章) ═══
│   ├── mod.rs           # Store: sqlx SQLite プール + マイグレーション
│   ├── transcript.rs    # チャットログ全文 (追記専用、検索)
│   └── memory_state.rs  # メモリ層スナップショット、バッチ、棚
│
├── gateway/             # ═══ 外界接続 (第11章) ═══
│   ├── mod.rs           # Gateway トレイト、Command/Envelope 型
│   ├── stdio.rs         # JSON Lines over stdin/stdout (開発・テスト用)
│   └── ws.rs            # WebSocket クライアント (api への接続、M5)
│
└── apiclient/           # contracts/openapi.yaml 由来の Go API クライアント (薄い手書き)
    └── mod.rs
```

依存方向(上→下のみ許可): `gateway`/`main` → `agent` → { `memory`, `tools`, `approval`, `provider` } → `store`/`types`。`provider` は他モジュールに依存しない純配管。

---

## 3. コアデータ型(pi 対照表付き)

### 3.1 メッセージとコンテンツブロック

**[事実]** pi の対応物: `pi:ai/src/types.ts:321-454`。

```rust
// provider/types.rs

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "snake_case")]
pub enum Message {
    User(UserMessage),
    Assistant(AssistantMessage),
    ToolResult(ToolResultMessage),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UserMessage {
    pub content: Vec<UserContent>,          // Text | Image
    pub timestamp: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AssistantMessage {
    pub content: Vec<AssistantContent>,     // Text | Thinking | ToolCall
    pub model: String,                      // 生成時のモデルID (クロスモデル再送判定に使う)
    pub provider: String,
    pub usage: Usage,
    pub stop_reason: StopReason,
    pub error_message: Option<String>,
    /// Sumi拡張: ハードステアで打ち切られた部分応答か (第6章)
    pub interrupted: bool,
    pub timestamp: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolResultMessage {
    pub tool_call_id: String,
    pub tool_name: String,
    pub content: Vec<UserContent>,          // Text | Image
    pub details: serde_json::Value,         // UI表示用の構造化データ (pi: details)
    pub is_error: bool,
    pub timestamp: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AssistantContent {
    Text { text: String },
    /// reasoning_content。signature_field は受信元フィールド名
    /// ("reasoning_content" | "reasoning" | "reasoning_text") を保持し、
    /// 再送時に同じフィールドへ書き戻す (pi: thinkingSignature の用法)
    Thinking { thinking: String, signature_field: String },
    ToolCall(ToolCall),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: serde_json::Value,       // 完成後は必ずObject
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason { Stop, Length, ToolUse, Error, Aborted }

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Usage {
    pub input: u64,          // 非キャッシュ入力 (prompt_tokens - cached)
    pub output: u64,
    pub cache_read: u64,
    pub cache_write: u64,
    pub reasoning: u64,      // output の内数
    pub total_tokens: u64,
}
```

**pi との差分と理由**:
- `ThinkingContent.thinkingSignature` → `signature_field` に改名。OpenAI互換系ではこのフィールドは「reasoning がどの JSON フィールドで届いたか」を記録して再送時に同じフィールドへ書き戻すために使われている **[事実]** (`pi:ai/src/api/openai-completions.ts:408-424, 996-1003`)。Anthropic の暗号署名の意味は Sumi には不要。
- `AssistantMessage.interrupted` は Sumi 拡張。pi は aborted メッセージを再送時に丸ごと捨てる **[事実]** (`pi:ai/src/api/transform-messages.ts:698-706`) が、Sumi のハードステアは部分応答を保持する必要があるため、「打ち切られたが再送対象」であることを示すフラグを持つ(第6章)。
- `api`/`responseId`/`diagnostics` フィールドは省略(単一APIなので不要。responseId はログにだけ流す)。

### 3.2 プロバイダイベント

**[事実]** pi の対応物: `AssistantMessageEvent` (`pi:ai/src/types.ts:464-476`)。contentIndex は `AssistantMessage.content` 配列内の位置で、UI とループが「どのブロックが今伸びているか」を追跡する要。

```rust
#[derive(Clone, Debug)]
pub enum ProviderEvent {
    Start,
    TextStart     { content_index: usize },
    TextDelta     { content_index: usize, delta: String },
    TextEnd       { content_index: usize, content: String },
    ThinkingStart { content_index: usize },
    ThinkingDelta { content_index: usize, delta: String },
    ThinkingEnd   { content_index: usize, content: String },
    ToolCallStart { content_index: usize },
    ToolCallDelta { content_index: usize, delta: String },   // 引数JSONの生delta
    ToolCallEnd   { content_index: usize, tool_call: ToolCall },
    Done  { reason: StopReason, message: AssistantMessage },  // Stop|Length|ToolUse
    Error { reason: StopReason, error: AssistantMessage },    // Error|Aborted
}
```

**pi との差分**: pi は全イベントに `partial: AssistantMessage`(組立途中のメッセージ全体)を同乗させる **[事実]**。Rust では毎イベントの clone が高くつくため、**ストリーム消費側(AgentLoop)が同じロジックでメッセージを組み立てる**方式にし、partial の同乗はやめる。組み立てロジックは `assembler.rs` に一元化し、プロバイダ層とループが同一の `MessageAssembler` 構造体を共有する(イベント列→メッセージの純関数として単体テスト可能にする)。**[推測]**

ストリームの型は `pi:ai/src/utils/event-stream.ts` の `EventStream`(push/AsyncIterator/最終結果Promise)に対応して:

```rust
pub struct ProviderEventStream {
    rx: tokio::sync::mpsc::Receiver<ProviderEvent>,
}
// 最終結果は Done/Error イベント自体が運ぶ (pi の result() Promise は不要:
// Rust では for-await ループの終端で最後のイベントから取り出す)
```

契約(pi と同一 **[事実]** `pi:ai/src/types.ts:301-313`): **stream 関数は決して panic/Err を返さない**。リクエスト失敗・モデルエラー・実行時失敗はすべてストリーム内の `Error` イベント(stopReason Error/Aborted + error_message 付き AssistantMessage)として届く。この一点が呼び出し側の異常系を劇的に単純化する。

### 3.3 エージェントイベント

**[事実]** pi の対応物: `AgentEvent` (`pi:agent/src/types.ts:415-430`)。

```rust
// agent/events.rs
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentEvent {
    AgentStart,
    AgentEnd,
    TurnStart,
    TurnEnd { message: Box<Message>, tool_results: Vec<ToolResultMessage> },
    MessageStart { message: Box<Message> },
    /// assistantストリーミング中のみ。ProviderEvent を素通しで包む
    MessageUpdate { event: ProviderEventJson },
    MessageEnd { message: Box<Message> },
    ToolExecutionStart { tool_call_id: String, tool_name: String, args: serde_json::Value },
    ToolExecutionUpdate { tool_call_id: String, partial: serde_json::Value },
    ToolExecutionEnd { tool_call_id: String, result: serde_json::Value, is_error: bool },
    // ═══ Sumi 拡張 ═══
    ApprovalRequested { request: ApprovalRequest },            // 第9章
    ApprovalResolved { request_id: String, decision: ApprovalDecision },
    Steered { mode: SteerMode },                               // 第6章 (UI通知用)
    MemoryMaintenance { kind: MemoryMaintKind },               // デバッグ/可観測性用
    Error { message: String },
}
```

イベント順序の契約(pi と同一 **[事実]** `pi:agent/src/agent-loop.ts:109-274` 実読より):

```
AgentStart → TurnStart
  → (注入メッセージごとに MessageStart/MessageEnd)
  → MessageStart(assistant) → MessageUpdate* → MessageEnd(assistant)
  → [ツールがあれば] (ToolExecutionStart → ToolExecutionUpdate* → ToolExecutionEnd
                       → MessageStart/End(toolResult)) × N
  → TurnEnd
→ (ツール継続 or steering あり) TurnStart … 繰り返し
→ AgentEnd
```

### 3.4 Context とツール定義

**[事実]** pi の対応物: `Context` (`pi:ai/src/types.ts:450-454`)、`AgentTool` (`pi:agent/src/types.ts:373-396`)。

```rust
// provider に渡す最終形 (3層メモリが組み立てる)
pub struct PromptContext {
    pub system_prompt: String,          // 憲法。不変
    pub messages: Vec<Message>,         // L2/L1注入済み + L0生messages
    pub tools: Vec<ToolDefinition>,     // 凍結原則
}

pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,  // JSON Schema (schemars 生成)
}

// tools/mod.rs
#[async_trait]
pub trait Tool: Send + Sync {
    fn def(&self) -> ToolDefinition;
    /// 承認要否のヒント (approval::policy が最終判断)
    fn risk(&self) -> ToolRisk;         // ReadOnly | Mutating | Exec
    async fn execute(&self, ctx: ToolCtx<'_>) -> Result<ToolOutput, ToolError>;
}

pub struct ToolCtx<'a> {
    pub call_id: &'a str,
    pub args: serde_json::Value,        // スキーマ検証済み
    pub cancel: CancellationToken,      // abort 伝播
    pub on_update: &'a dyn Fn(serde_json::Value), // ストリーミング部分結果 (pi: onUpdate)
    pub workspace: &'a WorkspacePaths,
}

pub struct ToolOutput {
    pub content: Vec<UserContent>,      // モデルに返る本文 (切詰め済み)
    pub details: serde_json::Value,     // UI用
}
```

型付きツールは薄いアダプタで包む(TypeBox → schemars の対応):

```rust
pub struct TypedTool<P: JsonSchema + DeserializeOwned> { /* name, desc, f */ }
// def(): schemars::schema_for!(P) から parameters を導出
// execute(): serde_json::from_value::<P>(args) → 型付きハンドラ呼び出し
```

**引数検証の方針**: pi は TypeBox でスキーマ検証+型強制(数値文字列→数値等)を行う **[事実]** (`pi:ai/src/utils/validation.ts:1148-1180`、`Value.Convert` と自前 coercion)。Rust では `serde_json::from_value` のデシリアライズ失敗をそのまま検証エラーとし、**エラーメッセージにスキーマと受信引数を添えてツール結果 (is_error=true) としてモデルに返す**(pi と同じ回復パターン。モデルが自分で修正して再発行する)。数値/真偽の文字列からの弱い型強制は serde の `#[serde(deserialize_with)]` ヘルパを1つ用意して主要ツールに適用する。**[推測]** Kimi/GLM は比較的正しい JSON を吐くため優先度は低いが、pi がこの coercion を持っているのは実運用で踏んだ証拠なので、M1 の実測でエラー頻度を見て判断。

### 3.5 pi 対照表(型サマリ)

| Sumi (Rust) | pi (TS) | 出典 | 備考 |
|---|---|---|---|
| `Message` enum | `Message` union | `pi:ai/src/types.ts:419` | 同型 |
| `AssistantContent::Thinking.signature_field` | `ThinkingContent.thinkingSignature` | 同 :333-341 | 意味を「受信フィールド名」に限定 |
| `AssistantMessage.interrupted` | (なし) | — | Sumi拡張(ステア) |
| `ProviderEvent` | `AssistantMessageEvent` | 同 :464-476 | partial 同乗を廃止 |
| `MessageAssembler` | stream() 内のクロージャ群 | `pi:ai/src/api/openai-completions.ts:229-344` | 純関数化して共有 |
| `AgentEvent` | `AgentEvent` | `pi:agent/src/types.ts:415-430` | Approval系を追加 |
| `Tool` trait | `AgentTool` | 同 :373-396 | executionMode は当面 sequential 固定 |
| `ToolCtx.on_update` | `AgentToolUpdateCallback` | 同 :370 | |
| `PromptContext` | `Context` | `pi:ai/src/types.ts:450` | |
| `Usage` | `Usage` | 同 :357-378 | cost 計算は M2 以降(ログのみ) |
| `StopReason` | `StopReason` | 同 :380 | 同一 |
| `ProviderEventStream` | `AssistantMessageEventStream` | `pi:ai/src/utils/event-stream.ts` | mpsc で代替 |
| `QueueMode` (all / one-at-a-time) | `QueueMode` | `pi:agent/src/types.ts:49` | 既定 one-at-a-time **[事実]** `pi:agent/src/agent.ts:222-223` |

---

## 4. プロバイダ層仕様(`provider/`)

対応 API は **OpenAI 互換 Chat Completions(SSE ストリーミング)のみ**。想定エンドポイント:

| プロバイダ | base_url | モデル | 備考 **[事実]**(調査レポート+pi モデルメタ) |
|---|---|---|---|
| Moonshot (Kimi) | `https://api.moonshot.ai/v1` | `kimi-k3` (1M ctx / 131k out), `kimi-k2.7-code` (256k) | 自動プレフィックスキャッシュ(明示API不要)。reasoning は Preserved Thinking 常時ON |
| Z.ai (GLM) | `https://api.z.ai/api/paas/v4` | `glm-5.2` (1M ctx / 128k out) | `tool_stream: true` でツールコールもストリーミング。定額プランはバックエンド利用禁止→従量API必須 |
| Umans | `https://api.code.umans.ai/v1` | `umans-kimi-k2.7`, `umans-glm-5.2`, `umans-flash` | 開発時の保険。同時4セッション制限 |

### 4.1 ModelSpec と Compat フラグ

pi の教訓の核心: **OpenAI「互換」は互換ではない**。pi は URL からの自動検出+モデル別上書きで方言を吸収する **[事実]** (`pi:ai/src/api/openai-completions.ts:1237-1320` の `detectCompat`)。Sumi は対象3系だけなので自動検出は持たず、**設定ファイルに明示する Compat 構造体**に絞る:

```rust
pub struct ModelSpec {
    pub id: String,               // "kimi-k3"
    pub base_url: String,
    pub api_key_env: String,      // "MOONSHOT_API_KEY" 等
    pub context_window: u64,
    pub max_tokens: u64,
    pub reasoning: bool,
    pub compat: Compat,
}

pub struct Compat {
    /// "max_tokens" | "max_completion_tokens"。Kimi/GLM とも max_tokens
    pub max_tokens_field: MaxTokensField,
    /// stream_options: {include_usage:true} を送るか。既定 true
    pub supports_usage_in_streaming: bool,
    /// thinking パラメータの方言
    pub thinking_format: ThinkingFormat,   // Off | Deepseek | Zai | OpenAIEffort
    /// 再送する全 assistant メッセージに reasoning_content:"" を要求 (Kimi)
    pub requires_reasoning_content_on_assistant: bool,
    /// GLM: tool_stream:true を送る
    pub zai_tool_stream: bool,
    /// tools[].function.strict を送るか (Kimi は不可)
    pub supports_strict_mode: bool,
    /// store:false を送るか (OpenAI 本家のみ true)
    pub supports_store: bool,
    /// system か developer ロールか。Kimi/GLM とも system
    pub supports_developer_role: bool,
}
```

初期プリセット(**[事実]** pi の生成メタデータより移植):

- `kimi-k3` (`pi:ai/src/providers/moonshotai.models.ts:171-189`): `thinking_format=Deepseek`(`thinking: {"type":"enabled"}`)、`requires_reasoning_content_on_assistant=true`、`max_tokens_field=max_tokens`、`supports_strict_mode=false`、`supports_store=false`、`supports_developer_role=false`
- `glm-5.2` (`pi:ai/src/providers/zai.models.ts:79-98`): `thinking_format=Zai`(`thinking: {"type":"enabled","clear_thinking":false}` + `reasoning_effort` 対応)、`zai_tool_stream=true`、`supports_store=false`、`supports_developer_role=false`
- Umans: OpenAI互換を名乗るが実体は上記モデルのプロキシ。**M1 の実測で決める**(まず Kimi/GLM 相当のプリセットを試す)。**[推測]**

pi から**移植しないもの**: Anthropic 型 cache_control(Kimi/GLM は自動キャッシュ)、prompt_cache_key(OpenAI 本家専用)、session affinity ヘッダ、deferredToolsMode "kimi"(ツール凍結原則により遅延ロード不使用)、OpenRouter/Vercel ルーティング、25方言の thinkingFormat のうち上記2種以外。

### 4.2 リクエスト組立(`request.rs`)

`PromptContext` → Chat Completions JSON への変換。**[事実]** 以下すべて `pi:ai/src/api/openai-completions.ts` の `buildParams`/`convertMessages`(:575-1150)からの移植項目:

1. **system prompt**: `{"role":"system","content":...}` を先頭に。L2/L1 メモリブロックはこの後に追加 system メッセージとして続ける(第7章)
2. **assistant content は常にプレーン文字列で送る**(content-block 配列にしない)。配列で送ると一部モデルが構造を鸚鵡返しする事故がある(:987-994 コメント)
3. **thinking の再送**: 同一モデルなら `signature_field` が示すフィールド(`reasoning_content` 等)へ全ターン分を書き戻す。**Kimi は過去全ターンの reasoning 保持が必須仕様**(調査レポート)。クロスモデル切替時は thinking をプレーンテキストに落とすか捨てる(`pi:ai/src/api/transform-messages.ts:609-626` の分岐を移植)
4. **`requires_reasoning_content_on_assistant`**: 再送する assistant メッセージに reasoning_content が無ければ `""` を補う(:1038-1044)
5. **tool_calls**: `{id, type:"function", function:{name, arguments: JSON文字列}}`。引数は必ず `serde_json::to_string` で直列化
6. **tool ロール**: `{"role":"tool","content":text,"tool_call_id":...}`。テキストが空で画像のみなら `"(see attached image)"`、両方空なら `"(no tool output)"` のプレースホルダ(:1073-1075)
7. **ツール結果内の画像**: tool メッセージには載らないため、直後に user メッセージ `"Attached image(s) from tool result:"` + image_url ブロックとして追送(:1109-1127)。※ Kimi K3 は image 入力可、GLM-5.2 text のみ **[事実]**(モデルメタ)。非対応モデルにはプレースホルダテキストに差替(`transform-messages.ts:521-566`)
8. **空 assistant のスキップ**: content も tool_calls も無い assistant メッセージは送らない(aborted 応答の残骸対策、:1045-1056)
9. **tools が空でも履歴にツールコールがあるなら `"tools": []` を送る**(プロキシ互換、:625-628)。※ Sumi はツール凍結原則なので通常発生しないが移植しておく
10. **サニタイズ**: 送信テキスト全部に不対サロゲート除去を適用。Rust の `String` は常に正しい UTF-8 なので pi の `sanitizeSurrogates` 相当は**受信側**(ツール出力のバイト列→String 変換時の `from_utf8_lossy`)で保証する。加えて `serde_json` は文字列中の生制御文字を正しくエスケープするため pi の repairJson 送信側問題は起きない **[推測、M1で確認]**
11. **stream_options**: `{"include_usage": true}`(compat で無効化可能)
12. **max_tokens / temperature / tool_choice**: オプション透過

### 4.3 SSE 受信とメッセージ組立(`sse.rs` + `assembler.rs`)

**[事実]** 組立ロジックの原典: `pi:ai/src/api/openai-completions.ts:229-511`。移植必須の細部:

- **ブロック管理**: `tool_calls[].index` による Map と `id` による Map の**二重引き**(:239-241, 307-344)。プロバイダによって index だけ・id だけ・両方が来るため。text/thinking ブロックは「現在開いているブロック」1個ずつを保持し、種類が切り替わったら閉じずに保持(同種 delta の続きが来たら継続)
- **ツール引数の逐次パース**: delta 到着ごとに `partial_args` 文字列へ追記し、`partial_json::parse_streaming` で「常に何かしらのオブジェクト」を得る(UI のツール進行表示用)。確定は `ToolCallEnd` 時に repair 付き厳密パース(:263-274)
- **reasoning フィールド検出**: delta 内の `reasoning_content` → `reasoning` → `reasoning_text` の順で**最初に見つかった非空フィールドだけ**採用(重複返却プロバイダ対策、:394-424)。採用フィールド名を `signature_field` に記録
- **usage**: `chunk.usage` を都度上書き。**Moonshot は `choices[0].usage` に入れてくる**フォールバックを移植(:362-366)。`prompt_tokens_details.cached_tokens` → cache_read、`completion_tokens_details.reasoning_tokens` → reasoning。`input = prompt_tokens - cached - cache_write`(:1168-1204)
- **finish_reason マップ**(:1206-1230): `stop|end→Stop`, `length→Length`, `tool_calls|function_call→ToolUse`, `content_filter→Error`, その他→Error(メッセージに finish_reason 原文を残す)
- **異常終了の検出**: ストリームが finish_reason 無しで終わったら `"Stream ended without finish_reason"` エラー(:482-484)。abort シグナル済みなら Aborted
- **エラー時のブロック掃除**: エラー確定時、組立途中の scratch(partial_args 等)は最終メッセージに残さない(:489-494)
- **`responseId`/`responseModel`**: chunk.id / chunk.model をログ用に記録(:350-354)

SSE 層(`sse.rs`)の仕様: reqwest の `bytes_stream()` を行分割し、`data: ` プレフィックスの JSON をイベントとして yield。`data: [DONE]` で終端。**HTTP レベルの失敗(非2xx)はボディを最大4000字で切り詰めてエラーメッセージ化**(**[事実]** `pi:ai/src/utils/error-body.ts:758` の `MAX_PROVIDER_ERROR_BODY_CHARS=4000` を踏襲。ステータス+ボディを `"{status}: {body}"` 形式で)。アイドルタイムアウト(チャンク間 120s、`tokio::time::timeout`)を仕込む **[推測]**(pi は SDK 任せ。長命プロセスでは必須)。

### 4.4 リトライ(`retry.rs`)

**[事実]** pi の実装: 判定は `pi:ai/src/utils/retry.ts`、ポリシーは `pi:coding-agent/src/core/agent-session.ts:2606-2673`。

- **判定**: error_message に対する正規表現2段構え。(a) 非リトライパターン(quota/billing/insufficient_quota 等)に該当→リトライしない。(b) リトライパターン(overloaded, rate limit, 429/500/502/503/504/524, timeout, connection系, "ended without", "try your request again" 等)に該当→リトライ。**コンテキスト溢れはリトライではなく溢れ処理へ回す**(先に `overflow::is_context_overflow` を判定)
- **ポリシー**: 最大3回、指数バックオフ 2s/4s/8s(pi 既定値)。バックオフ待機は CancellationToken で中断可能(ステア/abort が来たら即やめる)
- **実施位置**: プロバイダ層ではなく**エージェントループ側**(pi と同じ)。Error 停止した assistant メッセージを**コンテキストから取り除き**(ログには残す)、同じコンテキストで再ストリーム(`pi:agent-session.ts:2646-2650` の「state からは除去、session 履歴には保持」を Store 設計に反映)

### 4.5 コンテキスト溢れ検出(`overflow.rs`)

**[事実]** `pi:ai/src/utils/overflow.ts:379-501` から Sumi に関係するパターンのみ移植:

- エラーメッセージパターン: `exceeded model token limit`(Kimi)、`exceeds the context window` / `maximum context length`(OpenAI系プロキシ・Umans想定)、`context_length_exceeded` / `too many tokens` / `token limit exceeded`(汎用)
- **z.ai は溢れをエラーにせず黙って受けることがある** → 成功応答でも `usage.input + cache_read > context_window` なら溢れ扱い(:483-488)
- 非溢れ除外パターン(rate limit / too many requests)を先に判定(:415-419)
- 検出時の動作: リトライせず、3層メモリの緊急溢れ処理(第7.6節)を即時適用して再送

Sumi では 3層メモリが常時 70k 以内に抑えるため溢れは本来起きない(1M ctx モデル)。この検出は**保険+メモリバグの検知器**として入れ、発火したら `tracing::error!` で警報する。

---

## 5. エージェントループ仕様(`agent/`)

### 5.1 ループ本体(`run.rs`)

**[事実]** 原典: `pi:agent/src/agent-loop.ts:155-275` の `runLoop`。構造をそのまま移す:

```
外側ループ (follow-up 継続):
  内側ループ (ツール継続 or 注入メッセージあり):
    TurnStart
    注入待ちメッセージがあれば context に追加し Message イベント発行
    assistant 応答をストリーム (→ 3層メモリが直前に組んだ PromptContext)
    stopReason が Error/Aborted → リトライ判定 → 不可なら TurnEnd + AgentEnd で脱出
    ツールコールがあれば:
      stopReason==Length なら全ツールを「引数切断の恐れ」で一括失敗させる
      それ以外は順次実行 (承認フック込み)
      結果を context に追加
    TurnEnd
    steering キューを drain → あれば注入して継続
  follow-up キューを drain → あれば注入して継続、なければ AgentEnd
```

移植必須の細部:

- **Length 停止時のツール一括失敗 [事実]** (`pi:agent-loop.ts:207-215, 383-408`): 出力トークン上限で切れたメッセージのツールコールは、partial JSON サルベージにより「パースは通るが黙って不完全」な引数を持ちうる。1つも実行せず全部に `"Tool call was not executed: the response hit the output token limit..."` のエラー結果を返し、モデルに再発行させる
- **ツール実行は当面 sequential 固定**。pi の parallel モード(:491-556)は準備だけ順次・実行は並行だが、Sumi は承認フローが挟まるため M5 まで順次で十分。`Tool::risk` と将来の `execution_mode` で拡張余地だけ残す **[推測]**
- **steering ポーリング位置 [事実]** (:167, :259): ループ開始時(送信待ち中に打った分)と各 TurnEnd 後。Sumi のソフトステア(第6章)はこの機構をそのまま使う
- **キュー既定 one-at-a-time [事実]** (`pi:agent.ts:222-223`): 複数の割込みを1個ずつ消化し、各々に応答機会を与える
- **ツール結果メッセージの生成** (:774-787): `content: result.content ?? []` の null 正規化を含む
- **実行中 abort**: 各ツールに CancellationToken を渡し、`prepareToolCall` 後・実行後の2箇所で aborted チェック(:626-651)。abort されたら残りのツールは "Operation aborted" のエラー結果

### 5.2 Session(司令塔、`agent/mod.rs`)

**[事実]** 原典: `pi:agent/src/agent.ts` の `Agent` クラス。Rust では:

```rust
pub struct Session {
    state: SessionState,               // Idle | Streaming { cancel: CancellationToken }
    memory: ThreeLayerMemory,
    tools: ToolRegistry,
    approval: ApprovalBroker,
    store: Store,
    steering_q: MessageQueue,
    followup_q: MessageQueue,
    events_tx: mpsc::Sender<AgentEvent>,
}

impl Session {
    /// Gateway からのコマンドを1本の mpsc で受ける (actor パターン)
    pub async fn run(mut self, mut commands: mpsc::Receiver<Command>) { ... }
}
```

pi は JS 単線スレッドで `Agent` のメソッドを直接叩くが、Rust では **actor パターン**(コマンドチャネル1本 + イベントチャネル1本)にする。Gateway・Compactワーカー・ツール実行が並行に走るため、Session の可変状態はタスク1個に閉じ込める。**[推測]**

pi から移す挙動:
- **実行中の prompt() は拒否**(:337-345)。Sumi では「Streaming 中の user_message コマンド = ステア」と解釈するので UI からはエラーにならない(第6章)
- **run 失敗時の合成エラーメッセージ [事実]** (:494-510): ループが予期せず落ちたら stopReason=Error の assistant メッセージを合成してイベント列を正常形(MessageStart/End → TurnEnd → AgentEnd)で閉じる。**イベント消費者は「必ず正常形で閉じる」ことに依存してよい**という契約
- `waitForIdle` 相当: run 完了の通知(watch チャネル)

### 5.3 履歴再送時の正規化(transform)

**[事実]** 原典: `pi:ai/src/api/transform-messages.ts`。API コール直前に L0 へ適用する純関数として移植:

1. **孤児ツールコールへの合成結果**(:667-729): assistant のツールコールに対応する toolResult が無い場合(abort・クラッシュ・ステア切断)、`"No result provided"` の is_error 結果を合成して挿入。**user メッセージがツールフローを分断した位置にも挿入**。会話末尾の未解決分も同様
2. **Error/Aborted assistant のスキップ**(:698-706): 再送しない。**ただし Sumi 拡張: `interrupted=true` のものは除く**(第6章のステア部分応答。テキスト/thinking は保持し、未完了ツールコールブロックだけ落とす)
3. **クロスモデル thinking 降格**(:609-626): モデル切替後は thinking をテキスト化 or 破棄

---

## 6. ステア仕様(`agent/steer.rs`)— Sumi の独自領域 (1/3)

### 6.1 要件の再確認

画面構成書のコンポーザー状態機械 **[事実]**(docs/screen-composition.md:104-119): streaming 中も入力欄は生きており、「テキストを打って送信 = ステア(割り込み)。エージェントは現在の生成を中断し新入力を注入して継続」。ステアメッセージも通常のユーザー吹き出しとして履歴に残る。

pi の `steer()` は**キュー投入のみ**で、注入は「現在のツールバッチ完了後・次ターン開始前」**[事実]**(`pi:agent/src/agent.ts:274-281` + `agent-loop.ts:259`)。生成中の即時割り込みには `abort()`(部分応答は再送時に捨てられる)しかない。**Sumi 要件はターン境界注入では不十分**であり、abort + 部分応答保持 + 再注入を自作する。

### 6.2 二段構えの設計

ステアコマンド受信時、Session の状態で分岐:

| 状態 | 動作 | 名称 |
|---|---|---|
| Idle | 通常の prompt | — |
| Streaming(assistant 生成中) | **ハードステア**: cancel 発火 → 部分応答を確定 → 注入 → ループ再開 | hard |
| Streaming(ツール実行中) | **ソフトステア**: キュー投入(pi 方式)。実行中ツールは完走させ、次の API コール前に注入 | soft |

ツール実行中もハードにする(ツールを殺す)選択肢は、bash 実行の途中殺しが副作用を持つため既定にしない。**[要決定→第14章]** UI から「停止ボタン([■])」は別コマンド `abort` で、こちらはツールも殺す(CancellationToken 一斉発火)。

### 6.3 ハードステアのシーケンス

```
1. cancel.cancel()                    # reqwest リクエストが drop され SSE が切れる
2. assembler が組立途中のメッセージを確定:
   - 完了済み Text/Thinking ブロック: そのまま保持
   - 途中の Text/Thinking ブロック: そこまでの内容で閉じる
   - ToolCall ブロック: JSON が完結していても実行前なら全部**破棄**
     (実行に入っていないツールコールを「やったこと」として履歴に残さないため)
   - stop_reason = Aborted, interrupted = true
3. 部分メッセージを L0 + Store に記録 (MessageEnd イベント発行)
4. UI 通知: Steered { mode: Hard }
5. steering メッセージを user メッセージとして L0 に追加
6. ループ再開 (次の API コールへ)。再送時 transform が:
   - interrupted メッセージのテキスト/thinking を assistant メッセージとして再送
     (Kimi の reasoning_content 再送要件も満たす)
   - 末尾に「[この応答はユーザーの割り込みにより中断された]」マーカーテキストを付加
     し、モデルが「自分は途中で止められた」と認識できるようにする [推測、プロンプト実験で調整]
```

**設計根拠**: pi の transform は aborted を捨てる(第5.3節)が、それは「途中応答はノイズ」というコーディングエージェントの割切り。秘書エージェントでは「言いかけたこと」は会話の実体であり、ユーザーもそれを見た上で割り込んでいる。UI に見えているものと L0 が一致することが人格の連続性に直結する。

**注意点(実装時に必ずテスト)**:
- 部分 assistant(tool_calls なし)→ user の並びは OpenAI 互換的に合法。ただし**空文字 content の assistant は送らない**(第4.2-8 のスキップ規則が interrupted にも効く: テキストも thinking も空なら保持せず捨てる)
- thinking だけ生成して本文ゼロで割り込まれたケース: Kimi では reasoning_content のみの assistant 再送が受理されるか **[未検証→M2 検証ゲート]**。拒否されるならテキストに `"(応答準備中に中断)"` を補う
- ステア直後の API コールはプレフィックスキャッシュが「中断メッセージ挿入点」まで効く(末尾追記のみなので実質全ヒット)

### 6.4 abort(停止ボタン)

`abort` コマンド: cancel 一斉発火 → 実行中ツールへ伝播(bash は子プロセス kill、`tokio::process` の kill_on_drop + プロセスグループ)→ 部分応答はハードステアと同じ規則で確定・保持(interrupted=true)→ **再開はしない**(Idle へ)。pi の `agent-session.ts:1530-1535`(abortRetry → agent.abort → waitForIdle)と同じ「リトライ待機も殺す」順序を踏襲 **[事実]**。

---

## 7. 3層メモリ仕様(`memory/`)— Sumi の独自領域 (2/3)

docs/agent/memory.md(Draft v1)を実装仕様に落とす。**pi に相当機構は存在しない**(調査レポートで確定)が、バッチ境界規則・トークン見積・要約プロンプトの3点は pi の compaction 実装から流用できる。

### 7.1 プロンプト構成と各層の表現

```
[0] system: 憲法 (不変。人格核・行動規範)
[1] system: "# 遠い記憶 (統合済み)\n..."   ← L2 (~10k)   変更頻度: 最低
[2] system: "# 中間の記憶\n## <期間>\n..." ← L1 (~15k)   変更頻度: 低
[3...] 生 messages                          ← L0 (~40k)   末尾追記が基本
tools: 凍結 (変更はキャッシュ全壊)
```

- L2/L1 は**追加の system ロールメッセージ**として注入する。理由: user/assistant ロールだと会話の実体と混ざり、モデルが「言った/聞いた」と誤認するリスクがある。複数 system メッセージの受理は Kimi/GLM で **[未検証→M4 検証ゲート]**。拒否されたら role=user + `<memory>` タグ包みにフォールバック(フォールバックは Compat フラグ化)
- プレフィックスキャッシュ整合(調査レポートの一般原則): 照合は先頭からの連続一致なので、**揮発性の低い順に並ぶこの構成は通常ターンでほぼ全ヒット**。L0 先頭バッチ廃棄時のみ L1 以降(~35k)が再読み込みになる
- 憲法・ツール定義は起動時にハッシュを取り、変更検知したら `tracing::warn!`(キャッシュ全壊の可視化)

### 7.2 状態モデル

```rust
pub struct ThreeLayerMemory {
    l2: ConsolidatedMemory,       // 統合済みテキスト1本 + トークン数
    l1: VecDeque<L1Entry>,        // Compact済み要約 (バッチ由来)。FIFO
    l0: VecDeque<L0Batch>,        // 生messagesのバッチ列。最後尾のみ open
    shelf: HashMap<BatchId, CompactResult>,  // 棚: 先回りCompactの成果物
    calib: TokenCalibration,      // 見積校正 (7.5節)
    pending_apply: bool,          // 溢れ処理の適用予約フラグ
}

pub struct L0Batch {
    pub id: BatchId,              // uuid v7
    pub messages: Vec<Message>,
    pub est_tokens: u64,
    pub state: BatchState,        // Open | Sealed | Compacting | Compacted
}

pub struct L1Entry {
    pub source_batch: BatchId,
    pub summary: String,          // Compact結果
    pub est_tokens: u64,
    pub time_range: (DateTime<Utc>, DateTime<Utc>),  // 「いつの記憶か」を要約ヘッダに刻む
}
```

サイズ定数(設定で可変、既定値は memory.md 準拠):

```
L0_BATCH_MIN   = 5_000    # バッチ確定の下限
L0_LIMIT       = 40_000   # L0溢れ発火点
L0_DROP_TO     = 30_000   # ヒステリシス: ここを下回るまでFIFO廃棄 [推測、実測調整]
L1_LIMIT       = 15_000
L1_DROP_TO     = 11_000
L2_LIMIT       = 10_000
```

### 7.3 バッチ分割(`batch.rs`)

- open バッチにメッセージを追記し、`est_tokens >= L0_BATCH_MIN` に達したら**次の「きりのいい境界」で seal**
- きりのいい境界の定義 **[事実]**(pi の cut point 規則を採用、`pi:agent/src/harness/compaction/compaction.ts:265-303`): **user または assistant メッセージの直前のみ**。toolResult の直前では切らない(assistant のツールコールと結果が別バッチに泣き別れると、Compact 入力も再送プレフィックスも壊れるため)。Sumi 追加規則: **interrupted な assistant とそれに続く steering user メッセージの間でも切らない**(中断文脈の一体性)
- thinking ブロックはバッチのトークン計算に**含める**(Kimi では実際に再送されるため。memory.md 未決事項への回答)
- seal と同時に `compactor` へ非同期ジョブ投入(7.4節)し、状態を Compacting に

### 7.4 先回り Compact(`compactor.rs`)

- tokio task のワーカー1本 + mpsc ジョブキュー。**メインの会話経路とは完全非同期**(TTFT に乗せない)
- Compact 呼び出しは通常会話と同じ provider 層を使うが、**別モデル指定可**(既定: 会話と同じモデル。安価な `umans-flash`/`kimi-k2.7` への切替を設定で許す)**[要決定→第14章]**
- プロンプト: pi の構造化チェックポイント形式 **[事実]**(`pi:compaction.ts:383-457` の SUMMARIZATION_PROMPT / UPDATE_SUMMARIZATION_PROMPT)を秘書ドメインに書き換える。骨子:

```
system: あなたは記憶の圧縮係。会話を続けるな。要約だけ出力せよ。
user: <conversation>バッチ内容の直列化</conversation>
      <recent-memory>L1末尾エントリ (読み取り専用の文脈)</recent-memory>   ← memory.md 未決事項「Compactの入力」への対応
      指定フォーマット:
      ## 出来事           (何が起き、何を話したか。時刻付き)
      ## ユーザーについて分かったこと (好み・事実・関係性)
      ## 約束・宿題        (やると言ったこと、期限)
      ## 参照             (ワークスペースに書いたメモのパス、調べれば分かること)
      目標圧縮率: 入力の 1/8〜1/15、上限 800 トークン程度 [推測、実測調整]
```

  圧縮率の明示指定は memory.md 未決事項「Mastra で ~50倍圧縮されすぎ問題」への直接対応。max_tokens でも物理上限を掛ける(pi は reserveTokens×0.8 を maxTokens に指定 **[事実]** :470-473)
- 結果は `shelf` と Store に保存。**この時点では L0 から消さない**(先回り原則)
- 失敗時: リトライ2回、それでも駄目なら shelf に「未Compact」マークを残し、溢れ処理時に同期フォールバック(その場で Compact。このときだけ遅延が出る)。Compact 失敗でも会話は止めない
- ワーカーは Umans の同時4セッション制限を食う点に注意(会話ストリーム+Compact で2本)**[事実]**(調査レポート)

### 7.5 トークン見積と校正(`estimate.rs`)

pi の `estimateTokens` は chars/4 **[事実]**(`pi:compaction.ts:224-264`)だが、これは英語前提。日本語は 1トークン≈1〜2文字であり 4倍過小評価になる。Sumi 方式:

```
est(text) = ascii_chars / 4 + non_ascii_chars / 1.5   # 初期係数 [推測]
```

さらに **API 実測 usage で自己校正**する。pi の `estimateContextTokens` **[事実]**(:169-197)は「最後の assistant usage を錨とし、それ以降のメッセージだけ見積る」ハイブリッド方式。Sumi はこれを進めて:

- 毎 API 応答で `usage.input + cache_read`(=プロンプト全体の実トークン)を取得
- `実測 − (憲法+tools+L2+L1 の前回実測差分)` と L0 見積合計を比較し、補正係数 `calib.ratio` を EMA 更新
- 層のサイズ判定は `est × ratio` で行う。これで境界判定の誤差が実測に吸着する

### 7.6 溢れ処理(`memory/overflow.rs`)

1. **検知**: L0 追記のたびに `Σ est > L0_LIMIT` を確認 → `pending_apply = true` を立てるだけ(即時には何もしない)
2. **適用タイミング**: 次の API コール直前(ContextAssembler 内)。ただし**「ユーザーメッセージ起点の最初のコール」ではスキップ**(TTFT保護)。ツールコール継続・ステア再開・follow-up 起点のコールで適用。例外: `Σ est > L0_LIMIT × 1.2`(ハード上限)に達したら無条件適用 **[推測、係数は実測調整]**
3. **L0→L1**: 先頭から Sealed/Compacted バッチを `Σ est ≤ L0_DROP_TO` になるまで廃棄し、対応する shelf の要約を L1 末尾へ。shelf 未完(Compacting 中)のバッチに当たったら、(a) 完了を待たずそこで止める(次回コールで続き)、(b) ハード上限超過時のみ同期待ち。**open バッチは絶対に廃棄しない**
4. **L1→L2**: L1 溢れも同じ形。L1 エントリを古い順にまとめて(~4k分)「要約の要約」ジョブを非同期投入 → 完了後の次回適用で L1 から除去し L2 末尾へ連結
5. **L2 統合**: L2 が 10k 超過 → L2 全文を LLM で統合置換(非同期、完了後の次回適用で差替)。統合プロンプトは「古い記憶ほど粗く、人物像・長期の約束・関係性を優先して残す」
6. 全処理で `MemoryMaintenance` イベントを発行(デバッグ画面・検証ゲートの観測点)

### 7.7 ContextAssembler(API コール直前の一本道)

```
fn assemble(&mut self) -> PromptContext:
  1. pending_apply なら溢れ処理適用 (7.6-2 の条件判定込み)
  2. messages = concat(L2ブロック, L1ブロック, L0全バッチのmessages)
  3. transform適用 (孤児ツール結果合成・interrupted処理・Error/Abortedスキップ) ← 第5.3節
  4. PromptContext { 憲法, messages, tools凍結 }
```

transform は**送信用のビューを作る純関数**であり、L0 の保存形は変えない(ログと記憶の分離)。

---

## 8. ツールとワークスペース(`tools/`)

### 8.1 初期ツールセット

ワークスペース(コンテナ内 FS)+bash。コーディングエージェントではないが道具は同型:

| ツール | risk | 説明 |
|---|---|---|
| `read_file` | ReadOnly | パス+offset/limit。head 切詰め |
| `write_file` | Mutating | 全置換書込み |
| `edit_file` | Mutating | old_string/new_string 置換(一意性検査) |
| `list_dir` / `glob` | ReadOnly | |
| `grep` | ReadOnly | ripgrep 呼び出し(コンテナに同梱)。行長500字切詰め **[事実]**(`pi:truncate.ts:GREP_MAX_LINE_LENGTH`) |
| `bash` | Exec | ストリーミング出力、タイムアウト既定120s |

ドメイン操作ツール(ToDo 作成等、apiclient 経由)は contracts が太ってから追加(M5 以降)。ツール追加=キャッシュ全壊なので、**リリース単位でまとめて凍結**する運用を README に明記する。

### 8.2 出力切詰め(`truncate.rs`)— pi 忠実移植

**[事実]** 原典: `pi:agent/src/harness/utils/truncate.ts`(344行)。仕様:

- 二重上限: **2000行 / 50KB、先に達した方が勝つ**。部分行は返さない(bash tail の1行超過エッジケースを除く)
- `truncate_head`(ファイル読み): 先頭から。1行目が 50KB 超なら空+フラグ
- `truncate_tail`(bash): 末尾から(エラーと最終結果が見えることを優先)。全部超過時のみ末尾部分行
- 結果メタ(総行数・総バイト・切詰め理由)をツール結果の注記に含める: `"[出力 12,345行/2.1MB のうち末尾2000行を表示。全文: /tmp/bash-xxx.log]"`
- Rust 実装注意: バイト長は `str::len` で UTF-8 バイト数そのまま。行分割後の境界は必ず char boundary で(`floor_char_boundary` 相当の手書き)

### 8.3 bash 実行(`bash.rs` + `shell_capture.rs`)— pi 忠実移植

**[事実]** 原典: `pi:agent/src/harness/utils/shell-output.ts`(135行)。運用の知恵が詰まっているので必ず読んでから書く:

- stdout/stderr を**単一ストリームに合流**(時系列維持)
- **ローリングバッファ**: 上限 100KB(50KB×2)。超えたら先頭チャンクから捨てる → 最後に `truncate_tail` で 50KB/2000行に整える(=「メモリを無限に食わずに末尾を保持」)
- **全文退避**: 出力が 50KB を超えた時点でテンポラリファイル(`bash-*.log`)への追記を開始し、ツール結果に**全文パス**を含める。エージェントは必要なら read_file/grep で続きを読める(戦略的忘却と同じ思想)
- **バイナリサニタイズ**: 制御文字(TAB/LF/CR以外)除去、`\r` 除去(:sanitizeBinaryOutput)。Rust では `from_utf8_lossy` + 同フィルタ
- 中断: CancellationToken → プロセスグループごと SIGKILL。`cancelled: true` とそれまでの出力を返す(結果は捨てない)
- 実行シェル: `bash -c`、作業ディレクトリはワークスペースルート、環境変数は最小(PATH, HOME, LANG)

---

## 9. 権限承認(`approval/`)— Sumi の独自領域 (3/3)

### 9.1 フックとしての位置

pi の `beforeToolCall` フック(block 可能)**[事実]**(`pi:agent/src/types.ts:60-63, agent-loop.ts:618-651`)が土台。pi ではフックが同期的に block を返すだけだが、Sumi は「ユーザーに聞いて返事を待つ」**非同期状態機械**をフック内に実装する。

### 9.2 状態機械

```
ツールコール準備完了 (引数検証済み)
  → policy 評価:
      Auto      → 実行                      (ReadOnly ツール等)
      AlwaysAllowed(既存ルール一致) → 実行
      Ask       → ApprovalRequested イベント発行、Pending へ
Pending:
  - approval_decision コマンド待ち (oneshot チャネル)
  - 受理: ApproveOnce → 実行 / ApproveAlways → ルール保存+実行 / Deny → block
  - abort/ハードステア: Pending を Cancelled にし block
  - タイムアウト: なし (無限待ち)。ただし待機中も steering は受理される [要決定→14章]
block 時: pi と同じくエラーツール結果を合成 [事実] (agent-loop.ts:638-644)
  Deny:      "ユーザーがこの操作を拒否した。理由を推測せず、指示を仰ぐこと"
  Cancelled: "承認待ちが中断された"
```

```rust
pub struct ApprovalRequest {
    pub id: String,
    pub tool_call_id: String,
    pub tool_name: String,
    pub args_summary: serde_json::Value,   // UI表示用 (bashならコマンド文字列)
    pub reason: Option<String>,            // モデルが tool 引数 `_reason` で添える説明 [推測]
}
pub enum ApprovalDecision { ApproveOnce, ApproveAlways { rule: ApprovalRule }, Deny }
```

### 9.3 ポリシー(`policy.rs`)

- 既定: `risk() == ReadOnly` → Auto、`Mutating`(ワークスペース内)→ Auto **[要決定]**(自分の作業机への書込みまで聞くとうるさい。ハッカソン既定は Auto、設定で Ask に上げられる)、`Exec`(bash)→ Ask、将来のドメイン操作(api 書込み系)→ Ask
- AlwaysAllow ルールの粒度: ツール名 + パターン(bash はコマンド先頭トークン、fs はパスプレフィックス)。ローカル SQLite に保存
- ルールを api 側(ドメイン)に置くかは M5 で再検討 **[要決定→14章]**。UI の承認カード(画面構成書 C: 「今回のみ/常に許可/拒否」)と decision enum は一致済み

### 9.4 待機中の会話との整合

承認待ちはツールバッチの途中で停止するため、Session は `Streaming` のまま。この間の user メッセージはソフトステアとしてキューに積まれ、**承認解決後のツールバッチ完了 → 次ターン前に注入**される。「拒否と同時に言葉で指示する」自然な操作が成立する。abort は Pending を破棄して Idle へ。

---

## 10. 永続化(`store/`)

SQLite(sqlx、WAL モード)。DB ファイルはワークスペースの永続ボリューム上(`$WORKSPACE/.sumi/agent.db`)。**agent が自前で持ってよい永続化は「自身のメモリストアのみ」という ADR 0001 の原則の範囲内**。チャットログ全文をここに置くか api 側 DB に置くかは **[要決定→14章]**(ハッカソンはローカル SQLite で確定し、イベントを api に流しているので後から api 側へミラー可能)。

### 10.1 スキーマ(マイグレーション v1)

```sql
-- チャットログ全文 (追記専用。UI再構築・検索・監査の単一の源泉)
CREATE TABLE messages (
  id TEXT PRIMARY KEY,          -- uuid v7 (時系列)
  seq INTEGER NOT NULL,         -- 会話内の単調増加
  role TEXT NOT NULL,           -- user | assistant | tool_result
  payload TEXT NOT NULL,        -- Message の serde_json 全文 (thinking含む)
  interrupted INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL
);
CREATE VIRTUAL TABLE messages_fts USING fts5(text, content=messages); -- 検索用抽出テキスト

-- メモリ層の現在形 (再起動復元用)
CREATE TABLE memory_batches (
  id TEXT PRIMARY KEY,
  layer INTEGER NOT NULL,       -- 0 | 1 | 2
  ord INTEGER NOT NULL,
  state TEXT NOT NULL,          -- open|sealed|compacting|compacted|promoted|dropped
  est_tokens INTEGER NOT NULL,
  first_message_id TEXT, last_message_id TEXT,  -- L0: messages への参照 (本文は複製しない)
  summary TEXT,                 -- L1/L2 と shelf 結果
  updated_at TEXT NOT NULL
);

CREATE TABLE approval_rules (
  id TEXT PRIMARY KEY, tool TEXT NOT NULL, pattern TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE approval_log (
  id TEXT PRIMARY KEY, tool_call_id TEXT, decision TEXT, decided_at TEXT
);

CREATE TABLE kv ( key TEXT PRIMARY KEY, value TEXT NOT NULL );  -- calib.ratio, ハッシュ類

-- 恒久イベントログ (WS再送の単一の源泉。delta系イベントは含めない — 10.2節)
CREATE TABLE agent_events (
  seq INTEGER PRIMARY KEY,      -- 会話内単調増加 (Envelope.seq と同一)
  conversation_id TEXT NOT NULL,
  envelope TEXT NOT NULL,       -- Envelope の serde_json 全文
  created_at TEXT NOT NULL
);
```

### 10.2 書込み経路と再起動復元

- 書込みは**イベント駆動**で、イベントを二階級に分ける(T13/T17 の整合のため):
  - **恒久イベント**(MessageStart/End、ToolExecution 系、Approval 系、Turn/Agent 系、Steered、MemoryMaintenance): seq を採番し、StoreWriter が `agent_events` に**追記してから** Gateway へ転送する。Gateway 送出は StoreWriter の下流に置くことで「保存してから送信」を購読順序で保証する(delta を含まないため書込み頻度は低く、ホットパス影響は無視できる)
  - **揮発イベント**(MessageUpdate の delta 系): 永続化せず Session から Gateway へ直送(seq なし)。再送不可。切断中に流れた分は、再接続後に届く恒久イベント MessageEnd(全文)で回復する — UI は「一瞬止まって全文が出る」体験になる
  - messages / memory_batches への実体書込みは従来どおり StoreWriter(mpsc 経由、**プロセス終了時に flush 待ち**)
- 復元: 起動時に memory_batches から L0/L1/L2 を再構成(L0 の本文は messages から引く)。open バッチの途中状態も ord で復元。shelf は summary 列。**復元後の最初の API コールはキャッシュ全ミス**(プロセス再起動の宿命)なのでコンテナは安易に殺さない運用とする
- リトライで L0 から除去された Error assistant も messages には残る(pi の「state からは除去、session には保持」**[事実]** を踏襲)

---

## 11. Gateway(api 接続抽象)と contracts イベントスキーマ案

### 11.1 トレイト

```rust
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Command {
    UserMessage { text: String, attachments: Vec<Attachment> },
    Abort,
    ApprovalDecision { request_id: String, decision: ApprovalDecision },
    // steer は独立コマンドにしない: Streaming 中の UserMessage をステアと解釈 (6.2節)
    // これは画面構成書「入力欄はロックしない。打って送信=ステア」と同型
}

#[derive(Serialize)]
pub struct Envelope {
    pub seq: Option<u64>,            // 恒久イベントのみ採番 (再送基準)。delta系は None (10.2節)
    pub conversation_id: String,
    pub event: AgentEvent,
}

#[async_trait]
pub trait Gateway: Send {
    async fn next_command(&mut self) -> anyhow::Result<Command>;
    async fn send(&mut self, envelope: Envelope) -> anyhow::Result<()>;
}
```

- `stdio.rs`: 1行1JSON。開発時は `make agent-repl`(ラッパースクリプト)で人間が直接会話でき、E2E テストは期待イベント列をアサートできる。**M1 からこれで動かす**
- `ws.rs`(M5): agent がコンテナ内から api へ outbound WebSocket 接続(コンテナへの inbound を開けない)。接続時に `hello {conversation_id, last_sent_seq}`、api は自分の最終受信 seq を返し、agent は **`agent_events` テーブルから seq 差分を再送**する(恒久イベントのみ。delta は再送しない — 10.2節の二階級設計)

### 11.2 contracts/agent-events.yaml(スキーマ案)

contracts/ に OpenAPI とは別ファイルで JSON Schema を置く(消費者: agent(Rust serde)、api(Go)、web(TS))。本計画では形だけ提示し、M5 で確定させる:

```yaml
# contracts/agent-events.yaml (案)
$defs:
  Envelope: { seq: integer, conversation_id: string, event: { $ref: AgentEvent } }
  AgentEvent:
    oneOf: [AgentStart, AgentEnd, TurnStart, TurnEnd, MessageStart, MessageUpdate,
            MessageEnd, ToolExecutionStart, ToolExecutionUpdate, ToolExecutionEnd,
            ApprovalRequested, ApprovalResolved, Steered, Error]
  Command:
    oneOf: [UserMessage, Abort, ApprovalDecision]
```

web への転送方針(api の責務、参考): MessageUpdate の delta 系はそのまま流す(TTFT 最優先)。Thinking delta は既定で流すが UI 側で折り畳み。**Rust の enum 直列化(serde tag 形式)をそのままスキーマの正とし、Go/TS が追随**する運用が契約ファースト原則と両立する最小コスト。**[推測]**

---

## 12. pi から移植すべき細部の具体リスト

すべて 2026-07-17 時点の `earendil-works/pi` @ `216e672e` を実読した結果 **[事実]**。実装セッションは該当ファイルを**必ず開いてから**書くこと(本表は索引であり、コードの代替ではない)。

| # | 何を | pi のどこから | なぜ / どう移すか | Sumi の行き先 |
|---|---|---|---|---|
| 1 | メッセージ・イベント型体系 | `ai/src/types.ts:321-476` | 1年運用で安定した境界設計。contentIndex 方式のストリーミングイベント、`Done`/`Error` の二終端、「stream は決して throw しない」契約(:301-313 コメント) | `provider/types.rs`(第3章) |
| 2 | SSE→メッセージ組立の全細部 | `ai/src/api/openai-completions.ts:229-511` | ツールコールブロックの index/id 二重引き、text/thinking/toolcall の open-block 管理、finish 時の一括 finishBlock、エラー時の scratch 掃除。ここが最も事故りやすい | `provider/assembler.rs` |
| 3 | Moonshot の usage が `choices[0].usage` に入るフォールバック | 同 :362-366 | Kimi 直APIで usage を取り損ねると 3層メモリの校正が死ぬ | `assembler.rs` |
| 4 | reasoning フィールド3種の検出と「最初の非空だけ採用」 | 同 :394-424 | reasoning_content/reasoning/reasoning_text の方言+二重返却プロバイダ対策。採用フィールド名を再送に使う | `assembler.rs` + `request.rs` |
| 5 | usage 解釈(cached_tokens=読み、cache_write 別枠、input=prompt−cached−write) | 同 :1168-1204(OpenRouter PR#409 への言及コメント含む) | キャッシュヒット率の観測(M4 検証ゲート)の正確性の根拠 | `provider/types.rs::Usage::from_raw` |
| 6 | finish_reason マッピング表 | 同 :1206-1230 | content_filter/network_error→Error+原文保存、等 | `assembler.rs` |
| 7 | 「finish_reason 無しでストリーム終端 = エラー」 | 同 :482-484 | 静かな切断を成功と誤認しない | `assembler.rs` |
| 8 | assistant content を必ずプレーン文字列で再送 | 同 :957-1012(コメント含む) | content-block 配列だと DeepSeek 系が構造を鸚鵡返しする実バグ | `request.rs` |
| 9 | thinking 再送: signature フィールドへの書き戻し、`reasoning_content:""` 補完 | 同 :976-1044 | **Kimi の Preserved Thinking 必須仕様**への対応。litellm はここを落としてバグっている(調査レポート Issue #26156) | `request.rs` |
| 10 | ツール結果の空/画像プレースホルダ、画像の user メッセージ追送 | 同 :1058-1130 | 「either content or tool_calls」制約を踏まない | `request.rs` |
| 11 | 空 assistant(content 無し tool_calls 無し)のスキップ | 同 :1045-1056 | aborted 残骸で 400 を食らわない | `request.rs`/transform |
| 12 | ツールコール ID の 40 字正規化 | 同 :893-906 | OpenAI 系は 40 字制限。他モデル由来 ID の再送対策 | transform(第5.3節) |
| 13 | 逐次 JSON パース戦略(厳密→repair→partial→repair+partial→{}) | `ai/src/utils/json-parse.ts` 全文 | ストリーミング中のツール引数表示と、確定時の壊れ JSON サルベージ。repairJson(文字列内制御文字エスケープ、不正エスケープの二重化)は Kimi/GLM でも踏む | `provider/partial_json.rs`(テスト含め忠実移植) |
| 14 | リトライ可否の正規表現パターン集(retryable + non-retryable) | `ai/src/utils/retry.ts` 全文 | 各パターンにコメントで実 issue 番号が付いた運用知識の結晶。quota/billing 系を先に除外する順序も含めて移す | `provider/retry.rs` |
| 15 | リトライポリシー(3回、2s/4s/8s、中断可能 sleep、エラー assistant を state から除去しログには保持) | `coding-agent/src/core/agent-session.ts:2606-2673` | ポリシーと判定の分離。「溢れはリトライしない」ガードが先頭にある(:2610-2614) | `agent/run.rs` |
| 16 | コンテキスト溢れ検出パターン(Kimi「exceeded model token limit」、z.ai サイレント溢れの usage 判定、非溢れ除外) | `ai/src/utils/overflow.ts:379-501` | 溢れとレート制限の誤判別は復旧経路を間違える。Kimi/GLM/汎用分のみ抽出 | `provider/overflow.rs` |
| 17 | エラーボディの正規化(status+body 4000字切詰め) | `ai/src/utils/error-body.ts` | 「403 (no body)」型の情報消失を防ぐ。reqwest 直叩きなので SDK 形状プローブは不要、フォーマットだけ移す | `provider/sse.rs` |
| 18 | エージェントループ骨格(steering/followUp の 2 キュー、ポーリング位置、イベント発行順) | `agent/src/agent-loop.ts:155-275` | ループの正典。TurnEnd 後 steering→無ければ followUp→無ければ終了、の順序 | `agent/run.rs` |
| 19 | Length 停止時のツール一括失敗 | 同 :207-215, 383-408 | partial JSON サルベージ由来の「静かに不完全な引数」を実行しない安全弁 | `agent/run.rs` |
| 20 | beforeToolCall の block→エラーツール結果合成、abort 二重チェック | 同 :602-666 | 承認フローの土台。block reason がそのままモデルへの説明になる | `approval/` + `agent/run.rs` |
| 21 | ツール実行の onUpdate コールバック(settle 後の更新を無視するガード) | 同 :668-709 | bash ストリーミング表示。遅延 update がイベント順序を壊さない | `tools/mod.rs` |
| 22 | run 失敗時の合成エラーメッセージでイベント列を正常形で閉じる | `agent/src/agent.ts:494-510` | 消費者(UI/Store)が異常系を特別扱いしなくてよくなる契約 | `agent/mod.rs` |
| 23 | キュー既定 one-at-a-time | 同 :222-223 | 複数割込みへの応答機会を1個ずつ与える。UX 由来の既定値 | `agent/queue.rs` |
| 24 | 履歴正規化(孤児ツールコールの合成結果、user 分断時の挿入、Error/Aborted スキップ、クロスモデル thinking 降格) | `ai/src/api/transform-messages.ts` 全文 | 再送安全性の要。Sumi は interrupted 例外を追加(第6章) | `memory/` transform |
| 25 | 出力切詰め(2000行/50KB 二重上限、head/tail、部分行禁止、メタ情報) | `agent/src/harness/utils/truncate.ts` 全文 | 数値も含め実運用の落とし所。テストケースごと移植 | `tools/truncate.rs` |
| 26 | bash ローリングバッファ+全文テンポラリ退避+バイナリサニタイズ | `agent/src/harness/utils/shell-output.ts` 全文 | 「メモリ有限・末尾優先・全文はファイルで」パターン | `tools/shell_capture.rs` |
| 27 | バッチ/compaction のカット境界規則(user/assistant 直前のみ、toolResult 直前禁止) | `agent/src/harness/compaction/compaction.ts:265-380` | 3層メモリのバッチ境界(7.3節)の根拠 | `memory/batch.rs` |
| 28 | トークン見積(chars/4)+「直近 usage を錨に末尾だけ見積る」ハイブリッド | 同 :118-264 | 7.5節の校正方式の原型。日本語係数を追加 | `memory/estimate.rs` |
| 29 | 要約プロンプト構造(固定フォーマット指定、UPDATE 型の差分更新プロンプト、maxTokens 上限、「会話を続けるな」system) | 同 :383-522 | Compact プロンプト設計の出発点。秘書ドメインに書換え | `memory/compactor.rs` |
| 30 | Kimi K3 / GLM-5.2 の compat 実測値 | `ai/src/providers/moonshotai.models.ts:171-189`, `zai.models.ts:79-98` | pi が実機で当てたフラグ設定。そのまま初期プリセットに | `config.rs` プリセット |

**意図的に移植しないもの**(再掲+根拠): マルチプロバイダ層全体、compat の URL 自動検出(明示設定で代替)、Anthropic 型 cache_control / prompt_cache_key / session affinity(Kimi/GLM は自動キャッシュ)、deferredToolsMode(ツール凍結原則)、parallel ツール実行(承認フローと相性が悪い、M5 後に再検討)、pi の SessionManager/JSONL(SQLite で置換)、compaction の実行トリガ設計(同期・閾値式 → Sumi は先回り非同期式)、TUI/RPC/extension 機構。

---

## 13. マイルストーンと検証ゲート

締切 2026-08-01。今日 7/17 から実働 ~14日。**各 M の終わりに「動くもの+検証ゲート」**。順序は指定案(M1 最小ループ → M2 ステア+永続化 → M3 3層メモリ → M4 ワークスペースツール → M5 権限承認)を一部入れ替える: **ツール(旧M4)を M2 に前倒し**する。理由: (a) デモの最優先要素「ストリーミング+ツール実行+ステア」を最速で成立させる、(b) ステアの検証には「実行に時間のかかるツール」が必要(bash sleep が最良のテストベンチ)、(c) 3層メモリの検証には長い会話が必要でツールがあると会話を伸ばしやすい。

### M0: 足場(0.5日、〜7/18午前)

- `config.rs`(設定構造+環境変数のみ。**モデルプリセットの実値は M1 のリクエスト組立と同時に入れる** — M0 では構造体と TOML 読込だけ)、モジュールツリーの空実装、`gateway/stdio.rs`、tracing 初期化(JSON ログ + `SUMI_LOG` フィルタ)
- **ゲート**: `echo '{"type":"user_message","text":"hi"}' | cargo run` がエコー応答イベントを返す。`cargo clippy -- -D warnings` / `cargo fmt --check` が通る(turbo lint 経由)

### M1: プロバイダ層(3日、〜7/21)

- `provider/` 全体: 第4章+移植リスト #1〜17。types → sse → assembler → request → retry/overflow の順
- テスト: (a) **SSE フィクスチャ再生**: 実 API のストリームを `curl` で採取したファイル(Kimi text / Kimi tool call / Kimi reasoning / GLM tool_stream / エラー各種)を axum モックサーバで再生し、イベント列と最終メッセージをスナップショットアサート。(b) partial_json の pi テスト移植。(c) `SUMI_LIVE_TEST=1` でのライブ疎通(Umans/Kimi/GLM 三択)
- **ゲート**:
  1. `cargo test` 全緑(フィクスチャ再生で: ツールコール引数の逐次組立、reasoning 分離、usage 取得、finish_reason 全パターン)
  2. ライブ: 3プロバイダに対しツールコール1往復+reasoning 付き2ターン会話が完走。**2ターン目で Kimi に reasoning_content を再送しても 400 が返らない**こと
  3. TTFT 計測基盤: `MessageStart(user)受信 → HTTP リクエスト送出` と `送出 → 最初の TextDelta` を tracing span で分離計測し、stdio REPL に表示。**agent 内部オーバーヘッド p95 < 30ms**(モデル側 TTFB は記録のみ)
  4. abort: 生成中に CancellationToken 発火 → 1s 以内に Aborted イベントで正常形クローズ

### M2: ループ+ツール+ステア(3日、〜7/24)

- `agent/`(run.rs, Session, queue)+ `tools/`(fs, bash, truncate, shell_capture)+ ハードステア(steer.rs)。移植リスト #18-23, 25-26 + 第6章
- **ゲート**:
  1. stdio REPL で: 「~/ にメモ帳フォルダを作って今日の日付のメモを書いて」→ bash/write ツールが流れる様子がイベントで見える
  2. **ステア実証**(デモの核): `bash sleep 30` 実行中に user_message → ソフトステア(ツール完走後に注入)。テキスト生成中に user_message → ハードステア(部分応答が interrupted で確定し、続く応答が割込み内容を踏まえる)。両方をスクリプト化した E2E テストで自動判定
  3. 中断→再開後の Kimi 再送で reasoning のみ部分応答が受理されるか確認(6.3節の未検証点)。駄目なら回避策を実装しコメントに記録
  4. Length 停止のツール一括失敗をフィクスチャで再現

### M3: 永続化(2日、〜7/26)

- `store/` 全体 + StoreWriter + 再起動復元。リトライの「state から除去・ログに保持」もここで完成
- **ゲート**: 10ターン会話 → プロセス kill → 再起動 → 会話が続く(L0 復元)。`messages_fts` で過去発言が検索できる。イベント seq が復元後も単調継続
- **チーム同期ポイント**: Envelope/Command の JSON 形をこの時点で凍結し、contracts/agent-events.yaml のドラフトを起こして api 担当(Go)に渡す

### M4: 3層メモリ(3日、〜7/29)

- `memory/` 全体(第7章)。batch → estimate → compactor → overflow → ContextAssembler の順
- テストデータ: 実会話を伸ばすのは非効率なので、**過去メッセージを合成生成する長会話シミュレータ**(スクリプトで 200k トークン相当を投入)を用意
- **ゲート**:
  1. シミュレータ投入で L0→L1→L2 の昇格が全段発火し、プロンプト総量が常に 80k 未満(MemoryMaintenance イベントで観測)
  2. **キャッシュヒット率実測**: 通常ターン(末尾追記のみ)で `usage.cache_read / (input+cache_read) > 0.8` を Kimi 実機で確認。L0 先頭廃棄の直後ターンだけ低下し、次ターンで回復すること
  3. **TTFT 非劣化**: ユーザーメッセージ起点のコール前に溢れ処理・Compact が同期実行されていないことを span で証明(7.6-2 のスキップ規則)
  4. 複数 system メッセージが Kimi/GLM に受理されるか確認(7.1節)。駄目ならフォールバック実装
  5. 校正: est×ratio と実測 usage の乖離が ±15% 以内に収束

### M5: 権限承認+WS ゲートウェイ(2日、〜7/31)

- `approval/`(第9章)+ `gateway/ws.rs`(第11章)+ contracts/agent-events.yaml 確定 + apiclient 雛形
- **ゲート**:
  1. bash ツールが ApprovalRequested を発行 → stdio/WS 経由で decision → 承認で実行/拒否でエラーツール結果、AlwaysAllow ルールが SQLite に残り次回 Auto
  2. 承認待ち中の user_message がソフトステアとして機能
  3. web→api→agent の E2E: チャット UI から会話でき、ストリーミング+ツール+ステア+承認カードが見える(api/web 側と合同。**これがデモの完成条件**)
  4. WS 切断→再接続で seq 差分再送が効く

### 予備日(8/1): デモシナリオのリハーサル、負荷時の挙動確認(Umans 4セッション制限の回避=デモは直APIキーで)、憲法プロンプトの調整

依存関係: M1→M2→M3→M4→M5 は直列が基本だが、**M3(Store)と M2 後半(ステア磨き込み)は並行可**、**M5 の contracts ドラフトは M3 完了時点で先出し**(api 側の Go 実装リードタイム確保)。

テスト方針の総括: ユニット(純関数: assembler/truncate/partial_json/batch/estimate)+フィクスチャ再生(プロバイダ層)+スクリプト E2E(stdio ゲートウェイにコマンド列を流しイベント列をアサート)+ライブスモーク(env フラグでオプトイン)。**CI(GitHub Actions の agent パス)ではライブ以外を全部回す**。

---

## 14. リスクと未決事項

### 14.1 ユーザー(Founder)に決めてもらう点 **[要決定]**

| # | 論点 | 選択肢と推奨 |
|---|---|---|
| D1 | **チャットログ全文の置き場所** | (a) agent ローカル SQLite のみ(推奨・ハッカソン)/ (b) api 側 DB にミラー(イベント転送で後付け可能)/ (c) api 側のみ。web の履歴無限スクロールを api が返すなら最終的に (b)。M3 までに方針だけ確定 |
| D2 | **Compact 用モデル** | (a) 会話と同じモデル(品質・一貫性)/ (b) 安価モデル(umans-flash / kimi-k2.7)。コストと「記憶の質は人格の質」のトレードオフ。既定 (a) で設定切替可能にしておく |
| D3 | **ワークスペース書込み(write/edit)の承認要否** | 推奨: Auto(自分の机への書込みまで聞くとうるさい)。bash は Ask。デモの見栄え(承認カードを見せたい)なら bash で十分 |
| D4 | **承認待ちのタイムアウト** | 推奨: 無限待ち+通知タブに滞留(画面構成書と整合)。エージェントは待機中もステア可能なので詰まらない |
| D5 | **ツール実行中のハードステア** | 推奨: しない(ツール完走→注入)。「今すぐ止めて」は停止ボタン(abort)の仕事、と UI 上の意味を分ける |
| D6 | **AlwaysAllow ルールの置き場所** | ハッカソン: agent ローカル。将来: 権限モデルの強制点を api に一元化する原則に従い api 側へ移す(設定画面での管理も api 経由になる) |
| D7 | **デモのモデル構成** | 推奨: Kimi K3 直API を主役(reasoning+1M ctx+自動キャッシュ)、GLM-5.2 従量を控え。Umans は開発用(同時4セッション制限がデモの罠)。8/1 までに直APIキーの課金設定を済ませる |
| D8 | **OpenAPI→Rust クライアント生成** | 現状1エンドポイントなので手書きで開始し、ドメインAPIが3本を超えたら progenitor 導入を ADR 化(推奨) |

### 14.2 技術リスクと手当て

| リスク | 影響 | 手当て |
|---|---|---|
| **複数 system メッセージ非受理**(L2/L1 注入方式) | 3層メモリのプロンプト構成が崩れる | M4 ゲート4 で早期確認。フォールバック(user ロール+`<memory>` タグ)を Compat フラグとして最初から実装しておく |
| **interrupted 部分応答の再送を Kimi が拒む**(thinking のみ等のエッジ) | ハードステアの体験が濁る | M2 ゲート3 で確認。プレースホルダテキスト補完で回避可能(6.3節) |
| **Umans が pi の想定と違う方言を話す**(プロキシ実装の癖) | 開発効率低下 | M1 ライブゲートで3プロバイダ全部を通す。Compat は設定ファイルなので再コンパイル不要で調整できる |
| **トークン見積の日本語係数が外れる** | 層境界の誤判定(溢れの検知漏れ/過剰発火) | usage 校正(7.5節)が自動吸着。加えて溢れ検出(4.5節)が最終防衛線 |
| **Compact の品質不足**(圧縮されすぎ・人格の断絶) | 「育つ秘書」体験の毀損 | 目標圧縮率のプロンプト明示+L1 文脈の読み取り専用添付(7.4節)。M4 で実会話サンプルの要約を人間レビュー |
| **SQLite 書込み遅延がホットパスに漏れる** | TTFT 劣化 | StoreWriter を mpsc 非同期化済み(10.2節)。span 計測で監視 |
| **Kimi の自動キャッシュ TTL(5〜30分、未確定)** | 放置後の会話再開で全ミス→初回 TTFT 悪化 | 仕様上避けられない。実測して既知の挙動としてデモ台本に織り込む(冒頭に1回ウォームアップ発話) |
| **8/1 に api/web 側が間に合わない** | E2E デモ不成立 | stdio ゲートウェイ+簡易 CLI で agent 単体デモが常に成立する状態を保つ(M2 以降常時)。contracts ドラフトを M3 で先出しして統合期間を確保 |

### 14.3 本計画の前提が崩れたときの縮退順序

デモ最優先の縮退: M5 の WS 統合 > M5 承認(stdio では動く)> M4 の L2 統合(L1 昇格まででも会話は無限に続く)> M3 の FTS 検索。**M1+M2(ストリーミング+ツール+ステア)だけは何があっても削らない**。

---

## 付録A: 用語集

- **ソフトステア**: ターン境界(ツールバッチ完了後、次 API コール前)への割込み注入。pi の steer と同じ
- **ハードステア**: 生成中の abort+部分応答保持+注入+再開。Sumi 独自
- **棚(shelf)**: 先回り Compact の成果物置き場。適用(=L1 への昇格)までの待機場所
- **憲法**: System Prompt に置く不変の人格核。メモリの風化の影響を受けない
- **ツール凍結原則**: Tool Definitions の変更はプレフィックスキャッシュ全壊と同義なので、リリース単位でのみ変更する運用
- **正常形クローズ**: どんな異常でもイベント列が MessageEnd→TurnEnd→AgentEnd で閉じる契約

## 付録B: 実装セッションへの申し送り

1. pi のコードを読まずに本計画の要約だけで書き始めないこと。特に #2, #13, #24, #26 は行単位の細部に価値がある
2. 迷ったら「イベント列が正常形で閉じるか」「キャッシュプレフィックスを壊さないか」「ホットパスに同期 I/O を置いていないか」の3点で自己レビュー
3. Compat フラグの追加をためらわない。pi が25プロバイダで学んだ教訓は「互換 API の差異は enum とフラグで飼い慣らす」こと
4. 憲法プロンプト(人格)の執筆は本計画のスコープ外。Founder が書く。実装側はプレースホルダで進める
