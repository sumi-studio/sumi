# 画面構成書

- Status: Draft (v1)
- Original date: 2026-07-15
- Last amended: 2026-08-10
- 前提: [ADR 0001](adr/0001-frontend-stack.md) の技術選定 (React 19 + Vite + Tailwind v4 + Tauri 2、`packages/ui` カタログ、`packages/sdui`)

## 設計原則

要件「AI を人間として扱う」「AI と人間が同じ作業空間に住む」を UI に直訳する。

- エージェントは「機能」ではなく**メンバー**。会話一覧に人間と同列に並ぶ
- 会話は**シングルスレッド**。人間の会話にスレッド分岐は存在しないため、1 エージェント = 1 本の時系列。会話一覧はフラット (ワークスペース/チャンネル階層を作らない)
- ToDo・リマインダー等の機能は独立アプリではなく、**チャット内の SDUI カード + 横断一覧**として現れる
- SDUI (宣言データ + コンポーネントカタログ) は「メッセージの一種としてカードを描画する」仕組みとして最初から組み込む

## トップレベルナビゲーション

| # | 画面 | 役割 | 時期 |
|---|------|------|------|
| 1 | トーク | 会話一覧 (= エージェント一覧) + チャット本体 | MVP |
| 2 | タスク | ToDo・リマインダー・タイマー・アラームの横断一覧 | 早期 |
| 3 | 通知 | 通知センター + 権限リクエストの承認キュー | 早期 |
| 4 | 設定 | エージェント管理・権限管理・アカウント | MVP (最小) |

- デスクトップ / Web: 左サイドバー (ナビ + 一覧 | 本体 の 2 ペイン) を常時表示
- モバイル (Tauri シェル): 同じ中身 (ナビ + 一覧) をハンバーガーメニューから開くドロワーとして表示。チャット画面のヘッダー左の ☰ が開閉トリガー

MVP は 1 と 4 のガワのみ。2・3 はタブだけ用意して空で良い。

## 画面一覧

### A. トーク一覧 (MVP)

デスクトップでは左サイドバーに常時表示。モバイルではハンバーガーメニュー (☰) から開くドロワーの中身。

```
┌──────────────────────┐
│ Sumi          🔍  ＋ │
├──────────────────────┤
│ トーク タスク 通知 設定 │ ← ナビ (現在地: トーク)
├──────────────────────┤
│ ● Sumi (秘書)  14:02 │
│   「明日の予定ですが…」  │
│ ○ Kuro (開発)  13:40 │
│   「デプロイ完了しました」│
└──────────────────────┘
```

- 1 行 = 1 エージェント (= 1 会話)。未読ドット、最終メッセージプレビュー、時刻
- [＋] は新しいエージェントの追加 (当面は 1 体でも、構造は複数前提)
- ナビ行 (トーク/タスク/通知/設定) はデスクトップの左サイドバー・モバイルのドロワーで共通。1 箇所にしか存在しないため、両プラットフォームで実装・挙動が揃う

### B. メインチャット (MVP・最重要画面)

#### 要件

- 入力欄
- 音声入力 (ブラウザネイティブ)
- 送信ボタン / 停止ボタン
- コンテンツの貼り付け
- ステア (生成中の割り込み)
- TTFT < 500ms
- シングルスレッド
- 吹き出しはユーザーのみ
- ユーザー・アシスタントのアイコンなし
- タイムスタンプ表示とコピーボタン

#### レイアウト

Slack 型ではなく Claude/ChatGPT 型のドキュメント風。アシスタントの発話は UI の地の文として扱う。

```
┌──────────────────────────────────────┐
│ ☰ Sumi                              │ ← ☰ でトーク一覧・タスク・通知・設定のドロワーを開閉
├──────────────────────────────────────┤
│                    ┌───────────────┐ │
│                    │ 明日の予定は？   │ │ ← ユーザー: 右寄せ吹き出し
│                    └───────────────┘ │
│                          14:02 ⧉    │ ← メタ行 (ホバー/タップで表示)
│                                      │
│  明日は 9:00 から定例会議があります。    │ ← アシスタント: 全幅の地の文
│  ┌────────────────────────────┐      │
│  │ 📅 定例会議  明日 9:00       │      │ ← SDUI カードはインライン
│  │ [リマインドする]              │      │
│  └────────────────────────────┘      │
│  14:02 ⧉                            │
├──────────────────────────────────────┤
│ [＋]  メッセージ…            [🎤] [↑] │
└──────────────────────────────────────┘
```

#### メッセージの解剖

| 要素 | ユーザー | アシスタント |
|---|---|---|
| 表示 | 右寄せ吹き出し (背景色あり) | 全幅の地の文 (markdown + SDUI カード) |
| アイコン | なし | なし |
| メタ行 | タイムスタンプ + コピー ⧉ | 同じ |

- メタ行はデスクトップはホバー、モバイルはタップで出現。長い単一スレッドでの視覚ノイズを避ける
- コピーの内容: ユーザー = 生テキスト、アシスタント = markdown ソース
- メッセージ種別は最初から 3 種で設計する: **テキスト / SDUI カード / システム (権限リクエスト等)**。後続機能はすべてこのレールに乗せる

#### コンポーザーの状態機械

「送信/停止」「ステア」はボタンの排他ではなく状態遷移として設計する。

```
idle ──送信──▶ streaming ──完了──▶ idle
                │  │
     [■]停止 ──┘  └── 入力欄はロックしない。
                       テキストを打って送信 = ステア (割り込み)
                       → エージェントは現在の生成を中断し
                         新入力を注入して継続
```

- **idle**: 右端は送信 [↑] (入力が空なら disabled)
- **streaming**: 右端は停止 [■] に変化。入力欄は生きたままにするのが普通のチャット UI との最大の差 — 「人間への割り込み」と同じ操作でステアできる
- ステアで送ったメッセージも通常のユーザー吹き出しとして履歴に残る (会話はあくまで 1 本の時系列)

#### 音声入力

- Web Speech API (ブラウザネイティブ)。[🎤] 押下で録音開始、認識テキストを入力欄にライブ反映、確定後に手直しして送信
- 注意: Tauri iOS の WKWebView では `SpeechRecognition` の挙動が Safari 準拠。Web で動いても iOS シェルで要再検証

#### コンテンツの貼り付け

3 経路とも、入力欄上部の**添付チップ** (サムネイル + 削除ボタン) に積む。

1. クリップボードからの画像・ファイルペースト
2. ドラッグ & ドロップ
3. [＋] からのファイル選択

#### TTFT < 500ms の設計制約

UI 要件というよりアーキテクチャ制約。最初から効かせる。

1. WebSocket はアプリ起動時に張りっぱなし。メッセージ送信も HTTP POST ではなく WS 経由 (送信時に接続確立の RTT を払わない)
2. ユーザーメッセージは楽観的に即時描画。サーバー ack を待たない (失敗時は吹き出しに再送マーク)
3. アシスタント応答はトークンストリーミング。最初のトークンから地の文を描画し、スピナー画面は作らない
4. エージェント側: 3 層メモリの圧縮・昇格はリクエスト経路に乗せず非同期バッチ。システムプロンプト + メモリ層はプロンプトキャッシュが効く順序で組む
5. 単一スレッドで会話が無限に伸びるため、メッセージリストは最初から仮想化 (TanStack Virtual)

#### WebSocket と認証境界

- direct chat 画面は targetless な `GET /direct-chat/ws` を1本だけ常時接続し、送信・durable event replay・live-only delta を同じ接続で扱う。URL、browser command、browser eventへ内部の宛先identityを含めない。再接続時は最後に消費した durable event seq を hello で送り、API はその次から catch-up する。volatile delta は replay しない。
- この接続は `sumi_session` HttpOnly cookie の別署名 session (`tenant_id`、`user_id`、内部targetの`personality_agent_id`、expiry、`sumi:web:direct-chat` audience) だけを受け入れる。APIが検証済みsessionから宛先とdirect-chat provenanceを構成する。agent の short-lived bearer token、`PersonalityAgentId`、`ProcessGeneration`、provenance はbrowserへ渡さない。
- API は `SUMI_BROWSER_SESSION_SECRET`（base64 HMAC key）、任意の `SUMI_BROWSER_SESSION_AUDIENCE`、および browser origin allowlist `SUMI_BROWSER_WS_ALLOWED_ORIGINS` を必要とする。現在は `GET /auth/csrf`、`POST/GET /auth/session`、`POST /auth/logout` を実装済みで、Firebase Admin が検証した ID token と server-owned な UID/tenant/user/PersonalityAgent binding から session を発行する。ローカルの正式な入口は既定の `http://127.0.0.1:5173`、または明示した literal Tailnet IPv4 の `http://<ip>:5173` で、API はその 1 origin だけを許可し、Vite が `/auth` と `/direct-chat` を同一 origin proxy する。設定と human smoke は [Real local stack](local-development.md) を参照。
- browser-session cookie の署名鍵は上記base secretからprotocol-version付きの `v2` domainで導出する。pre-lineage cookieはupgrade後に局所検証で失効し、次のFirebase exchangeだけがv2 cookieを発行する一方、authority binding IDは元のbase secretから導出し続けるため同じbindingでは変わらない。この境界をまたぐ旧版とv2版のAPI replicaは互いのcookieを受理できないため、同一deploymentで混在させず、全replicaをdrainして同時に切り替える。このcutoverはone-wayであり、v2を有効化した後に同じ `SUMI_BROWSER_SESSION_SECRET` のまま旧binaryへrollbackしてはならない。rollbackが不可避ならsecretをrotateして全browser sessionとauthority bindingを安全側へresetするか、v2を最大session TTLの1時間より長く停止して既発行cookieがすべて失効してから旧版またはv2を再開する。

### C. current-call承認とstanding policy管理

agentが実際のtarget ToolCallを`Elevated`として提案したときの受け皿。genericな
`request_permission`画面は作らず、**チャット内のsystem card + 通知tabのqueue**の
2箇所に同じcurrent-call approval UIを出す。cardは、agent自身のcapabilityにHumanが
同意する操作か、Human accountを本当に一回使う操作かを明示する。

```
┌──────────────────────┐
│ 🔐 Sumi が操作承認を要求  │
│ 「予定Aを移動」           │
│ 実行主体: Sumi自身        │
│ 理由: 時間調整のため      │
│ [今回だけ承認][今回だけ拒否]│
│ 将来のルールを設定…       │
└──────────────────────┘
```

「将来のルールを設定…」では、常に許可、明示した期限まで許可、永続拒否を選べる。
これはcurrent-call decisionと同じpayloadにせず、別の認証済みstanding-policy mutationとして
送る。対象scope、precedence、expiry上限、appごとのrule語彙はADR 0013の未決事項であり、
opaqueな`ApproveAlways` ruleを先にwireへ戻さない。設定にはruleの一覧・編集・削除画面を置く。
Humanがcard上で対象やscopeを狭める場合は部分承認せず、digestの異なる新callとして
Escalation AutoReviewから再提示する。Human-account one-shotをstanding grantへ変換しない。

### D. タスク / 通知 / 設定 (骨のみ)

- **タスク**: ToDo・リマインダー・タイマー・アラームを種別フィルタ付きの 1 リストで横断表示。項目タップで生成元の会話位置へジャンプ (シームレス接続)
- **通知**: 時系列のアクティビティ + 未処理の権限リクエストを上部に固定
- **設定**: エージェント一覧、権限rule管理、アカウント、外部接続 (GitHub 等は後日)

## パッケージへの割り付け

| 場所 | 内容 |
|---|---|
| `packages/ui` | `MessageBubble` (user) / `AssistantMessage` / `MessageMeta` (timestamp + copy) / `Composer` / `AttachmentChip` — 見た目のみ、状態を持たない |
| `packages/sdui` | カード類 (リマインダー・確認・権限リクエスト) のスキーマ + レンダラー。チャット内では `AssistantMessage` の子として合流 |
| `apps/web` | 会話ストア (Zustand: streaming 状態機械)、WS クライアント、音声入力フック、仮想化リスト、ルーティング |

## 実装順

1. **MVP**: ナビゲーションシェル + トーク一覧 + チャット画面 (テキストのみ) + 設定の骨組み
2. SDUI カード描画基盤 (最小カタログ: リマインダー・確認ダイアログ・リスト)
3. タスクタブ + 通知タブ + 権限カード
4. 音声入力・添付の磨き込み
