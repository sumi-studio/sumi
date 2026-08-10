# ADR 0001: フロントエンド技術選定とリポジトリ構成

- Status: Partially Superseded
- Date: 2026-07-15
- Amended-by: [ADR 0002](0002-agent-stack.md) — ADR 0002 をエージェント基盤と、それに関係するリポジトリ/API 構成の現行仕様とする。前提5(pi が TypeScript)、リポジトリ構成の「agent (TS エージェント基盤)」、および「`@sumi/api-client` 経由」の各記述は ADR 0002 により置換された(agent は Rust、ドメイン操作は契約ファーストの Rust `apiclient` 経由)。本文は歴史的記録としてそのまま残す
- Superseded-by: [ADR 0014](0014-webapp-and-electron-runtime.md) — React / Vite / SDUIをrenderer正本とする判断は維持し、Tauri desktop/mobile shell、`src-tauri`、native mobile packaging、通知統合の選定をWebApp + Electronへ置換する。本文は2026-07-15時点の歴史的記録としてそのまま残す

## コンテキスト

Sumi は「人間と AI エージェントが同じ操作空間に住む」ことを前提とした Full Context Workspace。フロントエンド選定に効く要件は以下。

1. クライアントは iOS + Web が先行。将来 iPadOS / macOS / Windows / Linux / Android へ展開する。
2. 動的 UI: アプリに同梱したネイティブコンポーネントカタログを宣言データで組み合わせる方式 (SDUI)。Apple の審査ルール (実行コードのダウンロード禁止) に適合させる。
3. エージェントと人間が同じ「意味 + 空間」モデルを共有し、AI が画面に注釈できる。UI ツリーが宣言データとして機械可読であることが構造上ほぼ必須。
4. チャットのストリーミング、常時稼働バックエンドとのリアルタイム同期、通知・リマインダー・アラーム。ダイナミックアイランド (Live Activities) を使いたい。
5. エージェント基盤の有力候補 (pi) が TypeScript。
6. 開発は一人 + AI 駆動。Swift は自分では書かない。

SDUI を採用する時点で「スキーマ (宣言データの仕様)」と「レンダラー (各プラットフォームの解釈器)」が分離するため、選定の本質は「レンダラーを何個、何で書くか」に帰着する。

## 決定

**React 一本の Web コードベースに Tauri 2 をシェルとして被せ、全プラットフォームで同一のレンダラー実装を使う。**

| 領域 | 選定 |
|---|---|
| UI フレームワーク | React 19 + TypeScript + Vite |
| ルーティング | TanStack Router (SPA。ログイン型プロダクトで SEO 不要) |
| スタイリング | Tailwind CSS v4 + shadcn/ui を下地にカタログ自作 |
| 状態管理 | Zustand (クライアント状態) + TanStack Query (サーバー状態)、WebSocket でリアルタイム同期 |
| SDUI | zod スキーマ + component registry。宣言データ (JSON) → カタログ参照でレンダー |
| シェル | Tauri 2 (desktop: Win/Linux/macOS、mobile: iOS/Android) |
| API クライアント | contracts/openapi.yaml から型生成 (openapi-typescript / orval) |
| 通知 | Web Push (Web) / tauri-plugin-notification (ローカル) / APNs グルー + SwiftUI widget extension (iOS リモート・Live Activities) |
| 音声対話 | Web 標準 (getUserMedia + WebRTC / WebSocket 音声ストリーム) |
| モノレポ | pnpm workspaces + Turborepo、Lint/Format は Biome |

### リポジトリ構成

- Turborepo 流のデファクトに従い、ルート直下に `apps/` + `packages/`。
- Tauri は `create-tauri-app` の標準通り `apps/web/src-tauri/` に同居。Web 配信版とシェルでアプリを分けない (同じ Vite ビルド)。`src-tauri/gen/apple` は署名設定や widget extension を手で加えるためコミット対象。
- `apps/` は `web` / `api` (Go サービス API) / `agent` (TS エージェント基盤) の3分割。API はステートレスな要求応答型、agent は常時稼働・ステートフルな長命プロセスで、スケール戦略・デプロイ頻度・障害時の扱いが異なるため、デプロイ可能物として分離する。
- OpenAPI 定義はルートの `contracts/` に置く。消費者が web / agent / api の3者にまたがる中立資産のため、特定アプリ配下ではなく独立させる (`apps/api` と名前が衝突する問題も解消)。
- agent は DB を直接触らず `@sumi/api-client` 経由でドメイン操作する。権限モデルの強制点を API 層の1箇所に保つため。

## 検討した代替案

- **Expo (React Native + react-native-web) 一本**: 当初の推奨案。カタログ 1 実装で iOS/Android/Web を覆えるが、デスクトップ展開が弱く、Tauri を使いたいという方針により不採用。
- **SwiftUI + React の分離**: iOS の体験品質は最良だが、レンダラー/カタログが 2 系統になりイテレーションが倍遅くなる。Swift を書かない方針とも不一致。
- **Flutter 一本**: Web が弱点 (canvas 描画でテキスト中心 UI の品質・アクセシビリティが劣る) かつ TS エコシステムと断絶するため不採用。

## 引き受けるトレードオフ

- **iOS 上は WKWebView** になるため、スクロール慣性・キーボード・ジェスチャー等の操作感の磨き込みコストを引き受ける。どうしても足りなければ、UI コードベースは共通のまま iOS シェルだけ Capacitor に差し替える (モバイルプラグインが成熟)、または特定画面のネイティブ化で対処する。シェル層とアプリ層を疎結合に保つことでこの判断は後から低コストで変えられる。
- **Tauri mobile はまだ若い**。特にリモートプッシュ通知は公式プラグイン未整備で、APNs の受け口は自作の Swift グルーが前提。
- **Swift ゼロは不可能**。Live Activities (ダイナミックアイランド含む) の UI は Apple の設計上 WidgetKit + SwiftUI の widget extension でしか書けない (どのフレームワークでも同じ)。「Swift はアプリ本体に存在せず、`src-tauri/gen/apple` 配下の OS 統合 extension にのみ許可」と線を引き、AI に生成させる。
