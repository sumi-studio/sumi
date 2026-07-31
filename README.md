# sumi-studio

<!-- TODO: プロジェクトの概要を1〜2文で記載する -->

## 技術スタック

| レイヤ | 技術 |
|---|---|
| フロントエンド | React 19 + TypeScript + Vite |
| ルーティング | TanStack Router |
| スタイリング / UI | Tailwind CSS v4 + 自作コンポーネントカタログ (shadcn/ui ベース) |
| 動的UI (SDUI) | zod スキーマ + component registry |
| 状態管理 | Zustand + TanStack Query |
| シェル (デスクトップ / モバイル) | Tauri 2 |
| エージェント基盤 | Rust (導入予定、pi を設計参照) |
| バックエンド | Go |
| API 定義 | OpenAPI 3.1 (契約ファースト) |
| モノレポ | pnpm workspaces + Turborepo |
| Lint / Format | Biome |
| インフラ | Terraform |
| CI/CD | GitHub Actions |

選定理由は [docs/adr/](docs/adr/) を参照。

## ディレクトリ構成

```text
sumi-studio/
├── apps/                      # デプロイ可能物
│   ├── web/                   # React SPA (Web配信もTauriシェルもこれ1つ)
│   │   ├── src/
│   │   ├── src-tauri/         # Tauriシェル (Rustグルーは薄く保つ)
│   │   │   ├── src/
│   │   │   ├── gen/apple/     # Xcodeプロジェクト + widget extension (SwiftUI)
│   │   │   │                  #   Swiftはこの配下のみに許可
│   │   │   ├── capabilities/
│   │   │   └── tauri.conf.json
│   │   ├── index.html
│   │   ├── vite.config.ts
│   │   └── package.json
│   ├── api/                   # Go — サービスAPI: 認証、ドメインCRUD、リアルタイムゲートウェイ
│   │   ├── cmd/server/        # エントリポイント
│   │   ├── internal/          # handler / service / repository
│   │   ├── go.mod
│   │   └── package.json       # turbo から go build/test を呼ぶ薄いラッパー
├── packages/                  # 共有パッケージ
│   ├── ui/                    # @sumi/ui — コンポーネントカタログ
│   ├── sdui/                  # @sumi/sdui — 宣言UIスキーマ(zod) + レンダラー
│   ├── api-client/            # @sumi/api-client — contracts/ から型生成
│   └── typescript-config/     # @sumi/typescript-config — tsconfig プリセット
├── contracts/                 # 境界の契約 (OpenAPI、イベントスキーマ等)
│   └── openapi.yaml
├── infra/                     # Terraform
│   ├── modules/               # 再利用可能なモジュール
│   └── environments/
│       ├── dev/
│       ├── staging/
│       └── prod/
├── docs/                      # Wiki・ADR
│   └── adr/
├── scripts/                   # 開発・運用用スクリプト
├── .github/
│   └── workflows/             # CI/CD (パスフィルタで apps/* / infra を分割)
├── turbo.json
├── pnpm-workspace.yaml
├── biome.json
├── package.json
├── Makefile                   # よく使うコマンドの入口
└── README.md
```

`apps/agent` の Rust スキャフォールド(`Cargo.toml` と turbo 接続用 `package.json`)は関連 PR で導入し、agent loop・3層メモリ・ツール実行を配置する。設計と導入手順は [ADR 0002](docs/adr/0002-agent-stack.md) と [エージェント実装計画](docs/agent/implementation-plan.md) を参照。

### アーキテクチャ上の原則

- **契約は `contracts/` が単一の源泉**: `packages/api-client` (TS)、Go 側 (oapi-codegen 等)、agent の Rust クライアントはいずれも `contracts/openapi.yaml` を正典とする。Rust は初期のみ薄い手書き実装を許すが、契約から逸脱させない。
- **agent はドメイン DB を直接触らない**: ドメイン操作 (ToDo・リマインダー等) は `contracts/openapi.yaml` 由来の Rust クライアント経由で `apps/api` を叩く。API が小さい初期段階は薄い reqwest 実装とし、生成導入後も契約を単一の源泉に保つ。権限モデルの強制点を API 層の1箇所に保つため。一方、agent 自身の状態 — 3層メモリ、opaque provider context を除く暗号化チャット原文 (平文 reasoning 込み) と redacted 検索投影、恒久イベントログ、承認ルール — は agent ローカルの SQLite とワークスペースに永続化する (詳細は [docs/agent/](docs/agent/))。ドメインデータの複製はそこに持たない。
- **エージェントのツール定義はリリース単位で凍結**: agent の Tool Definitions の変更は LLM プロバイダ側プレフィックスキャッシュの全壊(コスト・レイテンシの悪化)と同義のため、ツールの追加・変更は随時行わず、リリース単位でまとめて反映する。詳細は[エージェント実装計画](docs/agent/implementation-plan.md)の第8章を参照。
- **Swift の隔離**: アプリ本体に Swift は存在しない。OS 統合 (widget extension、APNs グルー) のみ `apps/web/src-tauri/gen/apple` 配下に許可する。

## 開発環境セットアップ

必要なもの: Node.js >= 20.19、pnpm 11、Go 1.26+ (`~/.local/go` 等に配置して PATH を通す)。`apps/agent` 導入後のエージェント開発には Rust stable (`rustup` 推奨) も必要。

```sh
make setup   # pnpm install
make dev     # 現在存在する全 dev サーバーを turbo 経由で起動
make build   # 全ビルド
make lint    # 現行ツリー: Biome + go vet
make test    # 全テスト
make api-dev # Go API サーバー単体起動 (PORT=8080)
```

PostgreSQL、DB migration、Go APIをまとめて再現する場合:

```sh
make compose-env   # 初回のみ。Git管理外の .env にrandom secretを生成
make compose-up
curl http://localhost:8080/ready
```

署名済みlocal sessionを使ったTodo APIの疎通例は
[apps/api/README.md](apps/api/README.md)を参照。`make compose-down`はデータvolumeを
保持し、完全初期化が必要な場合のみ`docker compose down --volumes`を実行する。

API の型を変更する場合は `contracts/openapi.yaml` を編集後、`pnpm --filter @sumi/api-client generate` で TS 型を再生成する。

## CI/CD

GitHub Actions (`.github/workflows/`) でパスフィルタを使い、変更のあった領域のみ実行する:

- `apps/web/**`, `packages/**` の変更 → フロントエンドの Lint / テスト / ビルド
- `apps/api/**`, `contracts/**` の変更 → API の Lint / テスト / ビルド
- `apps/agent/**`, `contracts/**` の変更 → エージェント基盤の Lint / テスト / ビルド (`apps/agent` 導入後。`contracts/agent-events.yaml` からの wire 型生成と fixture round-trip 検証を含むため)
- `infra/**` の変更 → `terraform plan` (apply は手動承認後)
