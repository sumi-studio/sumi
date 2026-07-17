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
| エージェント基盤 | Rust (pi を設計参照) |
| バックエンド | Go |
| API 定義 | OpenAPI 3.1 (契約ファースト) |
| モノレポ | pnpm workspaces + Turborepo |
| Lint / Format | Biome |
| インフラ | Terraform |
| CI/CD | GitHub Actions |

選定理由は [docs/adr/](docs/adr/) を参照。

## ディレクトリ構成

```
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
│   └── agent/                 # Rust — エージェント基盤: agent loop、3層メモリ、ツール実行
│       ├── src/
│       └── package.json
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

### アーキテクチャ上の原則

- **契約は `contracts/` が単一の源泉**: `packages/api-client` (TS) も Go 側 (oapi-codegen 等) も `contracts/openapi.yaml` から一方向に生成する。
- **agent は DB を直接触らない**: ドメイン操作は `@sumi/api-client` 経由で `apps/api` を叩く。権限モデルの強制点を API 層の1箇所に保つため。agent が自前で持つ永続化は自身のメモリストアのみ。
- **Swift の隔離**: アプリ本体に Swift は存在しない。OS 統合 (widget extension、APNs グルー) のみ `apps/web/src-tauri/gen/apple` 配下に許可する。

## 開発環境セットアップ

必要なもの: Node.js >= 20.19、pnpm 11、Go 1.26+ (`~/.local/go` 等に配置して PATH を通す)

```sh
make setup   # pnpm install
make dev     # 全 dev サーバー起動 (web: Vite, agent: tsx watch)
make build   # 全ビルド
make lint    # Biome + go vet
make test    # 全テスト
make api-dev # Go API サーバー単体起動 (PORT=8080)
```

API の型を変更する場合は `contracts/openapi.yaml` を編集後、`pnpm --filter @sumi/api-client generate` で TS 型を再生成する。

## CI/CD

GitHub Actions (`.github/workflows/`) でパスフィルタを使い、変更のあった領域のみ実行する:

- `apps/web/**`, `packages/**` の変更 → フロントエンドの Lint / テスト / ビルド
- `apps/api/**`, `contracts/**` の変更 → API の Lint / テスト / ビルド
- `apps/agent/**`, `packages/**` の変更 → エージェント基盤の Lint / テスト / ビルド
- `infra/**` の変更 → `terraform plan` (apply は手動承認後)
