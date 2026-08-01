
# Sumi
A shared workspace where people and their AI secretaries work together.

### Description

Sumi brings conversations, tasks, calendars, notes, email, browsing, meetings, studying, and whatever else each person needs into one connected place.

The workspace takes shape around each person's routines and needs. As trust grows, their AI secretary can remain by their side and understand more of their everyday life and work as it unfolds.

The interface can adapt to what each moment calls for. AI secretaries can coordinate with people and one another, point things out on screen, and gradually take action with the permissions people give them. People and their AI secretaries use the same apps and inhabit the same workspace.

Each AI secretary lives there as an individual, moving through time alongside the people around them. What they live through together becomes part of who each secretary is and who they are becoming.

Sumi aims to democratize access to personal secretaries and extend what a personal secretary can be.

## 技術スタック

| レイヤ | 技術 |
|---|---|
| フロントエンド | React 19 + TypeScript + Vite |
| ルーティング | TanStack Router |
| スタイリング / UI | Tailwind CSS v4 + 自作コンポーネントカタログ (shadcn/ui ベース) |
| 動的UI (SDUI) | zod スキーマ + component registry |
| 状態管理 | Zustand + TanStack Query |
| シェル (デスクトップ / モバイル) | Tauri 2 |
| エージェント基盤 | Rust (durable runtime + 分離 tool executor) |
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

`apps/agent` には production bootstrap、durable agent loop、3 層メモリ、
provider 接続、分離 tool executor を実装している。設計判断は
[ADR 0002](docs/adr/0002-agent-stack.md)、
[ADR 0007](docs/adr/0007-production-runtime-bootstrap-boundary.md)、
[ADR 0008](docs/adr/0008-personality-agent-identity-and-execution-fabric.md) を参照。

### アーキテクチャ上の原則

- **契約は `contracts/` が単一の源泉**: `packages/api-client` (TS)、Go 側 (oapi-codegen 等)、agent の Rust クライアントはいずれも `contracts/openapi.yaml` を正典とする。Rust は初期のみ薄い手書き実装を許すが、契約から逸脱させない。
- **agent はドメイン DB を直接触らない**: ドメイン操作 (ToDo・リマインダー等) は `contracts/openapi.yaml` 由来の Rust クライアント経由で `apps/api` を叩く。API が小さい初期段階は薄い reqwest 実装とし、生成導入後も契約を単一の源泉に保つ。権限モデルの強制点を API 層の1箇所に保つため。一方、agent 自身の状態 — 3層メモリ、opaque provider context を除く暗号化チャット原文 (平文 reasoning 込み) と redacted 検索投影、恒久イベントログ、承認ルール — は agent ローカルの SQLite とワークスペースに永続化する (詳細は [docs/agent/](docs/agent/))。ドメインデータの複製はそこに持たない。
- **エージェントのツール定義はリリース単位で凍結**: agent の Tool Definitions の変更は LLM プロバイダ側プレフィックスキャッシュの全壊(コスト・レイテンシの悪化)と同義のため、ツールの追加・変更は随時行わず、リリース単位でまとめて反映する。詳細は[エージェント実装計画](docs/agent/implementation-plan.md)の第8章を参照。
- **Swift の隔離**: アプリ本体に Swift は存在しない。OS 統合 (widget extension、APNs グルー) のみ `apps/web/src-tauri/gen/apple` 配下に許可する。

## 開発環境セットアップ

必要なもの: Node.js >= 20.19、pnpm 11、Go、Rust stable、`curl`、
`openssl`、`flock`。ブラウザから実際のエージェントを使う手順と Firebase /
provider credential の設定は
[Real local stack](docs/local-development.md) を参照。

```sh
make setup     # pnpm install
make dev-check # Firebase/provider/identity 設定を検証
make dev       # 認証済み real stack を依存順・readiness gate 付きで起動
make build     # 全ビルド
make lint      # Biome + Go + Rust lint
make test      # 全テスト
```

既定の `make dev` URL は正確に `http://127.0.0.1:5173`。Vite が同一
origin の `/auth` (HTTP) と `/direct-chat` (WebSocket) を Go API へ
proxy する。別の Tailnet 端末から直接使う場合は、wildcard ではなく
`SUMI_PUBLIC_LISTEN=<literal-tailscale-ipv4>:8080` を設定する。詳細は
[Real local stack](docs/local-development.md#direct-tailnet-access) を参照。
`make dev-workspaces` は raw Turbo task 用であり、利用可能な product stack を
起動するコマンドではない。

API の型を変更する場合は `contracts/openapi.yaml` を編集後、`pnpm --filter @sumi/api-client generate` で TS 型を再生成する。

## CI/CD

GitHub Actions (`.github/workflows/`) でパスフィルタを使い、変更のあった領域のみ実行する:

- `apps/web/**`, `packages/**` の変更 → フロントエンドの Lint / テスト / ビルド
- `apps/api/**`, `contracts/**` の変更 → API の Lint / テスト / ビルド
- `apps/agent/**`, `contracts/**` の変更 → エージェント基盤の Lint / テスト / ビルド (`apps/agent` 導入後。`contracts/agent-events.yaml` からの wire 型生成と fixture round-trip 検証を含むため)
- `infra/**` の変更 → `terraform plan` (apply は手動承認後)
