# sumi-studio

<!-- TODO: プロジェクトの概要を1〜2文で記載する -->

## 技術スタック

| レイヤ     | 技術             |
|---------|----------------|
| フロントエンド |                |
| バックエンド  | Go             |
| API 定義  | OpenAPI 3.1    |
| インフラ    | Terraform      |
| CI/CD   | GitHub Actions |

## ディレクトリ構成

```
sumi-studio/
├── frontend/              # フロントエンド
│   ├── src/
│   └── package.json
├── backend/               # Go バックエンド
│   ├── cmd/
│   │   └── server/
│   │       └── main.go    # エントリポイント
│   ├── internal/          # 外部から import させないコード
│   │   ├── handler/       # HTTP ハンドラ
│   │   ├── service/       # ビジネスロジック
│   │   └── repository/    # データアクセス
│   ├── pkg/               # 公開してもよい共通コード
│   ├── go.mod
│   └── go.sum
├── api/                   # OpenAPI 定義
│   └── openapi.yaml
├── infra/                 # Terraform
│   ├── modules/           # 再利用可能なモジュール
│   └── environments/
│       ├── dev/
│       ├── staging/
│       └── prod/
├── scripts/               # 開発・運用用スクリプト
├── .github/
│   └── workflows/         # CI/CD (パスフィルタで frontend / backend / infra を分割)
├── Makefile               # よく使うコマンドの入口
└── README.md
```

## CI/CD

GitHub Actions (`.github/workflows/`) でパスフィルタを使い、変更のあった領域のみ実行する:

- `frontend/**` の変更 → フロントエンドの Lint / テスト / ビルド
- `backend/**` の変更 → バックエンドの Lint / テスト / ビルド
- `infra/**` の変更 → `terraform plan` (apply は手動承認後)
