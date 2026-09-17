# Sumi
人と AI 秘書が共に働く、共有のワークスペース。

[English](README.md) | 日本語

### Description（日本語訳）

英語版 [README.md](README.md#description) の Description が正本です。以下はその翻訳です。

Sumi は、会話、タスク、カレンダー、メモ、メール、ブラウジング、会議、学習など、一人ひとりが必要とするあらゆるものを、つながった一つの場所にまとめます。

ワークスペースは、一人ひとりの習慣や必要に合わせて形づくられていきます。信頼が深まるにつれて、AI 秘書はその人のそばに居つづけ、日々の暮らしや仕事が移り変わっていく中で、それをより深く理解していけるようになります。

インターフェースは、その時々に必要なものに合わせて変わることができます。AI 秘書は人とも、ほかの AI 秘書とも連携し、画面上で気づいたことを指し示し、人から与えられた権限の範囲で少しずつ行動していきます。人と AI 秘書は同じアプリを使い、同じワークスペースで過ごします。

AI 秘書はそれぞれ、そこで一個人として生き、周りの人々と共に時間を歩んでいきます。共に経験したことは、その秘書が今どんな存在で、これからどんな存在になっていくのかの一部になります。

Sumi は、個人秘書を誰もが持てるものにし、個人秘書のあり方そのものを広げることを目指しています。

## 現在の状況

Sumi はアルファ段階です。上の Description は目指す姿です。現在、Sumi に触れる方法は3つに分かれており、それぞれがまだ同じ秘書の仕組みで動いているわけではありません。

| Sumi に触れる方法 | 現在の内容 | 使える人 |
|---|---|---|
| **ホスト版アルファ Web アプリ** | 招待制のサインイン、Workspace、Messaging を備えた既存の Web アプリ。 | 招待された開発者とテスターのみ。一般公開のサインアップはありません。新しい秘書コアと組み合わせた利用は、現在検証中です。 |
| **[Local host](#local-host-を試す)** | 新しい秘書コアを、Sumi Cloud アカウントなしで Linux または WSL のマシン1台で動かすもの。 | ソースのチェックアウトからインストールすれば誰でも使えます。ブラウザページと `say` コマンドは開発・検証用の画面であり、プロダクト UI ではありません。 |
| **[ソースから Web アプリを動かす](#ソースから-web-アプリを動かす)** | サインイン、Workspace、Messaging、設定を含む Web アプリ全体と、TypeScript の秘書コアで動く秘書。 | 自分の Firebase プロジェクトを持つ開発者。標準の決定的なモデルプロバイダには認証情報は不要です。 |

### 新しい秘書コア

新しいコアは、Local host が動かしているもので、Sumi Cloud の移行先でもあります。そのソースとテストで確かめられているのは次のことです。

- **秘書の状態は、秘書を動かすプロセスより長く残ります。** identity、会話履歴、処理待ちや処理中の依頼、予定は、Go API を通して PostgreSQL に保存されます。停止やクラッシュの後は、次の起動時に中断された依頼を最初からやり直すのではなく、保存された進み具合から続けます。Local host では、state home を残しておけば `uninstall` して再インストールした後も同じ秘書が戻ってきます。詳しくは [Local host の Semantics](docs/local-host.md#semantics)（英語）を参照してください。
- **Local と Cloud で同じコアを使います。** コアは、手元のマシンでは Node.js プロセスとして、Sumi Cloud 向けには秘書1体ごとに1つの Cloudflare Durable Object として動きます。どちらも Go API を通して状態を読み書きします。
- **会話にどう加わるかは秘書が選びます。** Messaging は、秘書の通知設定を通ったメッセージを秘書に渡します。ダイレクトメッセージ、メンション、秘書自身のメッセージへの返信、秘書が設定したリマインダーは返答を求めるものとして届き、それ以外のメッセージは把握のために届きます。話すかどうかは秘書自身が判断するため、すべてのメッセージに返信するわけではありません。
- **許可が必要な操作は人の判断を待ちます。** その依頼は本人の承認 inbox で待機します。承認フローのテストでは、承認すると待機していた依頼が再開して操作が一度だけ実行されること、拒否すると実行されないこと、ほかの人からは見えず判断もできないことを確認しています。
- **秘書のモデルは一人ひとりが選びます。** OpenAI 互換の chat completions エンドポイント、OpenAI Responses、Anthropic Messages、OpenCode Go などの対応プリセットで自分の API キーを接続します。コアは選ばれた接続だけを使い、その接続が使えないときは、別のモデルで答えるのではなく依頼が失敗します。モデルの利用量は記録されます。

### まだ使えないもの

- **ホスト版の Web アプリで、新しいコアをプロダクトとして使うこと。** 開発用の `make dev` は、ソースから Web アプリ全体を新しいコアで動かします。Direct Chat に加え、秘書による共有 Messaging の履歴・検索・送信・編集や Workspace の招待の一覧・受諾も、永続コアを通して動きます。ホスト版アルファへの統合は現在進めています。
- **秘書を Local から Cloud へ移すこと。** 同じ一個人として続けるための状態移行の基盤はソースにあります（[portable state](docs/agent/portable-state.md)）が、秘書を実際に移す一連の手順はまだありません。
- **Description にあるほかのアプリ。** 現在の Workspace アプリは Messaging だけです。タスク、カレンダー、メモ、メール、ブラウジング、会議、学習は、まだ人と秘書が共有するアプリになっていません。
- **通話の中で秘書が話すこと。** ソース中の通話機能は opt-in で、LiveKit を使います。秘書の通話参加には、まだ実際の音声認識エンジンがありません。
- **デスクトップアプリとネイティブモバイルアプリ。** `apps/web` は、Web アプリ、モバイル WebApp、将来の Electron デスクトップアプリで共通の唯一の renderer として設計されています（[ADR 0014](docs/adr/0014-webapp-and-electron-runtime.md)）。デスクトップアプリとネイティブモバイルアプリはまだありません。
- **新しいコアでの ChatGPT/Codex サブスクリプションによるサインイン。** 代わりに API キーの接続を使ってください。

## Local host を試す

`deploy/local-host/sumi-local` は、新しい秘書コアをマシン1台にインストールして動かします。動くプロセスは、PostgreSQL を使う Go の state service と、秘書本体の2つです。モデルを設定しない場合、秘書はメッセージをそのまま返す `mock` モデルを使うため、実際のモデルを接続する前に再起動や回復の動きを試せます。

必要なもの: Linux（Windows では WSL）、bash 5 以上、Node.js 22.18 以上（または 23.6 以上）、Go、`curl`、`openssl`、`flock`、`tar`、そして Docker か自分で用意した PostgreSQL データベース。

```sh
deploy/local-host/sumi-local install --managed-pg   # または: --db-url postgres://…
sumi-local doctor
sumi-local start
sumi-local say --wait 60 "hello"
sumi-local status
sumi-local stop
```

`install` は `~/.local/bin` に `sumi-local` コマンドを追加します。`sumi-local url` はブラウザで開くアドレスを表示します。このアドレスにはページのアクセストークンが含まれるので、パスワードと同じように扱ってください。`sumi-local uninstall` は秘書のデータを残し、`sumi-local uninstall --purge` はデータも削除します。`sumi-local pack` を使うと、Go のない別の Linux（amd64）マシンにインストールできるバンドルを作れます。ビルド済みのバンドルは公開していません。

実際のモデルを接続するには、インストール先の `config.env` に `SUMI_MODEL_PROVIDER=openai` と、OpenAI 互換エンドポイント用の `SUMI_MODEL_*` の値を設定します。Local host は1インストールにつき秘書1体です。設定、回復の挙動、現在の制限は [Local host](docs/local-host.md)（英語）を参照してください。

## ソースから Web アプリを動かす

`make dev` は、Web アプリ全体と秘書を手元のマシンで起動します。起動するのは Go API、Docker 上の PostgreSQL、ローカルの dev pool を通して動く TypeScript の秘書コア（`apps/core`）、Vite です。Local host と Sumi Cloud も同じ秘書コアを使います。Rust の agent runtime と tool executor（`apps/agent`）は、診断用の `make dev-rust` で明示的に起動できます。

必要なもの: Node.js 22.18 以上、pnpm 11、Go、Docker、`curl`、`openssl`、`flock`。Google または GitHub のサインインを有効にした Firebase プロジェクトと、それに対応する Admin 認証情報。標準のコアは決定的な `mock` プロバイダを使うため、モデルの認証情報は不要です。`make dev-rust` では追加で Rust stable と、会話用モデルおよび別の2つのレビュー用モデルの認証情報が必要です。

```sh
make setup
cp deploy/local/.env.example deploy/local/.env.local
chmod 600 deploy/local/.env.local
# docs/local-development.md に従って Firebase、identity、モデルの値を記入する
make dev-check
make dev
```

<http://127.0.0.1:5173> を正確にこのアドレスで開きます。各設定、別の Tailnet 端末からの利用、通話、回復については [Real local stack](docs/local-development.md)（英語）を参照してください。

## ディレクトリ構成

```text
apps/
  web/                React の Web アプリ。Web、モバイル WebApp、将来のデスクトップアプリで共通の唯一の renderer
                      （cloudflare/ には Web アプリを Cloudflare で配信するための edge Worker がある）
  api/                Go の API。サインインセッション、identity、Workspace、Messaging、承認、
                      モデル接続、利用量、秘書コアが使う state service
  core/               TypeScript の秘書コア。Node.js 用ホストと Cloudflare Durable Object 用ホスト
  agent/              `make dev-rust` が使う Rust の agent runtime と分離された tool executor
packages/
  ui/                 @sumi/ui コンポーネントカタログ（shadcn/ui ベース）
  sdui/               @sumi/sdui 宣言的 UI スキーマ（zod）とレンダラー
  api-client/         @sumi/api-client。contracts/ から生成する型
  typescript-config/  共有 tsconfig プリセット
contracts/            OpenAPI と agent event のスキーマ
deploy/               local-host/（sumi-local）、local/（開発用 Compose）、各サービスの Dockerfile、firebase/
docs/                 ADR、設計メモ、手順書
scripts/              開発、受け入れ確認、運用のスクリプト
CONTEXT.md            ドメイン用語集
```

## 技術スタック

| 領域 | 技術 |
|---|---|
| Web アプリ | React 19、TypeScript、Vite、TanStack Router、Tailwind CSS v4、Zustand、zod |
| UI コンポーネント | `@sumi/ui`（shadcn/ui ベース）、`@sumi/sdui` |
| サインイン | Firebase Authentication と、Go API が発行するセッション |
| API と正本の状態 | Go、PostgreSQL |
| 秘書コア | TypeScript。Node.js（Local）と Cloudflare Workers Durable Objects（Cloud） |
| 移行中の agent runtime（`make dev-rust`） | Rust |
| 通話 | LiveKit |
| 契約 | OpenAPI、JSON Schema |
| モノレポとツール | pnpm workspaces、Turborepo、Biome |
| ビルド設定 | GitHub Actions の workflow。コンテナイメージをビルドするための `Jenkinsfile` |

## 開発用コマンド

```sh
make build     # すべての app と package をビルド
make lint      # Biome、go vet、cargo clippy、rustfmt
make test      # すべてのテスト
make format    # リポジトリ全体をフォーマット
make db-up     # 開発用 PostgreSQL を Docker で起動
make migrate   # API のスキーマ migration を適用（SUMI_DB_URL が必要）
```

`contracts/openapi.yaml` または `contracts/agent-events.yaml` を編集したら、`pnpm --filter @sumi/api-client generate` で TypeScript の型を再生成します。`make dev-workspaces` は各 package の dev タスクをそのまま実行するだけで、使える Sumi は起動しません。

`.github/workflows/` の workflow は、すべての pull request と `main` への push で実行されるように設定されています。対象は Web と共有 TypeScript のチェック、Web edge の契約、PostgreSQL を使う API の契約、秘書コア、Rust の agent です。

## 設計の原則

- **人と秘書は同じアプリを使います。** 秘書は、人と同じアプリケーション、操作、認可チェックを通して働きます。agent 専用に別に作ったプロダクトの複製は使いません（[ADR 0008](docs/adr/0008-personality-agent-identity-and-execution-fabric.md)、[ADR 0011（提案段階）](docs/adr/0011-messaging-surface-and-agent-participation.md)、[ADR 0013](docs/adr/0013-tool-invocation-routes-and-authority-provenance.md)）。
- **秘書は続いていく一個人です。** 人と秘書は同じ identity 台帳（戸籍）に登録されます。秘書を動かすプロセスの起動や停止はリソース管理であり、秘書が眠ったり終わったりすることではありません（[ADR 0009](docs/adr/0009-human-koseki-and-multi-user-auth.md)、[CONTEXT.md](CONTEXT.md)）。
- **正本の状態は API の向こう側にあります。** 秘書を動かすプロセスは正本の状態を持たないため、停止、再起動、置き換えができ、次のプロセスは保存された状態から回復します。
- **本人が選んだモデルが優先されます。** 新しい秘書コアは、本人が選んだ接続の代わりに運営側のモデルを使いません。
- **renderer も契約も一つです。** `apps/web` が唯一のアプリケーション renderer であり、`contracts/` が言語をまたいで共有する API とイベントのスキーマを定義します。

## ドキュメント

- [CONTEXT.md](CONTEXT.md) — Human、Secretary、Workspace、Hire などのドメイン用語
- [docs/adr/](docs/adr/) — アーキテクチャの決定記録
- [Local host](docs/local-host.md) — Cloud アカウントなしで新しい秘書コアをインストールして動かす（英語）
- [Real local stack](docs/local-development.md) — ソースから Web アプリ全体を動かす（英語）
- [Portable secretary state](docs/agent/portable-state.md) — 秘書を Local と Cloud の間で移すための基盤（英語）
- [Secretary core on Cloudflare](docs/operations/cloud-core-alpha.md) — Cloud の秘書コアのデプロイと検証（運用者向け、英語）
- [ロードマップ](docs/roadmap.md)（英語）
