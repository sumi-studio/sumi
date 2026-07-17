# エージェントワークスペース設計

- Status: Draft (v1)
- Date: 2026-07-17
- 前提: [ADR 0002](../adr/0002-agent-stack.md)、[3層メモリ設計](memory.md)

## 原則

**エージェントはユーザーごとの Linux ワークスペース (作業机) を持つ。**

- エージェントは Linux ファイルシステム上での作業が得意である。気軽にメモを取り、フォルダで整理し、計算は bash で行い、Python / Node も最低限使う。ファイルシステムの代替を自前で発明しない
- ユーザーごとにエージェントへやらせたいことが違うため、環境そのものを分離する
- 3層メモリの「戦略的忘却」はワークスペース上のメモが受け皿になる。**ディスクの永続性は製品の一部**であり、消せない

## 設計

- エージェントループ (apps/agent の Rust プロセス) は**ワークスペースコンテナの中で動く**が、制御プレーンとツール実行プレーンは OS 権限を分ける。agent ランタイムは `sumi-agent` UID、ファイル操作・bash は権限を落とした `sumi-tool` UID の executor プロセスで実行する
- 「エージェントが継続して存在すること」と「プロセスが常駐すること」は分離する。人格・記憶・会話はすべて永続データであり、コンテナは器。**ディスクは永続、コンピュートは寝かせられる**
- 自由に使える `/workspace` は `sumi-tool` に read/write で渡す。DB・メモリストア・承認ルール・API キー等の内部状態は `/var/lib/sumi` と agent ランタイム環境に置き、`sumi-agent` のみ read/write 可 (`0700`) とする。ツールから記憶を調べる必要がある場合は、生DBではなく read-only の専用ツールを経由する
- リマインダー・アラーム等の自発的動作は、常駐ではなく中央のスケジューラがエージェントを起こす形で実現する (スケジューラの設計は未決)

## 接続トポロジ

```
web (React) ⇔ api (Go, WebSocket ゲートウェイ) ⇔ ユーザーごとの agent コンテナ
```

- agent ⇔ api 間のイベント/コマンドプロトコルは contracts/ にスキーマを置く (現時点では未実装。認証・再送・ACKを含む案は [実装計画 第11章](implementation-plan.md) を参照)
- agent はドメイン操作を api 経由でのみ行う (ADR 0001 の原則を維持)。agent の直接の持ち物は、ワークスペース内のファイルと、ローカル SQLite の自己状態 (3層メモリ・チャットログ全文・恒久イベント・承認ルール — [実装計画 第10章](implementation-plan.md) 参照)。ドメインデータはここに複製しない
- agent→api の outbound WebSocket は短命の署名tokenで tenant / agent / conversation / process generation を束縛し、APIは最新generationだけを受理する。API→agent commandもdurable seq・command_id・Received/Applied ACKで再送と重複排除を行い、接続断を権限境界や副作用の二重実行へ波及させない

## セキュリティ境界

三段構えにする:

1. **アプリ層**: ツール実行前フックによる権限承認フロー (ユーザーへリクエスト → 承認/拒否)。「権限を要求する権限」の実装
2. **OS 層 (コンテナ内)**: `sumi-agent` と `sumi-tool` を別UIDにし、executor には `/workspace` だけを read/write で見せる。環境変数は `PATH/HOME/LANG` 等へ絞り、agent 親プロセスの `/proc` と内部状態ディレクトリを見せない。`read_file` 等も canonicalize 後のパスが workspace root 配下であることを確認し、symlink 越境を拒否する。外向き network は UID 分離では制限されないため、executor を専用 network namespace (non-loopback インターフェイスなし) で起動して egress を物理的に遮断する — 承認フローとは独立した OS 境界であり、bash からの外部通信は egress プロキシを設計するまで非対応 (実装計画 §8.3)
3. **テナント層**: ハッカソン〜開発期はユーザーごとの Docker コンテナ、他人同士を同居させる Sumi Cloud では microVM (Firecracker 系) に上げる。microVM はテナント/ホスト間の境界であり、同一ゲスト内の runtime/executor 分離の代替にはしない

## 段階的展開

| 段階 | 構成 |
|---|---|
| ハッカソン / 開発期 | EC2 1台 + ユーザーごとの Docker コンテナ + runtime/executor 別UID + 永続ボリューム |
| Sumi Cloud (マルチテナント) | 同じ runtime/executor 分離を保った microVM、ボリュームは EBS/EFS 等で永続化 |
| OSS ローカル版 | ユーザー自身のマシンでコンテナ or 素のプロセスとして動作 |

## 未決事項

- スケジューラ (リマインダー・アラームの起動主体) の設計
- コンテナのライフサイクル管理 (起こす・寝かせる・回収する) の実装方式
- ワークスペースの容量制限・バックアップ方針
