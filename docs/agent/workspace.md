# エージェントワークスペース設計

- Status: Draft (v1)
- Date: 2026-07-17
- 前提: [ADR 0002](../adr/0002-agent-stack.md)、[3層メモリ設計](memory.md)

## 原則

**エージェントはユーザーごとの Linux ワークスペース (作業机) を持つ。**

- エージェントは Linux ファイルシステム上での作業が得意である。気軽にメモを取り、フォルダで整理し、計算は bash で行い、Python / Node も最低限使う。ファイルシステムの代替を自前で発明しない
- ユーザーごとにエージェントへやらせたいことが違うため、環境そのものを分離する
- 3層メモリの「戦略的忘却」はワークスペース上のメモが受け皿になる。**セッション終了やコンピュート再作成で勝手に消えない永続性は製品の一部**である。一方、ユーザー削除・tenant retention・容量 quota に従う削除まで禁止する意味ではない

## 設計

- エージェントループ (apps/agent の Rust プロセス) とツール executor は同じ永続 `/workspace` を共有するが、別の OS sandbox として起動する。agent ランタイムは `sumi-agent` UID、ファイル操作・bash は `sumi-tool` UID とし、Docker 段階では executor を `network_mode=none` の sidecar コンテナ、microVM 段階では専用 mount/PID/network namespace 内のプロセスにする
- 「エージェントが継続して存在すること」と「プロセスが常駐すること」は分離する。人格・記憶・会話はすべて永続データであり、コンテナは器。**ディスクは永続、コンピュートは寝かせられる**
- 自由に使える `/workspace` は executor に read/write で渡す。executor の filesystem root は最小 rootfs とし、`/workspace` と明示した read-only runtime file 以外を mount しない。DB・メモリストア・承認ルール・API キー等の内部状態は runtime 側の `/var/lib/sumi` と環境に置き、executor へ mount しない。ツールから記憶を調べる必要がある場合は、生DBではなく read-only の専用ツールを経由する
- `/workspace` の POSIX 権限契約: `sumi-agent`/`sumi-tool` を共有 group (`sumi-workspace`) の副グループに所属させ、`/workspace` 配下の全ディレクトリに setgid ビットを立てて新規作成物の group 所有を継承させる。両プロセスの umask は `0007` に揃え、既定モードはファイル `0660`・ディレクトリ `2770` とする。所有 UID がどちらでも共有 group 経由で相手が read/write でき、ディレクトリの group write/execute により相手の作成物を create/rename/delete できる一方、group 外(other)には触らせない。`0600`/`0700` や group write のない `0640`/`2750` のように相手 UID を締め出すモードは作らない。runtime が生成する `.attachments/<conversation_id>` 等の artifact ディレクトリも同じ group/setgid/umask を継承し、片方の UID だけの排他所有にしない。default ACL (`setfacl -d`) はこの相互 read/write の基礎契約には使わず、group だけでは表せない追加の個別許可が必要な場合にだけ使う。M2/M4 の fault-injection テストで、`sumi-agent`/`sumi-tool` の双方が相手 UID の作成物を create/read/write/rename/delete できることを検証する
- `/workspace/.attachments/<conversation_id>` と `/workspace/.tool-output/<conversation_id>` は runtime が生成する conversation-owned artifact 専用prefixとし、ユーザー作成ファイルとは区別する。conversation reset は旧IDの2prefixだけを tombstone に従って冪等削除し、通常の workspace は残す。backup 復元時も旧IDのprefix削除を先に再適用する
- executor の起動主体は deployment supervisor とする。Docker では container orchestrator が sidecar を明示的な環境許可リスト (`PATH` / `HOME` / `LANG` / executor generation) と FD/mount allowlist で作成し、runtime に Docker socket を渡さない。microVM/ローカル process では guest supervisor が `env_clear` 後に同じ許可リストだけを設定し、stdio と専用 Unix socket 以外の継承 file descriptor を `close_range`/close-on-exec で閉じて起動する。executor から runtime へ到達できる経路は認証済み専用 IPC だけとし、socket directory 自体も runtime/executor の専用 volume とする
- リマインダー・アラーム等の自発的動作は、常駐ではなく中央のスケジューラがエージェントを起こす形で実現する (スケジューラの設計は未決)

## 接続トポロジ

```text
web (React) ⇔ api (Go, WebSocket ゲートウェイ) ⇔ ユーザーごとの agent コンテナ
```

- agent ⇔ api 間のイベント/コマンドプロトコルは contracts/ にスキーマを置く (現時点では未実装。認証・再送・ACKを含む案は [実装計画 第11章](implementation-plan.md) を参照)
- agent はドメイン操作を api 経由でのみ行う (ADR 0001 の原則を維持)。agent の直接の持ち物は、ワークスペース内のファイルと、ローカル SQLite の自己状態 (3層メモリ・公開チャット transcript・暗号化 provider context・恒久イベント・承認ルール — [実装計画 第10章](implementation-plan.md) 参照)。ドメインデータはここに複製しない
- agent→api の outbound WebSocket は短命の署名tokenで tenant / agent / conversation / process generation を束縛し、APIは最新generationだけを受理する。API→agent commandもdurable seq・command_id・Received/Applied ACKで再送と重複排除を行い、接続断を権限境界や副作用の二重実行へ波及させない

## セキュリティ境界

三段構えにする:

1. **アプリ層**: ツール実行前フックによる権限承認フロー (ユーザーへリクエスト → 承認/拒否)。「権限を要求する権限」の実装
2. **OS 層**: `sumi-agent` と `sumi-tool` を別UID・別 filesystem/PID/network sandbox にし、executor には `/workspace` だけを read/write で見せる。Docker では executor sidecar を `network_mode=none`、read-only rootfs、capability drop all、no-new-privileges で起動する。単一コンテナ内の `unshare(CLONE_NEWNET)` は Docker 既定 seccomp/capability では成立しないためリリース構成に使わない。microVM 内でも mount namespace + `pivot_root`/chroot 相当で同じ可視範囲を強制する。`read_file` 等の open は workspace dirfd を起点に `openat2(RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS)` 相当で行い、canonicalize→open の TOCTOU を許さない。外向き通信は egress プロキシを設計するまで非対応 (実装計画 §8.3)
3. **テナント層**: ハッカソン〜開発期はユーザーごとの Docker コンテナ、他人同士を同居させる Sumi Cloud では microVM (Firecracker 系) に上げる。microVM はテナント/ホスト間の境界であり、同一ゲスト内の runtime/executor 分離の代替にはしない

## 段階的展開

| 段階 | 構成 |
|---|---|
| ハッカソン / 開発期 | EC2 1台 + ユーザーごとの runtime container / executor sidecar + 永続 `/workspace` volume + 専用 IPC volume |
| Sumi Cloud (マルチテナント) | 同じ runtime/executor 分離を保った microVM、ボリュームは EBS/EFS 等で永続化 |
| OSS ローカル版 | ユーザー自身のマシンでコンテナ or 素のプロセスとして動作 |

段階展開の前に、Docker と microVM の両方で次の quota を強制できることを rollout gate とする。既定値は初期値であり tenant plan ごとに下げられる:

| 資源 | 既定上限 | Docker | microVM |
|---|---:|---|---|
| `/workspace` disk / inode | 2 GiB / 200,000 | project quota または上限付き volume | guest filesystem quota |
| PID | 64 | cgroup `pids.max` | guest cgroup |
| CPU bandwidth / command CPU-time | 1 vCPU cap / 120 CPU秒 | cgroup `cpu.max` + `cpu.stat` watcher | vCPU割当 + guest cgroup watcher |
| memory | 512 MiB | cgroup `memory.max`/`memory.events` | guest cgroup |
| 1 command wall runtime | 120秒 | executor watchdog + per-execution cgroup kill / sandbox recycle | guest watchdog + per-execution cgroup kill / PID namespace recycle |
| 1 command output / `.tool-output` 合計 | 10 MiB / 100 MiB | capture counter + volume quota | 同左 |

controller の意味を一律の「超過時 kill」にしない。`cpu.max` は1 vCPUへ throttle し、別の `cpu.stat` 差分が120 CPU秒へ達した場合だけ watchdog が停止を要求する。`pids.max` は fork を、disk/inode quota は write を拒否するため、executor は `pids.events` と `EAGAIN`/`EDQUOT`/`ENOSPC` を `ResourceLimit` へ分類する。memory は `memory.events` の max/oom/oom_kill を観測し、wall runtime・CPU-time・output counter は watchdog が強制終了する。停止が必要な経路は process group ではなく supervisor 所有の command child cgroup/PID namespace 全体を `cgroup.kill` 相当で回収し、delegation 不能なら command 専用 executor sandbox を破棄・再作成する。`setsid`/`setpgid` で離脱した descendant も `populated=0` 確認まで残さない。どの経路も wait/reap 後に limit 種別とそれまでの bounded output を返す。process-group kill は low-trust local mode の best-effort fallback に限り、Cloud rollout gate へ数えない。

deployment supervisor は runtime、executor、IPC に同じ世代番号を与え、command ごとの execution cgroup/sandbox も登録する。runtime 終了・heartbeat 喪失時にその世代の登録済み execution boundary と executor sandbox 全体を kill/reap してから新世代を起動する。再起動時に `running` だった tool execution は `indeterminate` として閉じ、同じ tool call を自動再実行しない。ドメイン操作は `command_id/tool_call_id` の idempotency key を apps/api まで伝播する。quota 拒否/throttle、`setsid ... &` でdetachしたdescendantの強制終了、再起動後の回収を fault-injection で確認するまで段階展開しない。

`indeterminate` で閉じた execution の再開・照合契約:

- **可視性**: `indeterminate` は `failed` とは別状態として `tool_executions.state`(実装計画 §10.1)に残り、対応する tool 結果は `MessageStart/End` → `TurnEnd` → `AgentEnd` の恒久イベントで閉じる(実装計画 §10.2)。tool 結果の本文には「結果不明(副作用未確認)」である旨と `tool_call_id`/`idempotency_key` を明記し、UI/API 側が「失敗」と区別してユーザーへ提示できるようにする
- **照合**: エージェントは自動再実行しない。domain mutation tool は `command_id/tool_call_id` を idempotency key として apps/api に伝播済みのため、ユーザーが確認を求めた場合はエージェントが該当ドメイン API を read-only に照会し、副作用が実際に完了していたかを事後確認する
- **リトライ**: 照合の結果「未完了」と確定した場合は、**元の execution と同じ `idempotency_key`** で再実行し、ドメイン側の冪等性処理により重複実行させない。ユーザーが照合なしに「もう一度実行して」と明示的に確認した場合に限り、新しい `tool_call_id`/`idempotency_key` を発行した別実行として扱ってよい(既定は同一キー再利用であり、新規キーは明示確認済みリトライの例外)

## 未決事項

- スケジューラ (リマインダー・アラームの起動主体) の設計
- コンテナのライフサイクル管理 (起こす・寝かせる・回収する) の実装方式
- バックアップ頻度と tenant plan ごとの quota 値
