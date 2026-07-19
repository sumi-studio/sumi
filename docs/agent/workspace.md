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

- エージェントループ (apps/agent の Rust プロセス) とツール executor は別の OS sandbox として起動する。永続`/workspace`をread/write mountするのは`/workspace`上の全操作を担うexecutor (`sumi-tool` UID)だけとし、agent runtime (`sumi-agent` UID)はworkspaceを直接mount/read/writeしない。conversation-owned artifact はさらに専用 artifact broker (`sumi-artifact` UID)へ分離し、永続 artifact volume を mount するのは broker だけとする。runtimeの`read_file`/`write_file`/`edit_file`/artifact保存/grep/bash要求は認証済みRPCへ渡し、結果はboundedなbyte streamまたはopaque artifact handleで受ける。Cloud は tenant ごとの microVM 内で runtime / executor / artifact broker を別UID・別 mount namespace として分離する。Docker は同じ境界を高速に検証するローカル/CIハーネスであり、別の製品仕様ではない
- 「エージェントが継続して存在すること」と「プロセスが常駐すること」は分離する。人格・記憶・会話はすべて永続データであり、コンテナは器。**ディスクは永続、コンピュートは寝かせられる**
- 自由に使える `/workspace` は executor にだけ read/write で渡す。executor の filesystem root は最小 rootfs とし、`/workspace` と明示した read-only runtime file 以外を mount しない。DB・メモリストア・承認ルール・API キー等の内部状態は runtime 側の `/var/lib/sumi` と環境に置き、conversation-owned artifact volume は artifact broker だけに置き、いずれも executor へ mount しない。ツールから記憶を調べる必要がある場合は、生DBではなく runtime が提供するread-only専用ツールを経由する
- `/workspace` の POSIX 権限契約は「runtime/executorの異UIDが任意の作成物を直接相互read/writeする」ことを要求しない。`umask`は要求modeから権限を削るだけで、bash子プロセスが明示作成した`0600`/`0700`へgroup権限を追加できないため、その保証には使えない。任意のbash子を含むworkspace操作は同じ`executor UID`のexecution boundary内で行い、後続のファイルツールもexecutor RPC経由で同じUIDとしてopenする。runtimeはpathを直接openせず、executorが返した検証済みbounded contentだけを扱う。親ディレクトリのcreate/rename/delete権限もexecutor/supervisor側に集約する
- artifact broker は `/var/lib/sumi-artifacts/<conversation_id>/{attachments,tool-output}` を管理し、呼出し側へ filesystem path ではなく `artifact://<conversation_id>/<kind>/<artifact_id>` のopaque handleだけを返す。`put_attachment`/`append_tool_output`/`read_artifact`/`grep_artifact` RPCは認証claimのconversation IDとhandleを照合し、broker所有dirfdから全componentを `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS` / `O_NOFOLLOW` 相当で開く。親を`mkdir(mode=0700)`後に`fchmod(0700)`、ファイルを`open(mode=0600)`後に`fchmod(0600)`し、書込み完了はfsync/close後に返す。runtime、executor、bashはartifact volumeをmountせず、broker socket/FDもbash子へ継承・公開しない
- conversation reset は旧IDのartifact subtreeだけをtombstoneに従って冪等削除し、通常のworkspaceは残す。削除はruntime/executor UIDの直接unlinkではなく、旧generationをfenceしたdeployment supervisorからの認証済み`delete_conversation_artifacts(old_id, tombstone_id)` RPCで行う。brokerは自身のvolume root dirfdからconversation IDを再検証し、symlinkを一切辿らないfd-relative walkで子をunlinkして親をfsyncする。backup復元時も新runtimeを起動する前に同じtombstoneから削除を先に再適用する
- executor と artifact broker の起動主体は deployment supervisor とする。Docker では container orchestrator が各sidecarを明示的な環境許可リスト (`PATH` / `HOME` / `LANG` / generation) と相互に排他的な FD/mount allowlist で作成し、runtime に Docker socket、workspace volume、artifact volumeを渡さない。microVM/ローカル process では guest supervisor が `env_clear` 後に同じ許可リストだけを設定し、stdio と各専用 Unix socket 以外の継承 file descriptor を `close_range`/close-on-exec で閉じて起動する。executor/artifact brokerからruntimeへ到達できる経路は認証済み専用 IPC だけとする。artifact broker socketはruntimeとexecutor brokerだけから到達可能にし、bash execution boundaryへはmount・継承しない
- リマインダー・アラーム等の自発的動作は、常駐ではなく中央control planeのスケジューラがエージェントを起こす形で実現する。agent runtimeは署名済みwake commandを通常のdurable command経路で受ける

## 接続トポロジ

```text
web (React) ⇔ api (Go, WebSocket ゲートウェイ) ⇔ tenantごとのmicroVM内のagent runtime
```

- agent ⇔ api 間のイベント/コマンドプロトコルは contracts/ にスキーマを置く (現時点では未実装。認証・再送・ACKを含む案は [実装計画 第11章](implementation-plan.md) を参照)
- agent はドメイン操作を api 経由でのみ行う (ADR 0001 の原則を維持)。agent の直接の持ち物は、ユーザー作成ワークスペース、broker管理のconversation artifact、ローカル SQLite の自己状態 (3層メモリ・公開チャット transcript・暗号化 provider context・恒久イベント・承認ルール — [実装計画 第10章](implementation-plan.md) 参照)。ドメインデータはここに複製しない
- agent→api の outbound WebSocket は短命の署名tokenで tenant / agent / conversation / process generation を束縛し、APIは最新generationだけを受理する。ConnectionSupervisorは切断ごとにfresh credentialで再認証・helloし、同じ接続epochのreader/writer両方を交換してdurable cursorからcatch-upする。API→agent commandもdurable seq・command_id・Received/Applied ACKで再送と重複排除を行い、接続断を権限境界や副作用の二重実行へ波及させない

## セキュリティ境界

三段構えにする:

1. **アプリ層**: ツール実行前フックによる権限承認フロー (ユーザーへリクエスト → 承認/拒否)。「権限を要求する権限」の実装
2. **OS 層**: `sumi-agent`、`sumi-tool`、`sumi-artifact` を別UID・別 filesystem/PID/network sandbox にし、executor とそのbash子にだけ `/workspace`、artifact brokerにだけ専用artifact volumeをread/writeで見せる。runtimeからは両volume、bash子からはartifact volumeとbroker IPCを外す。Docker では両sidecarを `network_mode=none`、read-only rootfs、capability drop all、no-new-privileges で起動する。単一コンテナ内の `unshare(CLONE_NEWNET)` は Docker 既定 seccomp/capability では成立しないためリリース構成に使わない。microVM 内でも mount namespace + `pivot_root`/chroot 相当で同じ可視範囲を強制する。workspaceの`read_file`等はexecutorが workspace dirfd を起点に `openat2(RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS)` 相当で行い、artifact操作はbrokerが専用root dirfdを起点に通常symlinkも禁止して、canonicalize→open の TOCTOU を許さない。外向き通信は egress プロキシを設計するまで非対応 (実装計画 §8.3)
3. **テナント層**: Sumi Cloud は tenant ごとの microVM (Firecracker 系) で tenant/host 間を分離する。microVM は同一ゲスト内の runtime/executor/broker 分離の代替にはしない

## 配置形態

| 用途 | 構成 |
|---|---|
| Sumi Cloud | runtime/executor/artifact broker 分離を保った tenant ごとの microVM。ボリュームは EBS/EFS 等で永続化 |
| ローカル開発・CI | runtime container / executor sidecar / artifact broker sidecar + 永続テストvolume + 専用IPC。同じ隔離契約を検証するがCloud releaseの代替経路にはしない |
| OSS ローカル版 | ユーザー自身のマシンでコンテナ or 素のプロセスとして動作 |

Cloud release 前に、microVM で次の quota を強制し、Dockerハーネスでも同じ分類・復旧契約を再現できることを release gate とする。既定値は製品既定であり tenant plan ごとに下げられる:

| 資源 | 既定上限 | 適用境界 | Docker | microVM | 超過時の境界 |
|---|---:|---|---|---|---|
| `/workspace` disk / inode | 2 GiB / 200,000 | tenant executor の workspace volume。runtime DB・artifact volume・guest rootfsは別quota | project quota または上限付き volume | guest filesystem project quota | writeを拒否しexecutor sandboxは継続。整合性を確認できなければexecutorだけrecycle |
| PID | 64 | command child cgroup/PID namespace。runtime・artifact broker・supervisorは保護されたsibling | cgroup `pids.max` | guest command cgroup | forkを拒否。既存commandが停止不能ならcommand child、delegation不能ならexecutor sandboxをrecycle |
| CPU bandwidth / command CPU-time | 1 vCPU cap / 120 CPU秒 | command child cgroup | cgroup `cpu.max` + `cpu.stat` watcher | vCPU割当内のguest command cgroup watcher | bandwidthはthrottle、CPU-time超過はcommand child、delegation不能ならexecutor sandboxをrecycle |
| 同時実行command / command cgroup / tenant aggregate CPU | 4 / 4 / 2 vCPU | tenant executor配下のcommand subtree。runtime・artifact broker・supervisorは別の予約領域 | bounded admission semaphore + command親cgroupの`cpu.max` + child数上限 | guest supervisorの同じsemaphore + command親cgroup | 上限中は新規commandを最大30秒FIFO待機させ、空かなければspawn/cgroup作成前に`ResourceLimit(Concurrency)`で拒否。既存commandはaggregate quota内で継続 |
| memory | 512 MiB | **command child cgroup**。runtime・artifact broker・supervisorには及ばない | command childの`memory.max`/`memory.events` | guest command child cgroup | command childをkill。delegation不能ならexecutor sandboxをrecycle |
| 1 command wall runtime | 120秒 | command child cgroup/PID namespace | executor watchdog + per-execution cgroup kill / sandbox recycle | guest watchdog + per-execution cgroup kill / PID namespace recycle | command child、delegation不能ならexecutor sandboxをrecycle |
| 1 command output / conversation artifact合計 | 10 MiB / 100 MiB | capture stream / conversation artifact volume。workspaceとは別quota | capture counter + artifact volume quota | 同左 | command出力を停止 / artifact writeを拒否。broker全体やmicroVMは停止しない |

tenant microVM 自体には上表とは別の aggregate CPU/memory/PID/disk/inode envelopeを設定し、その容量は「runtime・artifact broker・supervisor・DB/IPC用の予約容量」と「executor/command上限」の合計を下回らせない。command用の親cgroupは既定2 vCPUを全childで共有し、各childの1 vCPU上限とは別にaggregate throttleする。admission semaphoreとcommand cgroup registryは同じ上限4を正典とし、slotを獲得してregistryへ予約できた場合だけspawnする。runtime・broker・supervisorはcommand cgroupの外に置き、CPU weight/帯域予約、`memory.min`/`memory.low`相当、PID予約で保護する。aggregate envelopeへ達する前に新規commandを受付停止し、必要ならcommand childまたはexecutor sandboxを回収する設計とし、保護領域まで枯渇して制御不能になった場合だけ旧generation全体をfenceしてmicroVMをrecycleする。同時実行4件、5件目の30秒timeout、2 vCPU aggregate throttle、slot/cgroup解放後のFIFO再開、および負荷中もruntime・artifact broker・supervisorのheartbeat/IPCが維持されることをDocker/microVM双方のrelease gateで確認する。

controller の意味を一律の「超過時 kill」にしない。`cpu.max` は1 vCPUへ throttle し、別の `cpu.stat` 差分が120 CPU秒へ達した場合だけ watchdog が停止を要求する。`pids.max` は fork を、disk/inode quota は write を拒否するため、executor は `pids.events` と `EAGAIN`/`EDQUOT`/`ENOSPC` を `ResourceLimit` へ分類する。memory はcommand childの `memory.events` の max/oom/oom_kill を観測し、wall runtime・CPU-time・output counter は watchdog が強制終了する。停止が必要な経路は process group ではなく supervisor 所有の command child cgroup/PID namespace 全体を `cgroup.kill` 相当で回収し、delegation 不能なら command 専用 executor sandbox を破棄・再作成する。`setsid`/`setpgid` で離脱した descendant も `populated=0` 確認まで残さない。どの経路も wait/reap 後に limit 種別とそれまでの bounded output を返す。process-group kill は開発用 low-trust local harness の best-effort fallbackに限り、release gateへ数えない。

deployment supervisor は runtime、executor、artifact broker、IPC に同じ世代番号を与え、command ごとの execution cgroup/sandbox も登録する。runtime 終了・heartbeat 喪失時にその世代の登録済み execution boundary と executor sandbox 全体を kill/reapし、旧generationのbroker credential/key leaseを失効させてから新世代を起動する。ドメイン操作に加え、workspaceの`write_file`/`edit_file`/`delete`とbrokerの`append_tool_output`へも`command_id/tool_call_id`から導出した`idempotency_key`を伝播する。executor/brokerはkeyとrequest hash、処理状態、結果receiptをdurable journalへ保存し、同じkey・同じrequestの再送には保存済み結果を返し、同じkey・異なるrequestは拒否する。`append_tool_output`はchunk seq/offsetもkeyに含め、再送で重複追記しない。quota 拒否/throttle、`setsid ... &` でdetachしたdescendantの強制終了、再起動後の回収を fault-injection で確認するまでCloud releaseしない。

workspace境界のfault-injectionでは、bashが`0600`ファイル/`0700`ディレクトリを作っても後続`read_file`/`edit_file`/削除がexecutor RPC経由で成功すること、同じpathをruntime UIDから直接openできずruntime mount tableにもworkspaceが無いことを確認する。runtime起点artifactは呼出し元umaskを`0077`/`0000`へ変えても最終modeがfile `0600`/dir `0700`になること、runtime/executor/bashのmount tableにartifact volumeがなく、bash子からbroker socket/FDへ到達できないことを確認する。broker volume内のconversation ID位置・kind位置・子孫へ通常symlinkを置くfixtureは全操作で拒否し、reset中にkillしてもbrokerが旧conversation subtreeだけをno-followで再削除してユーザーworkspaceと他conversationを残すこともrelease gateとする。

`indeterminate` で閉じた execution の再開・照合契約:

- **可視性**: `indeterminate` は `failed` とは別状態として `tool_executions.state`(実装計画 §10.1)に残す。再起動後のrecovery workerが照合結果を確定し、対応するtool結果を `MessageStart/End` → `TurnEnd` のdurable terminal eventで必ず閉じる。同じrunに適用待ちsoft steerが無ければ`AgentEnd`、あれば保存済み次`TurnStart`へ継続する(実装計画 §10.2)。tool 結果には完了・未完了・補償済み・結果不明の別と `tool_call_id`/`idempotency_key` を明記し、UI/API 側が「失敗」と区別して提示できるようにする
- **照合**: recovery workerはexecutor/brokerのdurable receiptを取得した上でread-after-restartを行う。workspace mutationは対象pathの存在、content hash/version、delete tombstoneを、`append_tool_output`はartifact handleのcommitted offset/hashを照合し、domain mutationは該当APIをread-onlyに照会する。receiptと実体が一致した場合だけ完了、前状態のまま一致した場合だけ未完了と確定し、どちらにも一致しない状態は自動再実行せず`indeterminate`のままdurable terminal eventで閉じる
- **リトライと補償**: 未完了と確定した**同一request**は、元の execution と同じ `idempotency_key` で冪等再試行する。再試行不能な部分適用は、write/editならjournalに保存したpreimage/versionへ条件付き復元、deleteなら同一filesystem内のquarantine renameから復元、appendならbrokerが記録した最後のcommitted offsetへtruncateする。これらの補償は元requestのkeyを再利用せず、`UUIDv5(COMPENSATION_NAMESPACE, parent_idempotency_key || compensation_kind || target_receipt_version)` で一意に導出した `compensation_idempotency_key` を使い、journalへ `parent_idempotency_key`、補償request hash、対象receipt/version、処理状態、結果receiptを保存する。同じ導出key・同じ補償requestの再送は保存済み結果を返し、同じ導出keyでhashが異なるrequestは拒否する。これにより元requestと補償requestを別journal entryとして追跡しつつ、補償自体もcrash後に一度だけ再開できる。preimage等がなく安全に補償できない操作は自動で推測せず、結果不明として閉じて手動解決へ送る。ユーザーが照合なしに「もう一度実行して」と明示的に確認した場合に限り、新しい `tool_call_id`/`idempotency_key` を発行した別実行として扱ってよい

## 責務境界と運用既定

- スケジューラ (リマインダー・アラームの起動主体) は中央control planeの責務であり、このagent runtimeへ内蔵しない
- agentの起動・停止・回収はcontrol planeとdeployment supervisorが担う。起動時は最新generationを発行し、停止前に新規command受付をfenceしてdurable cursorをflushする。heartbeat喪失時は旧generationの全execution boundaryをkill/reapしてから再起動する。pending approval、未注入steer、memory jobが残るagentはidle sleep対象にしない
- Cloudのbackup既定はRPO 24時間、保持30日とし、tenant planはこれを短縮できる。RTOは運用SLOで別管理するが、release前に暗号化backupからの復元、deletion tombstone先行再適用、旧generation fenceを通す
- quota値は上表を製品既定とし、tenant planは同等以下へ制限できる。未設定を無制限として扱わない
