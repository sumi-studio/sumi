# Coordinated dogfood durability snapshot and scratch restore

この手順が守る単位は、dogfood host上で再起動後のSumiを同じ状態から続けるために
必要な、次の**一つの整合snapshot**である。

- Postgres上のcontrol plane、Messaging、Direct Chat metadata。
- APIのdurable command log、runtime state、Messaging attachment blob。
- Postgresに存在する全PersonalityAgentについて、Composeが所有するprivate volume
  10個（identity、state、workspace、artifactsを含む）のexact set。

DBだけ、host bindだけ、またはDocker volumeの一部だけをbackupして成功扱いしない。
一方、`/run/sumi`配下のsocket、boot nonce、temporary secret、supervisor lock、provider
credential、Firebase/Cloudflare secretは再生成または外部custodyの対象であり、この
snapshotへ含めない。LiveKit等の外部service上のmediaも含めない。

## Consistency protocol

`create.sh`は次の順序を固定する。

1. named Tunnelを止めて外部からの新規write admissionを閉じる。
2. runtime provisionerをdrainして停止し、新しいagent generationを作れなくする。
3. DBのauthoritative PersonalityAgent setと、実行中のprivate writer containerを照合し、
   exact container IDをmaintenance markerへ記録する。未知のagent project、allocation
   途中、unexpected serviceが一つでもあれば失敗する。
4. APIを生かしたまま、記録済みのagent runtime、broker、executorをexact container
   IDで停止し、最後のdurable eventを受け切る。その後APIを停止し、DB/host-state
   writeをdrainしてglobal snapshot pointを作る。
5. applied migrationをembedded name/SHA-256 manifestへ照合する。同じquiesced windowで
   DB dump、host-state manifest/archive、全agent volume manifest/archiveを作る。
6. snapshot ID、app SHA、API/provisioner/Postgres image digest、migration digest、
   全artifactのsize/SHA-256を`snapshot.json`へ固定する。
7. provisioner、API、記録済みagent writer、Tunnelの順にexact pre-snapshot writer setを
   再開する。その後にbundleを認証付き暗号で暗号化し、off-hostへhandoffする。

`quiesce-api.sh`は`<backup-root>/.maintenance/<snapshot-id>`へphaseとcontainer setを
残す。通常終了では`create.sh`のEXIT trapが`resume-api.sh`を呼ぶ。process killやhost
crashでtrapが走らなくてもmarkerは復旧判断の正本として残る。markerを手で削除して
成功扱いにせず、対象containerのidentityを確認して同じsnapshot IDでresumeする。

Postgresのmaintenance操作はhost binaryやpublished portを使わない。
`compose-database.sh`がdigest-pinned `database-client`をComposeのinternal `data`
network上で起動し、固定されたoperationだけを実行する。localhost、
`host.docker.internal`、host-only URLは拒否される。

## Backup inputs

値をshell `source`で評価せず、locked systemd `EnvironmentFile=`等のliteral loaderを
使う。少なくとも次を渡す。

```text
SUMI_DB_URL=postgres://sumi:<encoded-password>@postgres:5432/sumi?sslmode=disable
SUMI_APP_SHA=<exact source SHA>
SUMI_API_IMAGE=<registry/repository@sha256:digest>
SUMI_PROVISIONER_IMAGE=<registry/repository@sha256:digest>
SUMI_POSTGRES_IMAGE=<repository:tag@sha256:digest>

SUMI_BACKUP_WORK_ROOT=/srv/sumi/backup-work
SUMI_DOGFOOD_STATE_ROOT=/srv/sumi/dogfood
SUMI_ATTACHMENT_ROOT=/srv/sumi/dogfood/attachments
SUMI_DOGFOOD_OPERATOR_ENV_FILE=/etc/sumi/operator.env
SUMI_DOGFOOD_DOCKER_CONTEXT=default
SUMI_DOGFOOD_OPERATION_LOCK=/srv/sumi/dogfood/.operations.lock
SUMI_DOCKER_CONFIG_FILE=/etc/sumi/docker/config.json

SUMI_MIGRATE_BIN=<absolute path to compose-migrate.sh>
SUMI_DATABASE_HELPER=<absolute path to compose-database.sh>
SUMI_AGENT_VOLUME_HELPER=<absolute path to snapshot-agent-volumes.sh>
SUMI_TAR_BIN=/usr/bin/tar
SUMI_QUIESCE_HELPER=<absolute path to quiesce-api.sh>
SUMI_RESUME_HELPER=<absolute path to resume-api.sh>
SUMI_ENCRYPT_HELPER=<absolute authenticated-encryption helper>
SUMI_HANDOFF_HELPER=<absolute durable off-host handoff helper>
```

全binary/helperはabsolute executable regular non-symlinkでなければならない。work
root、dogfood state root、attachment rootもabsolute real non-root directoryにする。
attachment rootは必ず`SUMI_DOGFOOD_STATE_ROOT/attachments`の実体と一致させる。
Docker contextは対象hostのlocal Unix socketでなければならない。

```bash
./deploy/dogfood/backup/create.sh --check
./deploy/dogfood/backup/create.sh
```

`--check`は入力だけを検査し、quiesce、snapshot、encryption、handoff、restoreを一切
行わない。Postgres clientとvolume archive toolは記録済みのdigest-pinned imageから
実行する。古いsnapshotはまず記録済みimageとmigration manifestで復元し、その
復元点からforward migrationだけで現在へ進める。

## External helper contracts

`SUMI_ENCRYPT_HELPER CLEAR_BUNDLE ENCRYPTED_OUTPUT`は認証付き暗号を使い、成功時だけ
complete outputを残す。keyはrepository、bundle、handoff manifestとは別のfailure
domainで保管する。非認証暗号を使わない。

`SUMI_HANDOFF_HELPER ENCRYPTED_BUNDLE HANDOFF_JSON`は二つをoff-hostへ送り、remote
durabilityとremote hash/size照合が終わってから0を返す。upload開始を成功扱いに
しない。schedule、RPO、retention、容量/inode監視、失敗alert、on-call ownerは外部
運用入力であり、実値と実行証拠が揃うまで自動backup完了とは言わない。

local work rootにはclear dump/archive/manifestとencrypted copyが残る。host disk
encryption、mode 0700、local retentionと容量監視を必須にし、off-host copyの代替に
しない。handoff後は再生成可能なclear bundleだけを削除する。

## Scratch restore rehearsal

production DB、state root、元のagent volumeへ直接restoreしない。次を用意する。

- Compose `data` networkからDNS名で到達でき、schema objectが0件のscratch Postgres。
- 中身が空のabsolute state root。
- 対象hostのlocal Docker context。復元volumeはsnapshot IDを含む別名で新規作成する。
- snapshotを作ったAPI/provisioner/Postgres image digestをregistryに保持しておく。

```text
SUMI_RESTORE_DB_URL=postgres://sumi:<encoded-password>@scratch-postgres:5432/sumi?sslmode=disable
SUMI_RESTORE_CONFIRM_SCRATCH=<snapshot-id>
SUMI_RESTORE_WORK_ROOT=<absolute protected work root>
SUMI_DOGFOOD_OPERATOR_ENV_FILE=/etc/sumi/operator.env
SUMI_DOGFOOD_DOCKER_CONTEXT=default
SUMI_DOCKER_CONFIG_FILE=/etc/sumi/docker/config.json
SUMI_DECRYPT_HELPER=<absolute authenticated-decryption helper>
SUMI_DATABASE_HELPER=<absolute path to compose-database.sh>
SUMI_AGENT_RESTORE_HELPER=<absolute path to restore-agent-volumes.sh>
SUMI_MIGRATE_BIN=<absolute path to compose-migrate.sh>
SUMI_TAR_BIN=/usr/bin/tar
```

numeric ownerとmodeを再現するため、実rehearsalはroot operatorで実行する。

```bash
sudo ./deploy/dogfood/backup/restore-scratch.sh \
  /absolute/off-host-copy/<snapshot-id>.bundle.encrypted \
  /absolute/off-host-copy/<snapshot-id>.handoff.json \
  /absolute/empty-state-root
```

restoreは次をすべて満たすまで成功しない。

- encrypted bytes、handoff manifest、snapshot manifest、artifact setのhash/sizeが
  exact一致する。
- restore binaryのembedded migration manifestがsnapshotとexact一致する。
- target DBとstate rootが空である。
- `pg_restore --single-transaction`後のmigration manifestが一致する。
- command log、runtime state、attachment treeのpath、type、mode、uid/gid、size、
  file hashがsnapshotとexact一致する。
- DBから再exportしたattachment rowと復元blobがexact一致する。
- DB上の全PersonalityAgentに対するcanonical 10-volume setがexactで、各scratch
  volumeのmount rootを含む復元後content/owner/mode manifestが元volumeとexact一致する。

失敗したscratch DB、state root、scratch agent volume、restore work directoryは
部分復元の調査証拠であり、自動cleanupや再利用をしない。

このrepositoryのtestsは契約とlocal helperを検証するが、real hostのsnapshot、暗号化
handoff、off-host durability、real scratch restoreは実行していない。cutover前に実物の
rehearsalを通し、timestamp、snapshot ID、handoff hash、restored volume map、ownerを
cutover recordへ残して初めてこのgateを完了とする。
