# Coordinated messaging backup and scratch restore

この手順が守る単位は、Postgres上のcontrol-plane/messaging metadataと、
`SUMI_MESSAGING_ATTACHMENT_ROOT`上のattachment blob treeである。DBだけ、blobだけ
のbackupを成功扱いしない。command log、agent runtime state、PersonalityAgentの
private DB/workspace/volumeはこのsnapshotに含まれない。

## Consistency protocol

`create.sh`は次の順序を固定する。

1. APIをquiesceし、in-flight writeが終了して新規writeが入らない状態にする。
2. applied migrationをembedded name/SHA-256 manifestへ照合する。
3. 全attachment rowをexportし、canonical blob path、size、欠損、orphan、symlinkを
   検査しながら各blobのSHA-256 manifestを作る。
4. APIが停止した同じwindow内で`pg_dump --format=custom`とblob archiveを作る。
5. snapshot ID、app SHA、API/Postgres image digest、migration digest、全artifactの
   size/SHA-256を`snapshot.json`へ固定する。
6. APIをdependency-readyまで再開してから、bundleを暗号化してoff-hostへhandoff
   する。暗号化bundleとsnapshot manifestは別のhandoff manifestで再結合する。

`quiesce-api.sh`は実装済みの安全な既定helperである。exactly oneのrunning APIを
確認してstopし、`<backup-root>/.maintenance/<snapshot-id>`を残す。通常終了では
`create.sh`のEXIT trapが`resume-api.sh`を呼ぶ。process killやhost crashでtrapが
走らなくてもmarkerが残るため、monitor/operatorはmarkerを検知し、API状態を確認
して同じsnapshot IDで`resume-api.sh`を実行する。markerを手で消して成功扱いに
しない。

## Required inputs

backup jobは少なくとも以下を環境へ渡す。値をshell `source`で評価せず、locked
systemd `EnvironmentFile=`等のliteral loaderを使う。

```text
SUMI_DB_URL=<dogfood DB URL>
SUMI_APP_SHA=<exact source SHA>
SUMI_API_IMAGE=<registry/repository@sha256:digest>
SUMI_POSTGRES_IMAGE=<repository:tag@sha256:digest>
SUMI_BACKUP_WORK_ROOT=/srv/sumi/backup-work
SUMI_ATTACHMENT_ROOT=/srv/sumi/dogfood/attachments
SUMI_DOGFOOD_OPERATOR_ENV_FILE=/etc/sumi/operator.env
SUMI_DOGFOOD_DOCKER_CONTEXT=default
SUMI_DOGFOOD_OPERATION_LOCK=/srv/sumi/dogfood/.operations.lock
SUMI_DOCKER_CONFIG_FILE=/etc/sumi/docker/config.json

SUMI_MIGRATE_BIN=<absolute path to compose-migrate.sh>
SUMI_PSQL_BIN=/usr/bin/psql
SUMI_PG_DUMP_BIN=/usr/bin/pg_dump
SUMI_TAR_BIN=/usr/bin/tar
SUMI_QUIESCE_HELPER=<absolute path to quiesce-api.sh>
SUMI_RESUME_HELPER=<absolute path to resume-api.sh>
SUMI_ENCRYPT_HELPER=<absolute authenticated-encryption helper>
SUMI_HANDOFF_HELPER=<absolute durable off-host handoff helper>
```

全binary/helperはabsolute executable regular non-symlinkでなければならない。
backup work rootとattachment rootもabsolute real non-root directoryにする。
`create.sh --check`は入力だけを検査し、quiesce、snapshot、encryption、handoff、
restoreを一切行わない。

```bash
./deploy/dogfood/backup/create.sh --check
./deploy/dogfood/backup/create.sh
```

Postgres clientはdogfood Postgres major versionと互換なversionをpinする。
`SUMI_API_IMAGE`と`SUMI_POSTGRES_IMAGE`は実snapshotに記録されるため、registryで
保持する。新しいmigrationを追加した後も、古いsnapshotはまず記録済みimageと
migration manifestで復元し、その復元点から通常のforward migrationで現在へ
進める。

## External helper contracts

`SUMI_ENCRYPT_HELPER CLEAR_BUNDLE ENCRYPTED_OUTPUT`は認証付き暗号を使い、成功時に
のみcomplete outputを残す。keyはrepository、bundle、handoff manifestと別の
failure domainで保管する。単なる`openssl enc`相当の非認証暗号を使わない。

`SUMI_HANDOFF_HELPER ENCRYPTED_BUNDLE HANDOFF_JSON`は二つをoff-hostへ送り、remote
durabilityとremote hash/size照合が終わってから0を返す。upload開始を成功扱いに
しない。schedule、RPO、retention、容量/inode監視、失敗alert、on-call ownerは
外部運用入力であり、実値が決まるまで自動backup完了とは言わない。

local work rootにはclear snapshotが残る。host disk encryption、mode 0700、local
retentionと容量監視を必須にし、off-host encrypted copyの代替にはしない。
handoff成功後、重複するclear bundleだけは削除し、元のdump/archive/manifestと
encrypted local copyをretention対象として残す。

## Scratch restore rehearsal

production DB/rootへ直接restoreしない。空のscratch Postgres databaseと空の
attachment directoryを作り、snapshotに記録されたAPI/Postgres imageを使う。
decrypt helperは認証を検証し、失敗時にpartial plaintextを成功出力として残さない。

```text
SUMI_RESTORE_DB_URL=<empty scratch DB URL>
SUMI_RESTORE_CONFIRM_SCRATCH=<snapshot-id>
SUMI_RESTORE_WORK_ROOT=<absolute protected work root>
SUMI_DECRYPT_HELPER=<absolute authenticated-decryption helper>
SUMI_PSQL_BIN=/usr/bin/psql
SUMI_PG_RESTORE_BIN=/usr/bin/pg_restore
SUMI_MIGRATE_BIN=<snapshot API imageを使うcompose-migrate.sh>
SUMI_TAR_BIN=/usr/bin/tar
```

```bash
./deploy/dogfood/backup/restore-scratch.sh \
  /absolute/off-host-copy/<snapshot-id>.bundle.encrypted \
  /absolute/off-host-copy/<snapshot-id>.handoff.json \
  /absolute/empty-attachment-root
```

restoreは次をすべて満たすまで成功しない。

- encrypted bytesとhandoff manifestのhash/sizeが一致する。
- decrypt後の`snapshot.json`がhandoff時にhash固定したmanifestそのものである。
- archive pathとartifact setがexactで、全artifact hashが一致する。
- restore binaryのembedded migration manifestがsnapshotとexact一致する。
- target DBとattachment rootが空である。
- `pg_restore --single-transaction`後のapplied migrationが正しい。
- 復元DBから再exportしたattachment rowと復元blobのpath/size/SHA-256がsnapshotと
  exact一致する。

失敗したscratch DB/rootは部分復元の調査証拠であり、自動cleanupや再利用をしない。
cutover前にreal backupからこのrehearsalを通し、以後の周期とownerを記録する。
