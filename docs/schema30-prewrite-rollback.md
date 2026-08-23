# Schema 0030 pre-write rollback

This operator is only for reverting the sealed `0030_message_revisions`
migration to exact schema head 0029 before the schema-30 candidate has served
any writes. It is not a general migration rollback tool and is not reachable
from normal API startup.

## Hard boundary

The first write served by the schema-30 candidate is the fix-forward boundary.
After that write, do not use this procedure: restore service with a forward
fix. The database guard rejects any `messages.revision != 1`, but that cannot
detect every possible post-candidate insert, delete, or write in another table.
Passing preflight therefore does not prove whole-system rollback safety.

Before either command, externally verify and record that every API instance,
agent path, job, and operator capable of writing the database is stopped. Keep
writers quiesced through the rollback and old-version startup. This is a
mandatory precondition; the tool cannot verify process-level quiescence.

## Procedure

Provide `SUMI_DB_URL` through the operator's secret environment. Do not put the
URL in command arguments or logs.

The exact candidate API image contains the offline operator at
`/usr/local/bin/sumi-schema30-rollback`. Before running it, obtain `API_IID`
from the reviewed immutable-image manifest and require its canonical
`sha256:<64 lowercase hex>` form. Set `COMPOSE_PROJECT` to the exact existing
stack project and verify that `${COMPOSE_PROJECT}_default` is its retained
network and that its Postgres container is still running. Do not use a tag in
place of `API_IID`.

Run the non-mutating check while writers remain quiesced. `SUMI_DB_URL` is the
only environment value passed into this one-shot container:

```sh
test "$(docker image inspect --format '{{.Id}}' "${API_IID}")" = "${API_IID}"
docker network inspect "${COMPOSE_PROJECT}_default" >/dev/null
docker run --rm --pull=never --read-only --cap-drop=ALL \
  --security-opt=no-new-privileges \
  --network "${COMPOSE_PROJECT}_default" \
  --env SUMI_DB_URL \
  --entrypoint /usr/local/bin/sumi-schema30-rollback \
  "${API_IID}" preflight
```

Preflight takes the existing migration advisory lock and refuses unless:

- the exact migration head is 30;
- every recorded migration version and checksum is the canonical embedded
  history through the sealed 0030 up migration;
- the embedded 0030 up and down artifacts have their sealed checksums; and
- every existing message has revision 1.

If and only if external writer quiescence has been verified and preflight
passes, apply the rollback:

```sh
docker run --rm --pull=never --read-only --cap-drop=ALL \
  --security-opt=no-new-privileges \
  --network "${COMPOSE_PROJECT}_default" \
  --env SUMI_DB_URL \
  --entrypoint /usr/local/bin/sumi-schema30-rollback \
  "${API_IID}" apply
```

Apply reacquires the same advisory lock, exclusively locks `messages`, repeats
all preflight checks, and in one transaction runs the sealed 0030 down SQL,
deletes exactly the version-30 bookkeeping row, and verifies exact head 29.
Any error leaves the transaction uncommitted. The command intentionally does
not print the database URL or underlying connection errors.

Before admitting traffic, start the exact schema-29 application/migrator build
and confirm that its migration pass accepts head 29 without changing it. For
the rollback target in this runbook, that compatibility check is the migrator
from commit `5347d32fc29ff9826d0d40d5d344782940e40854`. Keep all writers stopped
until that check and the application replacement are complete.
