#!/usr/bin/env bash
# Ownership/lifecycle-safety test for `sumi-local`.
# Real fixtures: real Go service, real node core, real docker postgres per
# install. No mocks beyond the mock model provider.
#
# Covers:
#   - two managed installs run simultaneously: distinct container, volume,
#     compose project, and docker-assigned PG port
#   - install B's `uninstall --purge` cannot delete install A's volume/data
#   - same-install identity/history survives uninstall + reinstall (managed)
#   - orphan-volume refusal: config deleted, volume left -> install refuses
#   - pidfile identity: foreign/stale/cross-install pids never signaled;
#     pidfiles in unproven homes are never deleted
#   - lost pidfiles: orphans are still stopped by the prefix-anchored /proc
#     sweep (stop, restart, purge) — no duplicate cores, no DB-out-from-under
#   - unowned prefix/home refused; non-directory --home refused; generic
#     run//log//workspace/ dirs are never ownership evidence
#   - populated prefix: only the known payload is removed, foreign entries
#     stay
#   - mismatched SUMI_LOCAL_HOME/SUMI_LOCAL_PREFIX pair refused before any
#     mutation; marker/config id disagreement refused
#   - interactive purge confirmation happens BEFORE any teardown; cancelling
#     leaves processes, containers, and files untouched
#   - config-less + marker-less stop is a deliberate refusal, not an
#     accidental exit
#   - moved home: recorded (not path-derived) id stays authoritative;
#     reinstall resyncs the marker so it can never steer a later purge at
#     another install
#   - uninstall is idempotent: absent or foreign CLI shim still exits 0
#   - the real user's ~/.local/bin/sumi-local is never touched (isolated
#     OS HOME per fixture; sentinel checked before and after)
#   - database readiness needs a PostgreSQL reply: a docker-proxy port with
#     nothing behind it is refused before any service is spawned
#   - a state service that dies at startup is reported at once, with its
#     own (credential-redacted) log lines
#   - an unreadable config.env or home marker is named in a refusal that
#     changes nothing (not a silent non-zero exit)
#   - external-DB preflight opens what pgx will: an sslnegotiation=direct
#     URL through a TLS-only terminator starts; direct under sslmode=disable
#     stays plaintext
#
# Usage:
#   deploy/local-host/self-test-ownership.sh [workdir]
# Env:
#   SUMI_LOCAL_OWNERSHIP_PORT_BASE    listen ports base..base+9 (default: PB below)
#   SUMI_LOCAL_OWNERSHIP_DOCKER_PREFIX name prefix for containers this test
#                                      creates directly (default sumi-local-selftest)
# Requires: docker + compose v2, go, node. Skips cleanly without docker.
set -euo pipefail
cd "$(dirname "$0")/../.."
ROOT="$(pwd)"
OUT="$(cd "${1:-.}" && pwd)"
SRC="$ROOT/deploy/local-host/sumi-local"

command -v docker >/dev/null && docker compose version >/dev/null 2>&1 || {
  echo "SKIP: docker compose not available — managed-PG ownership test needs docker"
  exit 0
}

REAL_HOME=$HOME
FIX="$OUT/fix-ownership-$$"          # unique per run — concurrent runs never share
rm -rf "$FIX"; mkdir -p "$FIX"
OSH="$FIX/os-home"                   # isolated OS HOME: shims land here
OSH2="$FIX/os-home-2"                # second isolated HOME for foreign-shim probe
mkdir -p "$OSH" "$OSH2"
[[ -d $REAL_HOME/.cache/go-build ]] && export GOCACHE="$REAL_HOME/.cache/go-build"
[[ -d $REAL_HOME/go/pkg/mod ]] && export GOMODCACHE="$REAL_HOME/go/pkg/mod"
REAL_SHIM_BEFORE="$(readlink "$REAL_HOME/.local/bin/sumi-local" 2>/dev/null || echo __absent__)"

PB=${SUMI_LOCAL_OWNERSHIP_PORT_BASE:-9550}
for i in 0 1 2 3 4 5 6 7 8 9; do printf -v "P$i" '%s' "$((PB + i))"; done
NL_CTR="${SUMI_LOCAL_OWNERSHIP_DOCKER_PREFIX:-sumi-local-selftest}-nolisten-$$"

A_HOME="$FIX/home-a"; A_PREFIX="$FIX/prefix-a"
B_HOME="$FIX/home-b"; B_PREFIX="$FIX/prefix-b"

a() { env HOME="$OSH" SUMI_LOCAL_HOME="$A_HOME" SUMI_LOCAL_PREFIX="$A_PREFIX" "$SRC" "$@"; }
b() { env HOME="$OSH" SUMI_LOCAL_HOME="$B_HOME" SUMI_LOCAL_PREFIX="$B_PREFIX" "$SRC" "$@"; }

pass=0; fail=0
ok()  { echo "  ok: $*"; pass=$((pass+1)); }
bad() { echo "  FAIL: $*"; fail=$((fail+1)); }
expect_die() { # expect_die <desc> <want-substr> -- cmd...
  local desc="$1" want="$2"; shift 2
  local out rc=0
  out="$("$@" 2>&1)" || rc=$?
  if ((rc != 0)) && [[ $out == *"$want"* ]]; then
    ok "$desc (refused: $(echo "$out" | tail -1 | cut -c1-120))"
  else
    bad "$desc — rc=$rc out: $(echo "$out" | tail -3)"
  fi
}
expect_rc0() { # expect_rc0 <desc> -- cmd...
  local desc="$1"; shift
  local out rc=0
  out="$("$@" 2>&1)" || rc=$?
  ((rc == 0)) && ok "$desc" || bad "$desc — rc=$rc out: $(echo "$out" | tail -3)"
}

persona_of() { sed -n "s/^SUMI_PERSONA_ID='\\(.*\\)'$/\\1/p" "$1/config.env" | head -1; }
id_of()      { sed -n "s/^SUMI_LOCAL_ID='\\(.*\\)'$/\\1/p" "$1/config.env" | head -1; }
a_inputs() { docker exec "$A_CTR" psql -U sumi -d sumi -tAc 'select count(*) from core_inputs' 2>/dev/null | tr -d '[:space:]'; }

F2_HOME="$FIX/home-f2"; F2_PREFIX="$FIX/prefix-f2"
K_HOME="$FIX/home-k"; K_PREFIX="$FIX/prefix-k"
f2() { env HOME="$OSH" SUMI_LOCAL_HOME="$F2_HOME" SUMI_LOCAL_PREFIX="$F2_PREFIX" "$SRC" "$@"; }
k() { env HOME="$OSH" SUMI_LOCAL_HOME="$K_HOME" SUMI_LOCAL_PREFIX="$K_PREFIX" "$SRC" "$@"; }
for x in m n p q r s t v w x y z; do
  eval "${x}() { env HOME=\"\$OSH\" SUMI_LOCAL_HOME=\"$FIX/home-$x\" SUMI_LOCAL_PREFIX=\"$FIX/prefix-$x\" \"\$SRC\" \"\$@\"; }"
done

cleanup() {
  local x
  for x in a b c d e f f2 h i j k l m n o p q r s t td v w x y z; do
    "$x" stop >/dev/null 2>&1 || true
    "$x" uninstall --purge --yes >/dev/null 2>&1 || true
  done
  docker rm -f "$NL_CTR" >/dev/null 2>&1 || true
  [[ -n ${TLSD_PID:-} ]] && kill "$TLSD_PID" 2>/dev/null || true
  # r may have been moved to prefix-r2 mid-test
  env HOME="$OSH" SUMI_LOCAL_HOME="$FIX/home-r" SUMI_LOCAL_PREFIX="$FIX/prefix-r2" \
    "$SRC" uninstall --purge --yes >/dev/null 2>&1 || true
  rm -rf "$FIX" 2>/dev/null || true
}
trap cleanup EXIT

echo "== install A + B (managed, simultaneous, isolated OS HOME)"
a install --managed-pg --listen 127.0.0.1:$P0 >/dev/null
b install --managed-pg --listen 127.0.0.1:$P1 >/dev/null
A_ID="$(id_of "$A_HOME")"
B_ID="$(id_of "$B_HOME")"
[[ $A_ID != "$B_ID" ]] \
  && ok "distinct install ids (A=$A_ID B=$B_ID)" \
  || bad "install ids collide: $A_ID"
[[ -L $OSH/.local/bin/sumi-local ]] \
  && ok "shim created inside isolated HOME" \
  || bad "shim missing in isolated HOME"
[[ ! -e $REAL_HOME/.local/bin/sumi-local && $REAL_SHIM_BEFORE == __absent__ ]] \
  || [[ $(readlink "$REAL_HOME/.local/bin/sumi-local" 2>/dev/null || echo __absent__) == "$REAL_SHIM_BEFORE" ]] \
  && ok "real user shim untouched by installs" \
  || bad "real user shim mutated!"

echo "== a failed install never repoints the CLI shim"
# A distribution-acceptance run reproduced this: a fresh install that dies
# resolving the database (no docker, no --db-url) had already repointed
# ~/.local/bin/sumi-local to its half-written payload — over the working
# install's entry. The shim is the publication step; it must move only once
# the install is known-good.
SHIM_BEFORE="$(readlink "$OSH/.local/bin/sumi-local" 2>/dev/null || echo __absent__)"
NODOCKER_BIN="$FIX/nodocker-bin"; mkdir -p "$NODOCKER_BIN"
for t in /usr/bin/* /bin/*; do
  n="${t##*/}"; [[ $n == docker ]] || ln -sfn "$t" "$NODOCKER_BIN/$n"
done
ln -sfn "$(command -v go)" "$NODOCKER_BIN/go"
ln -sfn "$(command -v node)" "$NODOCKER_BIN/node"
expect_die "install with no db-url and no docker fails" "no database configured" \
  env HOME="$OSH" PATH="$NODOCKER_BIN" SUMI_LOCAL_HOME="$FIX/home-nd" \
      SUMI_LOCAL_PREFIX="$FIX/prefix-nd" "$SRC" install
[[ $(readlink "$OSH/.local/bin/sumi-local" 2>/dev/null || echo __absent__) == "$SHIM_BEFORE" ]] \
  && ok "failed install left the CLI shim untouched ($SHIM_BEFORE)" \
  || bad "failed install repointed shim -> $(readlink "$OSH/.local/bin/sumi-local" 2>/dev/null)"

echo "== start both; distinct resources"
a start >/dev/null; b start >/dev/null
A_CTR="sumi-local-pg-$A_ID"; B_CTR="sumi-local-pg-$B_ID"
A_VOL="sumi-local-pgdata-$A_ID"; B_VOL="sumi-local-pgdata-$B_ID"
docker inspect "$A_CTR" >/dev/null 2>&1 && docker inspect "$B_CTR" >/dev/null 2>&1 \
  && ok "both managed containers up ($A_CTR, $B_CTR)" \
  || bad "managed containers missing"
A_PGT="$(docker inspect "$A_CTR" --format '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}')"
B_PGT="$(docker inspect "$B_CTR" --format '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}')"
[[ $A_PGT != "$B_PGT" ]] \
  && ok "distinct docker-assigned PG ports (A=$A_PGT B=$B_PGT)" \
  || bad "PG ports collide: $A_PGT"

a say "alpha ping" >/dev/null && ok "A serves turns while B is running" \
  || bad "A failed while B running"
b say "beta ping" >/dev/null && ok "B serves turns while A is running" \
  || bad "B failed while A running"
A_PERSONA="$(persona_of "$A_HOME")"

echo "== pidfile pointing at ANOTHER INSTALL's process is never signaled"
cp "$B_HOME/run/service.pid" "$FIX/b-service.pid.saved"
cp "$A_HOME/run/service.pid" "$B_HOME/run/service.pid"
out="$(b stop 2>&1)"
[[ $out == *"leaving it"* ]] \
  && ok "cross-install pidfile detected, not signaled" \
  || bad "cross-install warning missing: $(echo "$out" | tail -3)"
a say "alpha survives" >/dev/null 2>&1 \
  && ok "A's service still healthy after B's stop" \
  || bad "A's service was signaled by B's stop!"
cp "$FIX/b-service.pid.saved" "$B_HOME/run/service.pid"
b start >/dev/null
b say "beta again" >/dev/null && ok "B ordinary stop/start still works" \
  || bad "B broken after pid-restore"

echo "== lost pidfiles: stop still stops the orphans (anchored /proc sweep)"
rm -f "$B_HOME/run/service.pid" "$B_HOME/run/local.pid"
b stop >/dev/null
if ! pgrep -f "$B_PREFIX/" >/dev/null 2>&1; then
  ok "lost pidfiles: orphaned service+core still stopped"
else
  bad "orphans survived stop: $(pgrep -af "$B_PREFIX/" | head -3)"
fi
# restart from a lost-pidfile running state must not duplicate the core
b start >/dev/null
rm -f "$B_HOME/run/service.pid" "$B_HOME/run/local.pid"
b restart >/dev/null
n_svc="$(pgrep -f "$B_PREFIX/bin/sumi-local-service" | wc -l)"
n_core="$(pgrep -f "$B_PREFIX/core/host/local.ts" | wc -l)"
[[ $n_svc == 1 && $n_core == 1 ]] \
  && ok "restart after pidfile loss: exactly 1 service + 1 core" \
  || bad "duplicate/orphan processes after restart (svc=$n_svc core=$n_core)"

echo "== foreign/stale pidfiles are never signaled or deleted"
sleep 600 & SLEEP_PID=$!
SLEEP_ST="$(awk '{print $22}' "/proc/$SLEEP_PID/stat")"
printf '%s %s\n' "$SLEEP_PID" "$SLEEP_ST" > "$B_HOME/run/foreign.pid"
out="$(b stop 2>&1)"
[[ $out == *"leaving it"* && -f $B_HOME/run/foreign.pid ]] \
  && ok "foreign pidfile in OWNED home: warned, not signaled, record kept as evidence" \
  || bad "owned-home foreign pidfile mishandled: $(echo "$out" | tail -3)"
kill -0 "$SLEEP_PID" 2>/dev/null && ok "foreign process still alive" \
  || bad "foreign process was signaled!"
# a pidfile in an UNPROVEN home is never even deleted
UNPROV="$FIX/unproven-home"; mkdir -p "$UNPROV/run"
printf '%s %s\n' "$SLEEP_PID" "$SLEEP_ST" > "$UNPROV/run/foreign.pid"
out="$(env HOME="$OSH" SUMI_LOCAL_HOME="$UNPROV" SUMI_LOCAL_PREFIX="$FIX/none" "$SRC" stop 2>&1 || true)"
[[ -f $UNPROV/run/foreign.pid ]] \
  && ok "unproven home: foreign pidfile left in place" \
  || bad "unproven home: pidfile was deleted!"
kill -0 "$SLEEP_PID" 2>/dev/null && ok "unproven home: foreign process untouched" \
  || bad "unproven home: process signaled!"
b start >/dev/null   # B running again for the pair-mismatch and cancel probes

echo "== mismatched home/prefix pair refused before any mutation"
expect_die "uninstall refuses mismatched pair" "does not match the prefix recorded" \
  env HOME="$OSH" SUMI_LOCAL_HOME="$A_HOME" SUMI_LOCAL_PREFIX="$B_PREFIX" "$SRC" uninstall --yes
expect_die "stop refuses mismatched pair" "does not match the prefix recorded" \
  env HOME="$OSH" SUMI_LOCAL_HOME="$A_HOME" SUMI_LOCAL_PREFIX="$B_PREFIX" "$SRC" stop
docker inspect "$B_CTR" >/dev/null 2>&1 && [[ -d $B_PREFIX && -d $A_HOME && -d $B_HOME ]] \
  && ok "both installs intact after refused pair" \
  || bad "mismatched pair partially mutated an install!"

echo "== interactive purge cancel mutates nothing"
out="$(printf 'no\n' | b uninstall --purge 2>&1 || true)"
[[ $out == *"aborted"* ]] \
  && ok "purge cancellation reported" \
  || bad "cancel output wrong: $(echo "$out" | tail -3)"
docker inspect "$B_CTR" --format '{{.State.Running}}' 2>/dev/null | grep -q true \
  && [[ -d $B_PREFIX && -f $B_HOME/config.env ]] \
  && pgrep -f "$B_PREFIX/bin/sumi-local-service" >/dev/null \
  && ok "cancelled purge: service, container, home, prefix all untouched" \
  || bad "cancelled purge mutated something!"

kill "$SLEEP_PID" 2>/dev/null || true

echo "== unowned prefix / home / non-directory refused"
INN_PRE="$FIX/innocent-prefix"; mkdir -p "$INN_PRE"; echo keep > "$INN_PRE/userfile"
# B's config records prefix-b; pointing the pair at a foreign prefix is a
# mismatched pair — refused before the unmarked-prefix check even runs
expect_die "uninstall against unowned prefix" "does not match the prefix recorded" \
  env HOME="$OSH" SUMI_LOCAL_HOME="$B_HOME" SUMI_LOCAL_PREFIX="$INN_PRE" "$SRC" uninstall --yes
[[ -f $INN_PRE/userfile ]] && ok "unowned prefix untouched" || bad "unowned prefix modified!"

INN_HOME="$FIX/innocent-home"; mkdir -p "$INN_HOME/log"; echo sentinel > "$INN_HOME/log/keep.txt"
INN_PRE2="$FIX/innocent-prefix2"; mkdir -p "$INN_PRE2"
rc=0; out="$(env HOME="$OSH" SUMI_LOCAL_HOME="$INN_HOME" SUMI_LOCAL_PREFIX="$INN_PRE2" "$SRC" uninstall --yes 2>&1)" || rc=$?
[[ $rc == 0 && $out == *"no sumi-local install evidence"* ]] \
  && ok "non-purge uninstall on foreign pair is a clean no-op" \
  || bad "foreign-pair uninstall wrong (rc=$rc): $(echo "$out" | tail -3)"
[[ -f $INN_HOME/log/keep.txt && $(cat "$INN_HOME/log/keep.txt") == sentinel && ! -e $INN_HOME/run ]] \
  && ok "non-purge uninstall left log-only home untouched (no run/ planted)" \
  || bad "non-purge uninstall mutated the wrong home!"
expect_die "purge still refuses the same wrong home" "refusing to purge" \
  env HOME="$OSH" SUMI_LOCAL_HOME="$INN_HOME" SUMI_LOCAL_PREFIX="$INN_PRE2" "$SRC" uninstall --purge --yes
[[ -f $INN_HOME/log/keep.txt && $(cat "$INN_HOME/log/keep.txt") == sentinel ]] \
  && ok "sentinel content survived purge attempt" || bad "wrong home was purged!"
for d in run log workspace; do
  W="$FIX/wrong-$d"; mkdir -p "$W/$d"; echo s > "$W/$d/x"
  expect_die "purge refuses $d-only home" "refusing to purge" \
    env HOME="$OSH" SUMI_LOCAL_HOME="$W" SUMI_LOCAL_PREFIX="$INN_PRE2" "$SRC" uninstall --purge --yes
  [[ -f $W/$d/x ]] && ok "$d-only home intact" || bad "$d-only home purged!"
done
touch "$FIX/not-a-dir"
expect_die "non-directory --home refused" "not a directory" \
  env HOME="$OSH" SUMI_LOCAL_HOME="$FIX/not-a-dir" SUMI_LOCAL_PREFIX="$INN_PRE2" "$SRC" uninstall --purge --yes

echo "== configless+markerless stop is a deliberate refusal"
EMPTY_HOME="$FIX/empty-home"; mkdir -p "$EMPTY_HOME"
out="$(env HOME="$OSH" SUMI_LOCAL_HOME="$EMPTY_HOME" SUMI_LOCAL_PREFIX="$INN_PRE2" "$SRC" stop 2>&1 || true)"
[[ $out == *"nothing to stop"* && ! -e $EMPTY_HOME/run ]] \
  && ok "bare-home stop refuses cleanly and creates nothing" \
  || bad "bare-home stop wrong: $(echo "$out" | tail -3)"

echo "== populated prefix: only the payload is removed"
H_HOME="$FIX/home-h"; H_PREFIX="$FIX/prefix-h"
mkdir -p "$H_PREFIX"; echo keepme > "$H_PREFIX/userfile"
h() { env HOME="$OSH" SUMI_LOCAL_HOME="$H_HOME" SUMI_LOCAL_PREFIX="$H_PREFIX" "$SRC" "$@"; }
h install --managed-pg --listen 127.0.0.1:$P6 >/dev/null
expect_rc0 "uninstall of populated-prefix install" h uninstall --yes
[[ -f $H_PREFIX/userfile && $(cat "$H_PREFIX/userfile") == keepme && ! -e $H_PREFIX/bin ]] \
  && ok "payload removed, foreign prefix entries kept" \
  || bad "populated prefix handling wrong"
# purge afterwards: the (now marker-less) prefix leftovers are skipped with a
# warning, the owned home and volume are still removed
rc=0; out="$(h uninstall --purge --yes 2>&1)" || rc=$?
[[ $rc == 0 && $out == *"leaving $H_PREFIX untouched"* && ! -d $H_HOME ]] \
  && [[ -f $H_PREFIX/userfile && $(cat "$H_PREFIX/userfile") == keepme ]] \
  && ok "purge removed owned home, kept unproven prefix leftovers" \
  || bad "post-uninstall purge wrong (rc=$rc): $(echo "$out" | tail -3)"

echo "== install via the installed prefix binary (self-copy)"
out="$(a_prefix="$A_PREFIX/bin/sumi-local"; env HOME="$OSH" SUMI_LOCAL_HOME="$A_HOME" SUMI_LOCAL_PREFIX="$A_PREFIX" "$a_prefix" install 2>&1)" && rc=0 || rc=$?
[[ $rc -ne 0 && "$out" == *"still running"* && "$out" == *"stop it first"* ]] \
  && ok "self-copy install refused while A is running (stop first)" \
  || bad "self-copy while running: rc=$rc $(echo "$out"|tail -2)"
a stop >/dev/null
out="$(a_prefix="$A_PREFIX/bin/sumi-local"; env HOME="$OSH" SUMI_LOCAL_HOME="$A_HOME" SUMI_LOCAL_PREFIX="$A_PREFIX" "$a_prefix" install 2>&1)" \
  && ok "install from installed prefix succeeded" || bad "self-copy install failed: $(echo "$out"|tail -2)"
a start >/dev/null

echo "== B purge cannot harm A"
b uninstall --purge --yes >/dev/null
! docker volume inspect "$B_VOL" >/dev/null 2>&1 \
  && ok "B's volume removed by B's purge" || bad "B's volume survived B's purge"
docker volume inspect "$A_VOL" >/dev/null 2>&1 \
  && ok "A's volume survived B's purge" || bad "A's volume was deleted by B's purge!"
a say "alpha still there" >/dev/null && ok "A still serving after B purge" \
  || bad "A broken after B purge"
[[ $(persona_of "$A_HOME") == "$A_PERSONA" ]] \
  && ok "A persona unchanged" || bad "A persona changed"
expect_rc0 "repeat purge is idempotent (absent shim)" b uninstall --purge --yes

echo "== foreign shim is kept and uninstall still exits 0"
mkdir -p "$OSH2/.local/bin"; ln -s /nonexistent/other-tool "$OSH2/.local/bin/sumi-local"
out="$(env HOME="$OSH2" SUMI_LOCAL_HOME="$B_HOME" SUMI_LOCAL_PREFIX="$B_PREFIX" "$SRC" uninstall --yes 2>&1)" \
  && ok "uninstall with foreign shim exits 0" \
  || bad "foreign-shim uninstall rc!=0: $(echo "$out" | tail -3)"
[[ -L $OSH2/.local/bin/sumi-local && $(readlink "$OSH2/.local/bin/sumi-local") == /nonexistent/other-tool ]] \
  && ok "foreign shim preserved" || bad "foreign shim removed!"

echo "== same-install reinstall keeps identity + history (managed)"
a say "remember alpha" >/dev/null
inputs_before="$(a_inputs)"
a uninstall --yes >/dev/null
[[ ! -e $A_PREFIX/bin && -d $A_HOME ]] && docker volume inspect "$A_VOL" >/dev/null 2>&1 \
  && ok "A uninstall kept data volume + home, removed payload" \
  || bad "A uninstall state wrong"
docker network ls --filter "name=^sumi-local-$A_ID" --format '{{.Name}}' | grep -q . \
  && bad "non-purge uninstall leaked A's compose network" \
  || ok "non-purge uninstall removed A's compose network"
a install >/dev/null
[[ $(persona_of "$A_HOME") == "$A_PERSONA" ]] \
  && ok "reinstall kept persona $A_PERSONA" || bad "reinstall changed persona"
a start >/dev/null
a say "again" >/dev/null
inputs_after="$(a_inputs)"
[[ $inputs_after =~ ^[0-9]+$ && $inputs_after -gt ${inputs_before:-0} ]] \
  && ok "history in A's volume survived uninstall+reinstall (inputs $inputs_before -> $inputs_after)" \
  || bad "history lost (inputs $inputs_before -> $inputs_after)"

echo "== database readiness: a proxy accept is not a PostgreSQL server"
# docker-proxy accepts on a published port while nothing listens inside the
# container. The old probe read that as ready, so a fresh managed volume
# could pass pg_up during the postgres image's initdb temp-server phase and
# hand the service a port whose first ping fails.
docker run -d --name "$NL_CTR" -p 127.0.0.1:0:5432 --entrypoint sleep postgres:17-alpine 600 >/dev/null
NL_ADDR="$(docker port "$NL_CTR" 5432/tcp | head -1)"
L_HOME="$FIX/home-l"; L_PREFIX="$FIX/prefix-l"
l() { env HOME="$OSH" SUMI_LOCAL_HOME="$L_HOME" SUMI_LOCAL_PREFIX="$L_PREFIX" "$SRC" "$@"; }
l install --db-url "postgres://sumi:x@$NL_ADDR/sumi?sslmode=disable" --listen 127.0.0.1:$P3 >/dev/null
out="$(l start 2>&1)" && rc=0 || rc=$?
[[ $rc != 0 && $out == *"cannot reach PostgreSQL at $NL_ADDR"* ]] \
  && ok "start refuses a published port with no PostgreSQL behind it" \
  || bad "proxy-only port treated as a database: rc=$rc $(echo "$out" | tail -3)"
[[ ! -f $L_HOME/run/service.pid ]] && ! pgrep -f "$L_PREFIX/bin/sumi-local-service" >/dev/null \
  && ok "no state service was spawned against it" \
  || bad "state service spawned against a proxy-only port"
docker rm -f "$NL_CTR" >/dev/null 2>&1 || true
l uninstall --purge --yes >/dev/null 2>&1 || true

echo "== a state service that dies at startup is reported at once, with its log"
A_PGADDR="127.0.0.1:$(docker inspect "$A_CTR" --format '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}')"
O_HOME="$FIX/home-o"; O_PREFIX="$FIX/prefix-o"
o() { env HOME="$OSH" SUMI_LOCAL_HOME="$O_HOME" SUMI_LOCAL_PREFIX="$O_PREFIX" "$SRC" "$@"; }
# sslnegotiation=direct is inert under sslmode=disable (pgx has no TLS
# config then), so the preflight must still open plaintext and let the
# service report the real error.
o install --db-url "postgres://sumi:wrong-password@$A_PGADDR/sumi?sslmode=disable&sslnegotiation=direct" --listen 127.0.0.1:$P4 >/dev/null
# The promise is about detection: from the spawn announcement to the failure
# report, not the preflight and /proc sweeps before the spawn. Each output
# line is stamped as it arrives; whole-start latency is reported separately.
# /proc/uptime is independent of wall-clock corrections on this Linux host.
now() { local uptime rest; read -r uptime rest </proc/uptime; printf '%s' "$uptime"; }
stamp() { local l; while IFS= read -r l; do printf '%s %s\n' "$(now)" "$l"; done; }
secs() { awk -v a="$1" -v b="$2" 'BEGIN { printf "%.1f", b - a }'; }
t0="$(now)"
out="$(o start 2>&1 | stamp)" && rc=0 || rc=$?
total="$(secs "$t0" "$(now)")"
spawn_at="$(awk '/starting state service/ { print $1; exit }' <<<"$out")"
report_at="$(awk '/ERROR: state service/ { print $1; exit }' <<<"$out")"
[[ $rc != 0 && $out == *"exited during startup"* && $out == *"password authentication failed"* ]] \
  && ok "startup death reported with the service's own error" \
  || bad "startup failure not diagnosable: rc=$rc $(echo "$out" | tail -4)"
if [[ -n $spawn_at && -n $report_at ]]; then
  detect="$(secs "$spawn_at" "$report_at")"
  awk -v d="$detect" 'BEGIN { exit !(d >= 0 && d < 20) }' \
    && ok "death reported ${detect}s after spawn, well inside the 30s health deadline" \
    || bad "failed start still waited out the health deadline (${detect}s after spawn)"
else
  bad "spawn/report lines not found in start output: $(echo "$out" | tail -3)"
fi
echo "  note: whole failed start took ${total}s (preflight and sweeps before spawn included)"
[[ $out != *wrong-password* ]] \
  && ok "failure report redacts the DB password" \
  || bad "failure report leaked the DB password"
! pgrep -f "$O_PREFIX/bin/sumi-local-service" >/dev/null \
  && ok "no service process left behind" \
  || bad "failed start left a service process"
o uninstall --purge --yes >/dev/null 2>&1 || true
a say "alpha unaffected" >/dev/null && ok "A unaffected by the failed foreign login" \
  || bad "A broken after failed-login probe"

echo "== external DB behind a direct-TLS terminator (sslnegotiation=direct) starts"
# pgx opens TLS from the first byte for sslnegotiation=direct; a terminator
# that speaks only TLS never answers a plaintext probe, so the preflight
# must open the same exchange the client will.
TLSD="$FIX/tls-direct"; mkdir -p "$TLSD"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=localhost \
  -keyout "$TLSD/key.pem" -out "$TLSD/cert.pem" >/dev/null 2>&1
A_PGPW="$(sed -n "s/^SUMI_LOCAL_PG_PASSWORD='\\(.*\\)'$/\\1/p" "$A_HOME/config.env" | tail -1)"
docker exec "$A_CTR" psql -U sumi -d sumi -qc 'create database tlsdirect' >/dev/null
node -e '
  const [port, backend, cert, key] = process.argv.slice(1), fs = require("fs"), net = require("net");
  const i = backend.lastIndexOf(":");
  require("tls").createServer({ cert: fs.readFileSync(cert), key: fs.readFileSync(key), ALPNProtocols: ["postgresql"] }, c => {
    const b = net.connect(+backend.slice(i + 1), backend.slice(0, i));
    c.pipe(b); b.pipe(c);
    const end = () => { c.destroy(); b.destroy(); };
    for (const x of [c, b]) { x.on("error", end); x.on("close", end); }
  }).on("tlsClientError", () => {}).listen(+port, "127.0.0.1");
' "$P6" "$A_PGADDR" "$TLSD/cert.pem" "$TLSD/key.pem" &
TLSD_PID=$!
for _ in $(seq 1 25); do (exec 3<>"/dev/tcp/127.0.0.1/$P6") 2>/dev/null && break; sleep 0.2; done
td() { env HOME="$OSH" SUMI_LOCAL_HOME="$FIX/home-td" SUMI_LOCAL_PREFIX="$FIX/prefix-td" "$SRC" "$@"; }
td install --db-url "postgres://sumi:$A_PGPW@127.0.0.1:$P6/tlsdirect?sslmode=require&sslnegotiation=direct" \
  --listen 127.0.0.1:$P5 >/dev/null
out="$(td start 2>&1)" && rc=0 || rc=$?
[[ $rc == 0 ]] && td say "tls direct" >/dev/null \
  && ok "direct-TLS URL: preflight passes and the service serves through the terminator" \
  || bad "direct-TLS external DB refused: rc=$rc $(echo "$out" | tail -3)"
td stop >/dev/null 2>&1 || true
td uninstall --purge --yes >/dev/null 2>&1 || true
kill "$TLSD_PID" 2>/dev/null || true

echo "== orphan refusal: lost config + leftover volume"
C_HOME="$FIX/home-c"; C_PREFIX="$FIX/prefix-c"
c() { env HOME="$OSH" SUMI_LOCAL_HOME="$C_HOME" SUMI_LOCAL_PREFIX="$C_PREFIX" "$SRC" "$@"; }
c install --managed-pg --listen 127.0.0.1:$P2 >/dev/null
C_ID="$(id_of "$C_HOME")"
c start >/dev/null; c stop >/dev/null
rm -f "$C_HOME/config.env"
expect_die "fresh install refuses orphan volume" "Refusing to adopt" \
  env HOME="$OSH" SUMI_LOCAL_HOME="$C_HOME" SUMI_LOCAL_PREFIX="$C_PREFIX" "$SRC" install --managed-pg --listen 127.0.0.1:$P2
docker rm -f "sumi-local-pg-$C_ID" >/dev/null 2>&1 || true
docker volume rm "sumi-local-pgdata-$C_ID" >/dev/null 2>&1 || true
c install --managed-pg --listen 127.0.0.1:$P2 >/dev/null \
  && ok "after explicit resource removal, fresh install proceeds" \
  || bad "fresh install still blocked after resource removal"
env HOME="$OSH" SUMI_LOCAL_HOME="$C_HOME" SUMI_LOCAL_PREFIX="$C_PREFIX" "$SRC" uninstall --purge --yes >/dev/null 2>&1 || true

echo "== lost config — marker recovers managed resources + real prefix"
D_HOME="$FIX/home-d"; D_PREFIX="$FIX/prefix-d"
d() { env HOME="$OSH" SUMI_LOCAL_HOME="$D_HOME" SUMI_LOCAL_PREFIX="$D_PREFIX" "$SRC" "$@"; }
d install --managed-pg --listen 127.0.0.1:$P3 >/dev/null
D_ID="$(id_of "$D_HOME")"
d start >/dev/null
d say "dee ping" >/dev/null && ok "D serving" || bad "D failed to start"
rm -f "$D_HOME/config.env"
out="$(d stop 2>&1)"
[[ $out == *"recorded evidence"* || $out == *"config unavailable"* ]] \
  && ok "config-less stop explains degraded mode" \
  || bad "config-less stop silent: $(echo "$out" | tail -3)"
! docker inspect "sumi-local-pg-$D_ID" --format '{{.State.Running}}' 2>/dev/null | grep -q true \
  && ok "config-less stop stopped D's managed container" \
  || bad "managed container still running after config-less stop"
# env prefix deliberately NOT passed — the marker's recorded prefix must locate it
out="$(env HOME="$OSH" SUMI_LOCAL_HOME="$D_HOME" "$SRC" uninstall --yes 2>&1)"
[[ $out == *"removed managed container"* ]] \
  && ok "config-less uninstall removed managed container" \
  || bad "container removal unreported: $(echo "$out" | tail -3)"
[[ ! -d $D_PREFIX ]] \
  && ok "marker-recorded prefix located and removed without env selector" \
  || bad "prefix survived config-less uninstall: $(echo "$out" | tail -3)"
docker volume inspect "sumi-local-pgdata-$D_ID" >/dev/null 2>&1 \
  && [[ $out == *"kept managed volume"* ]] \
  && ok "config-less uninstall kept + reported managed volume" \
  || bad "volume state/reporting wrong: $(echo "$out" | tail -3)"
docker network ls --filter "name=^sumi-local-$D_ID" --format '{{.Name}}' | grep -q . \
  && bad "compose network leaked" || ok "compose network removed"
[[ -d $D_HOME && -f $D_HOME/.sumi-local-home ]] \
  && ok "home + marker retained" || bad "home/marker wrongly removed"
expect_rc0 "config-less purge via marker" \
  env HOME="$OSH" SUMI_LOCAL_HOME="$D_HOME" "$SRC" uninstall --purge --yes
! docker volume inspect "sumi-local-pgdata-$D_ID" >/dev/null 2>&1 \
  && [[ ! -d $D_HOME ]] \
  && ok "config-less purge removed volume + home" \
  || bad "config-less purge incomplete"

echo "== purge with orphaned processes: swept first, DB not pulled from under them"
J_HOME="$FIX/home-j"; J_PREFIX="$FIX/prefix-j"
j() { env HOME="$OSH" SUMI_LOCAL_HOME="$J_HOME" SUMI_LOCAL_PREFIX="$J_PREFIX" "$SRC" "$@"; }
j install --managed-pg --listen 127.0.0.1:$P7 >/dev/null
J_ID="$(id_of "$J_HOME")"
j start >/dev/null
rm -f "$J_HOME/run/service.pid" "$J_HOME/run/local.pid"
j uninstall --purge --yes >/dev/null
if ! pgrep -f "$J_PREFIX/" >/dev/null 2>&1 \
   && ! docker inspect "sumi-local-pg-$J_ID" >/dev/null 2>&1 \
   && ! docker volume inspect "sumi-local-pgdata-$J_ID" >/dev/null 2>&1; then
  ok "purge swept orphans then removed container+volume"
else
  bad "purge left orphans/resources: $(pgrep -af "$J_PREFIX/" | head -3)"
fi

echo "== corrupt config — inert data, purge still recovers via marker"
E_HOME="$FIX/home-e"; E_PREFIX="$FIX/prefix-e"
e() { env HOME="$OSH" SUMI_LOCAL_HOME="$E_HOME" SUMI_LOCAL_PREFIX="$E_PREFIX" "$SRC" "$@"; }
e install --managed-pg --listen 127.0.0.1:$P4 >/dev/null
E_ID="$(id_of "$E_HOME")"
e start >/dev/null
{ printf 'SUMI_LOCAL_ID='"'"'slbadbad00'"'"'\n'
  printf 'SUMI_LOCAL_LISTEN='"'"'127.0.0.1:9'"'"'$(touch "%s")\n' "$FIX/pwned"
} > "$E_HOME/config.env"   # syntactically valid, incomplete, hostile-looking
out="$(e uninstall --purge --yes 2>&1)"
[[ ! -f $FIX/pwned ]] \
  && ok "config content never executed (data-only parse)" \
  || bad "config.env was executed as shell!"
[[ $out == *"config incomplete"* || $out == *"incomplete/corrupt"* ]] \
  && ok "corrupt config reported, not silently trusted" \
  || bad "corrupt config not reported: $(echo "$out" | tail -3)"
! docker inspect "sumi-local-pg-$E_ID" >/dev/null 2>&1 \
  && ! docker volume inspect "sumi-local-pgdata-$E_ID" >/dev/null 2>&1 \
  && [[ ! -d $E_HOME && ! -d $E_PREFIX ]] \
  && ok "corrupt-config purge removed all owned resources" \
  || bad "corrupt-config purge left resources behind"

echo "== marker/config id disagreement refuses destructive action"
I_HOME="$FIX/home-i"; I_PREFIX="$FIX/prefix-i"
i() { env HOME="$OSH" SUMI_LOCAL_HOME="$I_HOME" SUMI_LOCAL_PREFIX="$I_PREFIX" "$SRC" "$@"; }
i install --managed-pg --listen 127.0.0.1:$P8 >/dev/null
i stop >/dev/null 2>&1 || true
printf 'SUMI_LOCAL_ID='"'"'sl0deadbeef0'"'"'\n' > "$I_HOME/.sumi-local-home"  # hand-forged split
out="$(i uninstall --yes 2>&1 || true)"
[[ $out == *"disagrees"* && -d $I_HOME && -d $I_PREFIX ]] \
  && ok "marker/config id split refused, nothing removed" \
  || bad "split identity not refused: $(echo "$out" | tail -3)"
# restore the real marker so cleanup can take the fixture down
printf 'SUMI_LOCAL_ID='"'"'%s'"'"'\n' "$(id_of "$I_HOME")" > "$I_HOME/.sumi-local-home"
printf 'SUMI_LOCAL_PREFIX='"'"'%s'"'"'\n' "$I_PREFIX" >> "$I_HOME/.sumi-local-home"

echo "== failed install: marker-only home is recognized and cleanable"
G_HOME="$FIX/home-failed"; mkdir -p "$G_HOME/run" "$G_HOME/log"
printf 'SUMI_LOCAL_ID='"'"'sldeadbeef01'"'"'\n' > "$G_HOME/.sumi-local-home"
env HOME="$OSH" SUMI_LOCAL_HOME="$G_HOME" SUMI_LOCAL_PREFIX="$FIX/prefix-failed" "$SRC" uninstall --purge --yes >/dev/null 2>&1
[[ ! -d $G_HOME ]] && ok "marker-only (failed install) home purged" \
  || bad "marker-only home not cleanable"

echo "== moved home: recorded id stays authoritative; reinstall resyncs marker"
F_HOME="$FIX/home-f"; F_PREFIX="$FIX/prefix-f"
f() { env HOME="$OSH" SUMI_LOCAL_HOME="$F_HOME" SUMI_LOCAL_PREFIX="$F_PREFIX" "$SRC" "$@"; }
f install --managed-pg --listen 127.0.0.1:$P5 >/dev/null
F_ID="$(id_of "$F_HOME")"
f start >/dev/null; f stop >/dev/null
mv "$F_HOME" "$FIX/home-f-moved"; F_HOME="$FIX/home-f-moved"
f uninstall --purge --yes >/dev/null 2>&1
! docker volume inspect "sumi-local-pgdata-$F_ID" >/dev/null 2>&1 \
  && [[ ! -d $F_HOME ]] \
  && ok "moved-home purge removed recorded-id resources + home" \
  || bad "moved-home purge failed (id drifted to new path)"
# now the R6 sequence: fresh install at a moved home whose marker is stale
# must resync the marker to the effective id, never leave it stale
F2_HOME="$FIX/home-f2"; F2_PREFIX="$FIX/prefix-f2"
env HOME="$OSH" SUMI_LOCAL_HOME="$F2_HOME" SUMI_LOCAL_PREFIX="$F2_PREFIX" "$SRC" install --managed-pg --listen 127.0.0.1:$P5 >/dev/null
F2_ID="$(id_of "$F2_HOME")"
env HOME="$OSH" SUMI_LOCAL_HOME="$F2_HOME" SUMI_LOCAL_PREFIX="$F2_PREFIX" "$SRC" stop >/dev/null 2>&1 || true
mv "$F2_HOME" "$FIX/home-f2-moved"; F2_HOME="$FIX/home-f2-moved"
rm -f "$F2_HOME/config.env"
docker rm -f "sumi-local-pg-$F2_ID" >/dev/null 2>&1 || true
docker volume rm "sumi-local-pgdata-$F2_ID" >/dev/null 2>&1 || true
# reinstall at the moved path with a NEW prefix (the old one is marked for
# the stale id and is correctly refused to a new install)
F2_PREFIX="$FIX/prefix-f2b"
env HOME="$OSH" SUMI_LOCAL_HOME="$F2_HOME" SUMI_LOCAL_PREFIX="$F2_PREFIX" "$SRC" install --managed-pg --listen 127.0.0.1:$P5 >/dev/null
NEW_ID="$(id_of "$F2_HOME")"
MARK_ID="$(sed -n "s/^SUMI_LOCAL_ID='\\(.*\\)'$/\\1/p" "$F2_HOME/.sumi-local-home")"
[[ $MARK_ID == "$NEW_ID" ]] \
  && ok "moved-home reinstall resynced marker to effective id ($NEW_ID)" \
  || bad "marker stayed stale after reinstall ($MARK_ID vs $NEW_ID)"
env HOME="$OSH" SUMI_LOCAL_HOME="$F2_HOME" "$SRC" uninstall --purge --yes >/dev/null 2>&1 || true

echo "== external -> managed reinstall keeps a coherent identity"
K_HOME="$FIX/home-k"; K_PREFIX="$FIX/prefix-k"
env HOME="$OSH" SUMI_LOCAL_HOME="$K_HOME" SUMI_LOCAL_PREFIX="$K_PREFIX" "$SRC" \
  install --db-url 'postgres://sumi:x@127.0.0.1:1/none' --listen 127.0.0.1:$P9 >/dev/null
K_ID="$(id_of "$K_HOME")"
rm -f "$K_HOME/config.env"
env HOME="$OSH" SUMI_LOCAL_HOME="$K_HOME" SUMI_LOCAL_PREFIX="$K_PREFIX" "$SRC" \
  install --managed-pg --listen 127.0.0.1:$P9 >/dev/null
K_ID2="$(id_of "$K_HOME")"
K_MARK="$(sed -n "s/^SUMI_LOCAL_ID='\\(.*\\)'$/\\1/p" "$K_HOME/.sumi-local-home")"
K_PMARK="$(sed -n "s/^SUMI_LOCAL_ID='\\(.*\\)'$/\\1/p" "$K_PREFIX/.sumi-local-prefix")"
[[ $K_ID2 == "$K_ID" && $K_MARK == "$K_ID" && $K_PMARK == "$K_ID" ]] \
  && ok "external->managed reinstall: config/marker/prefix all agree ($K_ID)" \
  || bad "identity disagree after external->managed reinstall (cfg=$K_ID2 home=$K_MARK prefix=$K_PMARK)"
env HOME="$OSH" SUMI_LOCAL_HOME="$K_HOME" SUMI_LOCAL_PREFIX="$K_PREFIX" "$SRC" uninstall --purge --yes >/dev/null 2>&1 || true

# --- review-A F-A1: a retained state home must never steer signals at a
# different install that re-claimed the recorded prefix path -----------
echo "== stale home + reused prefix path: old stop/start cannot touch the new install"
M_HOME="$FIX/home-m"; N_HOME="$FIX/home-n"; MN_PREFIX="$FIX/prefix-m"
m() { env HOME="$OSH" SUMI_LOCAL_HOME="$M_HOME" SUMI_LOCAL_PREFIX="$MN_PREFIX" "$SRC" "$@"; }
n() { env HOME="$OSH" SUMI_LOCAL_HOME="$N_HOME" SUMI_LOCAL_PREFIX="$MN_PREFIX" "$SRC" "$@"; }
m install --managed-pg --listen 127.0.0.1:$P1 >/dev/null
m start >/dev/null
m uninstall >/dev/null    # keeps home-m; removes the prefix payload+marker
n install --managed-pg --listen 127.0.0.1:$P2 >/dev/null   # same prefix path, new home/id
n start >/dev/null
n say "n lives" >/dev/null && ok "new install on reused prefix is serving" \
  || bad "new install failed to start"
out="$(m stop 2>&1)" && rc=0 || rc=$?
[[ $rc != 0 && $out == *"different install"* ]] \
  && ok "stale-home stop refuses: prefix now owned by another install" \
  || bad "stale-home stop mishandled rc=$rc: $(echo "$out" | tail -3)"
n say "n still lives" >/dev/null \
  && ok "stale-home stop did not touch the new install's processes" \
  || bad "new install's processes were killed by the stale home's stop!"
out="$(m start 2>&1)" && rc=0 || rc=$?
[[ $rc != 0 ]] \
  && ok "stale-home start refused (prefix foreign-owned)" \
  || bad "stale-home start adopted another install's prefix/port"
n say "n endures" >/dev/null \
  && ok "new install still healthy after stale-home start attempt" \
  || bad "new install harmed by stale-home start"
n stop >/dev/null; n uninstall --purge --yes >/dev/null
env HOME="$OSH" SUMI_LOCAL_HOME="$M_HOME" "$SRC" uninstall --purge --yes >/dev/null 2>&1 || true
# m's own purge runs after n removed the shared prefix's marker — it may
# succeed or refuse depending on ordering; either way make sure no fixture
# volume leaks by removing m's exact resource names.
M_ID="$(sed -n "s/^SUMI_LOCAL_ID='\\(.*\\)'$/\\1/p" "$M_HOME/config.env" 2>/dev/null || true)"
[[ -n $M_ID ]] && { docker rm -f "sumi-local-pg-$M_ID" >/dev/null 2>&1; docker volume rm "sumi-local-pgdata-$M_ID" >/dev/null 2>&1; } || true

# --- review-B F1: config-less stop must not honor a foreign env prefix --
echo "== config-less stop with another install's env prefix is refused"
p install --managed-pg --listen 127.0.0.1:$P3 >/dev/null
q install --managed-pg --listen 127.0.0.1:$P4 >/dev/null
p start >/dev/null; q start >/dev/null
rm -f "$FIX/home-q/config.env"
expect_die "config-less stop refuses env prefix of another install" \
  "does not match the prefix recorded" \
  env HOME="$OSH" SUMI_LOCAL_HOME="$FIX/home-q" SUMI_LOCAL_PREFIX="$FIX/prefix-p" "$SRC" stop
# q's config is gone (that is the point of this probe) — verify its
# service process survived by its recorded pid rather than `say`
Q_SPID="$(cut -d' ' -f1 "$FIX/home-q/run/service.pid" 2>/dev/null || true)"
p say "p lives" >/dev/null && [[ -n $Q_SPID && -d /proc/$Q_SPID ]] \
  && ok "both installs alive after refused mismatched stop" \
  || bad "a refused config-less stop still signaled processes!"
[[ -f $FIX/home-q/run/service.pid ]] \
  && ok "refused stop kept the target's pidfiles (no lost-pidfile state)" \
  || bad "refused stop still deleted pidfiles"
# the correct degraded path still works: marker prefix locates the install
out="$(env HOME="$OSH" SUMI_LOCAL_HOME="$FIX/home-q" "$SRC" stop 2>&1)" && rc=0 || rc=$?
[[ $rc == 0 && ( $out == *"stopping unrecorded process"* || $out == *"stopped managed container"* ) ]] \
  && ok "config-less stop sweeps the install's own proven-prefix processes" \
  || bad "correct degraded stop failed: $(echo "$out" | tail -3)"
q uninstall --purge --yes >/dev/null 2>&1 || true
p stop >/dev/null; p uninstall --purge --yes >/dev/null 2>&1 || true

# --- review-A F-A2 / B F2: install over live processes is refused -------
echo "== install refuses while the install's processes are running"
r install --managed-pg --listen 127.0.0.1:$P5 >/dev/null
r start >/dev/null
expect_die "reinstall to a new prefix while running refused" "stop it first" \
  env HOME="$OSH" SUMI_LOCAL_HOME="$FIX/home-r" "$SRC" install \
    --managed-pg --prefix "$FIX/prefix-r2" --listen 127.0.0.1:$P5
expect_die "same-prefix reinstall while running refused" "stop it first" \
  r install --managed-pg --listen 127.0.0.1:$P5
r say "r lives" >/dev/null \
  && ok "running install unharmed by refused reinstall" \
  || bad "refused reinstall still touched the live install"
r stop >/dev/null
env HOME="$OSH" SUMI_LOCAL_HOME="$FIX/home-r" "$SRC" install \
  --managed-pg --prefix "$FIX/prefix-r2" --listen 127.0.0.1:$P5 >/dev/null \
  && ok "prefix move allowed once stopped" \
  || bad "stopped install could not move prefix"
r2() { env HOME="$OSH" SUMI_LOCAL_HOME="$FIX/home-r" SUMI_LOCAL_PREFIX="$FIX/prefix-r2" "$SRC" "$@"; }
r2 start >/dev/null && r2 say "r moved" >/dev/null \
  && ok "moved install serves from the new prefix" \
  || bad "moved install failed to start"
r2 stop >/dev/null; r2 uninstall --purge --yes >/dev/null 2>&1 || true

# --- review-A F-A3 / B O5: unparseable prefix marker is not ownership ---
echo "== corrupt/unparseable prefix marker never authorizes deletion"
U_HOME="$FIX/home-u"; U_PREFIX="$FIX/prefix-u"
mkdir -p "$U_HOME" "$U_PREFIX/bin" "$U_PREFIX/core"
printf "SUMI_LOCAL_ID='sl0123456789'\nSUMI_LOCAL_PREFIX='%s'\n" "$U_PREFIX" \
  > "$U_HOME/.sumi-local-home"
echo "userfile" > "$U_PREFIX/bin/userfile"
echo "userfile2" > "$U_PREFIX/core/userfile2"
echo "TOTAL GARBAGE no equals" > "$U_PREFIX/.sumi-local-prefix"
out="$(env HOME="$OSH" SUMI_LOCAL_HOME="$U_HOME" SUMI_LOCAL_PREFIX="$U_PREFIX" \
  "$SRC" uninstall --yes 2>&1)" && rc=0 || rc=$?
[[ $out == *"does not positively identify"* || $out == *"leaving $U_PREFIX untouched"* ]] \
  && ok "unparseable prefix marker: payload left untouched" \
  || bad "corrupt marker authorized deletion: $(echo "$out" | tail -3)"
[[ -f $U_PREFIX/bin/userfile && -f $U_PREFIX/core/userfile2 ]] \
  && ok "foreign bin/core entries preserved under corrupt marker" \
  || bad "payload entries were deleted despite corrupt marker!"
out="$(env HOME="$OSH" SUMI_LOCAL_HOME="$U_HOME" SUMI_LOCAL_PREFIX="$U_PREFIX" \
  "$SRC" uninstall --purge --yes 2>&1)" && rc=0 || rc=$?
[[ $rc == 0 && ! -d $U_HOME && -f $U_PREFIX/bin/userfile ]] \
  && ok "purge removed proven home; corrupt-marked prefix still preserved" \
  || bad "purge mishandled corrupt-marker prefix"

# --- review-B O6: a regular-file shim is authored content --------------
echo "== regular-file ~/.local/bin/sumi-local is never overwritten"
OSH3="$FIX/os-home3"; mkdir -p "$OSH3/.local/bin"
echo "my own tool" > "$OSH3/.local/bin/sumi-local"; chmod +x "$OSH3/.local/bin/sumi-local"
expect_die "install refuses to overwrite a regular-file shim" "not a symlink" \
  env HOME="$OSH3" SUMI_LOCAL_HOME="$FIX/home-shim" SUMI_LOCAL_PREFIX="$FIX/prefix-shim" \
    "$SRC" install --db-url 'postgres://x@127.0.0.1:1/n' --listen 127.0.0.1:$P6
[[ $(cat "$OSH3/.local/bin/sumi-local") == "my own tool" && ! -e $FIX/home-shim ]] \
  && ok "regular-file shim preserved; nothing was created" \
  || bad "regular-file shim was overwritten or install half-created dirs"

# --- review-B F3/O2: CRLF config parses; duplicate keys last-wins -------
echo "== CRLF config is readable; duplicate keys resolve last-wins"
w install --managed-pg --listen 127.0.0.1:$P7 >/dev/null
sed -i 's/$/\r/' "$FIX/home-w/config.env"
[[ $(w url 2>/dev/null) == *$P7* ]] \
  && ok "CRLF config.env still parses" \
  || bad "CRLF config treated as corrupt"
printf "SUMI_LOCAL_LISTEN='127.0.0.1:$P8'\n" >> "$FIX/home-w/config.env"
[[ $(w url 2>/dev/null) == *$P8* ]] \
  && ok "duplicate key resolves last-wins" \
  || bad "duplicate-key policy inconsistent: $(w url 2>/dev/null | head -1)"
w uninstall --purge --yes >/dev/null 2>&1 || true

# --- review-A F-A4: & | \ in a moved prefix must stay literal -----------
echo "== moved prefix with shell-special chars is recorded literally"
v install --managed-pg --listen 127.0.0.1:$P9 >/dev/null
env HOME="$OSH" SUMI_LOCAL_HOME="$FIX/home-v" "$SRC" install \
  --managed-pg --prefix "$FIX/prefix-v&amp;x" --listen 127.0.0.1:$P9 >/dev/null
[[ $(grep -cF "SUMI_LOCAL_INSTALLED_PREFIX='$FIX/prefix-v&amp;x'" "$FIX/home-v/config.env") == 1 ]] \
  && ok "recorded prefix with & is literal (no sed expansion)" \
  || bad "recorded prefix corrupted: $(grep SUMI_LOCAL_INSTALLED_PREFIX "$FIX/home-v/config.env")"
[[ -d "$FIX/prefix-v&amp;x/bin" \
  && $(sed -n "s/^SUMI_LOCAL_ID='\\(.*\\)'$/\\1/p" "$FIX/prefix-v&amp;x/.sumi-local-prefix") == "$(id_of "$FIX/home-v")" ]] \
  && ok "special-char prefix installed + marked" \
  || bad "special-char prefix payload/marker missing"
env HOME="$OSH" SUMI_LOCAL_HOME="$FIX/home-v" SUMI_LOCAL_PREFIX="$FIX/prefix-v&amp;x" \
  "$SRC" uninstall --purge --yes >/dev/null 2>&1 || true

# --- review-B F1 / f89: config-less reinstall must check the marker's
# recorded prefix for live processes BEFORE rewriting evidence ----------
echo "== config-less reinstall/move refuses while recorded prefix is live"
t install --managed-pg --listen 127.0.0.1:$P2 >/dev/null
t start >/dev/null
rm -f "$FIX/home-t/config.env"
T_MARK_BEFORE="$(cat "$FIX/home-t/.sumi-local-home")"
out="$(env HOME="$OSH" SUMI_LOCAL_HOME="$FIX/home-t" "$SRC" install \
  --managed-pg --prefix "$FIX/prefix-t2" --listen 127.0.0.1:$P2 2>&1)" && rc=0 || rc=$?
[[ $rc != 0 && $out == *"still running"* && $out == *"stop it first"* ]] \
  && ok "config-less move refused while marker prefix is live" \
  || bad "config-less move over live processes not refused: rc=$rc $(echo "$out" | tail -2)"
[[ $(cat "$FIX/home-t/.sumi-local-home") == "$T_MARK_BEFORE" && ! -e $FIX/prefix-t2 ]] \
  && ok "refusal preserved the marker and created nothing" \
  || bad "refused config-less move rewrote evidence!"
pgrep -f "$FIX/prefix-t/bin/sumi-local-service" >/dev/null \
  && ok "original service still running after refused move" \
  || bad "refused move still harmed the running service"
# stopped config-less move stays supported — but a managed install's
# leftover container/volume is never adopted without its config, so
# remove them explicitly first (the documented recovery step)
env HOME="$OSH" SUMI_LOCAL_HOME="$FIX/home-t" "$SRC" stop >/dev/null 2>&1 || true
T_ID="$(sed -n "s/^SUMI_LOCAL_ID='\\(.*\\)'$/\\1/p" "$FIX/home-t/.sumi-local-home")"
docker rm -f "sumi-local-pg-$T_ID" >/dev/null 2>&1 || true
docker volume rm "sumi-local-pgdata-$T_ID" >/dev/null 2>&1 || true
out="$(env HOME="$OSH" SUMI_LOCAL_HOME="$FIX/home-t" "$SRC" install \
  --managed-pg --prefix "$FIX/prefix-t2" --listen 127.0.0.1:$P2 2>&1)" && rc=0 || rc=$?
[[ $rc == 0 ]] \
  && ok "config-less move proceeds once stopped" \
  || bad "stopped config-less move refused: $(echo "$out" | tail -2)"
env HOME="$OSH" SUMI_LOCAL_HOME="$FIX/home-t" SUMI_LOCAL_PREFIX="$FIX/prefix-t2" \
  "$SRC" uninstall --purge --yes >/dev/null 2>&1 || true

# --- review-A F1 / f109: install joins the per-home operation lock -----
echo "== install serializes with other lifecycle ops on the same home"
x install --managed-pg --listen 127.0.0.1:$P1 >/dev/null
mkdir -p "$FIX/home-x/run"
# Ordering evidence, not elapsed time: elapsed time measured around a
# 4 s holder passes whether or not install waited, and bash $SECONDS
# follows the wall clock, which steps on some hosts. The holder takes the
# lock first, waits (bounded) until install is seen blocked in its own
# `flock -w 30 9`, keeps holding 1 s more, and records whether install's
# first under-lock write (the prefix marker) happened while it held the
# lock. The marker path reaches the holder via the environment: an argv
# element under the prefix is, correctly, a live process of this install
# to install's own pre-lock refusal.
XS="$FIX/lock-x"; mkdir -p "$XS"
XM="$FIX/prefix-x/.sumi-local-prefix" flock "$FIX/home-x/run/lock" bash -c '
  echo held >"$1/held"; st=gone
  for _ in $(seq 1 600); do
    ip=$(cat "$1/install.pid" 2>/dev/null)
    if [[ -n $ip ]]; then
      kill -0 "$ip" 2>/dev/null || { st=gone; break; }
      st=running
      for p in $(pgrep -x flock); do
        [[ $(tr "\0" " " <"/proc/$p/cmdline" 2>/dev/null) == "flock -w 30 9 " ]] || continue
        q=$p
        while q=$(awk "{print \$4}" "/proc/$q/stat" 2>/dev/null) && ((q > 1)); do
          [[ $q == "$ip" ]] && { st=blocked-in-lock; break 2; }
        done
      done
      [[ $st == blocked-in-lock ]] && break
    fi
    sleep 0.1
  done
  [[ $st == blocked-in-lock ]] && sleep 1
  printf "%s\n%s\n" "$(stat -c %y "$XM")" "$st" >"$1/release"' _ "$XS" &
LOCK_HOLDER=$!
XM="$FIX/prefix-x/.sumi-local-prefix"
for _ in $(seq 1 100); do [[ -s $XS/held ]] && break; sleep 0.1; done
m0="$(stat -c %y "$XM")"
x install --managed-pg --listen 127.0.0.1:$P1 >"$XS/install.out" 2>&1 &
echo "$!" >"$XS/install.pid"
wait "$!" && rc=0 || rc=$?
wait "$LOCK_HOLDER" 2>/dev/null || true
m1="$(stat -c %y "$XM")"
m_held="" st_held=""
{ read -r m_held; read -r st_held; } <"$XS/release" 2>/dev/null || true
if [[ ! -s $XS/held ]]; then
  bad "lock fixture never took the home lock — serialization not tested"
elif [[ $st_held == blocked-in-lock && $m_held == "$m0" && $rc == 0 && $m1 != "$m0" ]]; then
  ok "install blocked on the per-home lock, wrote nothing while it was held, then succeeded"
else
  bad "install did not serialize on the home lock (rc=$rc, install while held: ${st_held:-?}, marker written while held: $([[ $m_held == "$m0" ]] && echo no || echo yes), written after: $([[ $m1 != "$m0" ]] && echo yes || echo no)): $(tail -2 "$XS/install.out")"
fi
x uninstall --purge --yes >/dev/null 2>&1 || true

# --- review-A F2 / f110: nested home/prefix refused before mutation ----
echo "== nested home/prefix pairs refused before anything is created"
expect_die "prefix nested inside home refused" "must not nest" \
  env HOME="$OSH" SUMI_LOCAL_HOME="$FIX/home-nest" SUMI_LOCAL_PREFIX="$FIX/home-nest/payload" \
    "$SRC" install --db-url 'postgres://x@127.0.0.1:1/n' --listen 127.0.0.1:$P2
[[ ! -e $FIX/home-nest ]] \
  && ok "nested-prefix refusal created nothing" \
  || bad "nested-prefix refusal still created the home"
expect_die "home nested inside prefix refused" "must not nest" \
  env HOME="$OSH" SUMI_LOCAL_HOME="$FIX/prefix-nest/state" SUMI_LOCAL_PREFIX="$FIX/prefix-nest" \
    "$SRC" install --db-url 'postgres://x@127.0.0.1:1/n' --listen 127.0.0.1:$P2
[[ ! -e $FIX/prefix-nest ]] \
  && ok "nested-home refusal created nothing" \
  || bad "nested-home refusal still created the prefix"
expect_die "equal home/prefix refused" "must not nest" \
  env HOME="$OSH" SUMI_LOCAL_HOME="$FIX/same" SUMI_LOCAL_PREFIX="$FIX/same" \
    "$SRC" install --db-url 'postgres://x@127.0.0.1:1/n' --listen 127.0.0.1:$P2

# --- review-B F3 / f112: control-char paths + incomplete config --------
echo "== control characters in paths and incomplete configs are refused"
expect_die "newline in --prefix refused" "control characters" \
  env HOME="$OSH" SUMI_LOCAL_HOME="$FIX/home-ctl" SUMI_LOCAL_PREFIX="$(printf '%s\nEVIL=x' "$FIX/prefix-ctl")" \
    "$SRC" install --db-url 'postgres://x@127.0.0.1:1/n' --listen 127.0.0.1:$P3
[[ ! -e $FIX/home-ctl && ! -e $FIX/prefix-ctl ]] \
  && ok "control-char refusal created nothing" \
  || bad "control-char install partially created dirs"
expect_die "CR in --home refused" "control characters" \
  env HOME="$OSH" SUMI_LOCAL_HOME="$(printf '%s\r' "$FIX/home-cr")" SUMI_LOCAL_PREFIX="$FIX/prefix-cr" \
    "$SRC" install --db-url 'postgres://x@127.0.0.1:1/n' --listen 127.0.0.1:$P3
s install --db-url 'postgres://x@127.0.0.1:1/n' --listen 127.0.0.1:$P3 >/dev/null
S_CFG_BEFORE="$(cat "$FIX/home-s/config.env")"
# strip required keys -> incomplete config must be refused, kept verbatim
grep -v '^SUMI_CORE_STATE_TOKEN=' "$FIX/home-s/config.env" > "$FIX/home-s/config.tmp"
mv "$FIX/home-s/config.tmp" "$FIX/home-s/config.env"
out="$(s install --db-url 'postgres://x@127.0.0.1:1/n' --listen 127.0.0.1:$P3 2>&1)" && rc=0 || rc=$?
[[ $rc != 0 && $out == *"incomplete"* && $out == *"SUMI_CORE_STATE_TOKEN"* && $out == *"move config.env aside"* ]] \
  && ok "incomplete config refused with a repair path" \
  || bad "incomplete config mishandled: rc=$rc $(echo "$out" | tail -2)"
[[ $(cat "$FIX/home-s/config.env") == "$(grep -v '^SUMI_CORE_STATE_TOKEN=' <<<"$S_CFG_BEFORE")" ]] \
  && ok "incomplete config preserved verbatim (identity keys kept)" \
  || bad "refused install rewrote the incomplete config"
# repairing the listed key makes the SAME install usable again
printf 'SUMI_CORE_STATE_TOKEN='"'"'repaired-secret'"'"'\n' >> "$FIX/home-s/config.env"
s url >/dev/null 2>&1 \
  && ok "repaired config is usable (same install, same identity)" \
  || bad "repaired config still unusable"
s uninstall --purge --yes >/dev/null 2>&1 || true

# --- review-A F1: unreadable config/marker is named, never a silent exit --
echo "== unreadable config.env / home marker: named refusal, nothing changed"
if ((EUID == 0)); then
  ok "skipped as root (mode 000 does not restrict root)"
else
  s install --db-url 'postgres://x@127.0.0.1:1/n' --listen 127.0.0.1:$P3 >/dev/null
  S_H="$FIX/home-s"; S_SUM="$(cksum <"$S_H/config.env")"
  chmod 000 "$S_H/config.env"
  out="$(s install --db-url 'postgres://x@127.0.0.1:1/n' --listen 127.0.0.1:$P3 2>&1)" && rc=0 || rc=$?
  chmod 600 "$S_H/config.env"
  [[ $rc != 0 && $out == *"cannot read"*"config.env"* ]] \
    && ok "install over an unreadable config.env names the file (rc=$rc)" \
    || bad "unreadable config.env not diagnosed: rc=$rc out: $(echo "$out" | tail -2)"
  [[ $(cksum <"$S_H/config.env") == "$S_SUM" ]] \
    && ok "refused install left config.env byte-identical" \
    || bad "refused install rewrote config.env"
  mv "$S_H/config.env" "$FIX/s-config.saved"
  chmod 000 "$S_H/.sumi-local-home"
  out="$(s stop 2>&1)" && rc=0 || rc=$?
  chmod 600 "$S_H/.sumi-local-home"
  [[ $rc != 0 && $out == *"cannot read"*".sumi-local-home"* ]] \
    && ok "config-less stop over an unreadable marker names the file (rc=$rc)" \
    || bad "unreadable marker not diagnosed: rc=$rc out: $(echo "$out" | tail -2)"
  mv "$FIX/s-config.saved" "$S_H/config.env"
  [[ -f $S_H/.sumi-local-home && $(cksum <"$S_H/config.env") == "$S_SUM" ]] \
    && ok "marker and config intact after both refusals" \
    || bad "refusal removed or changed the marker/config"
  s uninstall --purge --yes >/dev/null 2>&1 || true
fi

# --- review-A F4 / f111: volume-ownership refusal precedes teardown ----
echo "== foreign-labelled volume refuses purge BEFORE own resources are removed"
# marker-only home names install slffff1111ff; the container/network carry
# this install's project label (removable), but the volume is labelled for
# a DIFFERENT project — the refusal must fire before either is torn down.
Y_ID=slffff1111ff; Y_HOME="$FIX/home-y"
mkdir -p "$Y_HOME"
printf "SUMI_LOCAL_ID='%s'\nSUMI_LOCAL_PREFIX='%s'\n" "$Y_ID" "$FIX/prefix-y" > "$Y_HOME/.sumi-local-home"
docker create --name "sumi-local-pg-$Y_ID" \
  --label "com.docker.compose.project=sumi-local-$Y_ID" \
  postgres:17-alpine >/dev/null
docker network create --label "com.docker.compose.project=sumi-local-$Y_ID" \
  "sumi-local-$Y_ID-default" >/dev/null
docker volume create --label com.docker.compose.project=other-proj \
  "sumi-local-pgdata-$Y_ID" >/dev/null
out="$(env HOME="$OSH" SUMI_LOCAL_HOME="$Y_HOME" SUMI_LOCAL_PREFIX="$FIX/prefix-y" \
  "$SRC" uninstall --purge --yes 2>&1)" && rc=0 || rc=$?
[[ $rc != 0 && $out == *"not labeled for project"* ]] \
  && ok "purge refused foreign-labelled volume" \
  || bad "foreign-labelled volume not refused: rc=$rc $(echo "$out" | tail -2)"
docker inspect "sumi-local-pg-$Y_ID" >/dev/null 2>&1 \
  && ok "own container NOT removed before the refused volume check" \
  || bad "container removed before volume ownership check!"
docker network inspect "sumi-local-$Y_ID-default" >/dev/null 2>&1 \
  && ok "own network NOT removed before the refused volume check" \
  || bad "network removed before volume ownership check!"
[[ -d $Y_HOME ]] \
  && ok "home preserved by refused purge" || bad "refused purge removed the home"
docker rm -f "sumi-local-pg-$Y_ID" >/dev/null 2>&1
docker network rm "sumi-local-$Y_ID-default" >/dev/null 2>&1
docker volume rm "sumi-local-pgdata-$Y_ID" >/dev/null 2>&1

# --- review-B F2 / f114: copied home adopts, never takes over ----------
echo "== copied state home joins the running install, no takeover"
z install --managed-pg --listen 127.0.0.1:$P5 >/dev/null
z start >/dev/null
z say "z ping" >/dev/null
Z_SPID="$(cut -d' ' -f1 "$FIX/home-z/run/service.pid")"
Z_CPID="$(cut -d' ' -f1 "$FIX/home-z/run/local.pid")"
cp -a "$FIX/home-z" "$FIX/home-z2"
rm -rf "$FIX/home-z2/run"   # a plain copy/mount lacks live run records
z2() { env HOME="$OSH" SUMI_LOCAL_HOME="$FIX/home-z2" SUMI_LOCAL_PREFIX="$FIX/prefix-z" "$SRC" "$@"; }
out="$(z2 start 2>&1)" && rc=0 || rc=$?
[[ $rc == 0 && $out == *"adopted"* ]] \
  && ok "copied-home start adopted the running install" \
  || bad "copied-home start did not adopt: rc=$rc $(echo "$out" | tail -3)"
Z2_SPID="$(cut -d' ' -f1 "$FIX/home-z2/run/service.pid" 2>/dev/null || true)"
[[ $Z2_SPID == "$Z_SPID" && -d /proc/$Z_SPID ]] \
  && ok "adopted pidfile records the ORIGINAL service pid (no takeover)" \
  || bad "service was replaced: orig=$Z_SPID copy=${Z2_SPID:-none}"
[[ $(pgrep -f "$FIX/prefix-z/bin/sumi-local-service" | wc -l) == 1 \
   && $(pgrep -f "$FIX/prefix-z/core/host/local.ts" | wc -l) == 1 ]] \
  && ok "exactly one service + one core after copied-home start" \
  || bad "duplicate processes after copied-home start"
z2 say "shared secretary" >/dev/null \
  && ok "copied home drives the same running secretary" \
  || bad "copied home could not reach the shared install"
# and the copy can stop the shared install it adopted
z2 stop >/dev/null
! pgrep -f "$FIX/prefix-z/" >/dev/null 2>&1 \
  && ok "copied home stopped the shared install" \
  || bad "copied-home stop left processes"
z uninstall --purge --yes >/dev/null 2>&1 || true
rm -rf "$FIX/home-z2"

# --- review-A F3 / f113: deleted-payload survivors are reported --------
echo "== stop reports survivors invisible to the exact-path sweep"
w install --managed-pg --listen 127.0.0.1:$P6 >/dev/null
w start >/dev/null
cp "$FIX/prefix-w/core/host/local.ts" "$FIX/local.ts.saved"
# F3's orphan: the payload file is gone AND the pidfile is lost — the
# exact-path glob no longer matches, so only the argv-anchored report sees it
rm -f "$FIX/prefix-w/core/host/local.ts" "$FIX/home-w/run/local.pid"
out="$(w stop 2>&1)" && rc=0 || rc=$?
[[ $out == *"processes still running with paths under"* ]] \
  && ok "stop reported the deleted-payload orphan" \
  || bad "stop silent about deleted-payload orphan: $(echo "$out" | tail -3)"
[[ -n $(pgrep -f "$FIX/prefix-w/core/host/local.ts" | head -1) ]] \
  && ok "reported orphan was NOT signaled (reporting != killing)" \
  || bad "deleted-payload orphan was signaled anyway"
# restoring the payload file makes the exact-path sweep work again
cp "$FIX/local.ts.saved" "$FIX/prefix-w/core/host/local.ts"
w stop >/dev/null 2>&1
[[ -z $(pgrep -f "$FIX/prefix-w/" | head -1) ]] \
  && ok "restored payload lets the sweep finish the orphan" \
  || bad "orphan survived a second stop after restore"
w uninstall --purge --yes >/dev/null 2>&1 || true

echo "== real user shim untouched across the whole run"
[[ $(readlink "$REAL_HOME/.local/bin/sumi-local" 2>/dev/null || echo __absent__) == "$REAL_SHIM_BEFORE" ]] \
  && ok "real user shim sentinel unchanged" \
  || bad "real user shim mutated during test!"

echo
echo "ownership test: $pass passed, $fail failed"
((fail == 0))
