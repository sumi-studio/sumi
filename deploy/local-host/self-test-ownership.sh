#!/usr/bin/env bash
# Ownership/lifecycle-safety test for `sumi-local` (fresh-review F1/F2/F9/F3).
# Real fixtures: real Go service, real node core, real docker postgres per
# install. No mocks beyond the mock model provider.
#
# Covers:
#   - two managed installs run simultaneously: distinct container, volume,
#     compose project, and docker-assigned PG port
#   - install B's `uninstall --purge` cannot delete install A's volume/data
#     (the reviewer's F1 kill scenario)
#   - same-install identity/history survives uninstall + reinstall (managed)
#   - orphan-volume refusal: config deleted, volume left -> fresh install
#     refuses to adopt
#   - F2: a pidfile recording a foreign live process (right pid+starttime,
#     wrong path) or a stale starttime is never signaled
#   - F9: uninstall refuses an unowned --prefix and refuses to purge an
#     unowned --home
#   - F3: running `install` from the installed prefix binary (self-copy) is
#     not an error
#
# Usage:
#   deploy/local-host/self-test-ownership.sh [workdir]
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

FIX="$OUT/fix-ownership"
rm -rf "$FIX"; mkdir -p "$FIX"
A_HOME="$FIX/home-a"; A_PREFIX="$FIX/prefix-a"
B_HOME="$FIX/home-b"; B_PREFIX="$FIX/prefix-b"

a() { env SUMI_LOCAL_HOME="$A_HOME" SUMI_LOCAL_PREFIX="$A_PREFIX" "$SRC" "$@"; }
b() { env SUMI_LOCAL_HOME="$B_HOME" SUMI_LOCAL_PREFIX="$B_PREFIX" "$SRC" "$@"; }

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

persona_of() { grep '^SUMI_PERSONA_ID=' "$1/config.env" | cut -d"'" -f2; }
a_inputs() { docker exec "$A_CTR" psql -U sumi -d sumi -tAc 'select count(*) from core_inputs' 2>/dev/null | tr -d '[:space:]'; }

cleanup() {
  a stop >/dev/null 2>&1 || true;  b stop >/dev/null 2>&1 || true
  a uninstall --purge --yes >/dev/null 2>&1 || true
  b uninstall --purge --yes >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "== install A + B (managed, simultaneous)"
a install --managed-pg --listen 127.0.0.1:9550 >/dev/null
b install --managed-pg --listen 127.0.0.1:9551 >/dev/null
A_ID="$(grep '^SUMI_LOCAL_ID=' "$A_HOME/config.env" | cut -d"'" -f2)"
B_ID="$(grep '^SUMI_LOCAL_ID=' "$B_HOME/config.env" | cut -d"'" -f2)"
[[ $A_ID != "$B_ID" ]] \
  && ok "distinct install ids (A=$A_ID B=$B_ID)" \
  || bad "install ids collide: $A_ID"

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

echo "== F2: pidfile pointing at ANOTHER INSTALL's process is never signaled"
# B's service.pid now records A's live service (pid + its real /proc start
# time). `b stop` must refuse to signal it: the cmdline is anchored on B's
# prefix, and A's binary lives under prefix-a.
cp "$B_HOME/run/service.pid" "$FIX/b-service.pid.saved"
cp "$A_HOME/run/service.pid" "$B_HOME/run/service.pid"
out="$(b stop 2>&1)"
[[ $out == *"leaving it alone"* ]] \
  && ok "cross-install pidfile detected, not signaled" \
  || bad "cross-install warning missing: $(echo "$out" | tail -3)"
a say "alpha survives" >/dev/null 2>&1 \
  && ok "A's service still healthy after B's stop" \
  || bad "A's service was signaled by B's stop!"
cp "$FIX/b-service.pid.saved" "$B_HOME/run/service.pid"
b start >/dev/null   # B's real service was never stopped; core restarts
b say "beta again" >/dev/null && ok "B ordinary stop/start still works" \
  || bad "B broken after pid-restore"

echo "== F2: foreign/stale pidfile is never signaled"
sleep 600 & SLEEP_PID=$!
SLEEP_ST="$(awk '{print $22}' "/proc/$SLEEP_PID/stat")"
# right pid + right starttime, but cmdline has no install path -> warn, never signal
printf '%s %s\n' "$SLEEP_PID" "$SLEEP_ST" > "$B_HOME/run/foreign.pid"
out="$(b uninstall 2>&1)"
[[ $out == *"leaving it alone"* ]] && ok "foreign live pid in pidfile not signaled" \
  || bad "foreign-pid warning missing: $(echo "$out" | tail -3)"
kill -0 "$SLEEP_PID" 2>/dev/null && ok "foreign process still alive" \
  || bad "foreign process was signaled!"
# right pid but wrong starttime (pid-reuse case) -> same
printf '%s %s\n' "$SLEEP_PID" "$((SLEEP_ST + 1))" > "$B_HOME/run/foreign.pid"
out="$(b uninstall 2>&1)"
[[ $out == *"leaving it alone"* ]] && ok "stale-starttime pidfile not signaled" \
  || bad "stale-pid warning missing: $(echo "$out" | tail -3)"
kill -0 "$SLEEP_PID" 2>/dev/null && ok "reused-pid guard held" \
  || bad "reused-pid guard failed"
kill "$SLEEP_PID" 2>/dev/null || true

echo "== F9: unowned prefix / home refused"
INN_PRE="$FIX/innocent-prefix"; mkdir -p "$INN_PRE"; echo keep > "$INN_PRE/userfile"
expect_die "uninstall against unowned prefix" "refusing to remove" \
  env SUMI_LOCAL_HOME="$B_HOME" SUMI_LOCAL_PREFIX="$INN_PRE" "$SRC" uninstall --yes
[[ -f $INN_PRE/userfile ]] && ok "unowned prefix untouched" || bad "unowned prefix modified!"
INN_HOME="$FIX/innocent-home"; mkdir -p "$INN_HOME"; echo keep > "$INN_HOME/userfile"
expect_die "purge against unowned home" "does not look like" \
  env SUMI_LOCAL_HOME="$INN_HOME" SUMI_LOCAL_PREFIX="$B_PREFIX" "$SRC" uninstall --purge --yes
[[ -f $INN_HOME/userfile ]] && ok "unowned home untouched" || bad "unowned home modified!"

echo "== F3: install via the installed prefix binary (self-copy)"
out="$(a_prefix="$A_PREFIX/bin/sumi-local"; env SUMI_LOCAL_HOME="$A_HOME" SUMI_LOCAL_PREFIX="$A_PREFIX" "$a_prefix" install 2>&1)" \
  && ok "install from installed prefix succeeded" || bad "self-copy install failed: $(echo "$out"|tail -2)"

echo "== F1: B purge cannot harm A"
b uninstall --purge --yes >/dev/null
! docker volume inspect "$B_VOL" >/dev/null 2>&1 \
  && ok "B's volume removed by B's purge" || bad "B's volume survived B's purge"
docker volume inspect "$A_VOL" >/dev/null 2>&1 \
  && ok "A's volume survived B's purge" || bad "A's volume was deleted by B's purge!"
a say "alpha still there" >/dev/null && ok "A still serving after B purge" \
  || bad "A broken after B purge"
[[ $(persona_of "$A_HOME") == "$A_PERSONA" ]] \
  && ok "A persona unchanged" || bad "A persona changed"

echo "== same-install reinstall keeps identity + history (managed)"
a say "remember alpha" >/dev/null
inputs_before="$(a_inputs)"
a uninstall --yes >/dev/null          # executables gone; home + volume kept
[[ ! -d $A_PREFIX ]] && docker volume inspect "$A_VOL" >/dev/null 2>&1 \
  && ok "A uninstall kept data volume, removed prefix" \
  || bad "A uninstall state wrong"
a install >/dev/null            # config.env kept -> same identity
[[ $(persona_of "$A_HOME") == "$A_PERSONA" ]] \
  && ok "reinstall kept persona $A_PERSONA" || bad "reinstall changed persona"
a start >/dev/null
a say "again" >/dev/null
inputs_after="$(a_inputs)"
[[ $inputs_after =~ ^[0-9]+$ && $inputs_after -gt ${inputs_before:-0} ]] \
  && ok "history in A's volume survived uninstall+reinstall (inputs $inputs_before -> $inputs_after)" \
  || bad "history lost (inputs $inputs_before -> $inputs_after)"

echo "== orphan refusal: lost config + leftover volume"
C_HOME="$FIX/home-c"; C_PREFIX="$FIX/prefix-c"
c() { env SUMI_LOCAL_HOME="$C_HOME" SUMI_LOCAL_PREFIX="$C_PREFIX" "$SRC" "$@"; }
c install --managed-pg --listen 127.0.0.1:9552 >/dev/null
C_ID="$(grep '^SUMI_LOCAL_ID=' "$C_HOME/config.env" | cut -d"'" -f2)"
c start >/dev/null; c stop >/dev/null
rm -f "$C_HOME/config.env"            # config lost, volume + home remain
expect_die "fresh install refuses orphan volume" "Refusing to adopt" \
  env SUMI_LOCAL_HOME="$C_HOME" SUMI_LOCAL_PREFIX="$C_PREFIX" "$SRC" install --managed-pg --listen 127.0.0.1:9552
# the documented recovery: the user removes the orphaned resources
docker rm -f "sumi-local-pg-$C_ID" >/dev/null 2>&1 || true
docker volume rm "sumi-local-pgdata-$C_ID" >/dev/null 2>&1 || true
c install --managed-pg --listen 127.0.0.1:9552 >/dev/null \
  && ok "after explicit resource removal, fresh install proceeds" \
  || bad "fresh install still blocked after resource removal"
c stop >/dev/null 2>&1 || true
env SUMI_LOCAL_HOME="$C_HOME" SUMI_LOCAL_PREFIX="$C_PREFIX" "$SRC" uninstall --purge --yes >/dev/null 2>&1 || true

echo
echo "ownership test: $pass passed, $fail failed"
((fail == 0))
