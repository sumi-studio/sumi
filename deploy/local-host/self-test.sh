#!/usr/bin/env bash
# self-test.sh — disposable end-to-end fixture for the local-host install.
#
# Exercises the real pieces: the built Go state service against a real
# PostgreSQL database, the real TypeScript secretary host, the mock
# provider, and an OpenAI-compatible stub provider — through install,
# start, chat, scheduled wake, stop/start persistence, SIGKILL mid-turn
# recovery, uninstall/reinstall, pack-install, and purge.
#
# Required env:
#   SUMI_TEST_DB_URL   postgres:// URL of a database this test owns
#                      exclusively (it will be written into; it is NOT
#                      dropped — the caller decides its lifecycle)
# Optional env:
#   SUMI_LOCAL_TEST_PORT    service port (default 9550; stub uses +1)
#   SUMI_LOCAL_TEST_HOME    state home dir    (default mktemp -d)
#   SUMI_LOCAL_TEST_PREFIX  install prefix    (default mktemp -d)
#   SUMI_LOCAL_TEST_WORK    work dir          (default mktemp -d)
#   SUMI_LOCAL_TEST_LOGS    where evidence logs are copied
#                           (default <work>/evidence-logs)
#   SUMI_LOCAL_TEST_KEEP=1  keep dirs, do not purge (for inspection)
#
# Exit 0 = all checks passed. Nothing outside the given dirs/DB/ports is
# touched. No secret values are printed.
set -Eeuo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAUNCH="$HERE/sumi-local"          # repo-layout launcher (builds payloads)

: "${SUMI_TEST_DB_URL:?set SUMI_TEST_DB_URL to a postgres:// URL for a DB this test owns}"

T_PORT=${SUMI_LOCAL_TEST_PORT:-9550}
T_STUB_PORT=$((T_PORT + 1))
T_HOME=${SUMI_LOCAL_TEST_HOME:-$(mktemp -d /tmp/sumi-local-test-home.XXXXXX)}
T_PREFIX=${SUMI_LOCAL_TEST_PREFIX:-$(mktemp -d /tmp/sumi-local-test-prefix.XXXXXX)}
T_WORK=${SUMI_LOCAL_TEST_WORK:-$(mktemp -d /tmp/sumi-local-test-work.XXXXXX)}
# The install links a CLI shim into $HOME/.local/bin — the OS HOME must be
# isolated too, or the test would mutate the real user's shim.
REAL_HOME=$HOME
T_OSHOME=${SUMI_LOCAL_TEST_OSHOME:-$(mktemp -d /tmp/sumi-local-test-oshome.XXXXXX)}
LOGS=${SUMI_LOCAL_TEST_LOGS:-$T_WORK/evidence-logs}
KEEP=${SUMI_LOCAL_TEST_KEEP:-0}

export HOME=$T_OSHOME
# keep the real user's build caches so the isolated HOME does not trigger
# a cold Go rebuild
[[ -d $REAL_HOME/.cache/go-build ]] && export GOCACHE="$REAL_HOME/.cache/go-build"
[[ -d $REAL_HOME/go/pkg/mod ]] && export GOMODCACHE="$REAL_HOME/go/pkg/mod"
REAL_SHIM_BEFORE="$(readlink "$REAL_HOME/.local/bin/sumi-local" 2>/dev/null || echo __absent__)"
export SUMI_LOCAL_HOME=$T_HOME
export SUMI_LOCAL_PREFIX=$T_PREFIX
# Short lease + idle window so crash recovery and schedule polling are quick.
export SUMI_LEASE_TTL_MS=4000
export SUMI_ONCE_IDLE_MS=1000

BIN="$T_PREFIX/bin/sumi-local"
STUB_PID=""
FAILED=0

step() { printf '\n=== %s\n' "$*"; }
ok()   { printf '  ok: %s\n' "$*"; }
fail() { printf '  FAIL: %s\n' "$*" >&2; FAILED=1; }

cleanup() {
  local rc=$?
  mkdir -p "$LOGS" 2>/dev/null || true
  cp -a "$T_HOME/log/." "$LOGS/" 2>/dev/null || true
  [[ -n $STUB_PID ]] && kill "$STUB_PID" 2>/dev/null || true
  "$LAUNCH" stop >/dev/null 2>&1 || true
  if [[ $KEEP != 1 ]]; then
    "$LAUNCH" uninstall --purge --yes >/dev/null 2>&1 || true
    rm -rf "$T_HOME" "$T_PREFIX" "$T_WORK" "$T_OSHOME" 2>/dev/null || true
  fi
  # the real user's shim must be exactly as we found it
  local after
  after="$(readlink "$REAL_HOME/.local/bin/sumi-local" 2>/dev/null || echo __absent__)"
  [[ $after == "$REAL_SHIM_BEFORE" ]] \
    || printf 'FAIL: real user shim changed: %s -> %s\n' "$REAL_SHIM_BEFORE" "$after" >&2
  exit $rc
}
trap cleanup EXIT

# --- helpers ----------------------------------------------------------------

cfg_get() { # key → value from the generated config.env
  sed -n "s/^$1='\\(.*\\)'$/\\1/p" "$T_HOME/config.env" | head -1
}

FMTOK=""
fm_token() {
  if [[ -z $FMTOK ]]; then
    FMTOK="$("$BIN" url | sed 's/.*fm=//')"
  fi
  printf '%s' "$FMTOK"
}
BASE="http://127.0.0.1:$T_PORT"

outbox() { curl -sf "$BASE/fm/$(cfg_get SUMI_PERSONA_ID)/outbox?after_seq=0" -H "Authorization: Bearer $(fm_token)"; }
events() { curl -sf "$BASE/fm/$(cfg_get SUMI_PERSONA_ID)/events?after_seq=0" -H "Authorization: Bearer $(fm_token)"; }

# wait_outbox INPUT_ID NEEDLE SECONDS — until a turn_completed outbox entry for
# INPUT_ID exists whose output text contains NEEDLE.
wait_outbox() {
  local deadline=$((SECONDS + $3))
  while ((SECONDS < deadline)); do
    outbox 2>/dev/null | node -e '
      let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{
        const j=JSON.parse(s), id=process.argv[1], needle=process.argv[2];
        const hit=(j.outbox||[]).find(o=>o.kind==="turn_completed"
          && o.payload?.input_id===id
          && (o.payload?.output?.text??"").includes(needle));
        process.exit(hit?0:1);
      });' "$1" "$2" && return 0
    sleep 1
  done
  return 1
}

outbox_count_for() { # INPUT_ID → number of terminal outbox entries
  outbox | node -e '
    let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{
      const j=JSON.parse(s), id=process.argv[1];
      console.log((j.outbox||[]).filter(o=>o.payload?.input_id===id).length);
    });' "$1"
}

# --- 0. preconditions --------------------------------------------------------
step "preconditions"
"$LAUNCH" doctor >/dev/null 2>&1 && fail "doctor should fail before install" || ok "doctor fails when not installed"

# --- 1. install ---------------------------------------------------------------
step "install (repo layout) → $T_PREFIX"
"$LAUNCH" install --home "$T_HOME" --prefix "$T_PREFIX" \
  --db-url "$SUMI_TEST_DB_URL" --listen "127.0.0.1:$T_PORT" \
  --persona-name "Fixture Secretary" --model-provider mock \
  || fail "install exited nonzero"
[[ -x $BIN ]] || fail "installed CLI missing"
[[ -x $T_PREFIX/bin/sumi-local-service ]] || fail "service binary missing"
[[ -f $T_PREFIX/core/host/local.ts ]] || fail "core payload missing"
[[ -f $T_HOME/config.env ]] || fail "config.env missing"
[[ $(stat -c %a "$T_HOME/config.env") == 600 ]] || fail "config.env must be 0600"
PERSONA="$(cfg_get SUMI_PERSONA_ID)"
[[ $PERSONA =~ ^[0-9a-f-]{36}$ ]] || fail "persona id not a uuid: $PERSONA"
ok "persona $PERSONA"

"$BIN" doctor || fail "doctor fails on a fresh install"
ok "doctor passes"

# --- 2. foreign-listener refusal ----------------------------------------------
step "start refuses a foreign listener on the configured port"
STUB_PORT=$T_PORT node "$HERE/test/stub-model.mjs" >/dev/null 2>&1 &
OCCUPANT=$!
sleep 0.7
if "$BIN" start >/dev/null 2>&1; then
  kill $OCCUPANT 2>/dev/null
  fail "start adopted a foreign listener"
else
  ok "refused to start over foreign listener"
fi
kill $OCCUPANT 2>/dev/null; wait $OCCUPANT 2>/dev/null || true

# --- 3. start + say (mock) -----------------------------------------------------
step "start"
"$BIN" start || fail "start exited nonzero"
ST_RC=0; ST_OUT="$("$BIN" status 2>&1)" || ST_RC=$?
[[ $ST_OUT == *"$PERSONA"* ]] && ok "status shows persona" || fail "status does not show persona"
[[ $ST_RC == 0 ]] || fail "status exit $ST_RC (want 0): $ST_OUT"

step "say (mock provider echoes)"
REPLY="$("$BIN" say --wait 30 hello fixture world 2>&1)" || fail "say exited nonzero"
[[ $REPLY == *"echo: hello fixture world"* ]] || fail "unexpected reply: $REPLY"
ok "reply: $REPLY"

step "scheduled wake"
"$BIN" say --id fixture-sched '!schedule.set {"wake_at":"+2000","payload":{"text":"scheduled-ping"}}' >/dev/null
DEADLINE=$((SECONDS + 45)); FIRED=""
while ((SECONDS < DEADLINE)); do
  if events 2>/dev/null | node -e '
      let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{
        const j=JSON.parse(s);
        process.exit((j.events||[]).some(e=>e.kind==="input_received"
          && e.payload?.actor_kind==="schedule")?0:1);
      });'; then
    FIRED=1; break
  fi
  sleep 1
done
[[ -n $FIRED ]] && ok "schedule input fired (actor_kind=schedule)" \
  || fail "no schedule-actor input fired within 45s"

# --- 4. stop/start persistence --------------------------------------------------
step "stop + start → same identity and history"
"$BIN" stop || fail "stop nonzero"
"$BIN" status >/dev/null 2>&1 && fail "status should be nonzero when stopped" || ok "status reports down"
sleep 1
"$BIN" start || fail "restart failed"
ST_OUT="$("$BIN" status 2>&1 || true)"
[[ $ST_OUT == *"$PERSONA"* ]] || fail "persona changed across restart"
REPLY="$("$BIN" say --wait 30 still there 2>&1)" || fail "say exited nonzero"
[[ $REPLY == *"echo: still there"* ]] || fail "reply after restart: $REPLY"
# history: the earlier replies are still in the outbox
[[ $(outbox_count_for fixture-sched) -ge 1 ]] || fail "history lost: fixture-sched outbox entry gone"
ok "identity and history persisted across stop/start"

# --- 5. SIGKILL mid-turn recovery ------------------------------------------------
step "SIGKILL mid-turn → recovery, exactly-once completion"
"$BIN" say --id fixture-kill '!slow 6000 kill-recovery-check' >/dev/null
sleep 1.5
COREPID="$(awk '{print $1}' "$T_HOME/run/local.pid")"
kill -9 "$COREPID"
ok "killed core pid $COREPID mid-turn"
"$BIN" start || fail "start after kill failed"   # waits out the dead holder's lease
wait_outbox fixture-kill kill-recovery-check 60 \
  || fail "no completion for killed input within 60s"
[[ $(outbox_count_for fixture-kill) == 1 ]] \
  || fail "expected exactly one terminal outbox entry for fixture-kill, got $(outbox_count_for fixture-kill)"
ok "killed turn recovered and completed exactly once"

# --- 6. openai-compatible provider ------------------------------------------------
step "OpenAI-compatible provider (local stub)"
STUB_PORT=$T_STUB_PORT node "$HERE/test/stub-model.mjs" >/dev/null 2>&1 &
STUB_PID=$!
sleep 0.7
"$BIN" stop >/dev/null
SUMI_MODEL_PROVIDER=openai \
SUMI_MODEL_BASE_URL="http://127.0.0.1:$T_STUB_PORT/v1" \
SUMI_MODEL_API_KEY=fixture-key \
SUMI_MODEL_MODEL=stub-9 \
  "$BIN" start || fail "start with openai overrides failed"
REPLY="$("$BIN" say --wait 30 ping over openai 2>&1)" || fail "say exited nonzero"
# context assembly prefixes an [actor] marker to user text; the stub echoes it
[[ $REPLY == *"stub(stub-9):"*"ping over openai"* ]] || fail "openai-stub reply: $REPLY"
ok "provider path works: $REPLY"
"$BIN" stop >/dev/null
kill $STUB_PID 2>/dev/null; STUB_PID=""
"$BIN" start >/dev/null || fail "start back on mock failed"
REPLY="$("$BIN" say --wait 30 back on mock 2>&1)" || fail "say exited nonzero"
[[ $REPLY == *"echo: back on mock"* ]] || fail "mock reply: $REPLY"
ok "provider override did not corrupt config"

# --- 7. uninstall keeps data; reinstall keeps identity ------------------------------
step "uninstall keeps authored data; reinstall keeps identity"
"$BIN" stop >/dev/null
"$BIN" uninstall || fail "uninstall nonzero"
[[ ! -e $T_PREFIX ]] || fail "prefix not removed"
[[ -f $T_HOME/config.env ]] || fail "config.env removed by uninstall"
ok "executables removed, state home kept"

step "pack → install from archive → same identity"
"$LAUNCH" pack "$T_WORK/sumi-local-pack.tar.gz" || fail "pack failed"
mkdir -p "$T_WORK/packroot"
tar -xzf "$T_WORK/sumi-local-pack.tar.gz" -C "$T_WORK/packroot"
PKBIN="$T_WORK/packroot/sumi-local/bin/sumi-local"
"$PKBIN" install --home "$T_HOME" --prefix "$T_PREFIX" \
  --db-url "$SUMI_TEST_DB_URL" --listen "127.0.0.1:$T_PORT" \
  || fail "pack install failed"
[[ $(cfg_get SUMI_PERSONA_ID) == "$PERSONA" ]] || fail "persona changed after pack reinstall"
"$BIN" start || fail "start after pack reinstall failed"
REPLY="$("$BIN" say --wait 30 still me after pack 2>&1)" || fail "say exited nonzero"
[[ $REPLY == *"echo: still me after pack"* ]] || fail "reply: $REPLY"
[[ $(outbox_count_for fixture-kill) == 1 ]] || fail "history lost across pack reinstall"
ok "pack-installed build continues the same secretary"

# --- 8. purge -----------------------------------------------------------------------
step "uninstall --purge removes state"
"$BIN" stop >/dev/null 2>&1 || true
"$BIN" uninstall --purge --yes || fail "purge nonzero"
[[ ! -e $T_HOME ]] || fail "state home still present after purge"
[[ ! -e $T_PREFIX ]] || fail "prefix still present after purge"
ok "purge removed the install"

step "RESULT"
if [[ $FAILED == 0 ]]; then
  echo "ALL CHECKS PASSED"
  echo "evidence logs: $LOGS"
  exit 0
else
  echo "FAILURES — see above; evidence logs: $LOGS" >&2
  exit 1
fi
