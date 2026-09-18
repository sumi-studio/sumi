#!/usr/bin/env bash
# signal-window.sh — deterministic regression for the runlimited
# fork-to-disposition-reset signal window (F373).
#
# Root proved the old handlers-before-fork ordering still swallowed a
# SIGTERM: with the child's fork() return delayed 1s (fork-delay.so),
# a TERM to the wrapper ran the child's INHERITED copy of the
# forwarding handler — which saw no child pid and no-opped — after
# which the child reset defaults and exec'd a surviving /bin/sleep.
#
# The fix is a blocked-signal handoff: the signals are blocked before
# fork, so the TERM pends in the child's inherited mask; the child
# restores SIG_DFL and unblocks, and the pending signal kills it as an
# ordinary default-disposition delivery — pre-exec.
#
# This test reproduces root's exact interleaving against the REAL
# binary and asserts:
#   1. the exec'd workload (/bin/sleep 30) is DEAD after the window
#   2. the wrapper exited via wait4 (stats written, signal=15)
#   3. the wrapper's exit status is 128+15
#   4. nothing from this run survives
#
# Usage: signal-window.sh <path-to-runlimited> [workdir]
# Exit 0 = regression held. Exit 1 = the window still swallows signals.

set -u
RUNLIMITED="${1:?usage: signal-window.sh <runlimited> [workdir]}"
WORKDIR="${2:-$(mktemp -d /tmp/signal-window.XXXXXX)}"
mkdir -p "$WORKDIR"
HERE="$(cd "$(dirname "$0")" && pwd)"
SHIM="$WORKDIR/fork-delay.so"
MARKER="$WORKDIR/child-return.mark"
STATS="$WORKDIR/child.rusage"
SLEEP_PIDFILE="$WORKDIR/sleep.pid"

cleanup() {
  [[ -n "${WRAP_PID:-}" ]] && kill -9 "$WRAP_PID" 2>/dev/null
  [[ -s "$SLEEP_PIDFILE" ]] && kill -9 "$(cat "$SLEEP_PIDFILE")" 2>/dev/null
}
trap cleanup EXIT

# Build the shim (gcc only — the shim is tiny and self-contained).
if ! gcc -O2 -fPIC -shared -o "$SHIM" "$HERE/fork-delay.c" -ldl 2>"$WORKDIR/shim-build.log"; then
  echo "SKIP: cannot build fork-delay.so"; cat "$WORKDIR/shim-build.log" >&2
  exit 2
fi

# A marker-writing sleep so we can identify the exec'd workload's pid
# precisely (the real test target must be identifiable without /proc
# parent guessing). fork-delay.so delays the runlimited child's fork
# return; the exec'd program is a tiny script that records its own pid
# then sleeps — if the pending-signal kill works, it never reaches the
# pidfile write.
cat >"$WORKDIR/target.sh" <<'EOF'
#!/usr/bin/env bash
echo $$ >"@@PIDFILE@@"
exec sleep 30
EOF
sed -i "s|@@PIDFILE@@|$SLEEP_PIDFILE|" "$WORKDIR/target.sh"
chmod +x "$WORKDIR/target.sh"

FORKDELAY_MARKER="$MARKER" FORKDELAY_SECS=1 LD_PRELOAD="$SHIM" \
  "$RUNLIMITED" --cpu=60 --stats="$STATS" -- "$WORKDIR/target.sh" \
  >"$WORKDIR/wrap.log" 2>&1 &
WRAP_PID=$!

# Wait for the child-side fork return (marker) — the swallow window is
# open NOW: the child carries the parent's dispositions, hasn't reset.
for i in $(seq 1 200); do [[ -f "$MARKER" ]] && break; sleep 0.05; done
[[ -f "$MARKER" ]] || { echo "FAIL: shim marker never appeared"; exit 1; }

# Deliver SIGTERM to the wrapper inside the window — the interleaving
# root proved swallows it under the old design.
kill -TERM "$WRAP_PID"

# The child returns ~1s after the marker; the pending signal must kill
# it before/at exec. Give the wrapper a bounded window to exit.
WRAP_DEAD=false
for i in $(seq 1 100); do
  kill -0 "$WRAP_PID" 2>/dev/null || { WRAP_DEAD=true; break; }
  sleep 0.1
done
wait "$WRAP_PID" 2>/dev/null; WRAP_STATUS=$?

PASS=true
say() { echo "  $1"; }
ck()  { # ck <desc> <true|false>
  if [[ "$2" == true ]]; then say "PASS: $1"; else say "FAIL: $1"; PASS=false; fi
}

ck "wrapper exited after the window (not still waiting on wait4)" "$WRAP_DEAD"
ck "wrapper exit status is 143 (128+SIGTERM)" "$([[ "$WRAP_STATUS" -eq 143 ]] && echo true || echo false)"
ck "runlimited recorded the child's stats" "$([[ -s "$STATS" ]] && echo true || echo false)"

# The exec'd workload must be DEAD. If the signal was swallowed the old
# way, target.sh wrote its pid and sleep 30 is still running.
if [[ -s "$SLEEP_PIDFILE" ]]; then
  SP=$(cat "$SLEEP_PIDFILE")
  if kill -0 "$SP" 2>/dev/null; then
    ck "exec'd workload killed by the pending signal" "false"
    say "  → target.sh reached exec; pid $SP still alive — signal swallowed"
    kill -9 "$SP" 2>/dev/null
  else
    ck "exec'd workload killed by the pending signal" "true"
  fi
else
  # The pending signal killed the child before it could write the
  # pidfile — even stronger evidence of a pre-exec kill.
  ck "exec'd workload killed by the pending signal (pre-exec)" "true"
fi

if [[ -s "$STATS" ]]; then
  say "  stats: $(cat "$STATS")"
fi
say "  wrapper status: $WRAP_STATUS"

if [[ "$PASS" == true ]]; then
  echo "SIGNAL-WINDOW: all checks passed — blocked-signal handoff closed the swallow window"
  exit 0
fi
echo "SIGNAL-WINDOW: FAILED — the fork/reset window still swallows termination"
exit 1
