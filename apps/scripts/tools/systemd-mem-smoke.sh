#!/usr/bin/env bash
# systemd-mem-smoke.sh — ROOT-RUN entry point for the script-job
# resource-bound proof. This wrapper now delegates entirely to
# tools/production-resource-proof.mts, which exercises the ACTUAL
# production path — real Runner.runJob → spawnJob(systemd scope) →
# workerd → drive() wall deadline → terminate() by verified identity —
# instead of a helper-copied spawn/cancel sequence.
#
# Contract proven (see the fixture's own header):
#   a) the job's memory footprint cannot escape the configured bound —
#      memory.max=cap AND memory.swap.max=0 verified LIVE in the scope's
#      own cgroup, with live current/peak/swap/events sampled;
#   b) a resource-throttled job cannot run forever — kernel oom_kill OR
#      the supervisor's wall_timeout termination (both accepted, and
#      distinguished in evidence);
#   c) a sibling job completes independently with exact success;
#   d) usage is measured (wait4) and labeled memory_enforcement=cgroup.
#
# A 'done' complete for the hog, a still-alive workload past its
# deadline, or a client timeout standing in for termination all FAIL —
# this helper no longer manufactures or requires an oom_kill counter.
#
# Required env:
#   WORKERD_BIN      path to the workerd binary
#   RUNLIMITED_BIN   path to the compiled runlimited wrapper
#   DISPATCHER_JS    path to apps/scripts/src/workerd/dispatcher.js
#   WORKDIR          writable dir for journal/sockets/evidence
#
# Optional: MEMORY_MIB (256) MEMHOG_MIB (512) WALL_MS_A (30000)
#           SIBLING_MS (15000) SIBLING_MEM_MIB (128) NODE_BIN (node)
#
# Exit codes are the fixture's: 0 contract held / 1 check failed /
# 2 env missing or no systemd scope mechanism. Evidence:
# <WORKDIR>/evidence.json + cgroup-samples.jsonl — never deleted.

set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
NODE_BIN="${NODE_BIN:-node}"

for v in WORKERD_BIN RUNLIMITED_BIN DISPATCHER_JS WORKDIR; do
  if [[ -z "${!v:-}" ]]; then
    echo "exit2: missing env $v" >&2
    exit 2
  fi
done

exec "$NODE_BIN" "$HERE/production-resource-proof.mts"
