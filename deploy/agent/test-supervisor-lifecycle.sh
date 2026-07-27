#!/usr/bin/env bash
# Deterministic local protocol harness for the host supervisor loop. It mocks
# kernel cgroup operations; it must not be reported as a Cloud cgroup proof.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TRACE_DIR="$(mktemp -d)"
trap 'rm -rf "${TRACE_DIR}"' EXIT
export SUMI_CONFIG_FILE="${TRACE_DIR}/missing.env"
export SUMI_ISOLATION_MODE=low-trust
export SUMI_ALLOW_LOW_TRUST=1
export SUMI_STATE_DIR="${TRACE_DIR}/state"
export SUMI_AGENT_RUNTIME_STATE_DIR="${TRACE_DIR}/runtime"
export SUMI_WORKSPACE="${TRACE_DIR}/workspace"
export SUMI_ARTIFACT_ROOT="${TRACE_DIR}/artifacts"
export SUMI_EXECUTOR_SOCKET="${TRACE_DIR}/ipc/executor.sock"
export SUMI_ARTIFACT_BROKER_SOCKET="${TRACE_DIR}/ipc/broker.sock"
export SUMI_T27_RECOVERY_REQUEST_DIR="${TRACE_DIR}/requests"
export SUMI_T27_SUPERVISOR_PROOF_DIR="${TRACE_DIR}/proofs"
export SUMI_TENANT_ID=t
export SUMI_AGENT_ID=a
export SUMI_CONVERSATION_ID=c
export SUMI_AGENT_WRAPPING_KEY=test-key
export SUMI_BIN=/bin/true
export SUMI_LIFECYCLE_TRACE="${TRACE_DIR}/trace"

# shellcheck source=supervisor
source "${ROOT}/deploy/agent/supervisor"

check_binary() { :; }
check_isolation() { :; }
prepare_directories() { mkdir -p "${SUMI_T27_RECOVERY_REQUEST_DIR}" "${SUMI_T27_SUPERVISOR_PROOF_DIR}"; }
allocate_identity() {
  SUMI_RPC_GENERATION="$(( ${SUMI_RPC_GENERATION:-0} + 1 ))"
  SUMI_RPC_NONCE="nonce-${SUMI_RPC_GENERATION}"
  SUMI_PROCESS_GENERATION_LEASE_ID="lease-${SUMI_RPC_GENERATION}"
  SUMI_GENERATION_RECOVERY_FENCE_ID="fence-${SUMI_RPC_GENERATION}"
  export SUMI_RPC_GENERATION SUMI_RPC_NONCE SUMI_PROCESS_GENERATION_LEASE_ID SUMI_GENERATION_RECOVERY_FENCE_ID
  printf 'allocate-%s\n' "${SUMI_RPC_GENERATION}" >> "${SUMI_LIFECYCLE_TRACE}"
}
prepare_cgroup_bases() {
  SUMI_EXECUTOR_CGROUP_BASE="${TRACE_DIR}/executor-g${SUMI_RPC_GENERATION}"
  SUMI_SERVICE_CGROUP_BASE="${TRACE_DIR}/service-g${SUMI_RPC_GENERATION}"
  mkdir -p "${SUMI_EXECUTOR_CGROUP_BASE}" "${SUMI_SERVICE_CGROUP_BASE}"
  printf 'prepare-%s\n' "${SUMI_RPC_GENERATION}" >> "${SUMI_LIFECYCLE_TRACE}"
}
recover_stale_generations() { :; }
wait_for_socket() { :; }
fulfill_recovery_requests() {
  printf 'proof-%s\n' "${SUMI_RPC_GENERATION}" >> "${SUMI_LIFECYCLE_TRACE}"
  : > "${TRACE_DIR}/proof-complete"
}
heartbeat_is_fresh() { :; }
wait_for_heartbeat() { :; }
spawn_broker() { sleep 60 & BROKER_PID=$!; }
spawn_executor() { sleep 60 & EXECUTOR_PID=$!; }
spawn_runtime() {
  if [[ "${SUMI_RPC_GENERATION}" == 2 ]]; then
    printf 'restart-2\n' >> "${SUMI_LIFECYCLE_TRACE}"
    [[ "$(tr '\n' ' ' < "${SUMI_LIFECYCLE_TRACE}")" == *'allocate-1 prepare-1'*'proof-1 reap-1 allocate-2 prepare-2 restart-2'* ]]
    exit 0
  fi
  printf 'runtime-1\n' >> "${SUMI_LIFECYCLE_TRACE}"
  (
    while [[ ! -f "${TRACE_DIR}/proof-complete" ]]; do
      sleep 0.01
    done
  ) &
  RUNTIME_PID=$!
}
reap_current_generation() {
  printf 'reap-%s\n' "${SUMI_RPC_GENERATION}" >> "${SUMI_LIFECYCLE_TRACE}"
  for pid in "${RUNTIME_PID:-}" "${EXECUTOR_PID:-}" "${BROKER_PID:-}"; do
    [[ -z "${pid}" ]] || kill "${pid}" 2>/dev/null || true
    [[ -z "${pid}" ]] || wait "${pid}" 2>/dev/null || true
  done
}

main
