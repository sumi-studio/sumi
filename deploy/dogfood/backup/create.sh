#!/usr/bin/env bash
set -Eeuo pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly check_only="${1:-}"

require_value() { [[ -n "${!1:-}" ]] || { printf '%s is required\n' "$1" >&2; exit 2; }; }
require_executable() {
  require_value "$1"
  local value="${!1}"
  [[ "${value}" == /* && -f "${value}" && -x "${value}" && ! -L "${value}" ]] || {
    printf '%s must be an absolute executable regular non-symlink\n' "$1" >&2
    exit 2
  }
}
require_directory() {
  require_value "$1"
  local value="${!1}"
  [[ "${value}" == /* && "${value}" != "/" && -d "${value}" && ! -L "${value}" ]] || {
    printf '%s must be an absolute real non-root directory\n' "$1" >&2
    exit 2
  }
}
require_protected_file() {
  require_value "$1"
  local value="${!1}"
  [[ "${value}" == /* && -f "${value}" && ! -L "${value}" ]] || {
    printf '%s must be an absolute regular non-symlink\n' "$1" >&2
    exit 2
  }
  local mode
  mode="$(stat -c '%a' -- "${value}")"
  (( (8#${mode} & 0077) == 0 )) || {
    printf '%s must not grant group/other permissions\n' "$1" >&2
    exit 2
  }
}

for name in SUMI_DB_URL SUMI_APP_SHA SUMI_API_IMAGE SUMI_PROVISIONER_IMAGE SUMI_POSTGRES_IMAGE; do require_value "${name}"; done
[[ "${SUMI_APP_SHA}" =~ ^([0-9a-f]{40}|[0-9a-f]{64})$ ]] || { printf 'SUMI_APP_SHA is not exact\n' >&2; exit 2; }
for name in SUMI_API_IMAGE SUMI_PROVISIONER_IMAGE SUMI_POSTGRES_IMAGE; do
  [[ "${!name}" =~ ^[a-z0-9./:_-]+@sha256:[0-9a-f]{64}$ ]] || { printf '%s is not an exact image digest\n' "${name}" >&2; exit 2; }
done
for name in SUMI_BACKUP_WORK_ROOT SUMI_DOGFOOD_STATE_ROOT SUMI_ATTACHMENT_ROOT; do require_directory "${name}"; done
[[ "$(readlink -e -- "${SUMI_ATTACHMENT_ROOT}")" == "$(readlink -e -- "${SUMI_DOGFOOD_STATE_ROOT}/attachments")" ]] || {
  printf 'SUMI_ATTACHMENT_ROOT must be the canonical dogfood state attachments directory\n' >&2
  exit 2
}
require_protected_file SUMI_DOCKER_CONFIG_FILE
[[ "${SUMI_DOCKER_CONFIG_FILE}" == */config.json ]] || { printf 'SUMI_DOCKER_CONFIG_FILE must be named config.json\n' >&2; exit 2; }
require_value SUMI_DOGFOOD_OPERATION_LOCK
[[ "${SUMI_DOGFOOD_OPERATION_LOCK}" == /* && -f "${SUMI_DOGFOOD_OPERATION_LOCK}" && ! -L "${SUMI_DOGFOOD_OPERATION_LOCK}" ]] || {
  printf 'SUMI_DOGFOOD_OPERATION_LOCK must be an absolute regular non-symlink\n' >&2
  exit 2
}
operation_lock_mode="$(stat -c '%a' -- "${SUMI_DOGFOOD_OPERATION_LOCK}")"
(( (8#${operation_lock_mode} & 0077) == 0 )) || { printf 'SUMI_DOGFOOD_OPERATION_LOCK must not grant group/other permissions\n' >&2; exit 2; }
for name in SUMI_MIGRATE_BIN SUMI_DATABASE_HELPER SUMI_AGENT_VOLUME_HELPER SUMI_TAR_BIN SUMI_QUIESCE_HELPER SUMI_RESUME_HELPER SUMI_ENCRYPT_HELPER SUMI_HANDOFF_HELPER; do
  require_executable "${name}"
done
if [[ "${check_only}" == "--check" ]]; then
  printf 'configuration valid; no snapshot, encryption, handoff, or restore was performed\n'
  exit 0
fi
[[ -z "${check_only}" ]] || { printf 'usage: create.sh [--check]\n' >&2; exit 2; }

exec {operation_lock_fd}<>"${SUMI_DOGFOOD_OPERATION_LOCK}"
if ! /usr/bin/flock --nonblock "${operation_lock_fd}"; then
  printf 'another dogfood deploy or backup operation holds %s\n' "${SUMI_DOGFOOD_OPERATION_LOCK}" >&2
  exit 1
fi

readonly snapshot_id="${SUMI_SNAPSHOT_ID_OVERRIDE:-$(date -u +%Y%m%dT%H%M%SZ)-${SUMI_APP_SHA:0:12}}"
[[ "${snapshot_id}" =~ ^[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}$ ]] || { printf 'invalid snapshot id\n' >&2; exit 2; }
readonly snapshot_dir="${SUMI_BACKUP_WORK_ROOT}/${snapshot_id}"
[[ ! -e "${snapshot_dir}" ]] || { printf 'snapshot already exists: %s\n' "${snapshot_dir}" >&2; exit 2; }
mkdir -m 0700 -- "${snapshot_dir}"

quiesced=0
resume() {
  if ((quiesced == 1)); then
    "${SUMI_RESUME_HELPER}" "${snapshot_id}"
    quiesced=0
  fi
}
on_exit() {
  local status=$?
  trap - EXIT
  if ! resume; then
    printf 'failed to resume writes after snapshot attempt %s\n' "${snapshot_id}" >&2
    status=1
  fi
  exit "${status}"
}
trap on_exit EXIT
"${SUMI_QUIESCE_HELPER}" "${snapshot_id}"
quiesced=1

SUMI_DB_URL="${SUMI_DB_URL}" "${SUMI_MIGRATE_BIN}" verify
SUMI_DB_URL="${SUMI_DB_URL}" "${SUMI_MIGRATE_BIN}" manifest > "${snapshot_dir}/migration-manifest.json"
"${SUMI_DATABASE_HELPER}" attachment-rows > "${snapshot_dir}/attachment-rows.tsv"
node "${script_dir}/verify-attachments.mjs" \
  "${SUMI_ATTACHMENT_ROOT}" \
  "${snapshot_dir}/attachment-rows.tsv" \
  "${snapshot_dir}/attachments.manifest.json"
"${SUMI_DATABASE_HELPER}" dump > "${snapshot_dir}/database.dump"
node "${script_dir}/host-state-manifest.mjs" create \
  "${SUMI_DOGFOOD_STATE_ROOT}" "${snapshot_dir}/host-state.manifest.json"
"${SUMI_TAR_BIN}" --create --numeric-owner --acls --xattrs --sort=name \
  --file="${snapshot_dir}/host-state.tar" --directory="${SUMI_DOGFOOD_STATE_ROOT}" \
  command-log runtime-state attachments
"${SUMI_AGENT_VOLUME_HELPER}" "${snapshot_dir}"
node "${script_dir}/snapshot-manifest.mjs" create \
  "${snapshot_dir}" "${snapshot_id}" "${SUMI_APP_SHA}" \
  "${SUMI_API_IMAGE}" "${SUMI_PROVISIONER_IMAGE}" "${SUMI_POSTGRES_IMAGE}" >/dev/null

# End the maintenance window before CPU/network-heavy encryption and handoff.
resume
readonly bundle="${snapshot_dir}/${snapshot_id}.bundle.tar"
readonly encrypted="${snapshot_dir}/${snapshot_id}.bundle.encrypted"
readonly handoff_manifest="${snapshot_dir}/${snapshot_id}.handoff.json"
"${SUMI_TAR_BIN}" --create --file="${bundle}" --directory="${snapshot_dir}" \
  database.dump host-state.tar host-state.manifest.json attachment-rows.tsv \
  attachments.manifest.json agent-volumes.tar agent-volume-set.json \
  migration-manifest.json snapshot.json
"${SUMI_ENCRYPT_HELPER}" "${bundle}" "${encrypted}"
node "${script_dir}/handoff-manifest.mjs" create "${encrypted}" "${handoff_manifest}" "${snapshot_dir}/snapshot.json" >/dev/null
"${SUMI_HANDOFF_HELPER}" "${encrypted}" "${handoff_manifest}"
unlink -- "${bundle}"
printf 'snapshot %s encrypted and handed off\n' "${snapshot_id}"
