#!/usr/bin/env bash
set -Eeuo pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly encrypted="${1:?usage: restore-scratch.sh ENCRYPTED HANDOFF_JSON EMPTY_STATE_ROOT}"
readonly handoff_manifest="${2:?usage: restore-scratch.sh ENCRYPTED HANDOFF_JSON EMPTY_STATE_ROOT}"
readonly state_root="${3:?usage: restore-scratch.sh ENCRYPTED HANDOFF_JSON EMPTY_STATE_ROOT}"

require_value() { [[ -n "${!1:-}" ]] || { printf '%s is required\n' "$1" >&2; exit 2; }; }
require_executable() {
  require_value "$1"
  local value="${!1}"
  [[ "${value}" == /* && -f "${value}" && -x "${value}" && ! -L "${value}" ]] || { printf '%s must be an absolute executable regular non-symlink\n' "$1" >&2; exit 2; }
}
for name in SUMI_RESTORE_DB_URL SUMI_RESTORE_CONFIRM_SCRATCH; do require_value "${name}"; done
for name in SUMI_DECRYPT_HELPER SUMI_DATABASE_HELPER SUMI_AGENT_RESTORE_HELPER SUMI_MIGRATE_BIN SUMI_TAR_BIN; do require_executable "${name}"; done
for path in "${encrypted}" "${handoff_manifest}"; do
  [[ "${path}" == /* && -f "${path}" && ! -L "${path}" ]] || { printf 'backup input must be an absolute regular non-symlink\n' >&2; exit 2; }
done
[[ "${state_root}" == /* && "${state_root}" != / && -d "${state_root}" && ! -L "${state_root}" ]] || { printf 'state restore root must be an absolute real non-root directory\n' >&2; exit 2; }
state_entry="$(find "${state_root}" -mindepth 1 -print -quit)"
if [[ -n "${state_entry}" ]]; then
  printf 'state restore root must be empty\n' >&2
  exit 2
fi
if (( EUID != 0 )) && [[ "${SUMI_RESTORE_ALLOW_NONROOT_FOR_TESTS:-}" != 1 ]]; then
  printf 'scratch restore must run as root to reproduce numeric ownership and modes\n' >&2
  exit 2
fi

readonly snapshot_id="$(node "${script_dir}/handoff-manifest.mjs" verify "${encrypted}" "${handoff_manifest}")"
[[ "${SUMI_RESTORE_CONFIRM_SCRATCH}" == "${snapshot_id}" ]] || { printf 'SUMI_RESTORE_CONFIRM_SCRATCH must equal %s\n' "${snapshot_id}" >&2; exit 2; }
readonly work_root="${SUMI_RESTORE_WORK_ROOT:-$(dirname "${state_root}")}"
[[ "${work_root}" == /* && "${work_root}" != / && -d "${work_root}" && ! -L "${work_root}" ]] || exit 2
readonly restore_dir="$(mktemp -d --tmpdir="${work_root}" "sumi-restore-${snapshot_id}.XXXXXX")"
readonly bundle="${restore_dir}/${snapshot_id}.bundle.tar"
readonly unpacked="${restore_dir}/unpacked"
mkdir -m 0700 -- "${unpacked}"
"${SUMI_DECRYPT_HELPER}" "${encrypted}" "${bundle}"
"${SUMI_TAR_BIN}" --list --file="${bundle}" | node "${script_dir}/safe-archive-paths.mjs"
"${SUMI_TAR_BIN}" --extract --file="${bundle}" --directory="${unpacked}" --no-same-owner --no-same-permissions
restored_id="$(node "${script_dir}/snapshot-manifest.mjs" verify "${unpacked}")"
[[ "${restored_id}" == "${snapshot_id}" ]] || { printf 'snapshot identities disagree\n' >&2; exit 1; }
node "${script_dir}/handoff-manifest.mjs" verify "${encrypted}" "${handoff_manifest}" "${unpacked}/snapshot.json" >/dev/null

recorded_image_output="$(node -e '
  const manifest = JSON.parse(require("node:fs").readFileSync(process.argv[1], "utf8"));
  for (const key of ["api_image", "provisioner_image", "postgres_image"]) console.log(manifest[key]);
' "${unpacked}/snapshot.json")"
readarray -t recorded_images <<< "${recorded_image_output}"
((${#recorded_images[@]} == 3)) || { printf 'snapshot image set is incomplete\n' >&2; exit 1; }
export SUMI_API_IMAGE="${recorded_images[0]}"
export SUMI_PROVISIONER_IMAGE="${recorded_images[1]}"
export SUMI_POSTGRES_IMAGE="${recorded_images[2]}"
export SUMI_RESTORE_DB_URL

current_manifest="${restore_dir}/current-migration-manifest.json"
"${SUMI_MIGRATE_BIN}" manifest > "${current_manifest}"
cmp --silent "${current_manifest}" "${unpacked}/migration-manifest.json" || { printf 'restore binary migration manifest differs from the snapshot\n' >&2; exit 1; }
object_count="$("${SUMI_DATABASE_HELPER}" scratch-object-count)"
[[ "${object_count}" == 0 ]] || { printf 'scratch database is not empty\n' >&2; exit 2; }
"${SUMI_DATABASE_HELPER}" scratch-restore < "${unpacked}/database.dump"

tar_restore=(--extract --preserve-permissions --same-owner --numeric-owner --acls --xattrs --file="${unpacked}/host-state.tar" --directory="${state_root}")
if [[ "${SUMI_RESTORE_ALLOW_NONROOT_FOR_TESTS:-}" == 1 ]]; then
  tar_restore=(--extract --no-same-owner --preserve-permissions --acls --xattrs --file="${unpacked}/host-state.tar" --directory="${state_root}")
fi
"${SUMI_TAR_BIN}" --list --file="${unpacked}/host-state.tar" |
  node "${script_dir}/safe-archive-paths.mjs"
"${SUMI_TAR_BIN}" "${tar_restore[@]}"
node "${script_dir}/host-state-manifest.mjs" verify "${state_root}" "${unpacked}/host-state.manifest.json"

SUMI_DB_URL="${SUMI_RESTORE_DB_URL}" "${SUMI_MIGRATE_BIN}" verify
"${SUMI_DATABASE_HELPER}" scratch-attachment-rows > "${restore_dir}/restored-attachment-rows.tsv"
node "${script_dir}/verify-attachments.mjs" "${state_root}/attachments" "${restore_dir}/restored-attachment-rows.tsv" "${restore_dir}/restored-attachments.manifest.json"
cmp --silent "${restore_dir}/restored-attachments.manifest.json" "${unpacked}/attachments.manifest.json" || { printf 'restored attachment tree differs from the snapshot\n' >&2; exit 1; }

mkdir -m 0700 -- "${unpacked}/agent-volumes"
"${SUMI_TAR_BIN}" --list --file="${unpacked}/agent-volumes.tar" |
  node "${script_dir}/safe-archive-paths.mjs"
"${SUMI_TAR_BIN}" --extract --file="${unpacked}/agent-volumes.tar" --directory="${unpacked}/agent-volumes" --no-same-owner --no-same-permissions
"${SUMI_AGENT_RESTORE_HELPER}" "${unpacked}/agent-volumes" "${unpacked}/agent-volume-set.json" "${snapshot_id}"
printf 'scratch restore %s verified; temporary evidence and scratch agent volumes remain at %s\n' "${snapshot_id}" "${restore_dir}"
