#!/usr/bin/env bash
set -Eeuo pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly encrypted="${1:?usage: restore-scratch.sh ENCRYPTED HANDOFF_JSON EMPTY_ATTACHMENT_ROOT}"
readonly handoff_manifest="${2:?usage: restore-scratch.sh ENCRYPTED HANDOFF_JSON EMPTY_ATTACHMENT_ROOT}"
readonly attachment_root="${3:?usage: restore-scratch.sh ENCRYPTED HANDOFF_JSON EMPTY_ATTACHMENT_ROOT}"

require_value() { [[ -n "${!1:-}" ]] || { printf '%s is required\n' "$1" >&2; exit 2; }; }
require_executable() {
  require_value "$1"
  local value="${!1}"
  [[ "${value}" == /* && -f "${value}" && -x "${value}" && ! -L "${value}" ]] || {
    printf '%s must be an absolute executable regular non-symlink\n' "$1" >&2
    exit 2
  }
}
for name in SUMI_RESTORE_DB_URL SUMI_RESTORE_CONFIRM_SCRATCH; do require_value "${name}"; done
for name in SUMI_DECRYPT_HELPER SUMI_PSQL_BIN SUMI_PG_RESTORE_BIN SUMI_MIGRATE_BIN SUMI_TAR_BIN; do require_executable "${name}"; done
for path in "${encrypted}" "${handoff_manifest}"; do
  [[ "${path}" == /* && -f "${path}" && ! -L "${path}" ]] || { printf 'backup input must be an absolute regular non-symlink\n' >&2; exit 2; }
done
[[ "${attachment_root}" == /* && "${attachment_root}" != "/" && -d "${attachment_root}" && ! -L "${attachment_root}" ]] || {
  printf 'attachment restore root must be an absolute real non-root directory\n' >&2
  exit 2
}
if find "${attachment_root}" -mindepth 1 -print -quit | grep -q .; then
  printf 'attachment restore root must be empty\n' >&2
  exit 2
fi

readonly snapshot_id="$(node "${script_dir}/handoff-manifest.mjs" verify "${encrypted}" "${handoff_manifest}")"
[[ "${SUMI_RESTORE_CONFIRM_SCRATCH}" == "${snapshot_id}" ]] || {
  printf 'SUMI_RESTORE_CONFIRM_SCRATCH must equal %s\n' "${snapshot_id}" >&2
  exit 2
}
readonly work_root="${SUMI_RESTORE_WORK_ROOT:-$(dirname "${attachment_root}")}"
[[ "${work_root}" == /* && "${work_root}" != "/" && -d "${work_root}" && ! -L "${work_root}" ]] || exit 2
readonly restore_dir="$(mktemp -d --tmpdir="${work_root}" "sumi-restore-${snapshot_id}.XXXXXX")"
readonly bundle="${restore_dir}/${snapshot_id}.bundle.tar"
readonly unpacked="${restore_dir}/unpacked"
mkdir -m 0700 -- "${unpacked}"
"${SUMI_DECRYPT_HELPER}" "${encrypted}" "${bundle}"
"${SUMI_TAR_BIN}" --list --file="${bundle}" | node "${script_dir}/safe-archive-paths.mjs"
"${SUMI_TAR_BIN}" --extract --file="${bundle}" --directory="${unpacked}" --no-same-owner --no-same-permissions
restored_id="$(node "${script_dir}/snapshot-manifest.mjs" verify "${unpacked}")"
[[ "${restored_id}" == "${snapshot_id}" ]] || { printf 'snapshot identities disagree\n' >&2; exit 1; }
node "${script_dir}/handoff-manifest.mjs" verify \
  "${encrypted}" "${handoff_manifest}" "${unpacked}/snapshot.json" >/dev/null

current_manifest="${restore_dir}/current-migration-manifest.json"
"${SUMI_MIGRATE_BIN}" manifest > "${current_manifest}"
cmp --silent "${current_manifest}" "${unpacked}/migration-manifest.json" || {
  printf 'restore binary migration manifest differs from the snapshot\n' >&2
  exit 1
}
object_count="$("${SUMI_PSQL_BIN}" "${SUMI_RESTORE_DB_URL}" -X -v ON_ERROR_STOP=1 -At \
  -c "SELECT count(*) FROM pg_class WHERE relnamespace='public'::regnamespace AND relkind IN ('r','p','v','m','S')")"
[[ "${object_count}" == "0" ]] || { printf 'scratch database is not empty\n' >&2; exit 2; }
"${SUMI_PG_RESTORE_BIN}" --exit-on-error --single-transaction --dbname="${SUMI_RESTORE_DB_URL}" "${unpacked}/database.dump"
"${SUMI_TAR_BIN}" --list --file="${unpacked}/attachments.tar" | node "${script_dir}/safe-archive-paths.mjs"
"${SUMI_TAR_BIN}" --extract --file="${unpacked}/attachments.tar" --directory="${attachment_root}" --no-same-owner --no-same-permissions
SUMI_DB_URL="${SUMI_RESTORE_DB_URL}" "${SUMI_MIGRATE_BIN}" verify
"${SUMI_PSQL_BIN}" "${SUMI_RESTORE_DB_URL}" -X -v ON_ERROR_STOP=1 -At -F $'\t' \
  -c 'SELECT attachment_id::text, size_bytes FROM message_attachments ORDER BY attachment_id' \
  > "${restore_dir}/restored-attachment-rows.tsv"
node "${script_dir}/verify-attachments.mjs" \
  "${attachment_root}" \
  "${restore_dir}/restored-attachment-rows.tsv" \
  "${restore_dir}/restored-attachments.manifest.json"
cmp --silent "${restore_dir}/restored-attachments.manifest.json" "${unpacked}/attachments.manifest.json" || {
  printf 'restored attachment tree differs from the snapshot\n' >&2
  exit 1
}
printf 'scratch restore %s verified; temporary evidence remains at %s\n' "${snapshot_id}" "${restore_dir}"
