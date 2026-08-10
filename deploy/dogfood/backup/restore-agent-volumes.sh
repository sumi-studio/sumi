#!/usr/bin/env bash
set -Eeuo pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly archive_root="${1:?usage: restore-agent-volumes.sh ARCHIVE_ROOT VOLUME_SET SNAPSHOT_ID}"
readonly volume_set="${2:?usage: restore-agent-volumes.sh ARCHIVE_ROOT VOLUME_SET SNAPSHOT_ID}"
readonly snapshot_id="${3:?usage: restore-agent-volumes.sh ARCHIVE_ROOT VOLUME_SET SNAPSHOT_ID}"
readonly docker_bin="${SUMI_DOCKER_BIN:-/usr/bin/docker}"
readonly docker_context="${SUMI_DOGFOOD_DOCKER_CONTEXT:?set SUMI_DOGFOOD_DOCKER_CONTEXT}"
readonly docker_config_file="${SUMI_DOCKER_CONFIG_FILE:?set SUMI_DOCKER_CONFIG_FILE}"
readonly provisioner_image="${SUMI_PROVISIONER_IMAGE:?set SUMI_PROVISIONER_IMAGE}"
readonly tar_bin="${SUMI_TAR_BIN:?set SUMI_TAR_BIN}"

[[ "${archive_root}" == /* && "${archive_root}" != / && -d "${archive_root}" && ! -L "${archive_root}" ]] || { printf 'agent archive root must be a real absolute non-root directory\n' >&2; exit 2; }
[[ "${volume_set}" == /* && -f "${volume_set}" && ! -L "${volume_set}" ]] || { printf 'agent volume set must be an absolute regular non-symlink\n' >&2; exit 2; }
[[ "${snapshot_id}" =~ ^[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}$ ]] || { printf 'invalid snapshot id\n' >&2; exit 2; }
[[ "${docker_bin}" == /* && -f "${docker_bin}" && -x "${docker_bin}" && ! -L "${docker_bin}" ]] || { printf 'invalid Docker binary\n' >&2; exit 2; }
[[ "${tar_bin}" == /* && -f "${tar_bin}" && -x "${tar_bin}" && ! -L "${tar_bin}" ]] || { printf 'invalid tar binary\n' >&2; exit 2; }
[[ "${docker_context}" =~ ^[A-Za-z0-9_.-]+$ ]] || { printf 'invalid Docker context\n' >&2; exit 2; }
[[ "${docker_config_file}" == */config.json && -f "${docker_config_file}" && ! -L "${docker_config_file}" ]] || { printf 'invalid Docker config file\n' >&2; exit 2; }
[[ "${provisioner_image}" =~ ^[a-z0-9./:_-]+@sha256:[0-9a-f]{64}$ ]] || { printf 'provisioner image is not an exact digest\n' >&2; exit 2; }
node "${script_dir}/agent-volume-set.mjs" verify "${archive_root}" "${volume_set}"

readonly docker_config_dir="${docker_config_file%/config.json}"
readonly docker=("${docker_bin}" --config "${docker_config_dir}" --context "${docker_context}")
context_endpoint="$("${docker[@]}" context inspect "${docker_context}" --format '{{.Endpoints.docker.Host}}')"
[[ "${context_endpoint}" == unix:///* ]] || { printf 'restore Docker context must target a local Unix socket\n' >&2; exit 2; }
readonly restore_plan="$(node "${script_dir}/agent-volume-set.mjs" list "${archive_root}" "${volume_set}")"
readonly restored_map="${archive_root}/restored-agent-volumes.tsv"
[[ ! -e "${restored_map}" ]] || { printf 'agent restore evidence already exists\n' >&2; exit 2; }
touch -- "${restored_map}"
chmod 0600 -- "${restored_map}"

while IFS=$'\t' read -r paid project logical source_volume archive manifest; do
  [[ -n "${paid}" ]] || continue
  paid_compact="${paid//-/}"
  snapshot_compact="${snapshot_id,,}"
  target="sumi-restore-${snapshot_compact}-${paid_compact}-${logical}"
  if "${docker[@]}" volume inspect "${target}" >/dev/null 2>&1; then
    printf 'scratch agent volume already exists: %s\n' "${target}" >&2
    exit 2
  fi
  created="$("${docker[@]}" volume create --driver local \
    --label "sumi.backup.snapshot=${snapshot_id}" \
    --label "sumi.backup.source-volume=${source_volume}" \
    "${target}")"
  [[ "${created}" == "${target}" ]] || { printf 'Docker created an unexpected scratch volume\n' >&2; exit 1; }
  "${tar_bin}" --list --file="${archive_root}/${archive}" |
    node "${script_dir}/safe-archive-paths.mjs"
  "${docker[@]}" run --rm --network none --read-only --cap-drop ALL \
    --security-opt no-new-privileges \
    --mount "type=volume,src=${target},dst=/restore" \
    --mount "type=bind,src=${archive_root}/${archive},dst=/backup/source.tar,readonly" \
    --entrypoint /bin/tar "${provisioner_image}" \
    --extract --preserve-permissions --same-owner --numeric-owner --acls --xattrs \
    --file=/backup/source.tar --directory=/restore
  restored_manifest="${archive_root}/${paid}/${logical}.restored.manifest"
  "${docker[@]}" run --rm --network none --read-only --cap-drop ALL \
    --security-opt no-new-privileges \
    --mount "type=volume,src=${target},dst=/source,readonly" \
    --mount "type=bind,src=${script_dir}/volume-tree-manifest.sh,dst=/usr/local/bin/sumi-volume-manifest,readonly" \
    --entrypoint /bin/bash "${provisioner_image}" \
    /usr/local/bin/sumi-volume-manifest /source > "${restored_manifest}"
  cmp --silent "${restored_manifest}" "${archive_root}/${manifest}" || {
    printf 'restored agent volume differs from snapshot: %s\n' "${source_volume}" >&2
    exit 1
  }
  printf '%s\t%s\t%s\t%s\n' "${paid}" "${logical}" "${source_volume}" "${target}" >> "${restored_map}"
done <<< "${restore_plan}"

printf 'agent volume restore verified; scratch volumes are recorded at %s and intentionally retained\n' "${restored_map}"
