#!/usr/bin/env bash
set -Eeuo pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly compose_file="${script_dir}/../compose.yaml"
readonly snapshot_id="${1:?usage: quiesce-api.sh SNAPSHOT_ID}"
readonly env_file="${SUMI_DOGFOOD_OPERATOR_ENV_FILE:?set SUMI_DOGFOOD_OPERATOR_ENV_FILE}"
readonly docker_bin="${SUMI_DOCKER_BIN:-/usr/bin/docker}"
readonly docker_context="${SUMI_DOGFOOD_DOCKER_CONTEXT:?set SUMI_DOGFOOD_DOCKER_CONTEXT}"
readonly docker_config_file="${SUMI_DOCKER_CONFIG_FILE:?set SUMI_DOCKER_CONFIG_FILE}"

[[ "${snapshot_id}" =~ ^[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}$ ]] || { printf 'invalid snapshot id\n' >&2; exit 2; }
[[ "${env_file}" == /* && -f "${env_file}" && ! -L "${env_file}" ]] || { printf 'operator env must be an absolute regular non-symlink\n' >&2; exit 2; }
env_mode="$(stat -c '%a' -- "${env_file}")"
(( (8#${env_mode} & 0077) == 0 )) || { printf 'operator env must not grant group/other permissions\n' >&2; exit 2; }
[[ "${docker_bin}" == /* && -f "${docker_bin}" && -x "${docker_bin}" && ! -L "${docker_bin}" ]] || { printf 'SUMI_DOCKER_BIN must be an absolute executable regular non-symlink\n' >&2; exit 2; }
[[ "${docker_context}" =~ ^[A-Za-z0-9_.-]+$ ]] || { printf 'invalid Docker context name\n' >&2; exit 2; }
[[ "${docker_config_file}" == */config.json && -f "${docker_config_file}" && ! -L "${docker_config_file}" ]] || { printf 'SUMI_DOCKER_CONFIG_FILE must be an absolute regular config.json\n' >&2; exit 2; }
[[ "${SUMI_BACKUP_WORK_ROOT:?set SUMI_BACKUP_WORK_ROOT}" == /* && -d "${SUMI_BACKUP_WORK_ROOT}" && ! -L "${SUMI_BACKUP_WORK_ROOT}" ]] || exit 2
readonly docker_config_dir="${docker_config_file%/config.json}"
readonly docker=("${docker_bin}" --config "${docker_config_dir}" --context "${docker_context}")
readonly compose=("${docker[@]}" compose --env-file "${env_file}" -f "${compose_file}")
context_endpoint="$("${docker[@]}" context inspect "${docker_context}" --format '{{.Endpoints.docker.Host}}')"
[[ "${context_endpoint}" == unix:///* ]] || { printf 'backup Docker context must target a local Unix socket\n' >&2; exit 2; }
readonly maintenance_root="${SUMI_BACKUP_WORK_ROOT}/.maintenance"
readonly marker="${maintenance_root}/${snapshot_id}"
[[ ! -L "${maintenance_root}" && ( ! -e "${maintenance_root}" || -d "${maintenance_root}" ) ]] || {
  printf 'maintenance root must be absent or a real directory\n' >&2
  exit 2
}
install -d -m 0700 -- "${maintenance_root}"
[[ -d "${maintenance_root}" && ! -L "${maintenance_root}" ]] || { printf 'maintenance root must be a real directory\n' >&2; exit 2; }
[[ ! -e "${marker}" ]] || { printf 'maintenance marker already exists: %s\n' "${marker}" >&2; exit 2; }

mapfile -t running < <("${compose[@]}" ps --status running -q api)
((${#running[@]} == 1)) || { printf 'quiesce requires exactly one running API container\n' >&2; exit 2; }
mkdir -m 0700 -- "${marker}"

rollback() {
  local status=$?
  trap - ERR
  printf 'quiesce failed; attempting to return the API to service\n' >&2
  if "${compose[@]}" up -d --no-deps api; then
    for _ in {1..60}; do
      if "${compose[@]}" exec -T api /busybox/wget -q -O /dev/null http://127.0.0.1:8080/ready; then
        rmdir -- "${marker}" || true
        break
      fi
      sleep 2
    done
  fi
  exit "${status}"
}
trap rollback ERR
"${compose[@]}" stop api
mapfile -t still_running < <("${compose[@]}" ps --status running -q api)
((${#still_running[@]} == 0)) || { printf 'API remained running after stop\n' >&2; false; }
trap - ERR
printf 'API quiesced for snapshot %s; marker %s must remain until resume\n' "${snapshot_id}" "${marker}"
