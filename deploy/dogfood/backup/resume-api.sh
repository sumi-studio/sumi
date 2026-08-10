#!/usr/bin/env bash
set -Eeuo pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly compose_file="${script_dir}/../compose.yaml"
readonly snapshot_id="${1:?usage: resume-api.sh SNAPSHOT_ID}"
readonly env_file="${SUMI_DOGFOOD_OPERATOR_ENV_FILE:?set SUMI_DOGFOOD_OPERATOR_ENV_FILE}"
readonly docker_bin="${SUMI_DOCKER_BIN:-/usr/bin/docker}"
readonly docker_context="${SUMI_DOGFOOD_DOCKER_CONTEXT:?set SUMI_DOGFOOD_DOCKER_CONTEXT}"
readonly docker_config_file="${SUMI_DOCKER_CONFIG_FILE:?set SUMI_DOCKER_CONFIG_FILE}"
readonly marker="${SUMI_BACKUP_WORK_ROOT:?set SUMI_BACKUP_WORK_ROOT}/.maintenance/${snapshot_id}"
readonly state_file="${marker}/phase"
readonly containers_file="${marker}/agent-containers.tsv"

[[ "${snapshot_id}" =~ ^[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}$ ]] || { printf 'invalid snapshot id\n' >&2; exit 2; }
[[ "${env_file}" == /* && -f "${env_file}" && ! -L "${env_file}" ]] || { printf 'operator env must be an absolute regular non-symlink\n' >&2; exit 2; }
env_mode="$(stat -c '%a' -- "${env_file}")"
(( (8#${env_mode} & 0077) == 0 )) || { printf 'operator env must not grant group/other permissions\n' >&2; exit 2; }
[[ "${docker_bin}" == /* && -f "${docker_bin}" && -x "${docker_bin}" && ! -L "${docker_bin}" ]] || { printf 'invalid Docker binary\n' >&2; exit 2; }
[[ "${docker_context}" =~ ^[A-Za-z0-9_.-]+$ ]] || { printf 'invalid Docker context name\n' >&2; exit 2; }
[[ "${docker_config_file}" == */config.json && -f "${docker_config_file}" && ! -L "${docker_config_file}" ]] || { printf 'invalid Docker config file\n' >&2; exit 2; }
[[ -d "${marker}" && ! -L "${marker}" && -f "${state_file}" && ! -L "${state_file}" && -f "${containers_file}" && ! -L "${containers_file}" ]] || { printf 'maintenance marker is incomplete: %s\n' "${marker}" >&2; exit 2; }
readonly docker_config_dir="${docker_config_file%/config.json}"
readonly docker=("${docker_bin}" --config "${docker_config_dir}" --context "${docker_context}")
readonly compose=("${docker[@]}" compose --env-file "${env_file}" -f "${compose_file}")
context_endpoint="$("${docker[@]}" context inspect "${docker_context}" --format '{{.Endpoints.docker.Host}}')"
[[ "${context_endpoint}" == unix:///* ]] || { printf 'backup Docker context must target a local Unix socket\n' >&2; exit 2; }

phase="$(<"${state_file}")"
[[ "${phase}" =~ ^(created|ingress-stopped|control-stopped|agents-recorded|agents-stopped|quiesced)$ ]] || { printf 'maintenance marker has an invalid phase\n' >&2; exit 2; }
previous=
while IFS=$'\t' read -r paid project service container_id; do
  [[ -n "${paid}" ]] || continue
  [[ "${paid}" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$ ]] || { printf 'maintenance marker has an invalid PAID\n' >&2; exit 2; }
  [[ "${project}" == "sumi-${paid//-/}" && "${service}" =~ ^(runtime|executor|broker)$ && "${container_id}" =~ ^[0-9a-f]{64}$ ]] || { printf 'maintenance marker has an invalid container binding\n' >&2; exit 2; }
  key="${paid}:${service}"
  [[ -z "${previous}" || "${key}" > "${previous}" ]] || { printf 'maintenance container set is not unique and sorted\n' >&2; exit 2; }
  previous="${key}"
  inspected="$("${docker[@]}" inspect --format '{{index .Config.Labels "com.docker.compose.project"}}\t{{index .Config.Labels "com.docker.compose.service"}}' "${container_id}")"
  [[ "${inspected}" == "${project}"$'\t'"${service}" ]] || { printf 'recorded agent container was replaced during maintenance\n' >&2; exit 1; }
done < "${containers_file}"

"${compose[@]}" up -d --no-deps runtime-provisioner
"${compose[@]}" up -d --no-deps api
for _ in {1..60}; do
  if "${compose[@]}" exec -T api /busybox/wget -q -O /dev/null http://127.0.0.1:8080/ready; then
    ready=1
    break
  fi
  sleep 2
done
[[ "${ready:-0}" == 1 ]] || { printf 'API did not become dependency-ready; maintenance marker remains\n' >&2; exit 1; }

for service in executor broker runtime; do
  while IFS=$'\t' read -r _ _ recorded_service container_id; do
    [[ "${recorded_service}" == "${service}" ]] || continue
    "${docker[@]}" start "${container_id}" >/dev/null
  done < "${containers_file}"
done
while IFS=$'\t' read -r _ _ _ container_id; do
  [[ -n "${container_id}" ]] || continue
  status="$("${docker[@]}" inspect --format '{{.State.Status}}' "${container_id}")"
  [[ "${status}" == running ]] || { printf 'recorded agent writer did not resume: %s\n' "${container_id}" >&2; exit 1; }
done < "${containers_file}"

"${compose[@]}" up -d --no-deps cloudflared

unlink -- "${state_file}"
unlink -- "${containers_file}"
rmdir -- "${marker}"
sync -f "$(dirname "${marker}")"
printf 'ingress, API, lifecycle daemon, and exact pre-snapshot agent writer set resumed for %s\n' "${snapshot_id}"
