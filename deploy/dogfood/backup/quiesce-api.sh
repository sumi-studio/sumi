#!/usr/bin/env bash
set -Eeuo pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly compose_file="${script_dir}/../compose.yaml"
readonly snapshot_id="${1:?usage: quiesce-api.sh SNAPSHOT_ID}"
readonly env_file="${SUMI_DOGFOOD_OPERATOR_ENV_FILE:?set SUMI_DOGFOOD_OPERATOR_ENV_FILE}"
readonly docker_bin="${SUMI_DOCKER_BIN:-/usr/bin/docker}"
readonly docker_context="${SUMI_DOGFOOD_DOCKER_CONTEXT:?set SUMI_DOGFOOD_DOCKER_CONTEXT}"
readonly docker_config_file="${SUMI_DOCKER_CONFIG_FILE:?set SUMI_DOCKER_CONFIG_FILE}"
readonly database_helper="${SUMI_DATABASE_HELPER:?set SUMI_DATABASE_HELPER}"
readonly resume_helper="${SUMI_RESUME_HELPER:-${script_dir}/resume-api.sh}"

[[ "${snapshot_id}" =~ ^[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}$ ]] || { printf 'invalid snapshot id\n' >&2; exit 2; }
[[ "${env_file}" == /* && -f "${env_file}" && ! -L "${env_file}" ]] || { printf 'operator env must be an absolute regular non-symlink\n' >&2; exit 2; }
env_mode="$(stat -c '%a' -- "${env_file}")"
(( (8#${env_mode} & 0077) == 0 )) || { printf 'operator env must not grant group/other permissions\n' >&2; exit 2; }
for helper in "${docker_bin}" "${database_helper}" "${resume_helper}"; do
  [[ "${helper}" == /* && -f "${helper}" && -x "${helper}" && ! -L "${helper}" ]] || { printf 'maintenance helper must be an absolute executable regular non-symlink\n' >&2; exit 2; }
done
[[ "${docker_context}" =~ ^[A-Za-z0-9_.-]+$ ]] || { printf 'invalid Docker context name\n' >&2; exit 2; }
[[ "${docker_config_file}" == */config.json && -f "${docker_config_file}" && ! -L "${docker_config_file}" ]] || { printf 'invalid Docker config file\n' >&2; exit 2; }
[[ "${SUMI_BACKUP_WORK_ROOT:?set SUMI_BACKUP_WORK_ROOT}" == /* && "${SUMI_BACKUP_WORK_ROOT}" != / && -d "${SUMI_BACKUP_WORK_ROOT}" && ! -L "${SUMI_BACKUP_WORK_ROOT}" ]] || exit 2
readonly docker_config_dir="${docker_config_file%/config.json}"
readonly docker=("${docker_bin}" --config "${docker_config_dir}" --context "${docker_context}")
readonly compose=("${docker[@]}" compose --env-file "${env_file}" -f "${compose_file}")
read_command_lines() {
  local -n destination="$1"
  shift
  local output
  output="$("$@")"
  destination=()
  if [[ -n "${output}" ]]; then
    mapfile -t destination <<< "${output}"
  fi
}
context_endpoint="$("${docker[@]}" context inspect "${docker_context}" --format '{{.Endpoints.docker.Host}}')"
[[ "${context_endpoint}" == unix:///* ]] || { printf 'backup Docker context must target a local Unix socket\n' >&2; exit 2; }
readonly maintenance_root="${SUMI_BACKUP_WORK_ROOT}/.maintenance"
readonly marker="${maintenance_root}/${snapshot_id}"
readonly state_file="${marker}/phase"
readonly containers_file="${marker}/agent-containers.tsv"
write_phase() {
  local value="$1"
  local temporary="${marker}/.phase.${BASHPID}"
  [[ "${value}" =~ ^(created|ingress-stopped|control-stopped|agents-recorded|agents-stopped|quiesced)$ ]] || return 2
  (umask 077; printf '%s\n' "${value}" > "${temporary}")
  sync -f "${temporary}"
  mv -- "${temporary}" "${state_file}"
  sync -f "${marker}"
}
[[ ! -L "${maintenance_root}" && ( ! -e "${maintenance_root}" || -d "${maintenance_root}" ) ]] || { printf 'maintenance root must be absent or a real directory\n' >&2; exit 2; }
install -d -m 0700 -- "${maintenance_root}"
[[ ! -e "${marker}" ]] || { printf 'maintenance marker already exists: %s\n' "${marker}" >&2; exit 2; }

declare -a running_api running_provisioner running_ingress
read_command_lines running_api "${compose[@]}" ps --status running -q api
read_command_lines running_provisioner "${compose[@]}" ps --status running -q runtime-provisioner
read_command_lines running_ingress "${compose[@]}" ps --status running -q cloudflared
((${#running_api[@]} == 1 && ${#running_provisioner[@]} == 1 && ${#running_ingress[@]} == 1)) || {
  printf 'quiesce requires exactly one running ingress, API, and runtime provisioner\n' >&2
  exit 2
}

mkdir -m 0700 -- "${marker}"
: > "${containers_file}"
chmod 0600 -- "${containers_file}"
sync -f "${containers_file}"
write_phase created

rollback() {
  local status=$?
  trap - ERR
  printf 'quiesce failed; attempting to restore the exact pre-maintenance writer set\n' >&2
  "${resume_helper}" "${snapshot_id}" || true
  exit "${status}"
}
trap rollback ERR

# Stop external admission, then drain the lifecycle daemon while the API and
# current agent runtimes are still connected. No new generation can appear
# after this point; the exact running writer set can now be recorded.
"${compose[@]}" stop cloudflared
write_phase ingress-stopped
"${compose[@]}" stop runtime-provisioner
write_phase control-stopped

declare -a agent_ids
read_command_lines agent_ids "${database_helper}" agent-ids
declare -A known_projects=()
previous=
for paid in "${agent_ids[@]}"; do
  [[ "${paid}" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$ ]] || { printf 'database returned a noncanonical PAID\n' >&2; false; }
  [[ -z "${previous}" || "${paid}" > "${previous}" ]] || { printf 'database PAID set is not unique and sorted\n' >&2; false; }
  previous="${paid}"
  known_projects["sumi-${paid//-/}"]="${paid}"
done

# A running canonical PAID project absent from the database is a writer we
# cannot include coherently. Enumerate every Compose-labeled container rather
# than assuming the current service vocabulary is complete.
declare -a compose_containers
read_command_lines compose_containers "${docker[@]}" ps --filter "label=com.docker.compose.project" -q
for container_id in "${compose_containers[@]}"; do
  [[ -n "${container_id}" ]] || continue
  project="$("${docker[@]}" inspect --format '{{index .Config.Labels "com.docker.compose.project"}}' "${container_id}")"
  if [[ "${project}" =~ ^sumi-[0-9a-f]{32}$ && -z "${known_projects[${project}]:-}" ]]; then
    printf 'orphan canonical agent writer project: %s\n' "${project}" >&2
    false
  fi
done

declare -a running
for paid in "${agent_ids[@]}"; do
  project="sumi-${paid//-/}"
  read_command_lines running "${docker[@]}" ps --filter "label=com.docker.compose.project=${project}" -q
  for short_id in "${running[@]}"; do
    inspected="$("${docker[@]}" inspect --format '{{.Id}}\t{{index .Config.Labels "com.docker.compose.project"}}\t{{index .Config.Labels "com.docker.compose.service"}}\t{{.State.Status}}' "${short_id}")"
    IFS=$'\t' read -r container_id actual_project service status <<< "${inspected}"
    [[ "${container_id}" =~ ^[0-9a-f]{64}$ && "${actual_project}" == "${project}" && "${status}" == running ]] || { printf 'agent container identity changed during quiesce\n' >&2; false; }
    case "${service}" in
      runtime|executor|broker) ;;
      allocator|prepare)
        printf 'agent %s has an allocation writer in flight; retry backup after it finishes\n' "${paid}" >&2
        false
        ;;
      *) printf 'agent %s has an unexpected running service %s\n' "${paid}" "${service}" >&2; false ;;
    esac
    printf '%s\t%s\t%s\t%s\n' "${paid}" "${project}" "${service}" "${container_id}" >> "${containers_file}"
  done
done
LC_ALL=C sort -o "${containers_file}" "${containers_file}"
sync -f "${containers_file}"
write_phase agents-recorded

# Stop every recorded private writer by exact immutable container id while the
# API is still available to accept its final durable events. Volumes remain
# attached to stopped containers and are never removed.
for service in runtime broker executor; do
  while IFS=$'\t' read -r _ _ recorded_service container_id; do
    [[ "${recorded_service}" == "${service}" ]] || continue
    "${docker[@]}" stop --time 120 "${container_id}" >/dev/null
  done < "${containers_file}"
done
write_phase agents-stopped

# External admission, lifecycle creation, and all private writers are now
# closed. Stop the API last so its own in-flight durable writes drain against
# Postgres and host state before the global snapshot point.
"${compose[@]}" stop api
write_phase quiesced
trap - ERR
printf 'all dogfood writers quiesced for snapshot %s; marker remains until exact resume\n' "${snapshot_id}"
