#!/usr/bin/env bash
set -Eeuo pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly env_file="${1:?usage: deploy-origin.sh /absolute/path/operator.env}"
readonly mode="${2:-}"

[[ -z "${mode}" || "${mode}" == "--check" ]] || {
  printf 'usage: deploy-origin.sh /absolute/path/operator.env [--check]\n' >&2
  exit 2
}

[[ "${env_file}" == /* && -f "${env_file}" && ! -L "${env_file}" ]] || {
  printf 'operator env must be an absolute, regular, non-symlink file\n' >&2
  exit 2
}
env_permissions="$(stat -c '%a' -- "${env_file}")"
(( (8#${env_permissions} & 0077) == 0 )) || {
  printf 'operator env must not grant group/other permissions\n' >&2
  exit 2
}

declare -A loaded_keys=()
while IFS= read -r line || [[ -n "${line}" ]]; do
  line="${line%$'\r'}"
  [[ "${line}" =~ ^[[:space:]]*$ || "${line}" =~ ^[[:space:]]*# ]] && continue
  [[ "${line}" =~ ^([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]] || {
    printf 'operator env contains a non-literal KEY=VALUE line\n' >&2
    exit 2
  }
  key="${BASH_REMATCH[1]}"
  value="${BASH_REMATCH[2]}"
  [[ "${key}" == SUMI_* ]] || { printf 'operator env key is outside the SUMI_ namespace: %s\n' "${key}" >&2; exit 2; }
  [[ -z "${loaded_keys[${key}]:-}" ]] || { printf 'operator env repeats key: %s\n' "${key}" >&2; exit 2; }
  loaded_keys["${key}"]=1
  [[ "${value}" != *$'\n'* && "${value}" != *$'\r'* ]] || exit 2
  printf -v "${key}" '%s' "${value}"
  export "${key}"
done < "${env_file}"
node "${script_dir}/validate-env.mjs"
readonly docker_context="${SUMI_DOGFOOD_DOCKER_CONTEXT}"
readonly docker_config_dir="${SUMI_DOCKER_CONFIG_FILE%/config.json}"
readonly docker=(docker --config "${docker_config_dir}" --context "${docker_context}")
readonly compose=("${docker[@]}" compose --env-file "${env_file}" -f "${script_dir}/compose.yaml")
context_endpoint="$("${docker[@]}" context inspect "${docker_context}" --format '{{.Endpoints.docker.Host}}')"
[[ "${context_endpoint}" == unix:///* ]] || {
  printf 'dogfood Docker context must target a local Unix socket, got %s\n' "${context_endpoint}" >&2
  exit 2
}
"${compose[@]}" --profile maintenance config --format json | node "${script_dir}/validate-compose.mjs"
if [[ "${mode}" == "--check" ]]; then
  printf 'dogfood origin preflight valid; no image pull, migration, restart, or deploy was performed\n'
  exit 0
fi

exec {operation_lock_fd}<>"${SUMI_DOGFOOD_OPERATION_LOCK}"
if ! /usr/bin/flock --nonblock "${operation_lock_fd}"; then
  printf 'another dogfood deploy or backup operation holds %s\n' "${SUMI_DOGFOOD_OPERATION_LOCK}" >&2
  exit 1
fi

"${compose[@]}" pull postgres host-state-init migrate runtime-provisioner api cloudflared
"${compose[@]}" up -d postgres runtime-provisioner

# One API process only: stop the old binary before changing its schema or
# creating the new process. Short downtime is preferable to allowing writes
# during migration or running the old binary against a newly advanced schema.
if [[ -n "$("${compose[@]}" ps -q api)" ]]; then
  "${compose[@]}" stop api
fi
"${compose[@]}" run --rm migrate apply
"${compose[@]}" up -d --no-deps --force-recreate api

for _ in {1..60}; do
  if "${compose[@]}" exec -T api /busybox/wget -q -O /dev/null http://127.0.0.1:8080/ready; then
    "${compose[@]}" up -d --no-deps --force-recreate cloudflared
    "${compose[@]}" ps
    exit 0
  fi
  sleep 2
done
printf 'new API did not become dependency-ready; cloudflared was not advanced\n' >&2
exit 1
