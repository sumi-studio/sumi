#!/usr/bin/env bash
set -Eeuo pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly env_file="${SUMI_DOGFOOD_OPERATOR_ENV_FILE:?set SUMI_DOGFOOD_OPERATOR_ENV_FILE}"
readonly docker_bin="${SUMI_DOCKER_BIN:-/usr/bin/docker}"
readonly docker_context="${SUMI_DOGFOOD_DOCKER_CONTEXT:?set SUMI_DOGFOOD_DOCKER_CONTEXT}"
readonly docker_config_file="${SUMI_DOCKER_CONFIG_FILE:?set SUMI_DOCKER_CONFIG_FILE}"
readonly mode="${1:?usage: compose-migrate.sh apply|verify|status|manifest}"

[[ "${mode}" =~ ^(apply|verify|status|manifest)$ && $# == 1 ]] || { printf 'invalid migrate mode\n' >&2; exit 2; }
[[ "${env_file}" == /* && -f "${env_file}" && ! -L "${env_file}" ]] || { printf 'operator env must be an absolute regular non-symlink\n' >&2; exit 2; }
env_mode="$(stat -c '%a' -- "${env_file}")"
(( (8#${env_mode} & 0077) == 0 )) || { printf 'operator env must not grant group/other permissions\n' >&2; exit 2; }
[[ "${docker_bin}" == /* && -f "${docker_bin}" && -x "${docker_bin}" && ! -L "${docker_bin}" ]] || { printf 'SUMI_DOCKER_BIN must be an absolute executable regular non-symlink\n' >&2; exit 2; }
[[ "${docker_context}" =~ ^[A-Za-z0-9_.-]+$ ]] || { printf 'invalid Docker context name\n' >&2; exit 2; }
[[ "${docker_config_file}" == */config.json && -f "${docker_config_file}" && ! -L "${docker_config_file}" ]] || { printf 'SUMI_DOCKER_CONFIG_FILE must be an absolute regular config.json\n' >&2; exit 2; }
[[ -n "${SUMI_DB_URL:-}" ]] || { printf 'SUMI_DB_URL is required\n' >&2; exit 2; }
export SUMI_DB_URL
readonly docker_config_dir="${docker_config_file%/config.json}"
readonly docker=("${docker_bin}" --config "${docker_config_dir}" --context "${docker_context}")
context_endpoint="$("${docker[@]}" context inspect "${docker_context}" --format '{{.Endpoints.docker.Host}}')"
[[ "${context_endpoint}" == unix:///* ]] || { printf 'backup Docker context must target a local Unix socket\n' >&2; exit 2; }

exec "${docker[@]}" compose --env-file "${env_file}" -f "${script_dir}/../compose.yaml" \
  run --rm --no-deps migrate "${mode}"
