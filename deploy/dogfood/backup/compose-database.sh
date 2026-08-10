#!/usr/bin/env bash
set -Eeuo pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly env_file="${SUMI_DOGFOOD_OPERATOR_ENV_FILE:?set SUMI_DOGFOOD_OPERATOR_ENV_FILE}"
readonly docker_bin="${SUMI_DOCKER_BIN:-/usr/bin/docker}"
readonly docker_context="${SUMI_DOGFOOD_DOCKER_CONTEXT:?set SUMI_DOGFOOD_DOCKER_CONTEXT}"
readonly docker_config_file="${SUMI_DOCKER_CONFIG_FILE:?set SUMI_DOCKER_CONFIG_FILE}"
readonly mode="${1:?usage: compose-database.sh attachment-rows|agent-ids|dump|scratch-object-count|scratch-restore|scratch-attachment-rows}"

[[ $# == 1 && "${mode}" =~ ^(attachment-rows|agent-ids|dump|scratch-object-count|scratch-restore|scratch-attachment-rows)$ ]] || {
  printf 'invalid database maintenance mode\n' >&2
  exit 2
}
[[ "${env_file}" == /* && -f "${env_file}" && ! -L "${env_file}" ]] || { printf 'operator env must be an absolute regular non-symlink\n' >&2; exit 2; }
env_mode="$(stat -c '%a' -- "${env_file}")"
(( (8#${env_mode} & 0077) == 0 )) || { printf 'operator env must not grant group/other permissions\n' >&2; exit 2; }
[[ "${docker_bin}" == /* && -f "${docker_bin}" && -x "${docker_bin}" && ! -L "${docker_bin}" ]] || { printf 'SUMI_DOCKER_BIN must be an absolute executable regular non-symlink\n' >&2; exit 2; }
[[ "${docker_context}" =~ ^[A-Za-z0-9_.-]+$ ]] || { printf 'invalid Docker context name\n' >&2; exit 2; }
[[ "${docker_config_file}" == */config.json && -f "${docker_config_file}" && ! -L "${docker_config_file}" ]] || { printf 'SUMI_DOCKER_CONFIG_FILE must be an absolute regular config.json\n' >&2; exit 2; }

require_compose_network_url() {
  local name="$1"
  local value="${!name:-}"
  [[ -n "${value}" && "${value}" == postgres://* ]] || { printf '%s must be a Postgres URL\n' "${name}" >&2; exit 2; }
  local authority="${value#postgres://}"
  authority="${authority%%/*}"
  local host_port="${authority##*@}"
  local host="${host_port%%:*}"
  [[ "${host}" =~ ^[a-z0-9][a-z0-9.-]*$ ]] || { printf '%s must use an internal Compose DNS hostname\n' "${name}" >&2; exit 2; }
  case "${host}" in
    localhost|localhost.*|127.*|0.0.0.0|host.docker.internal)
      printf '%s points at a host-only database; maintenance must traverse the internal Compose data network\n' "${name}" >&2
      exit 2
      ;;
  esac
}

require_compose_network_url SUMI_DB_URL
if [[ "${mode}" == scratch-* ]]; then
  require_compose_network_url SUMI_RESTORE_DB_URL
fi
export SUMI_DB_URL SUMI_RESTORE_DB_URL

readonly docker_config_dir="${docker_config_file%/config.json}"
readonly docker=("${docker_bin}" --config "${docker_config_dir}" --context "${docker_context}")
context_endpoint="$("${docker[@]}" context inspect "${docker_context}" --format '{{.Endpoints.docker.Host}}')"
[[ "${context_endpoint}" == unix:///* ]] || { printf 'backup Docker context must target a local Unix socket\n' >&2; exit 2; }
readonly compose=("${docker[@]}" compose --env-file "${env_file}" -f "${script_dir}/../compose.yaml")
readonly run_client=("${compose[@]}" run --rm --no-deps -T --entrypoint /bin/sh database-client -ceu)

case "${mode}" in
  attachment-rows)
    exec "${run_client[@]}" 'exec psql "$SUMI_DB_URL" -X -v ON_ERROR_STOP=1 -At -F "$(printf "\t")" -c "SELECT attachment_id::text, size_bytes FROM message_attachments ORDER BY attachment_id"'
    ;;
  agent-ids)
    exec "${run_client[@]}" 'exec psql "$SUMI_DB_URL" -X -v ON_ERROR_STOP=1 -At -c "SELECT personality_agent_id::text FROM agents ORDER BY personality_agent_id"'
    ;;
  dump)
    exec "${run_client[@]}" 'exec pg_dump --format=custom "$SUMI_DB_URL"'
    ;;
  scratch-object-count)
    exec "${run_client[@]}" 'exec psql "$SUMI_RESTORE_DB_URL" -X -v ON_ERROR_STOP=1 -At -c "WITH target AS (SELECT oid, nspname FROM pg_namespace WHERE nspname <> \$\$pg_catalog\$\$ AND nspname <> \$\$information_schema\$\$ AND nspname NOT LIKE \$\$pg_toast%\$\$ AND nspname NOT LIKE \$\$pg_temp_%\$\$) SELECT (SELECT count(*) FROM target WHERE nspname <> \$\$public\$\$)+(SELECT count(*) FROM pg_class,target WHERE relnamespace=target.oid)+(SELECT count(*) FROM pg_proc,target WHERE pronamespace=target.oid)+(SELECT count(*) FROM pg_type,target WHERE typnamespace=target.oid)+(SELECT count(*) FROM pg_constraint,target WHERE connamespace=target.oid)+(SELECT count(*) FROM pg_operator,target WHERE oprnamespace=target.oid)+(SELECT count(*) FROM pg_conversion,target WHERE connamespace=target.oid)+(SELECT count(*) FROM pg_opclass,target WHERE opcnamespace=target.oid)+(SELECT count(*) FROM pg_opfamily,target WHERE opfnamespace=target.oid)+(SELECT count(*) FROM pg_collation,target WHERE collnamespace=target.oid)+(SELECT count(*) FROM pg_ts_config,target WHERE cfgnamespace=target.oid)+(SELECT count(*) FROM pg_ts_dict,target WHERE dictnamespace=target.oid)+(SELECT count(*) FROM pg_ts_parser,target WHERE prsnamespace=target.oid)+(SELECT count(*) FROM pg_ts_template,target WHERE tmplnamespace=target.oid)+(SELECT count(*) FROM pg_statistic_ext,target WHERE stxnamespace=target.oid)"'
    ;;
  scratch-restore)
    exec "${run_client[@]}" 'exec pg_restore --exit-on-error --single-transaction --dbname="$SUMI_RESTORE_DB_URL"'
    ;;
  scratch-attachment-rows)
    exec "${run_client[@]}" 'exec psql "$SUMI_RESTORE_DB_URL" -X -v ON_ERROR_STOP=1 -At -F "$(printf "\t")" -c "SELECT attachment_id::text, size_bytes FROM message_attachments ORDER BY attachment_id"'
    ;;
esac
