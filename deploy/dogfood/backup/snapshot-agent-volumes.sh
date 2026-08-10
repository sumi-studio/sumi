#!/usr/bin/env bash
set -Eeuo pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly output_root="${1:?usage: snapshot-agent-volumes.sh SNAPSHOT_DIR}"
readonly docker_bin="${SUMI_DOCKER_BIN:-/usr/bin/docker}"
readonly docker_context="${SUMI_DOGFOOD_DOCKER_CONTEXT:?set SUMI_DOGFOOD_DOCKER_CONTEXT}"
readonly docker_config_file="${SUMI_DOCKER_CONFIG_FILE:?set SUMI_DOCKER_CONFIG_FILE}"
readonly database_helper="${SUMI_DATABASE_HELPER:?set SUMI_DATABASE_HELPER}"
readonly provisioner_image="${SUMI_PROVISIONER_IMAGE:?set SUMI_PROVISIONER_IMAGE}"
readonly tar_bin="${SUMI_TAR_BIN:?set SUMI_TAR_BIN}"
readonly logical_volumes=(
  allocator-root allocator-state artifacts broker-identity broker-ipc
  executor-identity executor-ipc runtime-identity state workspace
)

[[ "${output_root}" == /* && "${output_root}" != / && -d "${output_root}" && ! -L "${output_root}" ]] || { printf 'snapshot output must be a real absolute non-root directory\n' >&2; exit 2; }
for helper in "${docker_bin}" "${database_helper}" "${tar_bin}"; do
  [[ "${helper}" == /* && -f "${helper}" && -x "${helper}" && ! -L "${helper}" ]] || { printf 'snapshot helper must be an absolute executable regular non-symlink\n' >&2; exit 2; }
done
[[ "${docker_context}" =~ ^[A-Za-z0-9_.-]+$ ]] || { printf 'invalid Docker context name\n' >&2; exit 2; }
[[ "${docker_config_file}" == */config.json && -f "${docker_config_file}" && ! -L "${docker_config_file}" ]] || { printf 'invalid Docker config file\n' >&2; exit 2; }
[[ "${provisioner_image}" =~ ^[a-z0-9./:_-]+@sha256:[0-9a-f]{64}$ ]] || { printf 'provisioner image is not an exact digest\n' >&2; exit 2; }
readonly docker_config_dir="${docker_config_file%/config.json}"
readonly docker=("${docker_bin}" --config "${docker_config_dir}" --context "${docker_context}")
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
read_sorted_command_lines() {
  local -n destination="$1"
  shift
  local output
  output="$("$@")"
  destination=()
  if [[ -n "${output}" ]]; then
    output="$(printf '%s\n' "${output}" | LC_ALL=C sort)"
    mapfile -t destination <<< "${output}"
  fi
}
context_endpoint="$("${docker[@]}" context inspect "${docker_context}" --format '{{.Endpoints.docker.Host}}')"
[[ "${context_endpoint}" == unix:///* ]] || { printf 'backup Docker context must target a local Unix socket\n' >&2; exit 2; }

readonly staging="${output_root}/agent-volumes"
readonly rows="${output_root}/.agent-volume-rows.tsv"
[[ ! -e "${staging}" && ! -e "${rows}" && ! -e "${output_root}/agent-volume-set.json" && ! -e "${output_root}/agent-volumes.tar" ]] || {
  printf 'agent volume snapshot outputs already exist\n' >&2
  exit 2
}
mkdir -m 0700 -- "${staging}"
touch -- "${rows}"
chmod 0600 -- "${rows}"

declare -a agent_ids
read_command_lines agent_ids "${database_helper}" agent-ids
declare -A known_projects=()
declare -A canonical_volume_counts=()
previous=
for paid in "${agent_ids[@]}"; do
  [[ "${paid}" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$ ]] || { printf 'database returned a noncanonical PAID\n' >&2; exit 1; }
  [[ -z "${previous}" || "${paid}" > "${previous}" ]] || { printf 'database PAID set is not unique and sorted\n' >&2; exit 1; }
  previous="${paid}"
  project="sumi-${paid//-/}"
  known_projects["${project}"]=1
done

# Detect canonical Sumi volumes whose PAID no longer exists in the database.
# They are never captured opportunistically: an orphan makes the snapshot fail.
declare -a all_volume_names
read_command_lines all_volume_names "${docker[@]}" volume ls --format '{{.Name}}'
for volume_name in "${all_volume_names[@]}"; do
  if [[ "${volume_name}" =~ ^(sumi-[0-9a-f]{32})_([a-z-]+)$ ]]; then
    project="${BASH_REMATCH[1]}"
    logical="${BASH_REMATCH[2]}"
    [[ -n "${known_projects[${project}]:-}" ]] || { printf 'orphan canonical agent volume: %s\n' "${volume_name}" >&2; exit 1; }
    expected=0
    for candidate in "${logical_volumes[@]}"; do [[ "${logical}" == "${candidate}" ]] && expected=1; done
    (( expected == 1 )) || { printf 'unexpected canonical agent volume: %s\n' "${volume_name}" >&2; exit 1; }
    canonical_volume_counts["${project}"]="$(( ${canonical_volume_counts[${project}]:-0} + 1 ))"
  fi
done

declare -a observed
for paid in "${agent_ids[@]}"; do
  project="sumi-${paid//-/}"
  read_sorted_command_lines observed "${docker[@]}" volume ls --filter "label=com.docker.compose.project=${project}" --format '{{.Name}}'
  if ((${#observed[@]} == 0)); then
    [[ "${canonical_volume_counts[${project}]:-0}" == 0 ]] || {
      printf 'agent %s has canonical volumes without the canonical Compose project label\n' "${paid}" >&2
      exit 1
    }
    printf 'A\t%s\t%s\tunprovisioned\n' "${paid}" "${project}" >> "${rows}"
    continue
  fi
  expected_names=()
  for logical in "${logical_volumes[@]}"; do expected_names+=("${project}_${logical}"); done
  mismatch=0
  if ((${#observed[@]} != ${#expected_names[@]})); then
    mismatch=1
  else
    for index in "${!expected_names[@]}"; do
      [[ "${observed[${index}]}" == "${expected_names[${index}]}" ]] || mismatch=1
    done
  fi
  if (( mismatch == 1 )); then
    printf 'agent %s has a missing or unexpected Compose volume\n' "${paid}" >&2
    exit 1
  fi
  install -d -m 0700 -- "${staging}/${paid}"
  for logical in "${logical_volumes[@]}"; do
    volume_name="${project}_${logical}"
    "${docker[@]}" volume inspect "${volume_name}" |
      node "${script_dir}/validate-agent-volume.mjs" "${project}" "${logical}" "${volume_name}"
    archive_relative="${paid}/${logical}.tar"
    manifest_relative="${paid}/${logical}.manifest"
    "${docker[@]}" run --rm --network none --read-only --cap-drop ALL \
      --security-opt no-new-privileges \
      --mount "type=volume,src=${volume_name},dst=/source,readonly" \
      --entrypoint /bin/tar "${provisioner_image}" \
      --numeric-owner --acls --xattrs --sort=name -C /source -cpf - . \
      > "${staging}/${archive_relative}"
    "${docker[@]}" run --rm --network none --read-only --cap-drop ALL \
      --security-opt no-new-privileges \
      --mount "type=volume,src=${volume_name},dst=/source,readonly" \
      --mount "type=bind,src=${script_dir}/volume-tree-manifest.sh,dst=/usr/local/bin/sumi-volume-manifest,readonly" \
      --entrypoint /bin/bash "${provisioner_image}" \
      /usr/local/bin/sumi-volume-manifest /source \
      > "${staging}/${manifest_relative}"
    printf 'V\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "${paid}" "${project}" "${logical}" "${volume_name}" "${archive_relative}" "${manifest_relative}" >> "${rows}"
  done
done

node "${script_dir}/agent-volume-set.mjs" create "${staging}" "${rows}" "${output_root}/agent-volume-set.json"
"${tar_bin}" --create --numeric-owner --acls --xattrs --sort=name \
  --file="${output_root}/agent-volumes.tar" --directory="${staging}" .
rm -rf -- "${staging}"
unlink -- "${rows}"
