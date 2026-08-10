#!/usr/bin/env bash
set -Eeuo pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly source_dir="${1:?usage: stage-assets.sh WEB_DIST [DESTINATION]}"
readonly destination="${2:-${script_dir}/dist}"

[[ -d "${source_dir}" && ! -L "${source_dir}" ]] || {
  printf 'source must be an existing non-symlink directory: %s\n' "${source_dir}" >&2
  exit 2
}
[[ ! -e "${destination}" ]] || {
  printf 'destination already exists; use a fresh staging path: %s\n' "${destination}" >&2
  exit 2
}
[[ "${destination}" != "/" && "${destination}" != "${source_dir}" ]] || {
  printf 'unsafe staging destination: %s\n' "${destination}" >&2
  exit 2
}
if find "${source_dir}" -type l -print -quit | grep -q .; then
  printf 'static source contains a symlink; refusing an ambiguous upload tree\n' >&2
  exit 2
fi

mkdir -m 0700 -- "${destination}"
cp -a -- "${source_dir}/." "${destination}/"
rm -f -- "${destination}/mcp-app-sandbox.html"
install -m 0600 -- "${script_dir}/_headers" "${destination}/_headers"
printf '%s\n' 'mcp-app-sandbox.html' > "${destination}/.assetsignore"

if find "${destination}" -type f -name 'mcp-app-sandbox.html' -print -quit | grep -q .; then
  printf 'MCP sandbox artifact survived edge staging\n' >&2
  exit 1
fi
