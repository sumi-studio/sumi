#!/usr/bin/env bash
set -Eeuo pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly repository_root="$(cd "${script_dir}/../../.." && pwd)"
readonly required=(
  SUMI_DOGFOOD_SMOKE_BASE_URL
  SUMI_DOGFOOD_SMOKE_STORAGE_STATE
  SUMI_DOGFOOD_SMOKE_PLACE_ID
  SUMI_DOGFOOD_SMOKE_MESSAGING_PATH
  SUMI_DOGFOOD_RESTART_API_HELPER
  SUMI_DOGFOOD_RESTART_TUNNEL_HELPER
)

missing=()
for name in "${required[@]}"; do
  [[ -n "${!name:-}" ]] || missing+=("${name}")
done
if ((${#missing[@]} > 0)); then
  printf 'NOT COVERED: dogfood restart smoke requires %s\n' "${missing[*]}" >&2
  exit 2
fi

for name in SUMI_DOGFOOD_SMOKE_STORAGE_STATE SUMI_DOGFOOD_RESTART_API_HELPER SUMI_DOGFOOD_RESTART_TUNNEL_HELPER; do
  value="${!name}"
  [[ "${value}" == /* && -f "${value}" && ! -L "${value}" ]] || {
    printf 'NOT COVERED: %s must be an absolute regular non-symlink\n' "${name}" >&2
    exit 2
  }
  mode="$(stat -c '%a' -- "${value}")"
  (( (8#${mode} & 0077) == 0 )) || {
    printf 'NOT COVERED: %s must not grant group/other permissions\n' "${name}" >&2
    exit 2
  }
done
for name in SUMI_DOGFOOD_RESTART_API_HELPER SUMI_DOGFOOD_RESTART_TUNNEL_HELPER; do
  [[ -x "${!name}" ]] || {
    printf 'NOT COVERED: %s must be executable\n' "${name}" >&2
    exit 2
  }
done

cd "${repository_root}/apps/web"
pnpm exec playwright test e2e/dogfood-restart.spec.ts --workers=1 --reporter=line
