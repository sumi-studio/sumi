#!/usr/bin/env bash
set -Eeuo pipefail

readonly state_root="${1:?usage: sudo prepare-host.sh /absolute/persistent/state-root}"
(( EUID == 0 )) || {
  printf 'prepare-host.sh must run as root\n' >&2
  exit 2
}
[[ "${state_root}" == /* && "${state_root}" != "/" ]] || {
  printf 'state root must be an absolute non-root path\n' >&2
  exit 2
}
[[ "$(readlink -m -- "${state_root}")" == "${state_root}" ]] || {
  printf 'state root must be lexically clean\n' >&2
  exit 2
}

readonly operation_lock="${state_root}/.operations.lock"
readonly state_paths=(
  "${state_root}"
  "${state_root}/command-log"
  "${state_root}/runtime-state"
  "${state_root}/attachments"
)
for path in "${state_paths[@]}"; do
  [[ ! -L "${path}" ]] || {
    printf 'refusing symlinked state directory: %s\n' "${path}" >&2
    exit 2
  }
  [[ ! -e "${path}" || -d "${path}" ]] || {
    printf 'state directory path is occupied by a non-directory: %s\n' "${path}" >&2
    exit 2
  }
done
[[ ! -L "${operation_lock}" ]] || {
  printf 'refusing symlinked operation lock: %s\n' "${operation_lock}" >&2
  exit 2
}
[[ ! -e "${operation_lock}" || -f "${operation_lock}" ]] || {
  printf 'operation lock path is occupied by a non-file: %s\n' "${operation_lock}" >&2
  exit 2
}

# The API must traverse the bind-mount root to reach its private 0700 child
# directories. It may neither list nor write the root itself.
install -d -o 0 -g 0 -m 0711 -- "${state_root}"
install -d -o 65532 -g 65532 -m 0700 -- \
  "${state_root}/command-log" \
  "${state_root}/runtime-state" \
  "${state_root}/attachments"
touch -- "${operation_lock}"
chown 0:0 -- "${operation_lock}"
chmod 0600 -- "${operation_lock}"
install -d -o 0 -g 0 -m 0755 -- /run/sumi
install -d -o 65532 -g 20000 -m 0750 -- /run/sumi/local-control
install -d -o 0 -g 0 -m 0700 -- /run/sumi/runtime-secrets /run/sumi/supervisor-locks
install -d -o 0 -g 20000 -m 0750 -- /run/sumi/runtime-provisioner
