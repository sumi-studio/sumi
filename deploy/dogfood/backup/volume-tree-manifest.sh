#!/usr/bin/env bash
set -Eeuo pipefail

readonly root="${1:?usage: volume-tree-manifest.sh ROOT}"
[[ "${root}" == /* && -d "${root}" && ! -L "${root}" ]] || {
  printf 'volume root must be a real absolute directory\n' >&2
  exit 2
}
cd "${root}"
# Include `.` itself: ownership and mode on a named volume's mount root are
# executable state, not incidental container metadata.
find . -print0 |
  LC_ALL=C sort -z |
  while IFS= read -r -d '' path; do
  lexical="${path#./}"
  encoded="$(printf '%s' "${lexical}" | base64 --wrap=0)"
  if [[ -L "${path}" ]]; then
    printf 'symlink is forbidden in agent volume: %s\n' "${encoded}" >&2
    exit 1
  elif [[ -d "${path}" ]]; then
    printf 'd\t%s\t%s\t%s\t%s\n' \
      "$(stat -c '%u' -- "${path}")" \
      "$(stat -c '%g' -- "${path}")" \
      "$(stat -c '%a' -- "${path}")" \
      "${encoded}"
  elif [[ -f "${path}" ]]; then
    [[ "$(stat -c '%h' -- "${path}")" == 1 ]] || {
      printf 'hard-linked agent volume file is forbidden: %s\n' "${encoded}" >&2
      exit 1
    }
    printf 'f\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$(stat -c '%u' -- "${path}")" \
      "$(stat -c '%g' -- "${path}")" \
      "$(stat -c '%a' -- "${path}")" \
      "$(stat -c '%s' -- "${path}")" \
      "$(sha256sum -- "${path}" | cut -d ' ' -f 1)" \
      "${encoded}"
  else
    printf 'special entry is forbidden in agent volume: %s\n' "${encoded}" >&2
    exit 1
  fi
done
