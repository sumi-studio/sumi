#!/usr/bin/env sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
target=${1:-"$repo_root/.env"}

if [ -e "$target" ]; then
  echo "$target already exists; refusing to overwrite it" >&2
  exit 1
fi

umask 077
postgres_password=$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')
browser_secret=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | base64 | tr -d '\n')

{
  printf 'SUMI_POSTGRES_PASSWORD=%s\n' "$postgres_password"
  printf 'SUMI_BROWSER_SESSION_SECRET=%s\n' "$browser_secret"
  printf 'SUMI_DEFAULT_TIMEZONE=Asia/Tokyo\n'
  printf 'SUMI_SITE_ADDRESS=http://localhost\n'
  printf 'SUMI_HTTP_PORT=8080\n'
  printf 'SUMI_HTTPS_PORT=8443\n'
  printf 'SUMI_POSTGRES_PORT=55432\n'
  printf 'SUMI_AGENT_WS_ALLOWED_ORIGINS=http://localhost:8080\n'
  printf 'SUMI_BROWSER_WS_ALLOWED_ORIGINS=http://localhost:8080\n'
} >"$target"

echo "created $target with mode 600"
