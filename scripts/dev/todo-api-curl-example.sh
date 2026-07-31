#!/usr/bin/env sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
api_base=${SUMI_API_BASE_URL:-http://localhost:8080}
title=${1:-"curl sample Todo"}
timezone=${SUMI_TODO_TIMEZONE:-Asia/Tokyo}

for command in curl jq python3; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "$command is required" >&2
    exit 1
  fi
done

if [ ! -f "$repo_root/.env" ]; then
  echo "$repo_root/.env does not exist; run 'make compose-env' first" >&2
  exit 1
fi

cookie=$("$repo_root/scripts/dev/create-sumi-session.sh")
due_date=$(python3 - <<'PYTHON'
from datetime import date, timedelta
print((date.today() + timedelta(days=1)).isoformat())
PYTHON
)

request_file=$(mktemp /tmp/sumi-todo-request.XXXXXX.json)
response_file=$(mktemp /tmp/sumi-todo-response.XXXXXX.json)
trap 'rm -f "$request_file" "$response_file"' EXIT

echo "1. Health"
curl --fail --silent --show-error "$api_base/health" | jq .

echo "2. Create Todo"
jq -n \
  --arg title "$title" \
  --arg date "$due_date" \
  --arg timezone "$timezone" \
  '{title:$title, priority:"high", due:{kind:"date", date:$date, timezone:$timezone}}' \
  >"$request_file"
curl --fail --silent --show-error \
  -b "$cookie" \
  -H 'Content-Type: application/json' \
  --data-binary "@$request_file" \
  "$api_base/v1/todos" >"$response_file"
jq . "$response_file"

todo_id=$(jq -er '.id' "$response_file")
created_version=$(jq -er '.version' "$response_file")

echo "3. List Todos sorted by due"
curl --fail --silent --show-error \
  -b "$cookie" \
  "$api_base/v1/todos?status=open&sort=due" | jq .

echo "4. Mark Todo done with expected_version=$created_version"
jq -n --argjson version "$created_version" \
  '{expected_version:$version, status:"done"}' >"$request_file"
curl --fail --silent --show-error \
  -X PATCH \
  -b "$cookie" \
  -H 'Content-Type: application/json' \
  --data-binary "@$request_file" \
  "$api_base/v1/todos/$todo_id" >"$response_file"
jq . "$response_file"

updated_version=$(jq -er '.version' "$response_file")

echo "5. Confirm stale expected_version returns 409"
jq -n --argjson version "$created_version" \
  '{expected_version:$version, title:"must not overwrite"}' >"$request_file"
status=$(curl --silent --show-error \
  -o "$response_file" \
  -w '%{http_code}' \
  -X PATCH \
  -b "$cookie" \
  -H 'Content-Type: application/json' \
  --data-binary "@$request_file" \
  "$api_base/v1/todos/$todo_id")
if [ "$status" != "409" ]; then
  echo "expected HTTP 409, got $status" >&2
  jq . "$response_file" >&2 || true
  exit 1
fi
jq . "$response_file"

if [ "${SUMI_KEEP_SAMPLE_TODO:-0}" = "1" ]; then
  echo "6. Keeping Todo $todo_id because SUMI_KEEP_SAMPLE_TODO=1"
  exit 0
fi

echo "6. Delete Todo with expected_version=$updated_version"
status=$(curl --silent --show-error \
  -o "$response_file" \
  -w '%{http_code}' \
  -X DELETE \
  -b "$cookie" \
  "$api_base/v1/todos/$todo_id?expected_version=$updated_version")
if [ "$status" != "204" ]; then
  echo "expected HTTP 204, got $status" >&2
  jq . "$response_file" >&2 || true
  exit 1
fi
echo "deleted $todo_id"
