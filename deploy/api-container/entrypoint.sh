#!/busybox/sh
# Starts the Sumi API inside a Cloudflare Container.
#
# Worker secrets arrive as environment variables. The API and the Google
# client library read two of them from files, so they are written to a
# private tmpfs-like directory first and removed from the environment.
set -eu
umask 077
state=/tmp/sumi
secrets="$state/secrets"
/busybox/mkdir -p "$secrets" "$state/command-log" "$state/browser-events"
/busybox/chmod 700 "$state" "$secrets" "$state/command-log" "$state/browser-events"

if [ -n "${SUMI_GOOGLE_CREDENTIALS_JSON:-}" ]; then
  printf '%s' "$SUMI_GOOGLE_CREDENTIALS_JSON" > "$secrets/google-credentials.json"
  export GOOGLE_APPLICATION_CREDENTIALS="$secrets/google-credentials.json"
fi
unset SUMI_GOOGLE_CREDENTIALS_JSON

# Journal state lives in PostgreSQL; these directories are its local cache.
export SUMI_API_JOURNAL_MIRROR=postgres
export SUMI_COMMAND_LOG_DIR="$state/command-log"
export SUMI_BROWSER_EVENT_DIR="$state/browser-events"
export PORT="${PORT:-8080}"
# The local disk does not survive replacement, so attachment bytes (enabled
# by setting the attachment caps) are kept in PostgreSQL.
unset SUMI_MESSAGING_ATTACHMENT_ROOT
if [ -n "${SUMI_MESSAGING_ATTACHMENT_WORKSPACE_QUOTA_BYTES:-}${SUMI_MESSAGING_ATTACHMENT_WORKSPACE_QUOTA_OBJECTS:-}${SUMI_MESSAGING_ATTACHMENT_TOTAL_QUOTA_BYTES:-}${SUMI_MESSAGING_ATTACHMENT_TOTAL_QUOTA_OBJECTS:-}" ]; then
  export SUMI_MESSAGING_ATTACHMENT_STORE=postgres
else
  unset SUMI_MESSAGING_ATTACHMENT_STORE
fi

exec /usr/local/bin/sumi-api
