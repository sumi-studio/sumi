#!/usr/bin/env sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
env_file=${SUMI_COMPOSE_ENV_FILE:-"$repo_root/.env"}

if [ ! -f "$env_file" ]; then
  echo "$env_file does not exist; run scripts/dev/create-compose-env.sh first" >&2
  exit 1
fi

set -a
. "$env_file"
set +a

export SUMI_DEV_USER_ID=${SUMI_DEV_USER_ID:-019c0000-0000-7000-8000-000000000001}
export SUMI_DEV_CONVERSATION_ID=${SUMI_DEV_CONVERSATION_ID:-local-compose}

python3 <<'PYTHON'
import base64
import hashlib
import hmac
import json
import os
import time

def encode(value):
    raw = json.dumps(value, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

secret = base64.b64decode(os.environ["SUMI_BROWSER_SESSION_SECRET"], validate=True)
header = encode({"alg": "HS256", "typ": "JWT"})
claims = encode({
    "tenant_id": "local-compose",
    "user_id": os.environ["SUMI_DEV_USER_ID"],
    "conversation_id": os.environ["SUMI_DEV_CONVERSATION_ID"],
    "exp": int(time.time()) + 86400,
    "aud": "sumi:web:conversation",
})
signing_input = f"{header}.{claims}"
signature = base64.urlsafe_b64encode(
    hmac.new(secret, signing_input.encode(), hashlib.sha256).digest()
).rstrip(b"=").decode()
print(f"sumi_session={signing_input}.{signature}")
PYTHON
