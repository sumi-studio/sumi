#!/bin/sh
set -eu

: "${FIREBASE_PROJECT_ID:=sumi-studio}"
: "${FIREBASE_AUTH_EXPORT_DIR:=/var/lib/firebase-auth}"
export FIREBASE_PROJECT_ID

mkdir -p "${FIREBASE_AUTH_EXPORT_DIR}"

# --import tolerates an empty directory, which is the intentional first-start
# state. Firebase writes an export whenever the process is stopped normally.
exec firebase emulators:start \
  --only auth \
  --project "${FIREBASE_PROJECT_ID}" \
  --import "${FIREBASE_AUTH_EXPORT_DIR}" \
  --export-on-exit "${FIREBASE_AUTH_EXPORT_DIR}"
