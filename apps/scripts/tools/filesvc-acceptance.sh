#!/usr/bin/env bash
# filesvc-acceptance.sh — ROOT-RUN actual filesvc/JuiceFS acceptance probe.
#
# The sandboxed integration suite proves the state-API ledger contract
# against a protocol-correct in-process stub. This helper is the missing
# last hop: it exercises the REAL filesvc (JuiceFS-backed) for the exact
# behaviours the script file capability depends on — most importantly the
# keyed-resend semantics that settle 'unknown' ops after a lost response.
#
# RUN BY ROOT (or any principal holding a real filesvc credential) with a
# throwaway/owned scope. The sandbox must not run it.
#
#   FILESV_URL=https://filesvc.internal   base URL, no trailing slash
#   SCOPE=<32-lower-hex>                  canonical scope for the test persona
#                                         (personaID with '-' removed, lowered —
#                                         the ONLY shape fileaccess will emit)
#   TOKEN_FILE=/path/to/token-file        file containing the bearer token.
#                                         Read at invocation; never echoed,
#                                         never logged, never placed in argv.
#
# Optional:
#   PREFIX=sumi-scripts-smoke             directory prefix; a unique
#                                         per-run subdir is created.
#
# Guarantees this script keeps:
#   - touches ONLY paths under $PREFIX-<runid>/ inside the given scope
#   - the token travels inside a curl --config fd — never in argv, ps,
#     logs, or error output
#   - per-run unique idempotency keys, so a rerun in the same scope can
#     never collide with (or "replay" against) a previous run's receipts
#   - no other scopes, no public services, no enumeration
#
# Proves, in order:
#   1. keyed write commits the declared bytes (body on the wire, path in
#      the query — the previous helper's --get+--data-binary bug sent an
#      EMPTY body with the bytes stranded in the URL)
#   2. same-key resend returns the recorded RECEIPT (replayed=true, same
#      version — the 'unknown'-settlement path the ledger relies on)
#   3. same key + DIFFERENT body is refused (409 idempotency_conflict —
#      the ledger's 'diverged' path)
#   4. bytes round-trip through read (exact match, not a prefix)
#   5. stat reports the committed version and size
#   6. list sees the file under the prefix dir
#   7. keyed mkdir + replayed resend
#   8. keyed remove, then stat 404
#   9. cleanup removes only the paths this run created
#
# Exit 0 = all assertions held. Exit 1 = a contract violation (printed).
# Exit 2 = environment problem (no curl/python3, bad args, unreadable token).

set -u

FILESV_URL="${FILESV_URL:?set FILESV_URL}"
SCOPE="${SCOPE:?set SCOPE (32-lower-hex canonical scope)}"
TOKEN_FILE="${TOKEN_FILE:?set TOKEN_FILE}"
PREFIX="${PREFIX:-sumi-scripts-smoke}"

command -v curl >/dev/null || { echo "FATAL: curl required"; exit 2; }
command -v python3 >/dev/null || { echo "FATAL: python3 required for URL encoding/JSON checks"; exit 2; }

# Same client-side guard the fileaccess client enforces: refuse any scope
# that is not the canonical 32-lower-hex PAID form — prevents a mistyped
# scope from touching a wildcard or escaping the per-scope subtree.
if ! [[ "$SCOPE" =~ ^[0-9a-f]{32}$ ]]; then
  echo "FATAL: SCOPE '$SCOPE' is not canonical ([0-9a-f]{32})" >&2
  exit 2
fi
[[ -r "$TOKEN_FILE" ]] || { echo "FATAL: cannot read TOKEN_FILE" >&2; exit 2; }
TOKEN="$(tr -d '[:space:]' < "$TOKEN_FILE")"
[[ -n "$TOKEN" ]] || { echo "FATAL: TOKEN_FILE is empty" >&2; exit 2; }

# Per-run identity: unique op keys AND a unique directory, so a second run
# in the same owned scope never collides with an old receipt or path.
RUNID="$(date +%s)-$$"
BASE="${FILESV_URL%/}/v1/files/${SCOPE}"
DIR="${PREFIX}-${RUNID}"
F="$DIR/probe.txt"
D="$DIR/made"
R="$DIR/gone.txt"
PASS=0; FAIL=0

urlenc() { python3 -c "import sys,urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=''))" "$1"; }

# curl wrapper: the bearer token is supplied through a --config fd, so it
# never appears in argv, ps output, or curl's own diagnostics. Query
# params are URL-encoded into the request URI; request bodies go through
# --data-binary ONLY (never --get, which would silently move them into
# the query string — the defect this repair fixes).
req() { # req METHOD URL EXTRA_CURL_ARGS... — prints "BODY\nHTTP_CODE"
  local method="$1" url="$2"; shift 2
  curl -sS -X "$method" --max-time 30 --connect-timeout 10 \
    --config <(printf 'header = "Authorization: Bearer %s"\n' "$TOKEN") \
    "$@" -w '\n%{http_code}' "$url"
}
q() { echo "$BASE/$1?path=$(urlenc "$2")"; }

body_of() { python3 -c 'import sys; d=sys.stdin.read(); i=d.rfind("\n"); print(d[:i])'; }
code_of() { python3 -c 'import sys; d=sys.stdin.read(); i=d.rfind("\n"); print(d[i+1:])'; }
jfield() { python3 -c "import sys,json; print(json.load(sys.stdin).get('$1',''))"; }
# Optional-field contract for `replayed`: real filesvc OMITS it on a
# fresh success and sends replayed:true only on a receipt replay.
# fresh: absent or JSON false — anything else is a contract violation.
is_fresh() { python3 -c "import sys,json; r=json.load(sys.stdin); v=r.get('replayed'); print(str(v is None or v is False).lower())"; }
# replayed: exactly JSON true — a string/int/'truthy' value does NOT count.
is_replayed() { python3 -c "import sys,json; print(str(json.load(sys.stdin).get('replayed') is True).lower())"; }

check() { # check NAME EXPR
  if [[ "$2" == "true" ]]; then echo "  ok  $1"; PASS=$((PASS+1));
  else echo "  FAIL $1"; FAIL=$((FAIL+1)); fi
}

echo "filesvc acceptance: ${FILESV_URL} scope=${SCOPE} dir=${DIR} run=${RUNID}"
echo "(token read from file — never displayed, never in argv)"

# 1. Keyed write commits THE DECLARED BYTES. Body goes via --data-binary;
#    the path rides in the encoded query — they never mix.
OUT=$(req PUT "$(q write "$F")" \
  -H "If-Version: none" -H "X-Idempotency-Key: smk-$RUNID-w1" \
  -H "Content-Type: application/octet-stream" --data-binary "probe-bytes")
CODE=$(printf '%s' "$OUT" | code_of); BODY=$(printf '%s' "$OUT" | body_of)
V1=$(printf '%s' "$BODY" | jfield version)
check "keyed write committed" "$([[ "$CODE" == 200 && -n "$V1" && "$V1" != 0 ]] && printf '%s' "$BODY" | is_fresh | grep -q true && echo true || echo false)"

# 2. Same-key resend → receipt, no second commit. THE property the ledger
#    relies on when a response was lost after commit.
OUT=$(req PUT "$(q write "$F")" \
  -H "If-Version: none" -H "X-Idempotency-Key: smk-$RUNID-w1" \
  -H "Content-Type: application/octet-stream" --data-binary "probe-bytes")
CODE=$(printf '%s' "$OUT" | code_of); BODY=$(printf '%s' "$OUT" | body_of)
V2=$(printf '%s' "$BODY" | jfield version)
check "same-key resend returned receipt" "$([[ "$CODE" == 200 && "$V2" == "$V1" ]] && printf '%s' "$BODY" | is_replayed | grep -q true && echo true || echo false)"

# 3. Same key, different body → refused (the 'diverged' path).
OUT=$(req PUT "$(q write "$F")" \
  -H "If-Version: none" -H "X-Idempotency-Key: smk-$RUNID-w1" \
  -H "Content-Type: application/octet-stream" --data-binary "DIFFERENT")
CODE=$(printf '%s' "$OUT" | code_of); BODY=$(printf '%s' "$OUT" | body_of)
ERR=$(printf '%s' "$BODY" | jfield code)
check "same-key different-body refused 409 idempotency_conflict" \
  "$([[ "$CODE" == 409 && "$ERR" == *idempotency* ]] && echo true || echo false)"

# 4. Bytes round-trip — the actual committed payload, not a prefix.
OUT=$(req GET "$(q read "$F")")
CODE=$(printf '%s' "$OUT" | code_of); BODY=$(printf '%s' "$OUT" | body_of)
check "read returns the committed bytes exactly" \
  "$([[ "$CODE" == 200 && "$BODY" == "probe-bytes" ]] && echo true || echo false)"

# 5. Stat reports the committed version and size.
OUT=$(req GET "$(q stat "$F")")
CODE=$(printf '%s' "$OUT" | code_of); BODY=$(printf '%s' "$OUT" | body_of)
SV=$(printf '%s' "$BODY" | jfield version); SS=$(printf '%s' "$BODY" | jfield size)
check "stat reports committed version+size" \
  "$([[ "$CODE" == 200 && "$SV" == "$V1" && "$SS" == 11 ]] && echo true || echo false)"

# 6. List sees the file under the prefix dir.
OUT=$(req GET "$(q list "$DIR")")
CODE=$(printf '%s' "$OUT" | code_of); BODY=$(printf '%s' "$OUT" | body_of)
HAS=$(printf '%s' "$BODY" | python3 -c "import sys,json; e=json.load(sys.stdin).get('entries',[]); print(any('probe.txt' in str(x) for x in e))")
check "list sees probe.txt" "$([[ "$CODE" == 200 && "$HAS" == True ]] && echo true || echo false)"

# 7. Keyed mkdir + replay (optional-field contract on both sides).
OUT=$(req POST "$BASE/mkdir" \
  -H "Content-Type: application/json" -H "X-Idempotency-Key: smk-$RUNID-m1" \
  --data-binary "{\"path\":\"$D\"}")
M1=$(printf '%s' "$OUT" | body_of | is_fresh)
OUT=$(req POST "$BASE/mkdir" \
  -H "Content-Type: application/json" -H "X-Idempotency-Key: smk-$RUNID-m1" \
  --data-binary "{\"path\":\"$D\"}")
CODE=$(printf '%s' "$OUT" | code_of); BODY=$(printf '%s' "$OUT" | body_of)
M2=$(printf '%s' "$BODY" | is_replayed)
check "keyed mkdir replay" "$([[ "$CODE" == 200 && "$M1" == true && "$M2" == true ]] && echo true || echo false)"

# 8. Keyed remove, then stat 404.
req PUT "$(q write "$R")" \
  -H "If-Version: none" -H "X-Idempotency-Key: smk-$RUNID-w2" \
  -H "Content-Type: application/octet-stream" --data-binary "x" >/dev/null
OUT=$(req DELETE "$(q remove "$R")" \
  -H "If-Version: any" -H "X-Idempotency-Key: smk-$RUNID-r1")
CODE=$(printf '%s' "$OUT" | code_of)
OUT=$(req GET "$(q stat "$R")")
SCODE=$(printf '%s' "$OUT" | code_of)
check "keyed remove, stat 404" "$([[ "$CODE" == 200 && "$SCODE" == 404 ]] && echo true || echo false)"

# 9. Cleanup: remove ONLY the paths this run created — files first, then
#    the directories (real filesvc removes empty dirs). Best-effort;
#    leftovers are owned, timestamped, and reported — never foreign paths.
for p in "$F" "$R" "$D" "$DIR"; do
  req DELETE "$(q remove "$p")" -H "If-Version: any" >/dev/null 2>&1 || true
done
OUT=$(req GET "$(q list "$DIR")"); LC=$(printf '%s' "$OUT" | code_of)
[[ "$LC" == 404 ]] && echo "  cleanup: $DIR removed" || echo "  cleanup note: $DIR may remain (list -> $LC)"

echo
echo "RESULT: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]] || exit 1
