#!/usr/bin/env bash
set -euo pipefail

# Import labelers from a CSV into the running labeler-relay admin API.
# Usage: ./scripts/import-labelers.sh <csv_file> [relay_url] [admin_token]

CSV_FILE="${1:?Usage: $0 <csv_file> [relay_url] [admin_token]}"
RELAY_URL="${2:-http://localhost:8080}"
ADMIN_TOKEN="${3:-${LABELER_RELAY_ADMIN_TOKEN:-}}"

if [[ -z "$ADMIN_TOKEN" ]]; then
  echo "error: admin token required (arg 3 or LABELER_RELAY_ADMIN_TOKEN env)" >&2
  exit 1
fi

if [[ ! -f "$CSV_FILE" ]]; then
  echo "error: file not found: $CSV_FILE" >&2
  exit 1
fi

success=0
failed=0
skipped=0
total=0

# Python one-liner to properly parse CSV and emit only Declared DIDs, one per line.
dids=$(python3 -c "
import csv, sys
with open(sys.argv[1]) as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row.get('visibility', '').strip() == 'Declared':
            did = row.get('did', '').strip()
            if did:
                print(did)
" "$CSV_FILE")

total_dids=$(echo "$dids" | wc -l | tr -d ' ')
echo "Importing $total_dids Declared labelers from $CSV_FILE"
echo "Relay: $RELAY_URL"
echo "---"

while IFS= read -r did; do
  total=$((total + 1))

  if [[ -z "$did" ]]; then
    skipped=$((skipped + 1))
    continue
  fi

  response=$(curl -s -w "\n%{http_code}" -X POST \
    "${RELAY_URL}/admin/labelers" \
    -H "Authorization: Bearer ${ADMIN_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"did\":\"${did}\"}" 2>&1)

  http_code=$(echo "$response" | tail -1)
  body=$(echo "$response" | sed '$d')

  if [[ "$http_code" == "201" ]]; then
    success=$((success + 1))
    printf "[%3d/%d] OK  %s\n" "$total" "$total_dids" "$did"
  else
    failed=$((failed + 1))
    error=$(echo "$body" | python3 -c "import sys,json; print(json.load(sys.stdin).get('error','unknown'))" 2>/dev/null || echo "$body")
    printf "[%3d/%d] ERR %s — %s\n" "$total" "$total_dids" "$did" "$error"
  fi

  # Small delay to avoid hammering DID resolution.
  sleep 0.1
done <<< "$dids"

echo "---"
echo "Done: $success registered, $failed failed, $skipped skipped (of $total total)"
