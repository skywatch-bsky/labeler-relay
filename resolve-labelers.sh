#!/usr/bin/env bash
set -euo pipefail

INPUT="labelwatch-labelers.csv"
OUTPUT="labeler-endpoints.csv"

echo "name,did,labeler_endpoint,status" > "$OUTPUT"

tail -n +2 "$INPUT" | while IFS=, read -r name did rest; do
  # URL-safe DID
  endpoint=""
  status=""

  # Try plc.directory first
  resp=$(curl -sf "https://plc.directory/${did}" -H 'Accept: application/json' 2>/dev/null || true)

  if [ -n "$resp" ]; then
    endpoint=$(echo "$resp" | python3 -c "
import sys, json
try:
    doc = json.load(sys.stdin)
    services = doc.get('service', [])
    # Mirror the Go resolver (internal/firehose/resolve.go): match on type
    # first, then fall back to an ID containing '#atproto_labeler'.
    for svc in services:
        if svc.get('type') in ('AtprotoLabeler', 'atproto_labeler'):
            print(svc.get('serviceEndpoint', ''))
            break
    else:
        for svc in services:
            if '#atproto_labeler' in svc.get('id', ''):
                print(svc.get('serviceEndpoint', ''))
                break
except:
    pass
" 2>/dev/null || true)

    if [ -n "$endpoint" ]; then
      status="found"
    else
      status="no_labeler_service"
    fi
  else
    status="resolve_failed"
  fi

  # Escape commas in name
  safe_name=$(echo "$name" | sed 's/,/;/g')
  echo "${safe_name},${did},${endpoint},${status}" >> "$OUTPUT"
  echo "  ${status}: ${did} -> ${endpoint:-none}"

  # Be polite to PLC directory
  sleep 0.1
done

echo ""
echo "Done. Results in ${OUTPUT}"
echo "Summary:"
grep -c "found" "$OUTPUT" || echo "0 found"
grep -c "no_labeler_service" "$OUTPUT" || echo "0 missing"
grep -c "resolve_failed" "$OUTPUT" || echo "0 failed"
