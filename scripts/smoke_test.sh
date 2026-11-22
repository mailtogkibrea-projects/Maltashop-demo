#!/usr/bin/env bash
# Black-box check of the running stack, used by CI and handy locally.
# Fails (non-zero exit) unless the three saga outcomes are exactly as designed.
set -euo pipefail
BASE=${BASE:-http://localhost:8001}

place() {  # $1=sku $2=qty $3=token -> prints the order id
  curl -fsS -X POST "$BASE/orders" \
    -H 'Content-Type: application/json' \
    -H "X-Correlation-ID: smoke-$RANDOM" \
    -d "$(printf '{"customer_email":"smoke@example.mt","items":[{"sku":"%s","quantity":%s,"unit_price_cents":1000}],"card_token":"%s"}' "$1" "$2" "$3")" \
    | grep -o '"order_id":"[^"]*"' | head -1 | cut -d'"' -f4
}

wait_for() {  # $1=order id  $2=expected status
  for _ in $(seq 1 30); do
    if curl -fsS "$BASE/orders/$1" | grep -q "\"status\":\"$2\""; then
      echo "ok: order $1 reached $2"
      return 0
    fi
    sleep 1
  done
  echo "FAIL: order $1 did not reach $2" >&2
  curl -sS "$BASE/orders/$1" >&2 || true
  return 1
}

wait_for "$(place SKU-KETTLE 1 tok_visa)" CONFIRMED
wait_for "$(place SKU-LAMP 99 tok_visa)" CANCELLED
wait_for "$(place SKU-KETTLE 1 tok_declined)" CANCELLED
echo "smoke test passed"
