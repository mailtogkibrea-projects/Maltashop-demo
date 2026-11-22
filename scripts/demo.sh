#!/usr/bin/env bash
# Runs the three saga scenarios against a running stack and prints, for each one,
# the final order state and every log line that carries its correlation ID.
# Usage:  docker compose up --build -d --wait && ./scripts/demo.sh
set -euo pipefail
BASE=${BASE:-http://localhost:8001}

scenario() {  # $1=label  $2=sku  $3=quantity  $4=card token
  local cid="demo-$1-$(date +%s)"
  local body
  body=$(printf '{"customer_email":"maria@example.mt","items":[{"sku":"%s","quantity":%s,"unit_price_cents":2999}],"card_token":"%s"}' "$2" "$3" "$4")

  echo
  echo "=================================================================="
  echo "Scenario: $1    correlation id: $cid"
  echo "=================================================================="
  echo "> POST $BASE/orders"
  echo "> $body"
  local response
  response=$(curl -sS -i -X POST "$BASE/orders" \
    -H 'Content-Type: application/json' \
    -H "X-Correlation-ID: $cid" \
    -H "Idempotency-Key: $cid" \
    -d "$body")
  echo "$response"

  local order_id
  order_id=$(echo "$response" | grep -o '"order_id":"[^"]*"' | head -1 | cut -d'"' -f4)
  sleep 2   # let the saga finish (it is asynchronous)

  echo
  echo "> GET $BASE/orders/$order_id"
  curl -sS "$BASE/orders/$order_id"
  echo
  echo
  echo "> log lines carrying $cid (all services):"
  docker compose logs --no-log-prefix 2>/dev/null | grep "\"$cid\"" || true
}

scenario happy-path      SKU-KETTLE 1 tok_visa       # -> CONFIRMED
scenario out-of-stock    SKU-LAMP   9 tok_visa       # -> CANCELLED (no payment attempted)
scenario declined-card   SKU-KETTLE 2 tok_declined   # -> CANCELLED (stock released)

echo
echo "> Final stock levels (declined-card order must have been released):"
curl -sS http://localhost:8002/stock
echo
