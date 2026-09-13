#!/usr/bin/env bash
# Registers the Debezium Postgres CDC connector with the Kafka Connect REST API.
# Run this after `docker compose up -d postgres kafka connect` and once Connect
# is reachable (the script polls for readiness).
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONFIG_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/debezium/postgres-connector.json"

echo "Waiting for Kafka Connect at ${CONNECT_URL} ..."
for i in $(seq 1 60); do
  if curl -s -o /dev/null -w "%{http_code}" "${CONNECT_URL}/connectors" | grep -q "200"; then
    echo "Kafka Connect is up."
    break
  fi
  sleep 2
  if [ "$i" -eq 60 ]; then
    echo "Timed out waiting for Kafka Connect." >&2
    exit 1
  fi
done

echo "Registering connector from ${CONFIG_FILE} ..."
HTTP_CODE=$(curl -s -o /tmp/connector-response.json -w "%{http_code}" \
  -X POST -H "Content-Type: application/json" \
  --data @"${CONFIG_FILE}" \
  "${CONNECT_URL}/connectors")

if [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "200" ]; then
  echo "Connector registered successfully."
elif [ "$HTTP_CODE" = "409" ]; then
  echo "Connector already exists — leaving it as-is."
else
  echo "Failed to register connector (HTTP ${HTTP_CODE}):"
  cat /tmp/connector-response.json
  exit 1
fi

echo "Current connector status:"
curl -s "${CONNECT_URL}/connectors/accounting-postgres-connector/status" | python3 -m json.tool || true
