#!/usr/bin/env bash

## Cluster smoke test. Assumes a two-node cluster started by
## scripts/test/start-two-nodes-in-docker.sh is already running (containers
## node1.emqx.io, node2.emqx.io and haproxy on the emqx.io docker network).
##
## Verifies clustering at the MQTT level:
##   1. a subscription made on one node is replicated into the cluster-wide
##      routing table and is visible from BOTH nodes (cross-node routing);
##   2. a message published on a separate connection (typically the other node,
##      via HAProxy round-robin) is delivered to that subscriber end to end.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
NET="${NET:-emqx.io}"
NODE1="${NODE1:-node1.emqx.io}"
NODE2="${NODE2:-node2.emqx.io}"
PYTHON_IMAGE="${PYTHON_IMAGE:-python:3-slim}"
TOPIC="cluster/smoke/1"
PAYLOAD="cluster-smoke-payload"
SUB="cluster-smoke-sub"

cleanup() {
  docker rm -f "${SUB}" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

## Start the long-lived subscriber (installs paho-mqtt, then subscribes).
docker run -d --name "${SUB}" --net "${NET}" -v "${HERE}:/smoke:ro" "${PYTHON_IMAGE}" \
  sh -c "pip install --quiet --disable-pip-version-check --root-user-action=ignore paho-mqtt \
         && python /smoke/subscriber.py haproxy 1883 ${TOPIC}" >/dev/null

echo "Waiting for the subscriber to connect ..."
connected=false
for _ in $(seq 1 60); do
  if docker logs "${SUB}" 2>&1 | grep -q "SUBSCRIBER CONNECTED rc=0"; then
    connected=true
    break
  fi
  sleep 2
done
if [ "${connected}" != true ]; then
  echo "subscriber failed to connect; logs:"; docker logs "${SUB}" || true; exit 1
fi

## Assert the route is replicated cluster-wide: it must be visible from BOTH
## nodes, including the node the subscriber is NOT connected to.
route_count_on() {
  docker exec "$1" emqx eval \
    "length(emqx_router:lookup_routes(<<\"${TOPIC}\">>))." 2>/dev/null | tr -d '[:space:]'
}

for node in "${NODE1}" "${NODE2}"; do
  ok=false
  for _ in $(seq 1 30); do
    n="$(route_count_on "${node}")"
    if [ -n "${n}" ] && [ "${n}" -ge 1 ] 2>/dev/null; then ok=true; break; fi
    sleep 1
  done
  if [ "${ok}" != true ]; then
    echo "FAIL: route for ${TOPIC} not visible on ${node} (cross-node routing broken)"
    exit 1
  fi
  echo "PASS: route for ${TOPIC} visible on ${node}"
done

## Publish on a separate connection and confirm end-to-end delivery.
docker run --rm --net "${NET}" -v "${HERE}:/smoke:ro" "${PYTHON_IMAGE}" \
  sh -c "pip install --quiet --disable-pip-version-check --root-user-action=ignore paho-mqtt \
         && python /smoke/publisher.py haproxy 1883 ${TOPIC} ${PAYLOAD}"

echo "Waiting for delivery to the subscriber ..."
delivered=false
for _ in $(seq 1 30); do
  if docker logs "${SUB}" 2>&1 | grep -q "RECEIVED ${TOPIC} ${PAYLOAD}"; then
    delivered=true
    break
  fi
  sleep 1
done
if [ "${delivered}" != true ]; then
  echo "FAIL: published message was not delivered to the subscriber"
  echo "--- subscriber logs ---"; docker logs "${SUB}" || true
  exit 1
fi
echo "PASS: message delivered across the cluster"
echo "ALL PASSED"
