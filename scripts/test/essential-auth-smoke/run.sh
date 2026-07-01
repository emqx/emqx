#!/usr/bin/env bash

## Smoke test: boot a single EMQX node in ESSENTIAL mode with the built-in
## database authenticator (seeded from bootstrap.csv) and the file authorizer
## (acl.conf), then verify authentication and authorization behaviour with a
## real MQTT client.
##
## This proves that ESSENTIAL mode ships a usable authn/authz stack: auth is
## core infrastructure and is never gated off.
##
## Usage: run.sh IMAGE_TAG   (e.g. run.sh emqx/emqx-enterprise:latest)

set -euo pipefail

IMAGE_TAG="${1:-${_EMQX_DOCKER_IMAGE_TAG:-}}"
[ -z "${IMAGE_TAG}" ] && { echo "Usage: $0 IMAGE_TAG"; exit 1; }

HERE="$(cd "$(dirname "$0")" && pwd)"
CONTAINER="emqx-essential-auth-smoke"
PYTHON_IMAGE="${PYTHON_IMAGE:-python:3-slim}"

cleanup() {
  docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

ACL_IN_CONTAINER="/opt/emqx/etc/acl-smoke.conf"
CSV_IN_CONTAINER="/opt/emqx/data/auth-bootstrap.csv"

authentication="[{
  mechanism = password_based,
  backend = built_in_database,
  user_id_type = username,
  password_hash_algorithm = {name = plain, salt_position = disable},
  bootstrap_file = \"${CSV_IN_CONTAINER}\",
  bootstrap_type = plain
}]"

echo "Starting ${CONTAINER} from ${IMAGE_TAG} ..."
docker run -d --name "${CONTAINER}" \
  -v "${HERE}/acl.conf:${ACL_IN_CONTAINER}:ro" \
  -v "${HERE}/bootstrap.csv:${CSV_IN_CONTAINER}:ro" \
  -e EMQX_FEATURES=ESSENTIAL \
  -e EMQX_NODE__NAME="emqx@127.0.0.1" \
  -e EMQX_AUTHENTICATION="${authentication}" \
  -e EMQX_AUTHORIZATION__NO_MATCH=deny \
  -e EMQX_AUTHORIZATION__SOURCES="[{type = file, enable = true, path = \"${ACL_IN_CONTAINER}\"}]" \
  "${IMAGE_TAG}" >/dev/null

echo "Waiting for the TCP listener ..."
ready=false
for _ in $(seq 1 60); do
  if docker exec "${CONTAINER}" emqx ctl listeners 2>/dev/null \
       | grep -A6 'tcp:default' | grep -qE 'running *: true'; then
    ready=true
    break
  fi
  sleep 2
done
if [ "${ready}" != true ]; then
  echo "EMQX did not become ready; dumping logs:"
  docker logs "${CONTAINER}" || true
  exit 1
fi

## Sanity: exactly one user was bootstrapped from the CSV.
user_count="$(docker exec "${CONTAINER}" emqx eval \
  'mnesia:table_info(emqx_authn_mnesia, size).' 2>/dev/null | tr -d '[:space:]')"
echo "bootstrapped authn users: ${user_count}"
if [ "${user_count}" != "1" ]; then
  echo "expected exactly 1 bootstrapped user, got '${user_count}'"
  docker logs "${CONTAINER}" || true
  exit 1
fi

## Run the MQTT client checks from a throwaway python container that shares the
## broker's network namespace (so 127.0.0.1:1883 reaches the broker), avoiding
## any dependency on the host's python environment.
echo "Running MQTT authn/authz checks ..."
docker run --rm \
  --network "container:${CONTAINER}" \
  -v "${HERE}:/smoke:ro" \
  "${PYTHON_IMAGE}" \
  sh -c "pip install --quiet --disable-pip-version-check --root-user-action=ignore paho-mqtt && python /smoke/verify.py 127.0.0.1 1883"
