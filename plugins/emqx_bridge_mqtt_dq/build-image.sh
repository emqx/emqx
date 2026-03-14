#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  plugins/emqx_bridge_mqtt_dq/build-image.sh <emqx-version> [image-tag]

Arguments:
  emqx-version  Required. Used as EMQX_VERSION build arg and base image tag.
  image-tag     Optional. Defaults to emqx/emqx:<emqx-version>-dq-<plugin-version>

Examples:
  plugins/emqx_bridge_mqtt_dq/build-image.sh 6.1.1
  plugins/emqx_bridge_mqtt_dq/build-image.sh 6.1.1 myrepo/emqx-dq:dev
EOF
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
    usage >&2
    exit 1
fi

EMQX_VERSION="$1"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
PLUGIN_VERSION="$(tr -d '[:space:]' < "$SCRIPT_DIR/VERSION")"
IMAGE_TAG="${2:-emqx/emqx:${EMQX_VERSION}-dq-${PLUGIN_VERSION}}"

exec docker build \
    --build-arg "EMQX_VERSION=${EMQX_VERSION}" \
    --build-arg "PLUGIN_VSN=${PLUGIN_VERSION}" \
    -t "${IMAGE_TAG}" \
    -f "$SCRIPT_DIR/Dockerfile" \
    "$ROOT_DIR"
