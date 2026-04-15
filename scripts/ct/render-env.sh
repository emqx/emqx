#!/usr/bin/env bash

## Ensures any file-backed CT secrets exist. Safe to call multiple times:
## existing secret files are preserved so re-invocations stay consistent
## with an already-running compose stack.

set -euo pipefail

cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."

SECRETS_DIR='/tmp/emqx-ci-temp-secrets'
ELASTIC_PASSWORD_FILE="${SECRETS_DIR}/elastic_password"
KIBANA_PASSWORD_FILE="${SECRETS_DIR}/kibana_password"


mkdir -p "$SECRETS_DIR"

if [ ! -f "$ELASTIC_PASSWORD_FILE" ]; then
    openssl rand -hex 16 > "$ELASTIC_PASSWORD_FILE"
fi

if [ ! -f "$KIBANA_PASSWORD_FILE" ]; then
    openssl rand -hex 16 > "$KIBANA_PASSWORD_FILE"
fi
