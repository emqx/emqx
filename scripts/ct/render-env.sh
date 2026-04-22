#!/usr/bin/env bash

## Ensures any file-backed CT secrets exist. Safe to call multiple times:
## existing secret files are preserved so re-invocations stay consistent
## with an already-running compose stack.
##
## Also emits a shell-sourceable env file so docker compose can interpolate
## the same values into services that can't read the raw files (e.g.
## elasticsearch, which enforces strict perms on ELASTIC_PASSWORD_FILE).

set -euo pipefail

cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."

SECRETS_DIR='/tmp/emqx-ci-temp-secrets'
ELASTIC_PASSWORD_FILE="${SECRETS_DIR}/elastic_password"
KIBANA_PASSWORD_FILE="${SECRETS_DIR}/kibana_password"
ENV_FILE="${SECRETS_DIR}/passwords.env"

mkdir -p "$SECRETS_DIR"

if [ ! -f "$ELASTIC_PASSWORD_FILE" ]; then
    openssl rand -hex 16 > "$ELASTIC_PASSWORD_FILE"
fi

if [ ! -f "$KIBANA_PASSWORD_FILE" ]; then
    openssl rand -hex 16 > "$KIBANA_PASSWORD_FILE"
fi

{
    echo "export ELASTIC_PASSWORD=$(cat "$ELASTIC_PASSWORD_FILE")"
    echo "export KIBANA_PASSWORD=$(cat "$KIBANA_PASSWORD_FILE")"
} > "$ENV_FILE"
