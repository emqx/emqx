#!/usr/bin/env bash

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."
PROJ_ROOT="$(pwd)"

if [ -z "${1:-}" ]; then
    SCHEMA="${PROJ_ROOT}/_build/emqx/lib/emqx_dashboard/priv/www/static/schema.json"
else
    SCHEMA="$(realpath "$1")"
fi

set +e
docker run --rm -i --name spellcheck \
    -v "${PROJ_ROOT}"/scripts/spellcheck/dicts:/dicts \
    -v "$SCHEMA":/schema.json \
    ghcr.io/emqx/emqx-schema-validate:0.4.0 /schema.json

result="$?"

if [ "$result" -eq 0 ]; then
    echo "Spellcheck OK"
    exit 0
fi

echo "If this script finds a false positive (e.g. when it thinks that a protocol name is a typo),"
echo "Add the word to dictionary in scripts/spellcheck/dicts"
exit $result
