#!/usr/bin/env bash

set -euo pipefail

[ $# -ne 2 ] && { echo "Usage: $0 ip port"; exit 1; }

IP=$1
PORT=$2
URL="http://$IP:$PORT/status"

## Check if EMQX is responding
ATTEMPTS=10
while ! curl "$URL" >/dev/null 2>&1; do
    if [ $ATTEMPTS -eq 0 ]; then
        echo "emqx is not responding on $URL"
        exit 1
    fi
    sleep 5
    ATTEMPTS=$((ATTEMPTS-1))
done

## Check if the API docs are available
API_DOCS_URL="http://$IP:$PORT/api-docs/index.html"
API_DOCS_STATUS="$(curl -s -o /dev/null -w "%{http_code}" "$API_DOCS_URL")"
if [ "$API_DOCS_STATUS" != "200" ]; then
    echo "emqx is not responding on $API_DOCS_URL"
    exit 1
fi

## Check if the swagger.json contains hidden fields
## fail if it does
SWAGGER_JSON_URL="http://$IP:$PORT/api-docs/swagger.json"
## assert swagger.json is valid json
JSON="$(curl -s "$SWAGGER_JSON_URL")"
echo "$JSON" | jq . >/dev/null

if [ "${EMQX_SMOKE_TEST_CHECK_HIDDEN_FIELDS:-yes}" = 'yes' ]; then
    ## assert swagger.json does not contain trie_compaction (which is a hidden field)
    if echo "$JSON" | grep -q trie_compaction; then
        echo "swagger.json contains hidden fields"
        exit 1
    fi
fi
