#!/usr/bin/env bash

set -euo pipefail

matched_node1="$(curl --user admin:public -sf http://localhost:18083/api/v5/rules | jq --raw-output ".[0].node_metrics[] | select(.node==\"emqx@node1.emqx.io\") | .metrics.matched")"
matched_node2="$(curl --user admin:public -sf http://localhost:18084/api/v5/rules | jq --raw-output ".[0].node_metrics[] | select(.node==\"emqx@node2.emqx.io\") | .metrics.matched")"
success_node1="$(curl --user admin:public -sf http://localhost:18083/api/v5/rules | jq --raw-output ".[0].node_metrics[] | select(.node==\"emqx@node1.emqx.io\") | .metrics.\"actions.success\"")"
success_node2="$(curl --user admin:public -sf http://localhost:18084/api/v5/rules | jq --raw-output ".[0].node_metrics[] | select(.node==\"emqx@node2.emqx.io\") | .metrics.\"actions.success\"")"
webhook="$(curl -sf http://localhost:7077/counter | jq '.data')"

MATCHED_TOTAL="$(( matched_node1 + matched_node2 ))"
SUCCESS_TOTAL="$(( success_node1 + success_node2 ))"
COLLECTED_TOTAL="$webhook"

is_number() {
    re='^[0-9]+$'
    if ! [[ $2 =~ $re ]] ; then
       echo "error: $1=$2 is not a number" >&2; exit 1
    fi
}

is_number MATCHED_TOTAL "$MATCHED_TOTAL"
is_number SUCCESS_TOTAL "$SUCCESS_TOTAL"
is_number COLLECTED_TOTAL "$COLLECTED_TOTAL"

if [ "$MATCHED_TOTAL" -lt 290 ] || \
   [ "$SUCCESS_TOTAL" -lt 290 ] || \
   [ "$COLLECTED_TOTAL" -lt 290 ]; then
    echo "FAILED"
    echo "MATCHED_TOTAL=$MATCHED_TOTAL"
    echo "SUCCESS_TOTAL=$SUCCESS_TOTAL"
    echo "COLLECTED_TOTAL=$COLLECTED_TOTAL"
    exit 1
else
    echo "ALL_IS_WELL"
    exit 0
fi
