#!/usr/bin/env bash

set -euo pipefail

[ $# -ne 2 ] && { echo "Usage: $0 ip port"; exit 1; }

IP=$1
PORT=$2
URL="http://$IP:$PORT/status"

ATTEMPTS=10
while ! curl "$URL" >/dev/null 2>&1; do
    if [ $ATTEMPTS -eq 0 ]; then
        echo "emqx is not responding on $URL"
        exit 1
    fi
    sleep 5
    ATTEMPTS=$((ATTEMPTS-1))
done
