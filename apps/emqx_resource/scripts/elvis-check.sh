#!/bin/bash

set -euo pipefail

ELVIS_VERSION='1.0.0-emqx-2'

elvis_version="${2:-$ELVIS_VERSION}"

echo "elvis -v: $elvis_version"

if [ ! -f ./elvis ] || [ "$(./elvis -v | grep -oE '[1-9]+\.[0-9]+\.[0-9]+\-emqx-[0-9]+')" != "$elvis_version" ]; then
    curl  -fLO "https://github.com/emqx/elvis/releases/download/$elvis_version/elvis"
    chmod +x ./elvis
fi

./elvis rock --config elvis.config

