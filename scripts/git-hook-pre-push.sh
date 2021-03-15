#!/usr/bin/env bash

set -euo pipefail

url="$2"

if [ -f 'EMQX_ENTERPRISE' ]; then
    if [[ "$url" != *emqx-enterprise* ]]; then
        echo "$(tput setaf 1)error: enterprise_code_to_non_enterprise_repo"
        exit 1
    fi
fi
