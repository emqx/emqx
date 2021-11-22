#!/usr/bin/env bash

set -euo pipefail

url="$2"

# we keep this to secure OLD private repo
# even after we have disclosed new code under EMQ BSL 1.0
if [ -f 'EMQX_ENTERPRISE' ]; then
    if [[ "$url" != *emqx-enterprise* ]]; then
        echo "$(tput setaf 1)error: enterprise_code_to_non_enterprise_repo"
        exit 1
    fi
fi
