#!/usr/bin/env bash

set -euo pipefail

CONVERT_DATE="${1:-$(date -d "+4 years" '+%Y-%m-%d')}"

update() {
    local file="$1"
    sed -E "s#(^Change Date: *)(.*)#\1$CONVERT_DATE#g" -i "$file"
}

while read -r file; do
    if [[ $file != *BSL.txt ]]; then
        ## Ignore other files
        continue
    fi
    update "$file"
done < <(git ls-files)
