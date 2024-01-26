#!/usr/bin/env bash

set -euo pipefail

CONVERT_DATE="$(date -d "+4 years" '+%Y-%m-%d')"
NEWTEXT="Change Date:          $CONVERT_DATE"

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
