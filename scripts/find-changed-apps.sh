#!/usr/bin/env bash

set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <git-base-commit>"
    exit 1
fi

changed_files="$(git diff --ignore-blank-lines --name-only --diff-filter=ACMRTUXB "$1"...HEAD)"
changed_apps=()

for file in $changed_files; do
    if [[ "$file" == apps/* ]]; then
        app=$(echo "$file" | cut -d '/' -f 2)
        changed_apps+=("$app")
    fi
done

echo "${changed_apps[@]}" | tr ' ' '\n' | sort --unique
