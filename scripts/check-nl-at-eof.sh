#!/bin/bash

set -euo pipefail

cd -P -- "$(dirname -- "$0")/.."

nl_at_eof() {
    local file="$1"
    if ! [ -f $file ]; then
        return
    fi
    case "$file" in
        *.png|*rebar3)
            return
            ;;
    esac
    local lastbyte
    lastbyte="$(tail -c 1 "$file" 2>&1)"
    if [ "$lastbyte" != '' ]; then
        echo $file
    fi
}

while read -r file; do
    nl_at_eof "$file"
done < <(git ls-files)
