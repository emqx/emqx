#!/usr/bin/env bash

set -euo pipefail

cd -P -- "$(dirname -- "$0")/.."

nl_at_eof() {
    local file="$1"
    if ! [ -f "$file" ]; then
        return
    fi
    case "$file" in
        *.png|*rebar3)
            return
            ;;
        scripts/erlfmt)
            return
            ;;
    esac
    local lastbyte
    lastbyte="$(tail -c 1 "$file" 2>&1)"
    if [ "$lastbyte" != '' ]; then
        echo "$file"
        return 1
    fi
}

n=0
while read -r file; do
    if ! nl_at_eof "$file"; then
        echo "nl_at_eof: $file"
        n=$(( n + 1 ))
    fi
done < <(git ls-files)

exit $n
