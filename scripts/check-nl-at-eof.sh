#!/usr/bin/env bash

set -euo pipefail

cd -P -- "$(dirname -- "$0")/.."

nl_at_eof() {
    local file="$1"
    if ! [ -f "$file" ]; then
        return
    fi
    case "$file" in
        *.png|*.jpg|*.jpeg|*.gif|*.bmp|*.ico|*.svg|*rebar3)
            return
            ;;
        scripts/erlfmt)
            return
            ;;
        *.jks|*.p12|*.pem|*.crt|*.key|*.p7b|*.p7c)
            return
            ;;
        *.avro|*.parquet)
            return
            ;;
        *.tar.gz|*.tar|*.zip|*.gz|*.bz2|*.xz|*.7z)
            return
            ;;
        *.so|*.dll|*.dylib|*.a|*.lib)
            return
            ;;
        *.exe|*.bin|*.app|*.dmg|*.pkg|*.deb|*.rpm)
            return
            ;;
        *.db|*.sqlite|*.sqlite3)
            return
            ;;
        *.woff|*.woff2|*.ttf|*.eot)
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
        # shellcheck disable=SC1003
        sed -i -e '$a\' "$file"
        n=$(( n + 1 ))
    fi
done < <(git ls-files)

exit $n
