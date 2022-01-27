#!/usr/bin/env bash

## run this script once a year

set -euo pipefail

THIS_YEAR="$(date +'%Y')"

fix_copyright_year() {
    local file="$1"
    local copyright_line
    copyright_line="$(head -2 "$file" | grep -E "Copyright\s\(c\)\s.+\sEMQ" || true)"
    if [ -z "$copyright_line" ]; then
        ## No Copyright info, it is not intended to add one from this script
        echo "Ignored $file"
        return 0
    fi
    local begin_year
    begin_year="$(echo "$copyright_line" | sed -E 's#(.*Copyright \(c\)) (20..).*#\2#g')"
    if [ "$begin_year" = "$THIS_YEAR" ]; then
        ## new file added this year
        return 0
    fi
    sed -E "s#(.*Copyright \(c\) 20..)(-20.. | )(.*)#\1-$THIS_YEAR \3#g" -i "$file"
}

while read -r file; do
    if [[ $file != *.erl ]] && \
       [[ $file != *.ex ]] && \
       [[ $file != *.hrl ]] && \
       [[ $file != *.proto ]]; then
        ## Ignore other file
        continue
    fi
    fix_copyright_year "$file"
done < <(git ls-files)

fix_copyright_year apps/emqx/NOTICE
fix_copyright_year NOTICE
