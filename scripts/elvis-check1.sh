#!/usr/bin/env bash

file="$1"

if [ ! -f "$file" ]; then
    # file is deleted, skip
    exit
fi

if [[ $file != *.erl ]]; then
    # not .erl file
    exit
fi

echo "$file ..."

./elvis rock "$file" -c elvis.config
