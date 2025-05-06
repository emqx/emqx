#!/usr/bin/env bash

## Used in CI. this script enforces the naming pattern for the changes files.
## Expects two environment variables to be set: BEFORE_REF and AFTER_REF

set -euo pipefail

cd -P -- "$(dirname -- "$0")/.."

added_files=$(git diff --name-only --diff-filter=A "$BEFORE_REF" "$AFTER_REF" -- changes/)
error=0
for file in $added_files; do
  base=$(basename "$file")
  # If the filename starts with fix-, feat-, or perf- then enforce the naming pattern.
  if [[ "$base" =~ ^(fix|feat|perf)- ]]; then
    # The valid pattern is:
    # changes/{ce|ee}/{fix|feat|perf}-PRNUMBER.en.md
    if [[ ! "$file" =~ ^changes/(ce|ee)/(fix|feat|perf)-[0-9]+\.en\.md$ ]]; then
      echo "Error: '$file' does not follow the required pattern 'changes/{ce|ee}/{fix|feat|perf}-PRNUMBER.en.md'" >&2
      error=1
    fi
  fi
done

exit $error
