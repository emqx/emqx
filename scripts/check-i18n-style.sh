#!/usr/bin/env bash
set -euo pipefail

cd -P -- "$(dirname -- "$0")/.."

all_files="$(git ls-files '*i18n*.conf')"

./scripts/check-i18n-style.escript "$all_files"
