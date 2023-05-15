#!/usr/bin/env bash
set -euo pipefail

cd -P -- "$(dirname -- "$0")/.."

all_files="$(git ls-files 'rel/i18n/*.hocon')"

./scripts/check-i18n-style.escript "$all_files"
