#!/usr/bin/env bash
set -euo pipefail

cd -P -- "$(dirname -- "$0")/.."

./scripts/check-i18n-style.escript rel/i18n/*.hocon
