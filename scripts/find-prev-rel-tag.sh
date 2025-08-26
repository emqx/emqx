#!/usr/bin/env bash
set -euo pipefail

if [ -z "${PREV_TAG_MATCH_PATTERN:-}" ]; then
    PROFILE="${PROFILE:-${1:-}}"
    case "$PROFILE" in
        emqx-enterprise*)
            PREV_TAG_MATCH_PATTERN='e*'
            ;;
        emqx*)
            PREV_TAG_MATCH_PATTERN='v*'
            ;;
        *)
            echo "Unknown profile '$PROFILE'"
            echo "Usage: $0 PROFILE"
            exit 1
            ;;
    esac
fi

git describe --abbrev=0 --tags --match "${PREV_TAG_MATCH_PATTERN}" --exclude '*rc*' --exclude '*alpha*' --exclude '*beta*' --exclude '*docker*' --exclude '*-M*'
