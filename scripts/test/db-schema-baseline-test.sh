#!/usr/bin/env bash

## Boot a release and compare the mria tables it creates with the
## baseline committed for the current db_schema version
## (apps/emqx_machine/priv/db_schema/<version>/tables.eterm).
##
## The baseline is one half of what EMQX_DB_SCHEMA_VSN stands for; the
## other half, the set of transaction BPAPI versions that a replicant
## passes to a core by name, is checked by scripts/check-db-schema.escript.
##
## Usage:
##   scripts/test/db-schema-baseline-test.sh            # check
##   scripts/test/db-schema-baseline-test.sh --update   # rewrite the baseline from the booted node
##
## Needs a built release under _build/$PROFILE/rel/emqx. Set
## EMQX_NODE_NAME and the usual EMQX_* overrides to run next to another
## node.

set -euo pipefail

cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."

PROFILE="${PROFILE:-emqx-enterprise}"
REL="_build/${PROFILE}/rel/emqx"
BIN="${REL}/bin/emqx"
BASELINE_DIR="apps/emqx_machine/priv/db_schema"

UPDATE=false
case "${1:-}" in
    '') ;;
    --update) UPDATE=true ;;
    *) echo "Usage: $0 [--update]"; exit 1 ;;
esac

if [ ! -x "$BIN" ]; then
    echo "ERROR: ${BIN} not found; build the release first (make ${PROFILE}-rel)"
    exit 1
fi

export EMQX_NODE_NAME="${EMQX_NODE_NAME:-emqx-db-schema-test@127.0.0.1}"

stop_node() {
    "$BIN" stop >/dev/null 2>&1 || true
}
trap stop_node EXIT

"$BIN" start
attempts=60
until "$BIN" ping >/dev/null 2>&1; do
    attempts=$((attempts - 1))
    if [ "$attempts" -eq 0 ]; then
        echo "ERROR: node did not come up"
        exit 1
    fi
    sleep 1
done

## eval prints the return value of the expression, a string with its quotes.
VSN="$("$BIN" eval 'emqx_machine_db_schema:vsn().' | tr -d '"')"
FILE="${BASELINE_DIR}/${VSN}/tables.eterm"

if $UPDATE; then
    mkdir -p "${BASELINE_DIR}/${VSN}"
    ## The node writes the file, so no io output has to be separated from the return value.
    RESULT="$("$BIN" eval "file:write_file(\"$(pwd)/${FILE}\", emqx_machine_db_schema:format_tables()).")"
    if [ "$RESULT" != "ok" ]; then
        echo "ERROR: cannot write ${FILE}: ${RESULT}"
        exit 1
    fi
    ./scripts/erlfmt -w "$FILE"
    echo "Wrote ${FILE}"
    exit 0
fi

if [ ! -f "$FILE" ]; then
    echo "ERROR: ${FILE} not found; run: $0 --update"
    exit 1
fi

## check/1 takes the repository file, not the copy in the release priv
## dir, so a freshly updated baseline is checked without a rebuild.
RESULT="$("$BIN" eval "emqx_machine_db_schema:check(\"$(pwd)/${FILE}\").")"
if [ "$RESULT" = "ok" ]; then
    echo "OK: db_schema ${VSN} tables match ${FILE}"
    exit 0
fi

echo "ERROR: the tables the release creates differ from ${FILE}:"
echo "$RESULT"
echo
echo "A changed table definition needs a new db_schema version."
echo "Bump EMQX_DB_SCHEMA_VSN in apps/emqx/include/emqx_release.hrl, then run:"
echo "  ./scripts/check-db-schema.escript --update"
echo "  $0 --update"
exit 1
