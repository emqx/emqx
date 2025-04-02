#!/usr/bin/env bash

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."
PROJ_ROOT="$(pwd)"

NUM_JOBS=10
while getopts "j:" FLAG; do
  case "$FLAG" in
    j)
      NUM_JOBS="${OPTARG}"
      ;;
    *)
      echo "unknown flag: $FLAG"
      ;;
  esac
done
shift $((OPTIND-1))

if [ -z "${1:-}" ]; then
    SCHEMA="${PROJ_ROOT}/_build/docgen/emqx-enterprise/schema-en.json"
else
    SCHEMA="$(realpath "$1")"
fi

if ! [ -f "$SCHEMA" ]; then
  echo "Schema file $SCHEMA does not exist; did you forget to run 'make emqx{,-enterprise}' ?"
  exit 1
fi

if [[ -t 1 ]];
then
  DOCKER_TERMINAL_OPT="-t"
else
  DOCKER_TERMINAL_OPT=""
fi

set +e
# shellcheck disable=SC2086
docker run --rm -i ${DOCKER_TERMINAL_OPT} --name spellcheck \
    -v "${PROJ_ROOT}"/scripts/spellcheck/dicts:/dicts \
    -v "$SCHEMA":/schema.json \
    ghcr.io/emqx/emqx-schema-validate:0.5.1 -j "${NUM_JOBS}" /schema.json

result="$?"

if [ "$result" -eq 0 ]; then
    echo "Spellcheck OK"
    exit 0
fi

echo "If this script finds a false positive (e.g. when it thinks that a protocol name is a typo),"
echo "Add the word to dictionary in scripts/spellcheck/dicts"
exit $result
