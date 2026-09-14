#!/usr/bin/env bash

## Fails when the tree holds a private key that is not on the allowlist.
##
## A private key is a PEM private key block anywhere in a text file, inline in
## source included, or a file with a keystore extension. Test suites generate
## the keys they need at run time: `emqx_common_test_helpers:test_cert/1' and
## `mock_server_certs/2' in suites, `scripts/gen-test-certs.sh' in scripts,
## `scripts/ct/gen-compose-certs.sh' for the docker-compose services. The
## allowlist names the few fixtures that cannot be generated, each with the
## reason. An allowlisted file that no longer holds a key fails the check too,
## so the list does not outlive what it describes.

set -euo pipefail

cd -P -- "$(dirname -- "$0")/.."

ALLOWLIST='scripts/private-keys-allowlist.txt'
PEM_MARKER='-----BEGIN ([A-Z0-9]+ )*PRIVATE KEY( BLOCK)?-----'
KEYSTORE_EXT='\.(jks|p12|pfx|keystore|jceks|bks|ppk)$'

found="$(
    {
        git grep -l -I -E -e "$PEM_MARKER" || true
        git ls-files | grep -iE "$KEYSTORE_EXT" || true
    } | sort -u
)"
allowed="$(sed -e 's/#.*//' -e 's/[[:space:]]*$//' -e '/^$/d' "$ALLOWLIST" | sort -u)"

new="$(comm -23 <(echo "$found") <(echo "$allowed") | sed '/^$/d')"
stale="$(comm -13 <(echo "$found") <(echo "$allowed") | sed '/^$/d')"

rc=0
if [ -n "$new" ]; then
    echo "These files hold a private key and are not on the allowlist ($ALLOWLIST):"
    while IFS= read -r f; do echo "    $f"; done <<< "$new"
    echo "Generate the key at run time instead (see the header of $0)."
    echo "If the file must be committed, add it to $ALLOWLIST with the reason."
    rc=1
fi
if [ -n "$stale" ]; then
    echo "These files are on the allowlist but hold no private key; remove them from $ALLOWLIST:"
    while IFS= read -r f; do echo "    $f"; done <<< "$stale"
    rc=1
fi
exit $rc
