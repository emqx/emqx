#!/usr/bin/env bash

## Generates the certificate set the docker-compose test services mount from
## `.ci/docker-compose-file/certs/': a CA, a server certificate valid for every
## TLS service hostname on the compose network, and a client certificate, under
## the names `scripts/gen-test-certs.sh' uses everywhere else. None of it is
## committed. `scripts/ct/run.sh' runs this before `docker compose up'; run it
## by hand to prepare the set for a compose stack started some other way. An
## existing set is kept unless this script, the generator or the hostname list
## changed since it was made, or `--force' is given.

set -euo pipefail

cd -P -- "$(dirname -- "$0")/../.."

CERT_DIR='.ci/docker-compose-file/certs'
HOSTNAMES="$CERT_DIR/hostnames.txt"
STAMP="$CERT_DIR/.generated-by"
FILES=(cacert.pem cert.pem key.pem client-cert.pem client-key.pem)

## The server certificate's subject alternative names, one per line in
## `hostnames.txt'; `#' starts a comment.
SANS="$(sed -e 's/#.*//' -e '/^[[:space:]]*$/d' "$HOSTNAMES")"

generated_by="$(cat "$0" scripts/gen-test-certs.sh "$HOSTNAMES" | sha256sum | cut -d' ' -f1)"

up_to_date() {
    [ -f "$STAMP" ] && [ "$(cat "$STAMP")" = "$generated_by" ] || return 1
    for f in "${FILES[@]}"; do
        [ -f "$CERT_DIR/$f" ] || return 1
    done
}

if [ "${1:-}" != '--force' ] && up_to_date; then
    exit 0
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

EXTRA_DNS_SANS="$SANS" ./scripts/gen-test-certs.sh "$tmpdir" >/dev/null

## World-readable: the services run as assorted uids inside their containers.
mkdir -p "$CERT_DIR"
for f in "${FILES[@]}"; do
    install -m 644 "$tmpdir/$f" "$CERT_DIR/$f"
done
echo "$generated_by" > "$STAMP"

echo "Generated docker-compose test certificates in $CERT_DIR"
