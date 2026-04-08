#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CERT_DIR="$SCRIPT_DIR/certs"

mkdir -p "$CERT_DIR"
find "$CERT_DIR" -mindepth 1 ! -name '.gitignore' -delete
cd "$CERT_DIR"

openssl genrsa -out ca.key 2048 >/dev/null 2>&1
openssl req -new -x509 -days 365 -key ca.key -out ca.crt -subj "/CN=ca/O=EMQX/C=CN" >/dev/null 2>&1

for who in mysql-server mysql-client redis-server redis-client; do
    openssl genrsa -out "$who.key" 2048 >/dev/null 2>&1
    chmod 600 "$who.key"
    openssl req -new -key "$who.key" -out "$who.csr" -subj "/CN=$who/O=EMQX/C=CN" >/dev/null 2>&1
    openssl x509 -req -in "$who.csr" -CA ca.crt -CAkey ca.key -CAcreateserial -out "$who.crt" -days 365 >/dev/null 2>&1
    rm -f "$who.csr"
done
