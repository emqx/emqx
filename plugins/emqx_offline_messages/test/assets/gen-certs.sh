#!/bin/bash

set -xe
mkdir -p certs
cd certs

openssl genrsa -out ca.key 2048
openssl req -new -x509 -days 365 -key ca.key -out ca.crt -subj "/CN=ca/O=EMQX/C=CN"

for who in mysql-server mysql-client redis-server redis-client; do
    openssl genrsa -out "$who.key" 2048
    chmod 644 "$who.key"
    openssl req -new -key "$who.key" -out "$who.csr" -subj "/CN=$who/O=EMQX/C=CN"
    openssl x509 -req -in "$who.csr" -CA ca.crt -CAkey ca.key -CAcreateserial -out "$who.crt" -days 365
    rm "$who.csr"
done
