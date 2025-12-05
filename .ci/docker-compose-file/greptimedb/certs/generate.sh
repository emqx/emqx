#!/usr/bin/env bash

set -exuo pipefail

rm -f serial index.txt

touch index.txt
echo 1000 > serial

openssl req -config openssl.cnf \
        -key server.key \
        -new -sha256 -out server.csr

openssl ca -config openssl.cnf \
        -batch \
        -extensions req_ext \
        -days 3750 -notext -md sha256 \
        -in server.csr \
        -out server.crt
