#!/usr/bin/env bash

set -euo pipefail

CERT_DIR="${1:-apps/emqx/etc/certs}"

mkdir -p "$CERT_DIR"

tmpdir="$(mktemp -d)"
cleanup() {
    rm -rf "$tmpdir"
}
trap cleanup EXIT

mkdir -p "$tmpdir/newcerts"
: > "$tmpdir/index.txt"
echo 1000 > "$tmpdir/serial"

cat > "$tmpdir/ca.cnf" <<'EOF'
[ req ]
distinguished_name = dn
x509_extensions = v3_ca
prompt = no

[ dn ]
C = SE
ST = Stockholm
O = EMQ
CN = RootCA

[ v3_ca ]
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer
basicConstraints = critical, CA:true
keyUsage = critical, keyCertSign, cRLSign
EOF

cat > "$tmpdir/openssl.cnf" <<'EOF'
[ ca ]
default_ca = local_ca

[ local_ca ]
database = __TMP__/index.txt
new_certs_dir = __TMP__/newcerts
certificate = __CERT_DIR__/cacert.pem
private_key = __TMP__/ca.key
serial = __TMP__/serial
default_md = sha256
default_days = 1825
policy = policy_any
copy_extensions = none

[ policy_any ]
countryName = supplied
stateOrProvinceName = supplied
organizationName = supplied
commonName = supplied

[ req ]
distinguished_name = dn
prompt = no
EOF

sed -i "s#__TMP__#$tmpdir#g" "$tmpdir/openssl.cnf"
sed -i "s#__CERT_DIR__#$CERT_DIR#g" "$tmpdir/openssl.cnf"

cat > "$tmpdir/server.req.cnf" <<'EOF'
[ req ]
distinguished_name = dn
prompt = no

[ dn ]
C = SE
ST = Stockholm
O = EMQ
CN = localhost
EOF

cat >> "$tmpdir/openssl.cnf" <<'EOF'
[ v3_server ]
basicConstraints = critical,CA:false
keyUsage = critical, digitalSignature, nonRepudiation, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = localhost
IP.1 = 127.0.0.1
IP.2 = ::1
EOF

cat > "$tmpdir/client.req.cnf" <<'EOF'
[ req ]
distinguished_name = dn
prompt = no

[ dn ]
C = SE
ST = Stockholm
O = EMQ
CN = Client
EOF

cat >> "$tmpdir/openssl.cnf" <<'EOF'
[ v3_client ]
basicConstraints = critical,CA:false
keyUsage = critical, digitalSignature, nonRepudiation, keyEncipherment
extendedKeyUsage = clientAuth
EOF

openssl genrsa -out "$tmpdir/ca.key" 2048 >/dev/null 2>&1
openssl req -x509 -new -nodes \
    -key "$tmpdir/ca.key" \
    -sha256 \
    -days 1825 \
    -out "$CERT_DIR/cacert.pem" \
    -config "$tmpdir/ca.cnf" \
    >/dev/null 2>&1

openssl genrsa -out "$CERT_DIR/key.pem" 2048 >/dev/null 2>&1
openssl req -new \
    -key "$CERT_DIR/key.pem" \
    -out "$tmpdir/server.csr" \
    -config "$tmpdir/server.req.cnf" \
    >/dev/null 2>&1
openssl ca -batch \
    -config "$tmpdir/openssl.cnf" \
    -extensions v3_server \
    -in "$tmpdir/server.csr" \
    -out "$CERT_DIR/cert.pem" \
    -notext \
    >/dev/null 2>&1

openssl genrsa -out "$CERT_DIR/client-key.pem" 2048 >/dev/null 2>&1
openssl req -new \
    -key "$CERT_DIR/client-key.pem" \
    -out "$tmpdir/client.csr" \
    -config "$tmpdir/client.req.cnf" \
    >/dev/null 2>&1
openssl ca -batch \
    -config "$tmpdir/openssl.cnf" \
    -extensions v3_client \
    -in "$tmpdir/client.csr" \
    -out "$CERT_DIR/client-cert.pem" \
    -notext \
    >/dev/null 2>&1

echo "Generated certificates in $CERT_DIR"
