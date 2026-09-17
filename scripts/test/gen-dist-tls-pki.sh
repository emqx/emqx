#!/usr/bin/env bash

# Generate a CA, a server certificate and a client certificate for the Erlang
# distribution over TLS, plus the option file that names them.
#
# Usage: gen-dist-tls-pki.sh <dir> [extra-dns-sans]
#
# <dir> is created relative to the repository root, because this script cd's
# there (and so does start-two-nodes-in-docker.sh, which resolves -S the same
# way). Pass "tmp/dist-tls" from any working directory.
#
# <extra-dns-sans> defaults to the node names used by start-two-nodes-in-docker.sh.
# The server certificate has to carry them: EMQX 5.4 runs OTP 25, whose
# inet_tls_dist:do_setup_connect/8 prepends {server_name_indication, Address} to
# the client options (OTP 25 ssl/inet_tls_dist.erl:588), so an option file
# cannot disable the host name check there and the check must pass instead.
#
# The result is meant for scripts/test/start-two-nodes-in-docker.sh -S <dir>:
# both nodes mount the directory read-only at /mnt/dist-tls and use
# ssl_dist.conf from it, so that two images shipping different CAs (a mixed
# version test) can still verify each other.
set -euo pipefail

cd -P -- "$(dirname -- "$0")/../../"

DIR="${1:?usage: $0 <dir> [extra-dns-sans]}"
EXTRA_DNS_SANS="${2:-node1.emqx.io,node2.emqx.io}"

./scripts/gen-emqx-default-certs.sh "$DIR" "$EXTRA_DNS_SANS"

cat > "$DIR/ssl_dist.conf" <<'EOF'
%% Mounted at /mnt/dist-tls by scripts/test/start-two-nodes-in-docker.sh -S.
%% Both entries verify the peer certificate, and the server requires the client
%% to present one, so this file cannot be used with an EMQX version that has no
%% client certificate (see the release notes of the default configuration).
%% The client entry deliberately has no {server_name_indication, disable}: the
%% 5.4 image ignores it (OTP 25 prepends the peer host), and the generated
%% server certificate carries both node names instead.
[{server,
  [
   {certfile, "/mnt/dist-tls/cert.pem"},
   {keyfile, "/mnt/dist-tls/key.pem"},
   {cacertfile, "/mnt/dist-tls/cacert.pem"},
   {verify, verify_peer},
   {fail_if_no_peer_cert, true}
  ]},
 {client,
  [
   {certfile, "/mnt/dist-tls/client-cert.pem"},
   {keyfile, "/mnt/dist-tls/client-key.pem"},
   {cacertfile, "/mnt/dist-tls/cacert.pem"},
   {verify, verify_peer}
  ]}].
EOF

# The node containers run as a non-root user, and the generator writes the
# private keys as 0600. These are throwaway test keys, so making them readable
# (and the directory traversable) is fine here.
chmod -R a+rX "$DIR"

echo "Generated $DIR/ssl_dist.conf and the certificates it names"
