# Certificates for the docker-compose test services

Nothing in this directory is committed. `scripts/ct/gen-compose-certs.sh`
generates the set, and `scripts/ct/run.sh` runs it before `docker compose up`.
To prepare the set for a compose stack started some other way, run the script
by hand from the repository root.

The set, named as `scripts/gen-test-certs.sh` names its output everywhere:

- `cacert.pem`: the CA. Test suites that verify a service's certificate trust
  this.
- `cert.pem`, `key.pem`: the certificate every TLS service presents. Its
  subject alternative names cover every service hostname on the compose network
  and `toxiproxy`; the list lives in `gen-compose-certs.sh`.
- `client-cert.pem`, `client-key.pem`: a client certificate signed by the CA,
  for services that require one from their clients.

The CA private key is not kept. To change a name or add one, edit the list in
`gen-compose-certs.sh` and rerun it; a set made by an older version of the
script is regenerated automatically.

Kafka and Cassandra do not use this set: their `ssl_cert_gen` service builds
Java keystores into `/tmp/emqx-ci/emqx-shared-secret` at start.
