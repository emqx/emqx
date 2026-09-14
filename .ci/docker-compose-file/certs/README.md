# Certificates for the docker-compose test services

Nothing in this directory is committed. `scripts/ct/gen-compose-certs.sh`
generates the set, and `scripts/ct/run.sh` runs it before `docker compose up`.
To prepare the set for a compose stack started some other way, run the script
by hand from the repository root.

The set, named as `scripts/gen-test-certs.sh` names its output everywhere:

- `cacert.pem`: the CA. Test suites that verify a service's certificate trust
  this.
- `cert.pem`, `key.pem`: the certificate every TLS service presents. Its
  subject alternative names are the hostnames listed in `hostnames.txt` here:
  every service hostname on the compose network and `toxiproxy`.
- `client-cert.pem`, `client-key.pem`: a client certificate signed by the CA,
  for services that require one from their clients.

The CA private key is not kept. To add a hostname, append it to `hostnames.txt`;
the next `run.sh` (or `gen-compose-certs.sh`) regenerates the set, as it does
whenever the list or the generator changed.

Kafka and Cassandra do not use this set: their `ssl_cert_gen` service builds
Java keystores into `/tmp/emqx-ci/emqx-shared-secret` at start.
