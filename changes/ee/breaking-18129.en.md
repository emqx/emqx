# Example TLS certificates are no longer shipped; a unique certificate is generated at first boot

EMQX installation packages no longer include the example TLS certificate files (`etc/certs/cacert.pem`, `cert.pem`, `key.pem`, `client-cert.pem`, `client-key.pem`). These files were identical for every installation and were intended only for local testing.

On first start, EMQX now generates a unique self-signed certificate (`CN=localhost`, with subject alternative names `localhost`, `127.0.0.1`, and `::1`) and stores it as the managed-certificates bundle named `localhost` under `data/certs2/`. TLS servers without any certificate configuration — including the default `ssl` (port 8883) and `wss` (port 8084) MQTT listeners, the Dashboard HTTPS listener, and gateway TLS listeners — use this bundle. A node joining a cluster inherits the cluster's bundle, so all nodes serve the same certificate.

Migration notes:

- Configurations that reference the previously shipped files (for example `${EMQX_ETC_DIR}/certs/cert.pem`) must now place operator-provided certificate files at those paths, or drop the references to use the generated certificate.
- The `etc/certs` directory is still created by installation packages as the conventional location for operator-provided certificates.
- The `cacertfile`, `certfile`, and `keyfile` fields of TLS configurations no longer have default values pointing at the example files; when none of them (and no `managed_certs`) is set on an MQTT TLS listener, the generated `localhost` bundle is used.
- TLS clients that verified the broker against the example CA certificate must be updated to trust the generated CA certificate (`data/certs2/global/localhost/ca.pem`) or an operator-issued one.
