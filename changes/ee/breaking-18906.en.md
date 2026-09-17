EMQX no longer ships example TLS certificates in `etc/certs/`.

Every installation used to receive the same example key pair, so that key was not private to anyone. The files are gone; the directory stays, with a README, as the place to put your own.

A TLS listener with no certificate configured is unaffected: it serves the certificate the node generates for itself. Configuration that names the removed files explicitly, such as `certfile = "${EMQX_ETC_DIR}/certs/cert.pem"`, or `etc/ssl_dist.conf` when the cluster uses `inet_tls`, now needs those files provided. Clients that were configured to trust the example CA must be given the node's own certificate instead, from `data/certs2/global/localhost/chain.pem`.
