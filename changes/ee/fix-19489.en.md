Changed the default `tcp_backend` for MQTT TCP listeners back to `gen_tcp`.

6.3.0 made `socket` the default on Unix and Linux. It uses more memory per connection, so the
default returns to `gen_tcp` while that is addressed. So far only tests that open connections
without message traffic have measured that cost; tests with message traffic have not. `socket`
is planned to become the default again in v7.

The `socket` backend is unchanged and still available on Unix and Linux with
`tcp_backend = socket`. A listener that sets `tcp_backend` explicitly is unaffected. A listener
that relies on the default switches back to `gen_tcp`, which restarts it and closes its active
connections on upgrade.
