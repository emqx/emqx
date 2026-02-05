# NATS Gateway Gap Notes

- 2026-02-04: NATS Server does not proactively send PING frames within the short interval used by `t_server_to_client_ping`, while EMQX Gateway does. Test tolerates missing server PING on NATS.
- 2026-02-04: NATS Server returns permission violation errors with quoted subjects, while EMQX Gateway returns unquoted subjects. Tests accept both formats.
- 2026-02-04: With `echo=false`, EMQX Gateway still delivers published messages to the same client; NATS Server does not.
- 2026-02-04: EMQX Gateway accepts payloads larger than `max_payload` and responds `+OK`; NATS Server returns `-ERR`.
- 2026-02-04: NATS Server accepts `PUB` subjects containing wildcard tokens (e.g. `foo.*`), while EMQX Gateway rejects them as invalid subjects.
