# emqx_telemetry

In order for EMQ to understand how EMQX opensource edition is being used,
this app reports some telemetry data to [EMQ's telemetry server](https://telemetry.emqx.io/api/telemetry)

To turn it off, you can set `telemetry.enable = false` in emqx.conf,
or start EMQX with environment variable `EMQX_TELEMETRY__ENABLE=false`.

## Reported Data

This application is only relesaed in EMQX opensource edition.
There is nothing in the reported data which can back-track the origin.
The report interval is 7 days, so there is no performance impact.

Here is the full catalog of the reported data.

### Installation

- EMQX version string
- License (always stubed with "opensource")
- VM Stats (number of cores, total memory)
- Operating system name
- Operating system version
- Erlang/OTP version
- Number of enabled plugins
- Cluster UUID (auto-generated random ID)
- Node UUID (auto-generated random ID)

### Provisioning

- Advanced MQTT features
  - Number of retained messages
  - Number of topic-rewrite rules
  - Number of delay-publish messages
  - Number of auto-subscription rules
- Enabled authentication and authorization mechanisms
- Enabled gateway names
- Enabled bridge types
- Enabled exhooks (the number of endpoints and hookpoints)

### Runtime

- Uptime (how long the server has been running since last start/restart)
- Number of connected clients
- Number of messages sent
- Messaging rates


## More info

More introduction about [Telemetry](https://www.emqx.io/docs/en/v5.0/telemetry/telemetry.html#telemetry).

See HTTP API docs to [Enable/Disable telemetry](https://www.emqx.io/docs/en/v5.0/admin/api-docs.html#tag/Telemetry/paths/~1telemetry~1status/put),
[Get the enabled status](https://www.emqx.io/docs/en/v5.0/admin/api-docs.html#tag/Telemetry/paths/~1telemetry~1status/get)
and [Get the data of the module collected](https://www.emqx.io/docs/en/v5.0/admin/api-docs.html#tag/Telemetry/paths/~1telemetry~1data/get).
