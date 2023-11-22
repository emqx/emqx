# emqx_jt808

The JT/T 808 protocol is designed for data interaction between vehicle-mounted satellite terminals and IoT platforms.

The **JT/T 808 Gateway** in EMQX can accept JT/T 808 clients and translate their events
and messages into MQTT Publish messages.

In the current implementation, it has the following limitations:
- Only supports JT/T 808-2013 protocol, JT/T 808-2019 is not supported yet.
- Based TCP/TLS transport.
- Third-party authentication/registration http service required.

## Quick Start

In EMQX 5.0, JT/T 808 gateway can be configured and enabled through the Dashboard.

It can also be enabled via the HTTP API, and emqx.conf e.g, In emqx.conf:

```
```

> Note:
> Configuring the gateway via emqx.conf requires changes on a per-node basis,
> but configuring it via Dashboard or the HTTP API will take effect across the cluster.

More documentations: [JT/T 808 Gateway](https://www.emqx.io/docs/en/v5.0/gateway/jt808.html)
