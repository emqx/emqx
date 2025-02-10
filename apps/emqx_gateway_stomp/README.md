# emqx_stomp

The Stomp Gateway is based on the
[Stomp v1.2](https://stomp.github.io/stomp-specification-1.2.html) and is
compatible with the Stomp v1.0 and v1.1 specification.

## Quick Start

In EMQX 5.0, Stomp gateway can be configured and enabled through the Dashboard.

It can also be enabled via the HTTP API or emqx.conf, e.g. In emqx.conf:

```properties
gateway.stomp {

  mountpoint = "stomp/"

  listeners.tcp.default {
    bind = 61613
    acceptors = 16
    max_connections = 1024000
    max_conn_rate = 1000
  }
}
```

> Note:
> Configuring the gateway via emqx.conf requires changes on a per-node basis,
> but configuring it via Dashboard or the HTTP API will take effect across the cluster.

More documentations: [Stomp Gateway](https://www.emqx.io/docs/en/latest/gateway/stomp.html)
