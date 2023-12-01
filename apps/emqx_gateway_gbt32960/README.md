# emqx_gbt32960

The GBT32960 Gateway is based on the GBT32960 specification.

## Quick Start

In EMQX 5.0, GBT32960 gateway can be configured and enabled through the Dashboard.

It can also be enabled via the HTTP API or emqx.conf, e.g. In emqx.conf:

```properties
gateway.gbt32960 {

  mountpoint = "gbt32960/${clientid}"

  listeners.tcp.default {
    bind = 7325
  }
}
```

> Note:
> Configuring the gateway via emqx.conf requires changes on a per-node basis,
> but configuring it via Dashboard or the HTTP API will take effect across the cluster.
