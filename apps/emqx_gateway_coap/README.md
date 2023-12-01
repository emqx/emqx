# emqx_coap

The CoAP gateway implements publish, subscribe, and receive messages as standard
with [Publish-Subscribe Broker for the CoAP](https://datatracker.ietf.org/doc/html/draft-ietf-core-coap-pubsub-09).

## Quick Start

In EMQX 5.0, CoAP gateway can be configured and enabled through the Dashboard.

It can also be enabled via the HTTP API or emqx.conf, e.g. In emqx.conf:

```properties
gateway.coap {

  mountpoint = "coap/"

  connection_required = false

  listeners.udp.default {
    bind = "5683"
    max_connections = 1024000
    max_conn_rate = 1000
  }
}
```

> Note:
> Configuring the gateway via emqx.conf requires changes on a per-node basis,
> but configuring it via Dashboard or the HTTP API will take effect across the cluster.

More documentations: [CoAP Gateway](https://www.emqx.io/docs/en/v5.0/gateway/coap.html)
