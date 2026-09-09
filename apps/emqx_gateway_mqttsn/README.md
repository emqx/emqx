# emqx_mqttsn

The MQTT-SN gateway is based on the
[MQTT-SN v1.2](https://www.oasis-open.org/committees/download.php/66091/MQTT-SN_spec_v1.2.pdf).

## Quick Start

In EMQX 5.0, MQTT-SN gateway can be configured and enabled through the Dashboard.

It can also be enabled via the HTTP API or emqx.conf, e.g. In emqx.conf:

```properties
gateway.mqttsn {

  mountpoint = "mqtt/sn"

  gateway_id = 1

  broadcast = true

  enable_qos3 = true

  listeners.udp.default {
    bind = 1884
    max_connections = 10240000 max_conn_rate = 1000
  }
}
```

> Note:
> Configuring the gateway via emqx.conf requires changes on a per-node basis,
> but configuring it via Dashboard or the HTTP API will take effect across the cluster.

## Sleeping clients and UDP source changes

On a plaintext UDP listener, a sleeping client can resume with PINGREQ only through the UDP flow
already bound to its session. If the client's UDP source tuple changes, the gateway responds with
DISCONNECT and the client must send CONNECT again before the session can be resumed. Use an
authenticated DTLS configuration when transport-level client identity is required across network
changes.

More documentations: [MQTT-SN Gateway](https://www.emqx.io/docs/en/v5.0/gateway/mqttsn.html)
