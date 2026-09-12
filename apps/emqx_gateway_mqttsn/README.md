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

## Sleeping clients, NAT, and session resume

An MQTT-SN client enters the `asleep` state by sending `DISCONNECT` with a non-zero Duration.

On plaintext UDP, and on DTLS listeners where the client does not provide a certificate, the
gateway preserves the legacy MQTT-SN behavior: `PINGREQ(ClientId)` can find and wake the old
asleep/awake session even when the source IP or port has changed. This wake-up is intentionally
unauthenticated because MQTT-SN PINGREQ contains no password or token; it should only be used where
that risk is acceptable.

For DTLS clients authenticated with a verified client certificate, the gateway binds the session to
the peer certificate. A PINGREQ from a new association can resume a certificate-bound session only
when it presents the same certificate. Source IP and port changes do not affect this comparison.
A wake-up without a client certificate, including plaintext UDP or optional-certificate DTLS, or
with a different or reissued certificate is rejected. After certificate rotation, the client must
use CONNECT and complete the normal authentication and session takeover flow.

Configure the DTLS listener with `verify = verify_peer` and `fail_if_no_peer_cert = true` when every
client must present a verified certificate. Server-only DTLS follows the legacy ClientId-only
behavior.

More documentations: [MQTT-SN Gateway](https://www.emqx.io/docs/en/v5.0/gateway/mqttsn.html)
