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

MQTT-SN `asleep` and `CleanSession` are separate concepts:

* A connected client enters `asleep` by sending `DISCONNECT` with a non-zero Duration. This
  transition is accepted regardless of the `CleanSession` flag.
* `CleanSession=false` is required for CONNECT-based session recovery. The old session must still
  be alive, and recovery is subject to the sleep/session-expiry timer, session queue limits, and
  the subscription-resume configuration.

On plaintext UDP, and on DTLS listeners where the client does not provide a certificate, the
gateway preserves the legacy MQTT-SN behavior: `PINGREQ(ClientId)` can find and wake the old
asleep/awake session even when the source IP or port has changed. This is intentionally
unauthenticated because MQTT-SN PINGREQ contains no password or token; it should only be used where
that risk is acceptable.

`CleanSession=true` does not prevent a client from entering `asleep`. A PINGREQ wake does not carry
the CleanSession flag and may continue to use the existing in-memory session while it is alive. This
is not a persistence guarantee. A new CONNECT with `CleanSession=true` creates a clean session and
does not resume the old queued messages or subscriptions.

For a DTLS listener configured with `verify = verify_peer` and
`fail_if_no_peer_cert = true`, the gateway records the client's certificate subject DN and CN with
the session. A PINGREQ from a new DTLS association can resume
that session only when both values match. A missing client certificate or different subject DN/CN,
including a plaintext UDP request, is rejected for such a certificate-subject-bound session.
Server-only DTLS does not authenticate the client and follows the legacy behavior above. Because
this binding uses DN and CN, a different certificate with the same subject values is accepted. This
PINGREQ binding does not replace the separate MQTT-SN CONNECT credential-binding requirement.

More documentations: [MQTT-SN Gateway](https://www.emqx.io/docs/en/v5.0/gateway/mqttsn.html)
