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
* For a new connection, `CleanSession=false` is required to request session recovery. The old
  sleeping session must still be alive, and recovery is subject to the sleep/session-expiry timer,
  session queue limits, and the subscription-resume configuration.

On a plaintext UDP listener, `PINGREQ` carries only the Client ID and does not authenticate the
sender. A sleeping client can therefore be awakened with PINGREQ only through the UDP flow already
bound to its session. This may work while the flow is stable, but it is not a reliable WAN/NAT
mechanism: after NAT rebinding or another source-tuple change, the gateway cannot use the Client ID
to prove ownership of the session. The gateway responds with `DISCONNECT`; the client must establish
a new transport and send `CONNECT` with `CleanSession=false` to request recovery.

`CleanSession=true` does not prevent a client from entering `asleep`, and a same-flow wake-up may
continue to use the existing in-memory session. It does not provide a persistence guarantee,
however. On a new connection, `CleanSession=true` creates a clean session and does not resume the
old queued messages or subscriptions.

DTLS does not remove the reconnect requirement. Mutual DTLS authentication (mTLS or a unique PSK)
can provide an authenticated transport identity for a new DTLS association, but the gateway's
authentication policy must bind that identity to the MQTT-SN Client ID. Server-authenticated-only
DTLS does not authenticate the client. The current MQTT-SN gateway does not treat a new DTLS
association's PINGREQ as a session-resume credential; after a network change, use `CONNECT` with
`CleanSession=false`.

More documentations: [MQTT-SN Gateway](https://www.emqx.io/docs/en/v5.0/gateway/mqttsn.html)
